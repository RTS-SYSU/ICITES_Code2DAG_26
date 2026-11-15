"""Loop L1: optional, source-boundary static expansion; no C rewriting."""
import copy
import math
import re

from .condition_model import _sanitize_c, _parse_statement, _skip_ws, ConditionModelError
from .algo.common import topo_sort
from ..core.func_ranges import _strip_comments_and_strings


class LoopModelError(ValueError):
    pass


def require(ok, message):
    if not ok:
        raise LoopModelError(message)


def analyze_static_loop(source_text):
    clean = _sanitize_c(source_text)
    loops = list(re.finditer(r'\b(?:for|while|do)\b', clean))
    if not loops:
        return None
    require(len(loops) == 1 and loops[0].group() == 'for', 'Loop L1 requires exactly one static for; while/do/nested/multiple loops unsupported')
    require(not re.search(r'^\s*#\s*(?!include\b)\w+', _strip_comments_and_strings(source_text), re.M), 'Loop L1 rejects macros and non-include directives')
    require(not re.search(r'\b(?:if|else|switch|pthread_mutex_\w+|pthread_rwlock_\w+)\b', clean), 'Loop L1 rejects Condition/Lock combinations')
    from .condition_model import _match_pair
    lines = clean.splitlines()
    header_offset = loops[0].start()
    header = clean.count('\n', 0, header_offset) + 1
    message = 'Loop L1 requires multiline braced for (int i = 0; i < literal; i++)'
    require(not clean[clean.rfind('\n', 0, header_offset)+1:header_offset].strip(), message)
    opening = _skip_ws(clean, header_offset + 3)
    try:
        closing = _match_pair(clean, opening, '(', ')')
    except ConditionModelError as exc:
        raise LoopModelError(message) from exc
    match = re.fullmatch(r'\s*for\s*\(\s*int\s+(\w+)\s*=\s*0\s*;\s*\1\s*<\s*(0[xX][0-9a-fA-F]+|0[0-7]*|[1-9][0-9]*)\s*;\s*\1\s*\+\+\s*\)\s*', clean[header_offset:closing+1])
    require(match is not None, message)
    var, literal = match.groups()
    count = int(literal, 16 if literal.lower().startswith('0x') else 8 if literal.startswith('0') else 10)
    require(count <= 2147483647, 'Loop L1 bound exceeds supported C int range')
    brace = _skip_ws(clean, closing + 1)
    require(brace < len(clean) and clean[brace] == '{', message)
    line_end = clean.find('\n', brace)
    require(line_end >= 0 and not clean[brace+1:line_end].strip(), message)
    try:
        body_close = _match_pair(clean, brace, '{', '}')
    except ConditionModelError as exc:
        raise LoopModelError('Loop L1 requires standalone closing brace') from exc
    close = clean.count('\n', 0, body_close)
    require(lines[close].strip() == '}', 'Loop L1 requires standalone closing brace')
    body_offset = brace + 1
    body = clean[body_offset:body_close]
    require('{' not in body and '}' not in body, 'Loop L1 rejects compound or nested bodies')
    require(not re.search(r'\b(?:break|continue|return|goto|pthread_\w+|sem_\w+)\b|[?:]', body), 'Loop L1 rejects body control transfer, synchronization or conditional expressions')
    # Parentheses cannot conceal a write/address escape of the induction variable.
    mutation_body = body.translate(str.maketrans('', '', '()'))
    require(not re.search(rf'(?:\+\+|--)\s*\(*\s*\b{var}\b|\b{var}\s*\)*\s*(?:\+\+|--|(?:[+*/%&|^<>-]|<<|>>)?=(?!=))|&\s*\(*\s*\b{var}\b|\b(?:int|long|short|char|float|double)\s+{var}\b', mutation_body), 'Loop L1 rejects induction variable mutation, shadowing or address escape')
    statements = []
    cursor = _skip_ws(body, 0)
    try:
        while cursor < len(body):
            statement = _parse_statement(body, cursor)
            first = clean.count('\n', 0, body_offset + statement.start) + 1
            last = clean.count('\n', 0, body_offset + statement.end-1) + 1
            require(body[statement.start:statement.end].strip() != ';', 'Loop L1 requires nonempty ordinary body statements')
            require(not statements or first > statements[-1][1], 'Loop L1 rejects same-line body statements')
            statements.append((first, last))
            cursor = _skip_ws(body, statement.end)
    except ConditionModelError as exc:
        raise LoopModelError('Loop L1 malformed ordinary body statement') from exc
    require(bool(statements), 'Loop L1 requires a nonempty ordinary body')
    return dict(header_line=header, exit_line=close+1, body_start=statements[0][0], body_end=statements[-1][1], iteration_count=count, variable=var, statements=statements)


def build_loop_views(*, base_name, source_text, base_segments_json, base_dag_json):
    loop = analyze_static_loop(source_text)
    if loop is None:
        return (copy.deepcopy(base_segments_json), copy.deepcopy(base_dag_json), copy.deepcopy(base_segments_json), copy.deepcopy(base_dag_json), {'loop_count': 0})
    bases = base_segments_json['segments']
    h, x = loop['header_line'], loop['exit_line']
    owners = [s for s in bases if s['start_line'] <= h and x <= s['end_line']]
    require(len(owners) == 1, 'Loop L1 requires complete loop in one existing Level-2 segment')
    base = owners[0];old_id = base['seg_id'];fn = base['function'];lid = f'LOOP:{fn}@{h}'
    loop = dict(loop, loop_id=lid, function=fn, original_segment=old_id)
    clean_lines = _sanitize_c(source_text).splitlines()
    locs = base_segments_json.get('node_locations', {})
    struct = []; schedule = []; templates = []; source_map = {}; generation_map = {n: [] for n in locs}; legacy_map = {}; chain = []

    def executable(sid, start, end, role, iteration=None, template_id=None):
        origins = sorted(n for n,l in locs.items() if l['function']==fn and start <= l['line'] <= end)
        row = dict(seg_id=sid,function=fn,kind='compute',start_line=start,end_line=end,loop_id=lid,role=role,iteration_index=iteration,template_id=template_id,generation_origin=origins or None,source_segments=[old_id])
        identities = [f'LOOPSRC:{fn}:line:{line}' for line in range(start,end+1) if clean_lines[line-1].strip()]
        row['source_position_ids'] = identities
        for identity in identities:source_map.setdefault(identity, []).append(sid)
        for origin in origins:generation_map[origin].append(sid)
        struct.append(row);schedule.append(row);chain.append(sid)
        return row

    def trim(start,end):
        while start<=end and not clean_lines[start-1].strip():start+=1
        while end>=start and not clean_lines[end-1].strip():end-=1
        return (start,end) if start<=end else None

    pre = trim(base['start_line'],h-1);post = trim(x+1,base['end_line'])
    for row in bases:
        if row['seg_id'] != old_id:
            struct.append(copy.deepcopy(row));schedule.append(copy.deepcopy(row));legacy_map[row['seg_id']] = [row['seg_id']]
    if pre:
        executable(lid+':before',*pre,'before',template_id=lid+':before')
        templates.append(dict(template_id=lid+':before',role='before',start_line=pre[0],end_line=pre[1]))
    entry,exit = lid+':entry',lid+':exit'
    struct.append(dict(seg_id=entry,function=fn,kind='loop_entry',start_line=h,end_line=h,synthetic=True,loop_id=lid));chain.append(entry)
    tid=lid+':body_template'
    templates.append(dict(template_id=tid,role='body_template',start_line=loop['body_start'],end_line=loop['body_end']))
    # Record zero-iteration source identities explicitly with empty target lists.
    for a,b in loop['statements']:
        for line in range(a,b+1):source_map.setdefault(f'LOOPSRC:{fn}:line:{line}',[])
    for iteration in range(loop['iteration_count']):
        executable(f'{lid}:iteration:{iteration}',loop['body_start'],loop['body_end'],'iteration',iteration,tid)
    struct.append(dict(seg_id=exit,function=fn,kind='loop_exit',start_line=x,end_line=x,synthetic=True,loop_id=lid));chain.append(exit)
    if post:
        executable(lid+':after',*post,'after',template_id=lid+':after')
        templates.append(dict(template_id=lid+':after',role='after',start_line=post[0],end_line=post[1]))
    replaced = [n for n in chain if n not in (entry,exit)];legacy_map[old_id]=replaced
    # Existing generation nodes outside the split segment keep their original mapping.
    for node,segment in base_segments_json.get('node_to_segment',{}).items():
        if segment != old_id:generation_map[node] = [segment]
        elif node not in generation_map:generation_map[node] = replaced.copy() # function-level aggregate, not a fabricated call
    edges=[]
    for edge in base_dag_json['edges']:
        e=copy.deepcopy(edge)
        if e['src']==old_id:e['src']=chain[-1]
        if e['dst']==old_id:e['dst']=chain[0]
        edges.append(e)
    for left,right in zip(chain,chain[1:]):
        kind='loop_iteration_order' if ':iteration:' in left and ':iteration:' in right else 'loop_flow'
        edges.append(dict(src=left,dst=right,kind=kind,loop_id=lid))
    nodes=[s['seg_id'] for s in struct]
    require(len(nodes)==len(set(nodes)), 'Loop L1 duplicate expanded node ID')
    topo_sort(nodes,[(e['src'],e['dst']) for e in edges])
    # Project only entry/exit; leave every executable node and sequential relation intact.
    scheduled={s['seg_id'] for s in schedule};adj={n:[] for n in nodes}
    for e in edges:adj[e['src']].append(e)
    projected=[]
    for n in nodes:
        if n not in scheduled:continue
        pending=[(e['dst'],[e]) for e in adj[n]]
        while pending:
            target,path=pending.pop()
            if target in scheduled:
                e=copy.deepcopy(path[0]) if len(path)==1 else dict(src=n,dst=target,kind='loop_flow',loop_id=lid,control_projection=copy.deepcopy(path))
                projected.append(e)
            else:pending.extend((e['dst'],path+[e]) for e in adj[target])
    topo_sort(list(scheduled),[(e['src'],e['dst']) for e in projected])
    mappings=copy.deepcopy(base_segments_json.get('mappings',{}))
    mappings.update(generation_node_to_execution_segments=generation_map,original_segment_to_execution_segments=legacy_map,source_position_to_execution_segments=source_map)
    payload=copy.deepcopy(base_segments_json)
    payload.update(rule_name='loop_static',loops=[loop],templates=templates,mappings=mappings,capabilities=dict(scheduler_compatible=True,runtime_instrumentation_supported=False),timing_policy='explicit template copy; reject split legacy weights')
    st=dict(payload,segments=struct,view='struct');ss=dict(payload,segments=schedule,view='sched')
    # Do not publish the old singular node_to_segment as if it described the expanded view.
    st.pop('node_to_segment',None);ss.pop('node_to_segment',None)
    sd=dict(base_name=base_name,nodes=nodes,edges=edges);dd=dict(base_name=base_name,nodes=[s['seg_id'] for s in schedule],edges=projected)
    return st,sd,ss,dd,dict(rule_name='loop_static',level='level2',loop_count=1,scheduler_compatible=True,runtime_instrumentation_supported=False)


def copy_loop_timing(segments_json, timing_json):
    """Copy explicit source-template metrics; never infer a split legacy weight."""
    weights=timing_json.get('weights',{});loops=segments_json.get('loops',[])
    require(isinstance(weights,dict), 'Loop L1 timing.weights must be a dict')
    require(not any(l['original_segment'] in weights for l in loops), 'Loop L1 rejects whole-segment timing spanning loop boundaries')
    known={t['template_id'] for t in segments_json.get('templates',[])} | {s['seg_id'] for s in segments_json['segments'] if not s.get('loop_id')}
    require(set(weights)<=known,'Loop L1 timing contains unknown template or legacy segment')
    result={}
    for key in known:
        require(key in weights,'Loop L1 timing missing template: '+key)
        metric=weights[key]
        require(isinstance(metric,dict) and 'avg_ns' in metric,'Loop L1 template requires avg_ns: '+key)
        require(all(isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) and v>=0 for v in metric.values()),'Loop L1 invalid template metric: '+key)
    for row in segments_json['segments']:
        key=row.get('template_id') or row['seg_id'];result[row['seg_id']]=copy.deepcopy(weights[key])
    return dict(base_name=segments_json.get('base_name'),weights=result,timing_origin='synthetic or explicit template input; copied without aggregation')
