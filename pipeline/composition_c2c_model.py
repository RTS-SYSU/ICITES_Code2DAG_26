"""C1 only: disjoint Condition, mutex regions and a supported static Loop family.

All source masks are analysis copies with physical offsets preserved. Source C,
legacy rules and scheduling algorithms are never modified by this adapter.
"""
import copy
import hashlib
import json
import re

from ..core.func_ranges import _strip_comments_and_strings, is_ignored_compiler_fn
from .algo.common import topo_sort
from .condition_model import _sanitize_c, _match_pair, _skip_ws, analyze_conditionals, build_condition_views
from .lock_model import build_lock_views, contract_lock_groups
from .loop_nested_model import analyze_nested_loops, build_loop_nested_views
from .loop_pair_model import analyze_loop_pair
from .loop_model import analyze_static_loop



def _c2c_lock_groups(*, source_text, base_segments_json):
    """Build one LOCKGROUP per independent critical section (same m1 allowed).

    Reuses the function-scoped lock-region matcher.  Unlike L2 lock_grouped,
    this accepts worker threads created by another thread, and it does not
    merge two independent critical sections into one execution node.
    """
    from .lock_regions import match_lock_regions, LockRegionError
    rows=base_segments_json['segments']; by_id={r['seg_id']:r for r in rows}
    ranges={}
    for r in rows:
        a,b=ranges.get(r['function'],(r['start_line'],r['end_line'])); ranges[r['function']]=(min(a,r['start_line']),max(b,r['end_line']))
    clean=_sanitize_c(source_text); lines=clean.splitlines()
    decls=list(re.finditer(r'^\s*(?:static\s+)?pthread_mutex_t\s+(\w+)\s*=\s*PTHREAD_MUTEX_INITIALIZER\s*;',clean,re.M))
    mutexes=[m.group(1) for m in decls]
    require(mutexes and len(set(mutexes))==len(mutexes) and len(mutexes)==len(re.findall(r'\bpthread_mutex_t\b',clean)), 'C2c requires direct statically initialized mutexes')
    try: regions=match_lock_regions(source_text,ranges)
    except LockRegionError as exc: require(False,'C2c '+str(exc))
    require(regions, 'C2c missing matched Lock')
    require(sum(r['mutex_id']==m for r in regions)>=2 for m in mutexes) if False else None
    for m in mutexes: require(sum(r['mutex_id']==m for r in regions)>=2, 'C2c mutex requires at least two independent regions')
    out=[]
    for region in regions:
        fn=region['function']; a,b=region['segment_range']
        overlap=[r for r in rows if r['function']==fn and r['start_line']<=b and a<=r['end_line']]
        require(len(overlap)==1, 'C2c critical section must correspond to exactly one base segment')
        row=overlap[0]
        require(row['kind']=='mutex_cs', 'C2c critical section is not a mutex_cs segment')
        effective=[i for i in range(row['start_line'],row['end_line']+1) if lines[i-1].strip()]
        require(effective and effective[0]==a and effective[-1]==b, 'C2c mutex segment includes outside code')
        member=dict(region); member['member_segment_id']=member['member_node_id']=row['seg_id']
        gid=f"LOCKGROUP:{region['mutex_id']}@{fn}@{region['lock_range'][0]}"
        out.append({'group_id':gid,'mutex_id':region['mutex_id'],'member_node_ids':[row['seg_id']],
                    'member_segment_ids':[row['seg_id']],'members':[member],
                    'aggregation_method':'fieldwise_sum_of_member_weights','analysis_only':True})
    return out

def _contract_c2c_lock_groups(base_dag_json, groups):
    owner={}
    for g in groups:
        for sid in g['member_segment_ids']:
            require(sid not in owner, 'C2c member belongs to multiple groups')
            owner[sid]=g['group_id']
    segment_map={n:owner.get(n,n) for n in base_dag_json['nodes']}
    edges={}; removed=[]
    for i,e in enumerate(base_dag_json['edges']):
        src,dst=segment_map[e['src']],segment_map[e['dst']]
        origin={'base_edge_index':i,'edge':copy.deepcopy(e)}
        if e['src'] in owner and e['dst'] in owner and src==dst:
            removed.append(origin); continue
        key=(src,dst,e['kind'])
        if key not in edges:
            edges[key]=(copy.deepcopy(e) if e['src'] not in owner and e['dst'] not in owner else {'src':src,'dst':dst,'kind':e['kind']})
            edges[key]['provenance']=[]
        edges[key]['provenance'].append(origin)
    out=copy.deepcopy(base_dag_json)
    out.update(nodes=[n for n in base_dag_json['nodes'] if n not in owner]+[g['group_id'] for g in groups],
               edges=[edges[k] for k in sorted(edges)],removed_internal_edges=removed)
    require(len(out['nodes'])==len(base_dag_json['nodes'])-sum(len(g['member_node_ids'])-1 for g in groups), 'C2c grouped node count mismatch')
    return groups,segment_map,out

RULE = 'composition_c2c'
CONTROL = {'condition_entry', 'condition_exit', 'loop_entry', 'loop_exit'}


class CompositionError(ValueError):
    pass


def require(ok, message):
    if not ok:
        raise CompositionError(message)


def digest(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def features(text):
    clean = _sanitize_c(text)
    result = set()
    if re.search(r'\bif\b', clean): result.add('condition')
    if re.search(r'\b(for|while|do)\b', clean): result.add('loop')
    if re.search(r'\bpthread_mutex_(?!t\b)\w+\s*\(', clean): result.add('lock')
    # Indirect uses must not silently evade Lock validation.
    for name in re.findall(r'\bpthread_mutex_t\s+(\w+)', clean):
        if len(re.findall(r'\b'+re.escape(name)+r'\b', clean)) > 1: result.add('lock')
    return result


def _mask(text, spans):
    chars = list(text)
    for a, b in spans:
        for i in range(a, b):
            if chars[i] not in '\r\n': chars[i] = ' '
    return ''.join(chars)


def _ordered_unique(items):
    return list(dict.fromkeys(items))


def _project(rows, dag):
    """Remove only control vertices; retain complete path and edge attributes."""
    active = {r['seg_id'] for r in rows if r['kind'] not in CONTROL}
    adj = {n: [] for n in dag['nodes']}
    for e in dag['edges']: adj[e['src']].append(e)
    result = []
    for node in dag['nodes']:
        if node not in active: continue
        pending = [(e['dst'], [e]) for e in adj[node]]
        while pending:
            target, path = pending.pop()
            if target not in active:
                pending.extend((e['dst'], path+[e]) for e in adj[target])
                continue
            transitions = [e for e in path if e['kind'] in ('condition_branch', 'condition_merge')]
            external = [e for e in path if e['kind'] not in ('intra', 'condition_branch', 'condition_merge', 'loop_flow', 'loop_iteration_order')]
            for witness in external or [path[0]]:
                out = copy.deepcopy(witness)
                out.update(src=node, dst=target)
                if len(path) > 1: out['control_projection'] = copy.deepcopy(path)
                if transitions:
                    out['condition_transitions'] = copy.deepcopy(transitions)
                    for key in ('exclusive_group', 'branch_id', 'guard'): out[key] = transitions[-1][key]
                result.append(out)
    return list({json.dumps(e, sort_keys=True): e for e in result}.values())


def build_condition_lock_views(*, base_name, source_text, base_segments_json, base_dag_json, _branch_locks=False):
    kinds = features(source_text)
    require(kinds == {"condition", "lock"}, "C2c requires Condition and Lock without Loop")
    
    raw = copy.deepcopy(base_segments_json); raw_dag = copy.deepcopy(base_dag_json)
    bases = raw['segments']; by_id = {r['seg_id']: r for r in bases}
    require(len(by_id) == len(bases) and set(by_id) == set(raw_dag['nodes']), 'C2c raw IDs must match uniquely')
    topo_sort(raw_dag['nodes'], [(e['src'], e['dst']) for e in raw_dag['edges']])
    clean = _sanitize_c(source_text); lines = clean.splitlines()
    directive_text = _strip_comments_and_strings(source_text)
    require(not re.search(r'^\s*#\s*(?!include\b)\w+', directive_text, re.M), 'C2c rejects active non-include directives')
    ranges = {}
    for r in bases:
        a,b = ranges.get(r['function'], (r['start_line'], r['end_line']))
        ranges[r['function']] = (min(a,r['start_line']), max(b,r['end_line']))
    analysis = analyze_conditionals(source_text, ranges)
    require(not analysis.diagnostics, 'C2c invalid Condition source')
    roots = [r for r in analysis.regions if r.parent_id is None]
    require(len(analysis.regions) == len(re.findall(r'\bif\b', clean)), 'C2c Condition outside known raw functions')
    for fn in {r.function for r in analysis.regions}:
        regions = [r for r in analysis.regions if r.function == fn]
        # No Loop family: accept the existing bounded Condition tree forms
        # used by G3 (a root with nested inner branches); still require exactly
        # one root per function and a finite small tree.
        require(1 <= len(regions) <= 8 and len([r for r in regions if r.parent_id is None]) == 1,
                f'C2c rejects unbounded Condition forms: {fn} regions={[(r.condition_id,r.parent_id,r.entry_line,r.exit_line) for r in regions]}')
    spans = []
    for m in re.finditer(r'\bfor\b', clean):
        close = _match_pair(clean, _skip_ws(clean, m.end()), '(', ')')
        end = _match_pair(clean, _skip_ws(clean, close+1), '{', '}')+1
        spans.append((m.start(), end))
    line = lambda p: clean.count('\n', 0, p)+1
    require(not spans, 'C2c rejects static Loop input')
    conditional_spans = [(r.start_offset, r.end_offset) for r in roots]
    family_span = [(min(a for a,b in spans), max(b for a,b in spans))] if spans else []
    for a,b in conditional_spans:
        require(not any(max(a,c) < min(b,d) for c,d in family_span), 'C2c rejects Condition/Loop nesting or overlap')
    # Detect mutex placement before masking any control region.
    stacks = {}; mutex_spans = []
    from .lock_regions import owner_instances, source_function_name, LockRegionError
    for m in re.finditer(r'\bpthread_mutex_(lock|unlock)\s*\(\s*&\s*(\w+)\s*\)\s*;', clean):
        try:
            instances = owner_instances(ranges, line(m.start()))
        except LockRegionError:
            require(False, 'C2c mutex call has no unique raw function owner')
        stack = stacks.setdefault(source_function_name(instances[0]), [])
        if m.group(1) == 'lock':
            require(not stack, 'C2c rejects nested mutexes'); stack.append((m.group(2), m.start()))
        else:
            require(stack and stack[-1][0] == m.group(2), 'C2c rejects unpaired mutex operations')
            _,a = stack.pop(); mutex_spans.append((a,m.end()))
    require(not any(stacks.values()), 'C2c rejects unpaired mutex operations')
    for a,b in mutex_spans:
        if _branch_locks:
            from .composition_c2a_model import validate_lock_placement
            validate_lock_placement(a,b,analysis,family_span)
        else:
            require(not any(max(a,c) < min(b,d) for c,d in family_span+conditional_spans),
                    'C2c rejects Lock/Condition/Loop nesting or overlap')
    # Lock's existing lexical contract still checks declarations, aliases, worker
    # creation, complete CS segments, wrappers, returns and supported calls.
    if _branch_locks:
        from .composition_c2a_model import flatten_condition_headers
        lock_source = _mask(flatten_condition_headers(source_text,analysis),spans)
    else:
        lock_source = _mask(source_text, spans+conditional_spans)
    lock_input = copy.deepcopy(raw)
    lock_input['node_locations'] = {n:l for n,l in raw.get('node_locations',{}).items() if not is_ignored_compiler_fn(n)}
    if _branch_locks:
        from .composition_c2a_model import build_branch_lock_groups
        groups = build_branch_lock_groups(source_text=lock_source, base_segments_json=lock_input)
    else:
        # C2c uses the function-scoped matcher and keeps each critical section
        # as its own LOCKGROUP (two task5/task7 groups, both mutex_id=m1).
        groups = _c2c_lock_groups(source_text=lock_source, base_segments_json=lock_input)
    if _branch_locks: require(1 <= len(groups) <= 2, 'C2a supports one or two mutexes')
    # All other pthread sites are outside Loop regions and validated as source
    # context. Mask their lines only for the existing Loop analyzer's global guard.
    offsets = [0]
    for m in re.finditer('\n', source_text): offsets.append(m.end())
    pthread_lines = [(offsets[i], offsets[i+1] if i+1 < len(offsets) else len(source_text))
                     for i,l in enumerate(lines) if re.search(r'\bpthread_\w+\b', l)]
    # C2c has no Loop family.  The raw Condition/Lock node budget is the
    # condition-view expansion plus the base segments; loops/templates stay empty.
    require(not spans, 'C2c rejects static Loop input')
    count = 0
    require(count+len(bases)+4*len(analysis.regions) <= 10000, 'C2c structural node limit exceeded')
    cs,cd,ce,_,cm = build_condition_views(base_name=base_name, source_text=source_text,
                                        base_segments_json=raw, base_dag_json=raw_dag)
    require(cm['scheduler_compatible'], 'C2c incompatible Condition: '+str(cm['blocked_reasons']))
    # Condition's whole-line trimming legitimately drops trailing braces. A
    # disjoint Loop still needs its real closing boundary in the local owner.
    for a,b in spans:
        h,x = line(a),line(b-1)
        owners = [r for r in cs['segments'] if not r.get('synthetic') and r['start_line']<=h<=r['end_line']]
        require(len(owners)==1 and owners[0]['kind']=='compute', 'C2c Loop has no unique compute window')
        owners[0]['end_line'] = max(owners[0]['end_line'],x)
    memberships = {r['seg_id']:r.get('condition_memberships',[]) for r in ce['segments']}
    aliases = {}
    for r in cs['segments']:
        old = r['seg_id']
        full = [b for b in bases if b['function']==r['function'] and b['kind']==r['kind'] and
                (b['start_line'],b['end_line'])==(r['start_line'],r['end_line'])]
        if not r.get('synthetic') and not memberships.get(old) and len(full)==1:
            aliases[old] = full[0]['seg_id']
            saved = copy.deepcopy(full[0]); saved.update(r); r.clear(); r.update(saved)
            r['seg_id'] = aliases[old]
        if memberships.get(old): r['condition_memberships'] = copy.deepcopy(memberships[old])
    for e in cd['edges']:
        e['src'] = aliases.get(e['src'],e['src']); e['dst'] = aliases.get(e['dst'],e['dst'])
    cd['nodes'] = [r['seg_id'] for r in cs['segments']]
    # Preserve a raw-edge ledger even when lexical intra edges are replaced by
    # proper Condition paths. No original attribute is erased by that rebuilding.
    ledger = []
    for i,e in enumerate(raw_dag['edges']):
        def ports(sid):
            b=by_id[sid]
            return sorted([r for r in cs['segments'] if r['function']==b['function'] and
                           max(r['start_line'],b['start_line'])<=min(r['end_line'],b['end_line'])],
                          key=lambda r:(r['start_line'],0 if r['kind']=='condition_entry' else 2 if r['kind']=='condition_exit' else 1,r['end_line']))
        left,right = ports(e['src']),ports(e['dst'])
        boundary = _branch_locks and not (left and right)
        if boundary:
            from .composition_c2a_model import boundary_edge_replacements
            matches = boundary_edge_replacements(e,left,right,by_id,cs['segments'],cd,analysis,lines)
        else:
            require(left and right, 'C2c cannot trace raw edge endpoints')
            matches = [x for x in cd['edges'] if x['src']==left[-1]['seg_id'] and x['dst']==right[0]['seg_id'] and x['kind']==e['kind']]
        require(matches or e['kind']=='intra', 'C2c cannot preserve external dependency')
        for x in matches:
            for key,value in e.items():
                if key not in ('src','dst','kind'): x.setdefault(key,copy.deepcopy(value))
            x.setdefault('provenance',[]).append(dict(base_edge_index=i,edge=copy.deepcopy(e)))
        ledger.append(dict(base_edge_index=i,edge=copy.deepcopy(e),disposition='projected' if matches else 'lexical_intra_rebuilt_for_condition'))
        if boundary:
            ledger[-1].update(disposition='condition_boundary_replacement',replacement_edges=[{k:copy.deepcopy(x[k]) for k in ('src','dst','kind','exclusive_group','branch_id','guard')} for x in matches])
    payload = copy.deepcopy(raw); payload['segments'] = cs['segments']
    payload['node_to_segment'] = {}
    for n,l in raw.get('node_locations',{}).items():
        targets = [r for r in cs['segments'] if r['function']==l['function'] and r['start_line']<=l['line']<=r['end_line']]
        if targets: payload['node_to_segment'][n] = targets[0]['seg_id']
    # No Loop family: explicitly empty loops/templates (ordinary execution
    # templates are added below).
    st,sd = copy.deepcopy(payload),copy.deepcopy(cd)
    st.update(loops=[],templates=[])
    rows = st['segments']; ids = [r['seg_id'] for r in rows]
    require(len(ids)==len(set(ids)), 'C2c duplicate node ID')
    topo_sort(ids,[(e['src'],e['dst']) for e in sd['edges']])
    execution = [r for r in rows if r['kind'] not in CONTROL]
    # Recover raw-owner provenance, not transient BR owner identities.
    for r in execution:
        owners = [b['seg_id'] for b in bases if b['function']==r['function'] and
                  max(b['start_line'],r['start_line'])<=min(b['end_line'],r['end_line'])]
        r['source_segments'] = owners
        r['source_position_ids'] = [f"LOOPSRC:{r['function']}:line:{i}" for i in range(r['start_line'],r['end_line']+1) if lines[i-1].strip() and lines[i-1].strip() not in ('{','}')]
        r['generation_origin'] = sorted(n for n,l in raw.get('node_locations',{}).items()
                                        if not is_ignored_compiler_fn(n) and l['function']==r['function'] and r['start_line']<=l['line']<=r['end_line']) or None
    for loop in st['loops']:
        owner = [b['seg_id'] for b in bases if b['function']==loop['function'] and b['start_line']<=loop['header_line'] and loop['exit_line']<=b['end_line']]
        require(len(owner)==1, 'C2c Loop must stay inside one raw segment')
        loop['original_segment'] = owner[0]
    templates = copy.deepcopy(st['templates']); tids = {t['template_id'] for t in templates}
    for r in execution:
        if not r.get('template_id'):
            r['template_id'] = f"C1TPL:{r['function']}@{r['start_line']}-{r['end_line']}"
            require(r['template_id'] not in tids, 'C2c duplicate ordinary template')
            templates.append(dict(template_id=r['template_id'],role='execution',function=r['function'],start_line=r['start_line'],end_line=r['end_line']))
            tids.add(r['template_id'])
    projected = _project(rows,sd)
    pre_dag = dict(base_name=base_name,nodes=[r['seg_id'] for r in execution],edges=projected)
    members = {r['seg_id']:r for r in execution}
    if _branch_locks:
        from .composition_c2a_model import branch_local_groups
        groups = branch_local_groups(groups,execution)
    for g in groups:
        for m in g['members']:
            require(m['member_segment_id'] in members, 'C2c lost complete mutex segment')
            member = members[m['member_segment_id']]
            require((_branch_locks or not member.get('condition_memberships')) and not member.get('loop_id'), 'C2c conditional/repeated lock member')
            member['lock_group'] = dict(group_id=g['group_id'],mutex_id=g['mutex_id'],lock_range=m['lock_range'],unlock_range=m['unlock_range'],critical_range=m['critical_range'])
            m['execution_segment'] = copy.deepcopy(member)
    if _branch_locks:
        from .composition_c2a_model import contract_branch_groups
        groups,projection,dd = contract_branch_groups(pre_dag,groups)
    else:
        groups,projection,dd = _contract_c2c_lock_groups(pre_dag,groups)
    topo_sort(dd['nodes'],[(e['src'],e['dst']) for e in dd['edges']])
    grouped = {sid for g in groups for sid in g['member_segment_ids']}
    final = [copy.deepcopy(r) for r in execution if r['seg_id'] not in grouped]
    for g in groups:
        final.append(dict(copy.deepcopy(g),seg_id=g['group_id'],function='<lockgroup>',kind='lock_group',synthetic=True,
                          start_line=min(m['segment_range'][0] for m in g['members']),end_line=max(m['segment_range'][1] for m in g['members'])))
    oldmap = {b['seg_id']:_ordered_unique(projection[r['seg_id']] for r in execution if b['seg_id'] in r['source_segments']) for b in bases}
    generation = {}
    for n,sid in raw.get('node_to_segment',{}).items():
        if is_ignored_compiler_fn(n): continue
        if n in raw.get('node_locations',{}):
            generation[n] = _ordered_unique(projection[r['seg_id']] for r in execution if n in (r['generation_origin'] or []))
        else: generation[n] = oldmap[sid].copy()
    positions = {}
    for key,targets in raw.get('mappings',{}).get('source_position_to_execution_segments',{}).items():
        replacements = []
        for sid in targets:
            values = oldmap.get(sid,[sid])
            # A split owner describes all of its executions, whereas a source
            # position describes only the executions containing that position.
            if re.fullmatch(r'LOOPSRC:.+:line:[1-9]\d*', key) and sid in oldmap and values != [sid]:
                values = [projection[r['seg_id']] for r in execution
                          if sid in r['source_segments'] and key in r['source_position_ids']]
            replacements.extend(values)
        positions[key] = _ordered_unique(replacements)
    for r in execution:
        identities = r['source_position_ids']
        r['source_position_ids'] = identities
        for key in identities: positions.setdefault(key,[]).append(projection[r['seg_id']])
    for loop in st['loops']:
        for a,b in loop.get('statements',[]):
            for i in range(a,b+1): positions.setdefault(f"LOOPSRC:{loop['function']}:line:{i}",[])
    positions = {key:_ordered_unique(values) for key,values in positions.items()}
    mappings = copy.deepcopy(raw.get('mappings',{}))
    mappings.update(generation_node_to_execution_segments=generation,original_segment_to_execution_segments=oldmap,
                    source_position_to_execution_segments=positions,
                    grouped_node_to_member_segments={g['group_id']:g['member_segment_ids'] for g in groups},
                    grouped_node_to_mutex_and_source={g['group_id']:g for g in groups},
                    generation_node_to_structural_nodes={n:[r['seg_id'] for r in rows if r['function']==l['function'] and r['start_line']<=l['line']<=r['end_line']] for n,l in raw.get('node_locations',{}).items() if not is_ignored_compiler_fn(n)})
    common = dict(base_name=base_name,source_file=raw['source_file'],source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),
                  rule_name=RULE,level='level2',inputs=raw.get('inputs',{}),thread_entry_functions=raw.get('thread_entry_functions',[]),
                  ignored_generation_nodes=[n for n in raw.get('node_locations',{}) if is_ignored_compiler_fn(n)],condition_regions=cs['condition_regions'],loops=st['loops'],templates=templates,
                  lock_groups=groups,mappings=mappings,composition_edge_provenance=ledger,
                  composition_input_sha256=digest([raw,raw_dag]),
                  capabilities=dict(scheduler_compatible=True,timing_compatible=False,instrumentation_compatible=False,analysis_only=True))
    ss = dict(common,view='sched',segments=final)
    structural = dict(copy.deepcopy(common),view='struct',segments=rows)
    sd['exclusive_groups'] = copy.deepcopy(cd['exclusive_groups']);dd['exclusive_groups'] = copy.deepcopy(cd['exclusive_groups'])
    return structural,sd,ss,dd,dict(rule_name=RULE,level='level2',scheduler_compatible=True,analysis_only=True)
