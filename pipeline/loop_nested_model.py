"""Optional Loop L3: exactly two perfectly nested, static L1-shaped loops."""
import copy
import re

from .condition_model import _sanitize_c, _skip_ws, _match_pair, _parse_statement, ConditionModelError
from .loop_model import LoopModelError, require
from .loop_pair_model import build_loop_pair_views
from .algo.common import topo_sort
from ..core.func_ranges import _strip_comments_and_strings

MAX_STRUCTURAL_NODES = 10000
_HEADER = r'for\s*\(\s*int\s+([A-Za-z_]\w*)\s*=\s*0\s*;\s*\1\s*<\s*(0[xX][0-9a-fA-F]+|0[0-7]*|[1-9][0-9]*)\s*;\s*\1\s*\+\+\s*\)'


def _header(clean, match):
    message = 'Loop L3 requires multiline braced for (int i = 0; i < literal; i++)'
    line = lambda offset: clean.count('\n', 0, offset) + 1
    require(not clean[clean.rfind('\n', 0, match.start()) + 1:match.start()].strip(), message)
    try:
        closing = _match_pair(clean, _skip_ws(clean, match.end()), '(', ')')
        head = re.fullmatch(_HEADER, clean[match.start():closing + 1])
        require(head is not None, message)
        variable, literal = head.groups()
        count = int(literal, 16 if literal.lower().startswith('0x') else 8 if literal.startswith('0') else 10)
        require(count <= 2147483647, 'Loop L3 bound exceeds supported C int range')
        brace = _skip_ws(clean, closing + 1)
        end = _match_pair(clean, brace, '{', '}')
    except ConditionModelError as exc:
        raise LoopModelError(message) from exc
    eol = clean.find('\n', brace)
    require(eol >= 0 and not clean[brace + 1:eol].strip(), message)
    require(clean.splitlines()[line(end) - 1].strip() == '}', 'Loop L3 requires standalone closing braces')
    return dict(header_line=line(match.start()), exit_line=line(end), variable=variable,
                iteration_count=count, start_offset=match.start(), brace_offset=brace, end_offset=end)


def _check_induction(body, statements, variables):
    flat = body.translate(str.maketrans('', '', '()'))
    for variable in variables:
        require(not re.search(rf'(?:\+\+|--)\s*\b{variable}\b|\b{variable}\s*(?:\+\+|--|(?:[+*/%&|^<>-]|<<|>>)?=(?!=))|&\s*\b{variable}\b|\b(?:int|long|short|char|float|double)\s+{variable}\b', flat),
                'Loop L3 rejects induction variable mutation, shadowing or address escape: ' + variable)
    # Also cover ordinary pointer and comma-separated primitive declarations.
    for statement in statements:
        statement = re.sub(r'\b(?:const|volatile|restrict|static|extern|register|auto)\b', ' ', statement)
        declaration = re.match(r'\s*(?:(?:const|volatile|unsigned|signed|long|short|int|char|float|double|_Bool)\s+)+', statement)
        if not declaration:
            declaration = re.match(r'\s*(?:struct|union|enum)\s+[A-Za-z_]\w*\s+', statement)
        if not declaration:
            declaration = re.match(r'\s*[A-Za-z_]\w*\s+(?=[A-Za-z_])', statement)
        if not declaration:
            continue
        tail = statement[declaration.end():];parts = [];start = depth = 0
        for pos, char in enumerate(tail):
            if char in '([': depth += 1
            elif char in ')]': depth -= 1
            elif char == ',' and depth == 0: parts.append(tail[start:pos]);start = pos + 1
        parts.append(tail[start:])
        for part in parts:
            name = re.match(r'[\s(*]*([A-Za-z_]\w*)', part)
            require(name is None or name.group(1) not in variables,
                    'Loop L3 rejects induction variable mutation, shadowing or address escape')


def analyze_nested_loops(source_text):
    """Return None for old-rule inputs; never weaken either old analyzer."""
    clean = _sanitize_c(source_text)
    keywords = list(re.finditer(r'\b(?:for|while|do)\b', clean))
    if len(keywords) <= 1:
        return None
    require(len(keywords) == 2 and all(m.group() == 'for' for m in keywords),
            'Loop L3 requires exactly two nested static for loops')
    # Locate spans before deciding whether the existing sequential rule owns input.
    spans = []
    try:
        for match in keywords:
            close = _match_pair(clean, _skip_ws(clean, match.end()), '(', ')')
            brace = _skip_ws(clean, close + 1)
            spans.append((brace, _match_pair(clean, brace, '{', '}')))
    except ConditionModelError as exc:
        raise LoopModelError('Loop L3 requires complete braced headers and bodies') from exc
    if spans[0][1] < keywords[1].start():
        return None
    require(not re.search(r'^\s*#\s*(?!include\b)\w+', _strip_comments_and_strings(source_text), re.M),
            'Loop L3 rejects macros and non-include directives')
    require(not re.search(r'\b(?:if|else|switch|pthread_\w+|sem_\w+)\b', clean),
            'Loop L3 rejects Condition/Lock/synchronization combinations')
    outer, inner = [_header(clean, match) for match in keywords]
    require(outer['brace_offset'] < inner['start_offset'] < inner['end_offset'] < outer['end_offset'],
            'Loop L3 requires complete inner loop inside outer body')
    require(not clean[outer['brace_offset'] + 1:inner['start_offset']].strip() and
            not clean[inner['end_offset'] + 1:outer['end_offset']].strip(),
            'Loop L3 outer body may only contain the complete inner loop')
    require(outer['variable'] != inner['variable'], 'Loop L3 requires distinct induction variables')
    offset = inner['brace_offset'] + 1;body = clean[offset:inner['end_offset']]
    require('{' not in body and '}' not in body, 'Loop L3 rejects compound or nested inner bodies')
    require(not re.search(r'\b(?:break|continue|return|goto)\b|[?:]', body),
            'Loop L3 rejects body control transfer or conditional expressions')
    statements = [];texts = [];cursor = _skip_ws(body, 0)
    try:
        while cursor < len(body):
            statement = _parse_statement(body, cursor)
            first = clean.count('\n', 0, offset + statement.start) + 1
            last = clean.count('\n', 0, offset + statement.end - 1) + 1
            require(body[statement.start:statement.end].strip() != ';', 'Loop L3 requires nonempty ordinary body statements')
            require(not statements or first > statements[-1][1], 'Loop L3 rejects same-line body statements')
            statements.append((first, last));texts.append(body[statement.start:statement.end])
            cursor = _skip_ws(body, statement.end)
    except ConditionModelError as exc:
        raise LoopModelError('Loop L3 malformed ordinary body statement') from exc
    require(bool(statements), 'Loop L3 requires nonempty ordinary inner body')
    _check_induction(body, texts, [outer['variable'], inner['variable']])
    outer.update(body_start=inner['header_line'], body_end=inner['exit_line'])
    inner.update(body_start=statements[0][0], body_end=statements[-1][1], statements=statements)
    return outer, inner


def build_loop_nested_views(*, base_name, source_text, base_segments_json, base_dag_json):
    regions = analyze_nested_loops(source_text)
    if regions is None:
        return build_loop_pair_views(base_name=base_name, source_text=source_text,
                                     base_segments_json=base_segments_json, base_dag_json=base_dag_json)
    outer, inner = regions; bases = base_segments_json['segments']
    owners = [r for r in bases if r['start_line'] <= outer['header_line'] and outer['exit_line'] <= r['end_line']]
    require(len(owners) == 1 and owners[0]['kind'] == 'compute',
            'Loop L3 requires complete outer loop in one existing compute Level-2 segment')
    owner = owners[0];old = owner['seg_id'];fn = owner['function'];lines = _sanitize_c(source_text).splitlines()
    def trim(a, b):
        while a <= b and not lines[a - 1].strip(): a += 1
        while b >= a and not lines[b - 1].strip(): b -= 1
        return (a, b) if a <= b else None
    pre = trim(owner['start_line'], outer['header_line'] - 1)
    post = trim(outer['exit_line'] + 1, owner['end_line'])
    n1, n2 = outer['iteration_count'], inner['iteration_count']
    expected = int(pre is not None) + int(post is not None) + n1 * n2 + 2 + 2 * n1
    require(expected + len(bases) - 1 <= MAX_STRUCTURAL_NODES,
            f'Loop L3 structural node limit exceeded: {expected + len(bases) - 1} > {MAX_STRUCTURAL_NODES}')
    # No expanded node/coordinate list is allocated before the size guard.
    oid = f'LOOP:{fn}@{outer["header_line"]}';iid = f'LOOP:{fn}@{inner["header_line"]}'
    loops = [dict(outer, loop_id=oid, parent_loop_id=None, depth=1, function=fn, original_segment=old),
             dict(inner, loop_id=iid, parent_loop_id=oid, depth=2, function=fn, original_segment=old)]
    for loop in loops:
        for key in ('start_offset', 'brace_offset', 'end_offset'): loop.pop(key)
    struct = [];execution = [];chain = [];templates = []
    locations = base_segments_json.get('node_locations', {})
    generation = {n: [] for n in locations}
    positions = copy.deepcopy(base_segments_json.get('mappings', {}).get('source_position_to_execution_segments', {}))
    for identity, targets in positions.items():
        positions[identity] = list(dict.fromkeys(target for target in targets if target != old))
    body_positions = [f'LOOPSRC:{fn}:line:{line}' for a,b in inner['statements'] for line in range(a,b + 1)]
    for identity in body_positions: positions.setdefault(identity, [])
    def executable(sid, bounds, role, lid, tid, i=None, j=None):
        a,b = bounds;origins = sorted(n for n,l in locations.items() if l['function'] == fn and a <= l['line'] <= b)
        identities = body_positions.copy() if role == 'iteration' else [f'LOOPSRC:{fn}:line:{line}' for line in range(a,b + 1) if lines[line - 1].strip()]
        row = dict(seg_id=sid, function=fn, kind='compute', start_line=a, end_line=b, loop_id=lid,
                   role=role, template_id=tid, iteration_index=j, outer_iteration_index=i, inner_iteration_index=j,
                   parent_loop_id=oid if role == 'iteration' else None, generation_origin=origins or None,
                   source_segments=[old], source_position_ids=identities)
        execution.append(row);struct.append(row);chain.append(sid)
        for identity in identities:
            targets = positions.setdefault(identity, [])
            if sid not in targets: targets.append(sid)
        for origin in origins: generation[origin].append(sid)
    def control(sid, lid, kind, line, i=None):
        struct.append(dict(seg_id=sid, function=fn, kind=kind, start_line=line, end_line=line,
                           synthetic=True, loop_id=lid, parent_loop_id=oid if lid == iid else None,
                           outer_iteration_index=i));chain.append(sid)
    if pre:
        executable(oid + ':before', pre, 'before', oid, oid + ':before')
        templates.append(dict(template_id=oid + ':before', loop_id=oid, role='before', start_line=pre[0], end_line=pre[1]))
    control(oid + ':entry', oid, 'loop_entry', outer['header_line'])
    tid = iid + ':body_template'
    templates.append(dict(template_id=tid, loop_id=iid, role='body_template', start_line=inner['body_start'], end_line=inner['body_end']))
    for i in range(n1):
        control(f'{iid}:outer:{i}:entry', iid, 'loop_entry', inner['header_line'], i)
        for j in range(n2):
            executable(f'{iid}:outer:{i}:iteration:{j}', (inner['body_start'], inner['body_end']), 'iteration', iid, tid, i, j)
        control(f'{iid}:outer:{i}:exit', iid, 'loop_exit', inner['exit_line'], i)
    control(oid + ':exit', oid, 'loop_exit', outer['exit_line'])
    if post:
        executable(oid + ':after', post, 'after', oid, oid + ':after')
        templates.append(dict(template_id=oid + ':after', loop_id=oid, role='after', start_line=post[0], end_line=post[1]))
    replaced = [r['seg_id'] for r in execution]
    legacy = {}
    all_struct = [];all_execution = []
    for row in bases:
        if row['seg_id'] == old:
            all_struct.extend(struct);all_execution.extend(execution);legacy[old] = replaced.copy()
        else:
            all_struct.append(copy.deepcopy(row));all_execution.append(copy.deepcopy(row));legacy[row['seg_id']] = [row['seg_id']]
    for node, segment in base_segments_json.get('node_to_segment', {}).items():
        if segment != old: generation[node] = [segment]
        elif node not in generation: generation[node] = replaced.copy()  # existing aggregate, not a fabricated GCC call
    edges = []
    for raw in base_dag_json['edges']:
        edge = copy.deepcopy(raw)
        if edge['src'] == old: edge['src'] = chain[-1]
        if edge['dst'] == old: edge['dst'] = chain[0]
        edges.append(edge)
    external_edge_ids = {id(e) for e in edges}
    for a,b in zip(chain, chain[1:]):
        edges.append(dict(src=a, dst=b, kind='loop_iteration_order' if ':iteration:' in a and ':iteration:' in b else 'loop_flow', loop_id=oid))
    nodes = [r['seg_id'] for r in all_struct];scheduled = [r['seg_id'] for r in all_execution]
    require(len(nodes) == len(set(nodes)), 'Loop L3 duplicate expanded node ID')
    topo_sort(nodes, [(e['src'],e['dst']) for e in edges])
    adj = {n: [] for n in nodes}
    for edge in edges: adj[edge['src']].append(edge)
    active = set(scheduled);projected = []
    for node in scheduled:
        pending = [(e['dst'],[e]) for e in adj[node]]
        while pending:
            target,path = pending.pop()
            if target in active:
                if len(path) == 1: edge = copy.deepcopy(path[0])
                else:
                    edge = dict(src=node, dst=target, kind='loop_flow', control_projection=copy.deepcopy(path))
                    # Keep all external attributes alongside full path provenance.
                    external = [e for e in path if id(e) in external_edge_ids]
                    if external:
                        for original in external:
                            preserved = copy.deepcopy(original);preserved.update(src=node, dst=target, control_projection=copy.deepcopy(path))
                            projected.append(preserved)
                        continue
                projected.append(edge)
            else: pending.extend((e['dst'],path + [e]) for e in adj[target])
    topo_sort(scheduled, [(e['src'],e['dst']) for e in projected])
    mappings = copy.deepcopy(base_segments_json.get('mappings', {}))
    mappings.update(generation_node_to_execution_segments=generation, original_segment_to_execution_segments=legacy,
                    source_position_to_execution_segments=positions)
    payload = copy.deepcopy(base_segments_json);payload.pop('node_to_segment', None)
    payload.update(rule_name='loop_static_nested', loops=loops, templates=templates, mappings=mappings,
                   capabilities=dict(scheduler_compatible=True, runtime_instrumentation_supported=False),
                   timing_policy='one explicit inner body template copied per coordinate; no legacy split')
    return (dict(payload, segments=all_struct, view='struct'), dict(base_name=base_name, nodes=nodes, edges=edges),
            dict(payload, segments=all_execution, view='sched'), dict(base_name=base_name, nodes=scheduled, edges=projected),
            dict(rule_name='loop_static_nested', level='level2', loop_count=2, scheduler_compatible=True, runtime_instrumentation_supported=False))
