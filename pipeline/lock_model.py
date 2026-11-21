"""L1/L2 analysis-only simultaneous contraction. This is a deliberately bounded lexical validator."""
from __future__ import annotations

import copy
import hashlib
import json
import re

from ..core.func_ranges import _strip_comments_and_strings
from .algo.common import topo_sort
from .errors import ValidationError
from .source_syntax import mask_supported_enum_typedefs, direct_thread_calls, SourceSyntaxError


class LockModelError(ValueError):
    pass


def _require(test, message):
    if not test:
        raise LockModelError(message)


def _analyze_lock_source(source, *, l1_only):
    """Validate direct straight-line mutex regions; preserve the narrow L1 entry point."""
    clean = mask_supported_enum_typedefs(_strip_comments_and_strings(source))
    lines = clean.splitlines()
    line_offsets = []
    cursor = 0
    for line in clean.splitlines(keepends=True):
        line_offsets.append(cursor)
        cursor += len(line)
    for line in lines:
        if line.lstrip().startswith('#'):
            _require(bool(re.fullmatch(r'\s*#\s*include\s*<(pthread|stdio|stdlib|stdint|stddef)\.h>\s*', line)),
                     'L1 rejects macros and unsupported includes')
    _require(not re.search(r'\b(if|else|for|while|do|switch|case|goto|break|continue|setjmp|longjmp|exit|pthread_exit|pthread_cancel|pthread_detach)\b|\?|\\', clean),
             'L1 requires unconditional straight-line functions; control transfers are unsupported')
    _require(not re.search(r'\bsem_\w*\b|\bpthread_(?:mutexattr|rwlock)\w*\b', clean),
             'L1 does not support semaphores, lock attributes, or rwlocks')
    decl = list(re.finditer(r'^\s*(?:static\s+)?pthread_mutex_t\s+(\w+)\s*=\s*PTHREAD_MUTEX_INITIALIZER\s*;', clean, re.M))
    _require(decl and len(re.findall(r'\bpthread_mutex_t\b', clean)) == len(decl),
             'Lock requires directly named statically initialized pthread_mutex_t declarations')
    mutexes = sorted(m.group(1) for m in decl)
    _require(len(set(mutexes)) == len(mutexes), 'duplicate mutex declaration')
    if l1_only:
        _require(len(mutexes) == 1, 'L1 requires exactly one mutex')
    funcs = {}
    for m in re.finditer(r'\b(\w+)\s*\(([^{};]*)\)\s*\{', clean):
        name = m.group(1)
        _require(name not in funcs, 'duplicate or ambiguous function definition')
        end = clean.find('}', m.end())
        _require(end >= 0 and '{' not in clean[m.end():end], 'L1 rejects nested lexical/control blocks')
        body = clean[m.end():end]
        returns = list(re.finditer(r'\breturn\b', body))
        if returns:
            _require(len(returns) == 1 and bool(re.fullmatch(r'return\s+(NULL|0)\s*;\s*', body[returns[0].start():])),
                     'L1 permits only a final return NULL/0, not early returns')
        funcs[name] = {'start': m.start(), 'body_start': m.end(), 'end': end, 'body': body}
    _require(not any(f['start'] <= d.start() <= f['end'] for f in funcs.values() for d in decl), 'mutex must be global, not a local/shadowed object')
    _require(not re.search(r'\btypedef\b', clean), 'L1 rejects typedef/indirect call aliases')
    _require('main' in funcs, 'Lock requires a direct main with one create per worker')
    calls = []
    for i, line in enumerate(lines, 1):
        if not re.search(r'\bpthread_mutex_(?!t\b)\w+', line):
            continue
        m = re.fullmatch(r'\s*pthread_mutex_(lock|unlock)\s*\(\s*&\s*(\w+)\s*\)\s*;\s*', line)
        _require(m is not None, 'L1 requires standalone direct lock/unlock(&mutex); trylock/wrappers/aliases unsupported')
        _require(m.group(2) in mutexes, 'unknown mutex name')
        offset = line_offsets[i-1]
        owners = [name for name,f in funcs.items() if f['body_start'] <= offset <= f['end']]
        _require(len(owners) == 1, 'mutex call has no unambiguous function owner')
        calls.append((owners[0], m.group(1), i, m.group(2)))
    if l1_only:
        _require(len(calls) == 4, 'L1 requires two paired lock/unlock calls')
    remainder = clean
    for d in reversed(decl):
        remainder = remainder[:d.start()]+remainder[d.end():]
    for mutex in mutexes:
        remainder = re.sub(r'pthread_mutex_(?:lock|unlock)\s*\(\s*&\s*'+re.escape(mutex)+r'\s*\)\s*;', '', remainder)
        _require(not re.search(r'\b'+re.escape(mutex)+r'\b', remainder), 'mutex aliases, shadowing, or additional uses unsupported')
    owners = sorted({fn for fn,_,_,_ in calls})
    _require(owners and 'main' not in owners, 'Lock requires direct worker functions')
    if l1_only:
        _require(len(owners) == 2, 'L1 requires two distinct worker functions')
    regions = []
    for fn in owners:
        sequence = [(op,line,mutex) for name,op,line,mutex in calls if name == fn]
        if l1_only:
            _require([op for op,_,_ in sequence] == ['lock','unlock'], 'unpaired, nested, or repeated mutex operations')
        active = None
        for op,line,mutex in sequence:
            if op == 'lock':
                _require(active is None, 'nested or crossing mutex regions unsupported')
                active = (line,mutex)
                continue
            _require(active is not None and active[1] == mutex, 'unpaired or crossing mutex unlock')
            lock = active[0];unlock = line
            body_lines = [i for i in range(lock+1, unlock) if lines[i-1].strip()]
            _require(body_lines and unlock > lock+1, 'Lock requires a nonempty multiline critical section')
            regions.append({'function': fn, 'mutex_id': mutex,
                            'lock_range': [lock,lock], 'unlock_range': [unlock,unlock],
                            'critical_range': [body_lines[0],body_lines[-1]], 'segment_range': [lock,unlock]})
            active = None
        _require(active is None, 'unpaired mutex lock')
    for mutex in mutexes:
        _require(sum(r['mutex_id']==mutex for r in regions) >= 2, 'each mutex group requires at least two complete regions')
    try:
        creates = direct_thread_calls(funcs['main']['body'], 'create', standalone=True)
    except SourceSyntaxError as exc:
        raise LockModelError('Lock requires one standalone create per worker in main, with NULL/0 attributes/arguments') from exc
    _require(len(creates) == len(owners) and len(re.findall(r'\bpthread_create\b', clean)) == len(owners),
             'Lock requires one standalone create per worker in main, with NULL attributes/arguments')
    _require({m.entry for m in creates} == set(owners) and len({m.handle for m in creates}) == len(owners),
             'Lock requires distinct handles and exactly one create per worker')
    try:
        joins = direct_thread_calls(funcs['main']['body'], 'join', standalone=True)
    except SourceSyntaxError as exc:
        raise LockModelError('Lock requires one direct join for each worker') from exc
    _require(len(joins) == len(owners) and {m.handle for m in joins} == {m.handle for m in creates}
             and len(re.findall(r'\bpthread_join\b', clean)) == len(owners), 'Lock requires one direct join for each worker')
    _require(max(m.end for m in creates) < min(m.start for m in joins), 'all creates must precede joins')
    for fn in owners:
        _require(len(re.findall(r'\b'+re.escape(fn)+r'\b', clean)) == 2,
                 'worker used outside its definition and one create; wrappers/extra calls unsupported')
    # Only statically visible helper calls and a small known external API set.
    allowed = set(funcs) | {'pthread_create','pthread_join','pthread_mutex_lock','pthread_mutex_unlock','puts','printf'}
    adjacency = {}
    for fn,f in funcs.items():
        names = re.findall(r'\b(\w+)\s*\(', f['body'])
        _require(set(names) <= allowed, 'L1 rejects unknown/wrapped/indirect calls')
        adjacency[fn] = [n for n in names if n in funcs]
        _require(not re.search(r'\(\s*\*', f['body']), 'L1 rejects indirect calls')
    topo_sort(list(funcs), [(a,b) for a,bs in adjacency.items() for b in bs])
    return mutexes, regions


def analyze_lock_l1(source):
    mutexes, regions = _analyze_lock_source(source, l1_only=True)
    return mutexes[0], regions


def analyze_lock_l2(source):
    return _analyze_lock_source(source, l1_only=False)


def _canonical_lock_groups(groups, valid_nodes):
    """Validate disjoint membership before any contraction or weight summation."""
    groups = sorted(copy.deepcopy(groups), key=lambda g:g['group_id'])
    seen_groups = set();owner = {}
    for group in groups:
        gid = group['group_id'];members = group['member_node_ids']
        _require(gid == 'LOCKGROUP:'+group['mutex_id'] and gid not in seen_groups and gid not in valid_nodes, 'duplicate/invalid lock group ID')
        seen_groups.add(gid)
        _require(len(members)>=2 and len(set(members))==len(members), 'duplicate or insufficient group members')
        _require(len(group['member_segment_ids'])==len(members) and set(members)==set(group['member_segment_ids']), 'duplicate member or group node/segment membership mismatch')
        for sid in members:
            _require(sid in valid_nodes, 'unknown member execution segment')
            _require(sid not in owner, 'member belongs to multiple mutex groups: '+sid)
            owner[sid] = gid
        _require(len(group['members'])==len(members) and {r['member_segment_id'] for r in group['members']}==set(members), 'incomplete member source metadata')
        _require(all(r['mutex_id']==group['mutex_id'] and r['member_node_id']==r['member_segment_id'] for r in group['members']), 'member mutex/source metadata mismatch')
        group['member_node_ids'] = sorted(members)
        group['member_segment_ids'] = sorted(members)
        group['members'] = sorted(group['members'], key=lambda r:r['member_segment_id'])
    return groups, owner


def contract_lock_groups(base_dag_json, groups):
    """Simultaneous quotient of the original graph; no per-group graph mutation."""
    groups, owner = _canonical_lock_groups(groups, set(base_dag_json['nodes']))
    segment_map = {n:owner.get(n,n) for n in sorted(base_dag_json['nodes'])}
    edges = {};removed = []
    for i,e in enumerate(base_dag_json['edges']):
        src,dst = segment_map[e['src']],segment_map[e['dst']]
        origin = {'base_edge_index':i,'edge':copy.deepcopy(e)}
        if e['src'] in owner and e['dst'] in owner and src==dst:
            removed.append(origin);continue
        key = (src,dst,e['kind'])
        if key not in edges:
            # Unaffected edges retain their attributes; all sources remain auditable.
            edges[key] = (copy.deepcopy(e) if e['src'] not in owner and e['dst'] not in owner else
                          {'src':src,'dst':dst,'kind':e['kind']})
            edges[key]['provenance'] = []
        edges[key]['provenance'].append(origin)
    sched_dag = copy.deepcopy(base_dag_json)
    sched_dag.update(nodes=[n for n in base_dag_json['nodes'] if n not in owner]+[g['group_id'] for g in groups],
                     edges=[edges[k] for k in sorted(edges)],removed_internal_edges=removed)
    _require(len(sched_dag['nodes'])==len(base_dag_json['nodes'])-sum(len(g['member_node_ids'])-1 for g in groups), 'grouped node count mismatch')
    return groups, segment_map, sched_dag


def build_lock_views(*, base_name, source_text, base_segments_json, base_dag_json):
    mutexes, regions = analyze_lock_l2(source_text)
    _require(base_segments_json.get('base_name')==base_name, 'base segment name mismatch')
    original = copy.deepcopy(base_segments_json)
    rows = original['segments']
    by_id = {r['seg_id']:r for r in rows}
    _require(len(by_id) == len(rows) and set(base_dag_json['nodes']) == set(by_id), 'segment/DAG IDs must match uniquely')
    _require(set(r['function'] for r in regions) <= set(original.get('thread_entry_functions', [])), 'workers not confirmed as thread entries in base metadata')
    for e in base_dag_json['edges']:
        _require(e['src'] in by_id and e['dst'] in by_id and isinstance(e.get('kind'),str), 'invalid base edge')
    topo_sort(base_dag_json['nodes'], [(e['src'],e['dst']) for e in base_dag_json['edges']])
    clean_lines = _strip_comments_and_strings(source_text).splitlines()
    _require(sum(r['kind']=='mutex_cs' for r in rows)==len(regions), 'every existing mutex_cs segment must match one lexical region')
    members = []
    for region in regions:
        fn = region['function'];a,b = region['segment_range']
        overlap = [r for r in rows if r['function']==fn and r['start_line']<=b and a<=r['end_line']]
        _require(len(overlap)==1, 'critical section must correspond to exactly one complete existing segment')
        row = overlap[0]
        effective = [i for i in range(row['start_line'],row['end_line']+1) if clean_lines[i-1].strip()]
        _require(row['kind']=='mutex_cs' and effective and effective[0]==a and effective[-1]==b,
                 'existing mutex segment includes outside code or is not a complete critical section')
        region['member_node_id'] = region['member_segment_id'] = row['seg_id']
        members.append(row['seg_id'])
    _require(len(set(members))==len(regions), 'member belongs to multiple mutex groups or regions')
    locations = original.get('node_locations', {})
    node_map = original.get('node_to_segment', {})
    _require(locations and all(n in node_map for n in locations) and all(sid in by_id for sid in node_map.values()), 'incomplete original-node mapping')
    for n,loc in locations.items():
        sid = node_map[n];row = by_id.get(sid)
        _require(row is not None and row['function']==loc['function'] and row['start_line']<=loc['line']<=row['end_line'], 'invalid original-node source mapping')
    for region in regions:
        for api,key in [('pthread_mutex_lock','lock_range'),('pthread_mutex_unlock','unlock_range')]:
            found = [n for n,loc in locations.items() if loc['function']==region['function'] and loc['line']==region[key][0] and re.search(r'/'+api+r'\d*$',n)]
            _require(len(found)==1 and node_map[found[0]]==region['member_segment_id'], 'base metadata does not confirm direct mutex call location')
    groups = []
    for mutex in mutexes:
        group_id = 'LOCKGROUP:'+mutex
        member_regions = [r for r in regions if r['mutex_id']==mutex]
        ids = [r['member_segment_id'] for r in member_regions]
        groups.append({'group_id':group_id,'mutex_id':mutex,'member_node_ids':ids,'member_segment_ids':ids,
                       'members':member_regions,'aggregation_method':'fieldwise_sum_of_member_weights','analysis_only':True})
    groups, segment_map, sched_dag = contract_lock_groups(base_dag_json,groups)
    mappings = copy.deepcopy(original.get('mappings', {}))
    mappings.update({'original_node_to_execution_segment':copy.deepcopy(node_map),
                     'original_node_to_grouped_node':{n:segment_map[sid] for n,sid in node_map.items()},
                     'original_segment_to_grouped_node':segment_map,
                     'grouped_node_to_member_nodes':{g['group_id']:g['member_node_ids'] for g in groups},
                     'grouped_node_to_member_segments':{g['group_id']:g['member_segment_ids'] for g in groups},
                     'grouped_node_to_mutex_and_source':{g['group_id']:g for g in groups}})
    reasons = []
    try:topo_sort(sched_dag['nodes'],[(e['src'],e['dst']) for e in sched_dag['edges']])
    except ValidationError:reasons.append('lock contraction creates a cycle; all contracted edges retained')
    capabilities = {'scheduler_compatible':not reasons,'blocked_reasons':reasons,'timing_compatible':False,
                    'instrumentation_compatible':False,'analysis_only':True}
    common = {'rule_name':'lock_grouped','level':'level2','source_sha256':hashlib.sha256(source_text.encode()).hexdigest(),
              'lock_groups':groups,'mappings':mappings,'capabilities':capabilities}
    structural = copy.deepcopy(original);structural.update(common,view='struct')
    for row in structural['segments']:
        if row['seg_id'] in members:
            row['lock_group'] = copy.deepcopy(next(r for r in regions if r['member_segment_id']==row['seg_id']))
            row['lock_group']['group_id'] = segment_map[row['seg_id']]
    grouped = copy.deepcopy(original);grouped.update(common,view='sched')
    # Bounding lines exist for legacy algorithm schema only; never executable.
    group_rows = [dict(g,seg_id=g['group_id'],function='<lockgroup>',kind='lock_group',
                       start_line=min(by_id[sid]['start_line'] for sid in g['member_segment_ids']),
                       end_line=max(by_id[sid]['end_line'] for sid in g['member_segment_ids']),synthetic=True) for g in groups]
    grouped['segments'] = [copy.deepcopy(r) for r in rows if r['seg_id'] not in members]+group_rows
    grouped['node_to_segment'] = copy.deepcopy(mappings['original_node_to_grouped_node'])
    structural_dag = copy.deepcopy(base_dag_json)
    meta = dict(common,group_ids=[g['group_id'] for g in groups],scheduler_compatible=not reasons,blocked_reasons=reasons)
    if len(groups)==1:
        meta['group_id'] = groups[0]['group_id']
    return structural, structural_dag, grouped, sched_dag, meta


TIMING_FIELDS = ('total_ns','count','avg_ns','min_ns','max_ns')


def aggregate_lock_timing(segments, timing):
    """Sum existing member fields; count counts contributing real samples, not virtual executions."""
    _require(segments['capabilities']['scheduler_compatible'], 'cannot aggregate timing for incompatible lock graph')
    _require(timing.get('base_name')==segments.get('base_name'), 'timing base_name mismatch')
    original = timing.get('weights', {})
    needed = set(segments['mappings']['original_segment_to_grouped_node'])
    _require(set(original)==needed, 'timing must contain every original execution segment exactly once')
    groups, owner = _canonical_lock_groups(segments['lock_groups'], needed)
    expected_map = {sid:owner.get(sid,sid) for sid in needed}
    _require(expected_map==segments['mappings']['original_segment_to_grouped_node'], 'group membership/mapping mismatch')
    members = set(owner)
    for sid in needed:
        w = original[sid]
        _require(isinstance(w,dict) and set(w)==set(TIMING_FIELDS), 'missing or unsupported timing fields for '+sid)
        _require(all(type(w[f]) is int and w[f]>=0 for f in TIMING_FIELDS), 'timing fields must be nonnegative integers: '+sid)
        _require((sid not in members or w['count']>0) and w['min_ns']<=w['avg_ns']<=w['max_ns'], 'missing member samples or invalid timing bounds: '+sid)
    result = copy.deepcopy(timing)
    weights = {sid:copy.deepcopy(original[sid]) for sid,target in segments['mappings']['original_segment_to_grouped_node'].items() if sid==target}
    for group in groups:
        weights[group['group_id']] = {f:sum(original[sid][f] for sid in group['member_segment_ids']) for f in TIMING_FIELDS}
    result.update(rule_name='lock_grouped',view='sched',weights=weights,
                  lock_aggregation={'method':'fieldwise_sum','count_semantics':'sum of contributing real member samples, not virtual group execution count',
                                    'avg_ns_semantics':'sum of member means; not total_ns divided by count',
                                    'min_max_semantics':'sums of member sample extrema, not measured group extrema or proven WCET',
                                    'source_sha256':segments['source_sha256']})
    return result
