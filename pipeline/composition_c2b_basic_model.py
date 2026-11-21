"""Bounded C2b extension: main, sequential trees and direct void helpers.

The original C is never rewritten. Condition views come from the existing
source analyzer. A helper body is instantiated per source call and execution
context, rather than used as one shared execution vertex.
"""
import copy
import hashlib
import re
from collections import Counter

from .composition_model import require, CONTROL, _project, digest, _ordered_unique
from .condition_model import _sanitize_c, _match_pair, analyze_conditionals, build_condition_views
from .lock_regions import match_lock_regions
from .source_syntax import direct_thread_calls
from .algo.common import topo_sort
from ..core.func_ranges import normalize_func_name, is_ignored_compiler_fn

MAX_SOURCE_CONDITIONS = 8
MAX_HELPER_CONTEXTS = 16
MAX_LOCK_PATHS = 32
_HELPER = re.compile(r'\bvoid\s+([A-Za-z_]\w*)\s*\(\s*(?:void\s*)?\)\s*\{')


def helper_definitions(text):
    clean = _sanitize_c(text)
    result = {}
    for match in _HELPER.finditer(clean):
        name = match.group(1)
        if name == 'main':
            continue
        closing = _match_pair(clean, match.end()-1, '{', '}')
        require(name not in result, 'C2b ambiguous helper definition')
        result[name] = dict(start=match.start(), body=match.end(), end=closing,
                            first=clean.count('\n', 0, match.start())+1,
                            last=clean.count('\n', 0, closing)+1)
    return result


def _helper_sites(text, helpers):
    clean = _sanitize_c(text)
    sites = []
    for line, whole in enumerate(clean.splitlines(), 1):
        for name in helpers:
            if re.search(r'\b'+re.escape(name)+r'\s*\(', whole):
                if helpers[name]['first'] <= line <= helpers[name]['last']:
                    continue
                # A definition/prototype is not a call. Calls are intentionally
                # restricted to one direct no-argument statement per line.
                if re.search(r'\bvoid\s+'+re.escape(name)+r'\s*\(', whole):
                    continue
                require(re.fullmatch(r'\s*'+re.escape(name)+r'\s*\(\s*\)\s*;\s*', whole),
                        'C2b helper call must be a standalone no-argument statement')
                sites.append((name, line))
    return sites


def helper_condition_inside_lock(text, ranges, locks):
    helpers = helper_definitions(text)
    for name, line in _helper_sites(text, helpers):
        h = helpers[name]
        if analyze_conditionals(text, {name:(h['first'],h['last'])}).regions:
            if any(lock['lock_range'][0] < line < lock['unlock_range'][0] for lock in locks):
                return True
    return False


def needs_basic_extension(text):
    from .composition_c2b_model import _source_matched_regions
    ranges, locks = _source_matched_regions(text)
    analysis = analyze_conditionals(text, ranges)
    return (any(r['function']=='main' for r in locks)
            or len([r for r in analysis.regions if r.parent_id is None]) != 1
            or len(analysis.regions) != len(re.findall(r'\bif\b', _sanitize_c(text)))
            or bool(_helper_sites(text, helper_definitions(text))))


def _thread_sites(raw, text):
    """Reuse the generator's instance-aware cut metadata, not guessed aliases."""
    from ..level1.segment_dag import _iter_stage1_cut_points, _load_functions_full, _load_internal_meta
    from pathlib import Path
    inputs = raw.get('inputs', {})
    require('functions_full' in inputs and 'mycalls_meta_internal' in inputs,
            'C2b basic extension requires formal generation metadata')
    cuts = _iter_stage1_cut_points(functions_full=_load_functions_full(Path(inputs['functions_full'])),
                                  internal_meta=_load_internal_meta(Path(inputs['mycalls_meta_internal'])))
    creates = direct_thread_calls(text, 'create', standalone=True)
    joins = direct_thread_calls(text, 'join', standalone=True)
    require(Counter(c.handle for c in creates)==Counter(c.handle for c in joins),
            'C2b basic extension requires one direct join per create handle')
    by_line = {text.count('\n',0,c.start)+1:c for c in creates}
    create_sites = {}
    handle_instance = {}
    for cut in cuts:
        if cut.kind=='create' and cut.entry_fn:
            require(cut.line in by_line, 'C2b missing source create callsite')
            call = by_line[cut.line]
            require(call.handle not in handle_instance, 'C2b ambiguous create handle')
            handle_instance[call.handle] = cut.entry_fn
            create_sites[cut.entry_fn] = (cut.function, cut.line)
    require(len(handle_instance)==len(creates), 'C2b missing generated execution instance')
    join_sites = {handle_instance[c.handle]:text.count('\n',0,c.start)+1 for c in joins}
    return create_sites, join_sites


def build_basic_views(*, base_name, source_text, base_segments_json, base_dag_json):
    from .composition_c2b_model import _source_matched_regions, MAX_LOCK_REGIONS
    raw = copy.deepcopy(base_segments_json)
    dag = copy.deepcopy(base_dag_json)
    bases = raw['segments']
    by_base = {r['seg_id']:r for r in bases}
    clean = _sanitize_c(source_text)
    lines = clean.splitlines()
    require(len(lines)<=10000, 'C2b basic extension source limit exceeded')
    require(not re.search(r'\b(?:for|while|do)\b', clean),
            'C2b main/sequential/helper extension currently requires no Loop')
    require(not re.search(r'\(\s*\*\s*\w+\s*\)\s*\(|(?:->|\.)\s*\w+\s*\(|\]\s*\(',clean),
            'C2b basic extension rejects indirect calls and function pointers')
    source_ranges, source_locks = _source_matched_regions(source_text)
    declarations=list(re.finditer(r'^\s*(?:static\s+)?pthread_mutex_t\s+(\w+)\s*=\s*PTHREAD_MUTEX_INITIALIZER\s*;',clean,re.M))
    mutexes=[m.group(1) for m in declarations]
    require(1<=len(mutexes)<=2 and len(set(mutexes))==len(mutexes)
            and len(mutexes)==len(re.findall(r'\bpthread_mutex_t\b',clean)),
            'C2b requires direct statically initialized mutexes')
    require(all(clean[:m.start()].count('{')==clean[:m.start()].count('}') for m in declarations),
            'C2b mutex declarations must be global')
    remainder=list(clean)
    for m in declarations:
        for i in range(m.start(),m.end()):
            if remainder[i] not in '\r\n':remainder[i]=' '
    remainder=re.sub(r'pthread_mutex_(?:lock|unlock)\s*\(\s*&\s*\w+\s*\)\s*;','', ''.join(remainder))
    require(not any(re.search(r'\b'+re.escape(m)+r'\b',remainder) for m in mutexes),
            'C2b mutex aliases or extra uses unsupported')
    helpers = helper_definitions(source_text)
    sites = _helper_sites(source_text, helpers)
    for name,h in helpers.items():
        body = clean[h['body']:h['end']]
        returns=list(re.finditer(r'\breturn\b',body))
        require(not returns or (len(returns)==1 and re.fullmatch(r'return\s*;\s*',body[returns[0].start():])),
                'C2b helper permits only a final void return')
        require(not re.search(r'\b(?:goto|break|continue|switch|pthread_\w+|sem_\w+)\b',body),
                'C2b helper contains unsupported control transfer or synchronization')
        require(not any(re.search(r'\b'+re.escape(n)+r'\s*\(',body) for n in helpers),
                'C2b recursive/nested helper calls are unsupported')
    all_source_ranges = dict(source_ranges)
    all_source_ranges.update({n:(h['first'],h['last']) for n,h in helpers.items()})
    source_analysis = analyze_conditionals(source_text, all_source_ranges)
    require(not source_analysis.diagnostics and 1<=len(source_analysis.regions)<=MAX_SOURCE_CONDITIONS
            and len(source_analysis.regions)==len(re.findall(r'\bif\b',clean)),
            'C2b requires bounded, completely owned source Conditions')
    for condition in source_analysis.regions:
        for lock in source_locks:
            if condition.function==lock['function'] and max(condition.start_offset,lock['start_offset'])<min(condition.end_offset,lock['end_offset']):
                require(lock['start_offset']<condition.start_offset and condition.end_offset<lock['end_offset'],
                        'C2b Condition must stay completely inside its lock')
    # Each original tree remains bounded; sequential trees are distinct choices.
    for root in [r for r in source_analysis.regions if r.parent_id is None]:
        tree = [r for r in source_analysis.regions if root.start_offset<=r.start_offset and r.end_offset<=root.end_offset and r.function==root.function]
        require(len(tree)<=2, 'C2b basic extension supports bounded Condition trees')
    for name,line in sites:
        owners = [fn for fn,(a,b) in source_ranges.items() if a<=line<=b]
        require(len(owners)==1 and any(r['function']==owners[0] and r['lock_range'][0]<line<r['unlock_range'][0] for r in source_locks),
                'C2b helper call must stay inside one main/worker critical region')
    called = {name for name,_ in sites}
    require(all(r.function not in helpers or r.function in called for r in source_analysis.regions),
            'C2b helper Condition has no confirmed callsite')
    ranges = {}
    for r in bases:
        a,b = ranges.get(r['function'], (r['start_line'],r['end_line']))
        ranges[r['function']] = min(a,r['start_line']),max(b,r['end_line'])
    regions = match_lock_regions(source_text, ranges)
    require(1<=len(regions)<=MAX_LOCK_REGIONS, 'C2b basic extension exceeds complete Lock region limit')
    allowed = set(raw.get('thread_entry_functions',[])) | {'main'}
    for region in regions:
        fn = region['function'];a,b = region['segment_range']
        require(fn in allowed, 'C2b unconfirmed lock execution context')
        old = [r for r in bases if r['function']==fn and r['kind']=='mutex_cs' and r['start_line']<=a<=b<=r['end_line']]
        require(len(old)==1, 'C2b lock must have one formal base segment')
        region['member_segment_id']=old[0]['seg_id']
        region['execution_region_id']='C2BREG:'+fn+'@'+str(a)+'-'+str(b)
        region['thread_entry_function']=normalize_func_name(fn)
        body='\n'.join(lines[a:b-1])
        require(not re.search(r'\b(?:return|goto|break|continue|switch|pthread_\w+|sem_\w+)\b',body),
                'C2b synchronization/control transfer inside critical region is unsupported')
    create_sites,join_sites = _thread_sites(raw,source_text)
    # Partition only the analysis rows. Physical source lines and compiled C
    # remain untouched; each fragment retains its original segment witness.
    split=[]
    for base in bases:
        for line in range(base['start_line'],base['end_line']+1):
            split.append(dict(base,seg_id=base['seg_id']+':line:'+str(line),start_line=line,end_line=line))
    from .composition_c4a_model import _formal_function_spans
    formal,formal_bodies=_formal_function_spans(raw)
    for name in called:
        require(name in formal,'C2b helper lacks formal generation range')
        if not any(r['function']==name for r in split):
            a,b=formal_bodies[name]
            split.append(dict(seg_id='HELPER_TEMPLATE:'+name,function=name,kind='compute',start_line=a,end_line=b))
    payload=dict(raw,segments=split)
    cs,cd,ce,_,meta=build_condition_views(base_name=base_name,source_text=source_text,
                                       base_segments_json=payload,base_dag_json={'nodes':[r['seg_id'] for r in split],'edges':[]})
    require(meta['scheduler_compatible'], 'C2b incompatible Condition: '+str(meta['blocked_reasons']))
    members={r['seg_id']:r.get('condition_memberships',[]) for r in ce['segments']}
    rows=cs['segments'];edges=cd['edges'];conditions=cs['condition_regions'];groups=cd['exclusive_groups']
    for row in rows:
        row['condition_memberships']=copy.deepcopy(members.get(row['seg_id'],[]))
        row['source_function']=normalize_func_name(row['function'])
        row['execution_context']=row['function']
        row['source_segments']=[b['seg_id'] for b in bases if b['function']==row['function'] and b['start_line']<=row['start_line']<=row['end_line']<=b['end_line']]
        owner=[r for r in regions if r['function']==row['function'] and r['segment_range'][0]<=row['start_line']<=row['end_line']<=r['segment_range'][1]]
        require(len(owner)<=1,'C2b ambiguous lock membership')
        if row['kind'] not in CONTROL:
            # Base segmentation may absorb an adjacent create/join into a mutex
            # segment. Only the source lock interval remains mutex-owned here.
            row['kind']='mutex_cs' if owner else 'compute'
        if owner:
            row['owning_lock']=owner[0]['execution_region_id'];row['mutex_id']=owner[0]['mutex_id']
    helper_rows={n:[r for r in rows if r['function']==n] for n in helpers}
    helper_ids={r['seg_id'] for rs in helper_rows.values() for r in rs}
    helper_conditions={n:[r for r in conditions if r['function']==n] for n in helpers}
    original_edges=copy.deepcopy(edges)
    rows=[r for r in rows if r['seg_id'] not in helper_ids]
    edges=[e for e in edges if e['src'] not in helper_ids and e['dst'] not in helper_ids]
    conditions=[r for r in conditions if r['function'] not in helpers]
    for n in helpers:
        for c in helper_conditions[n]:groups.pop(c['condition_id'],None)
    call_contexts={}
    for name,line in sites:
        calls=[r for r in list(rows) if r['function'] not in helpers and r['start_line']==line==r['end_line']
               and normalize_func_name(r['function']) in source_ranges]
        require(calls, 'C2b helper call has no source execution row')
        for call in calls:
            require(len(call_contexts)<MAX_HELPER_CONTEXTS,'C2b helper context limit exceeded')
            ctx=name+'@call['+call['function']+':'+str(line)+']'
            originals=helper_rows[name];require(originals,'C2b missing formal helper body')
            ids={r['seg_id']:'CALL:'+ctx+':'+r['seg_id'] for r in originals}
            cids={r['condition_id']:'CALLCOND:'+ctx+':'+r['condition_id'] for r in helper_conditions[name]}
            outer=copy.deepcopy(call['condition_memberships'])
            def membership(value):
                return [{**m,'exclusive_group':cids.get(m['exclusive_group'],m['exclusive_group'])} for m in value]
            for original in originals:
                r=copy.deepcopy(original);r['seg_id']=ids[r['seg_id']];r['function']=ctx
                r['execution_context']=call['function'];r['callsite_line']=line
                r['condition_memberships']=outer+membership(r['condition_memberships'])
                if 'condition_id' in r:r['condition_id']=cids[r['condition_id']]
                if 'owning_lock' in call:
                    r['owning_lock']=call['owning_lock'];r['mutex_id']=call['mutex_id']
                rows.append(r)
            local=[e for e in original_edges if e['src'] in ids and e['dst'] in ids]
            for e in local:
                e=copy.deepcopy(e);e['src']=ids[e['src']];e['dst']=ids[e['dst']]
                if 'exclusive_group' in e:e['exclusive_group']=cids[e['exclusive_group']]
                edges.append(e)
            for c in helper_conditions[name]:
                c=copy.deepcopy(c);old=c['condition_id'];c['condition_id']=cids[old];c['function']=ctx
                c['source_function']=name;c['execution_context']=call['function'];c['callsite_line']=line
                if c.get('parent_id'):c['parent_id']=cids[c['parent_id']]
                elif outer:c['parent_id']=outer[-1]['exclusive_group'];c['parent_branch_id']=outer[-1]['branch_id']
                conditions.append(c);groups[c['condition_id']]=[c['condition_id']+':then',c['condition_id']+':else']
            incoming={e['dst'] for e in local};outgoing={e['src'] for e in local}
            starts=[r for r in originals if r['seg_id'] not in incoming]
            ends=[r for r in originals if r['seg_id'] not in outgoing]
            require(len(starts)==len(ends)==1,'C2b helper must have one entry and exit')
            successor=[e for e in edges if e['src']==call['seg_id']]
            edges=[e for e in edges if e['src']!=call['seg_id']]
            edges.append(dict(src=call['seg_id'],dst=ids[starts[0]['seg_id']],kind='call',callsite_line=line))
            for e in successor:
                edges.append(dict(e,src=ids[ends[0]['seg_id']],kind='call_return',original_kind=e['kind'],callsite_line=line))
            call_contexts[ctx]=dict(helper=name,caller=call['function'],line=line,source_call_segment=call['seg_id'],
                                   condition_memberships=outer,owning_lock=call.get('owning_lock'))
    by_id={r['seg_id']:r for r in rows}
    def ports(fn,a,b,direction):
        selected=[r for r in rows if r['function']==fn and max(a,r['start_line'])<=min(b,r['end_line'])]
        if not selected:
            # Compiler/base segmentation may publish a blank tail. Trace that
            # no-op to the nearest real port in the same execution context.
            selected=[r for r in rows if r['function']==fn and (r['end_line']<a if direction=='exit' else r['start_line']>b)]
        require(selected,'C2b missing execution port')
        key=lambda r:(r['start_line'],0 if r['kind']=='condition_entry' else 2 if r['kind']=='condition_exit' else 1,r['end_line'])
        return sorted(selected,key=key)[-1 if direction=='exit' else 0]['seg_id']
    ledger=[]
    for index,e in enumerate(dag['edges']):
        a=by_base[e['src']];b=by_base[e['dst']]
        if a['function'] in helpers or b['function'] in helpers:
            require(a['function']==b['function'] and e['kind']=='intra',
                    'C2b ambiguous external dependency on a shared helper definition')
            ledger.append(dict(base_edge_index=index,edge=e,disposition='helper_definition_expanded_per_call'));continue
        if e['kind']=='intra':
            require(a['function']==b['function'],'C2b cross-context intra edge is unsupported')
            ledger.append(dict(base_edge_index=index,edge=e,disposition='source_sequence_replacement'));continue
        src=ports(a['function'],a['start_line'],a['end_line'],'exit')
        dst=ports(b['function'],b['start_line'],b['end_line'],'entry')
        if e['kind']=='create':
            require(b['function'] in create_sites,'C2b missing create instance binding')
            fn,line=create_sites[b['function']];src=ports(fn,line,line,'exit')
        if e['kind']=='join':
            require(a['function'] in join_sites,'C2b missing join instance binding')
            line=join_sites[a['function']];dst=ports(b['function'],line,line,'entry')
        edges.append(dict(e,src=src,dst=dst,base_edge_index=index))
        ledger.append(dict(base_edge_index=index,edge=e,disposition='projected'))
    struct_dag=dict(base_name=base_name,nodes=[r['seg_id'] for r in rows],edges=edges,exclusive_groups=groups)
    topo_sort(struct_dag['nodes'],[(e['src'],e['dst']) for e in edges])
    templates=[]
    for r in rows:
        sf=r['source_function'];fn=r['function']
        r['generation_origin']=[n for n,l in raw.get('node_locations',{}).items()
                                if not is_ignored_compiler_fn(n) and normalize_func_name(l['function'])==sf
                                and r['start_line']<=l['line']<=r['end_line']] or None
        r['source_position_ids']=['LOOPSRC:'+fn+':line:'+str(i) for i in range(r['start_line'],r['end_line']+1)] if r['kind'] not in CONTROL else []
        if r['kind'] not in CONTROL:
            r['template_id']='C2BTPL:'+r['seg_id']
            templates.append(dict(template_id=r['template_id'],function=fn,start_line=r['start_line'],end_line=r['end_line'],role='execution'))
    for ctx,call in call_contexts.items():
        call['generation_origin']=by_id[call['source_call_segment']]['generation_origin']
    execution=[r for r in rows if r['kind'] not in CONTROL]
    candidates=[];absorbed=set();owner={}
    for region in regions:
        local=[r for r in rows if r.get('owning_lock')==region['execution_region_id']]
        ids={r['seg_id'] for r in local}
        first=ports(region['function'],region['lock_range'][0],region['lock_range'][0],'entry')
        last=ports(region['function'],region['unlock_range'][0],region['unlock_range'][0],'exit')
        require(first in ids and last in ids,'C2b incomplete lock execution ports')
        adj={n:[] for n in ids}
        for e in edges:
            if e['src'] in ids and e['dst'] in ids:adj[e['src']].append(e)
        pending=[(first,[],[])];seen=set();count=0
        while pending:
            node,path,choices=pending.pop();path=path+[node]
            require(len(path)<=len(ids),'C2b cyclic lock path')
            if node!=last:
                for e in reversed(adj[node]):
                    choice=[{k:e[k] for k in ('exclusive_group','branch_id','guard')}] if e['kind']=='condition_branch' else []
                    pending.append((e['dst'],path,choices+choice))
                require(len(pending)<=MAX_LOCK_PATHS*len(ids),'C2b lock path limit exceeded')
                continue
            key='|'.join(m['exclusive_group']+'='+m['branch_id'] for m in choices) or 'unconditional'
            require(key not in seen,'C2b duplicate path identity');seen.add(key);count+=1
            require(count<=MAX_LOCK_PATHS,'C2b lock path limit exceeded')
            parts=[by_id[n] for n in path if by_id[n]['kind'] not in CONTROL]
            candidates.append(dict(seg_id='LOCKPATH:'+region['execution_region_id']+'@'+key,
                kind='lock_path_candidate' if choices else 'mutex_cs',function=region['function'],
                start_line=region['lock_range'][0],end_line=region['unlock_range'][0],path_id=key,
                mutex_id=region['mutex_id'],condition_memberships=choices,source_structural_nodes=path,
                source_segments=_ordered_unique(s for n in path for s in by_id[n]['source_segments']),
                source_position_ids=_ordered_unique(s for r in parts for s in r['source_position_ids']),
                generation_origin=_ordered_unique(n for r in parts for n in (r['generation_origin'] or [])) or None,
                member_segment_ids=[r['seg_id'] for r in parts],member_template_ids=[r['template_id'] for r in parts],
                source_ranges=[dict(function=r['function'],source_function=r['source_function'],execution_context=r['execution_context'],start_line=r['start_line'],end_line=r['end_line']) for r in parts],
                lock_group=dict(group_id='LOCKGROUP:'+region['mutex_id'],mutex_id=region['mutex_id'],source_region=region)))
        require(count,'C2b lock has no complete path')
        absorbed.update(r['seg_id'] for r in execution if r['seg_id'] in ids)
        for n in ids:owner[n]=region['member_segment_id']
    final=[r for r in execution if r['seg_id'] not in absorbed]+candidates
    projection={r['seg_id']:[c['seg_id'] for c in candidates if r['seg_id'] in c['source_structural_nodes']] if r['seg_id'] in absorbed else [r['seg_id']] for r in execution}
    final_edges=[];internal=[]
    for e in _project(rows,struct_dag):
        if e['src'] in absorbed and e['dst'] in absorbed and owner[e['src']]==owner[e['dst']]:
            internal.append(e);continue
        for a in projection[e['src']]:
            for b in projection[e['dst']]:final_edges.append(dict(e,src=a,dst=b))
    unique={digest(e):e for e in final_edges}
    sched_dag=dict(base_name=base_name,nodes=[r['seg_id'] for r in final],edges=list(unique.values()),exclusive_groups=groups,removed_internal_edges=internal)
    topo_sort(sched_dag['nodes'],[(e['src'],e['dst']) for e in sched_dag['edges']])
    oldmap={b['seg_id']:[r['seg_id'] for r in final if b['seg_id'] in r['source_segments']] for b in bases}
    positions={};physical={};gen={};by_context={}
    for r in final:
        for key in r['source_position_ids']:positions.setdefault(key,[]).append(r['seg_id'])
        source_ranges=r.get('source_ranges')
        if source_ranges is None:
            source_ranges=[dict(function=r['function'],source_function=r['source_function'],execution_context=r['execution_context'],start_line=r['start_line'],end_line=r['end_line'])]
        for item in source_ranges:
            for line in range(item['start_line'],item['end_line']+1):
                physical.setdefault('LOOPSRC:'+item['source_function']+':line:'+str(line),[]).append(r['seg_id'])
        for n in r['generation_origin'] or []:gen.setdefault(n,[]).append(r['seg_id'])
    for r in execution:
        for n in r['generation_origin'] or []:by_context.setdefault(r['function'],{}).setdefault(n,[]).extend(projection[r['seg_id']])
    for n,sid in raw.get('node_to_segment',{}).items():
        if n not in raw.get('node_locations',{}) and not is_ignored_compiler_fn(n):gen[n]=oldmap[sid]
    mappings=dict(original_segment_to_execution_segments=oldmap,
                  source_position_to_execution_segments={k:_ordered_unique(v) for k,v in positions.items()},
                  source_position_to_execution_segments_by_source={k:_ordered_unique(v) for k,v in physical.items()},
                  generation_node_to_execution_segments={k:_ordered_unique(v) for k,v in gen.items()},
                  generation_node_to_execution_segments_by_instance={fn:{k:_ordered_unique(v) for k,v in table.items()} for fn,table in by_context.items()},
                  structural_node_to_execution_segments={r['seg_id']:[c['seg_id'] for c in final if r['seg_id'] in c.get('source_structural_nodes',[c['seg_id']])] for r in rows},
                  helper_call_contexts=call_contexts,
                  grouped_node_to_member_segments={c['seg_id']:c['member_segment_ids'] for c in candidates},
                  grouped_node_to_mutex_and_source={c['seg_id']:c['lock_group'] for c in candidates})
    common=dict(base_name=base_name,level='level2',rule_name='composition_c2b',source_file=raw['source_file'],
                source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),composition_input_sha256=digest([raw,dag]),
                condition_regions=conditions,lock_groups=[dict(group_id='LOCKGROUP:'+m,mutex_id=m,members=[r for r in regions if r['mutex_id']==m],candidate_segment_ids=[c['seg_id'] for c in candidates if c['mutex_id']==m]) for m in sorted({r['mutex_id'] for r in regions})],
                loops=[],templates=templates,mappings=mappings,composition_edge_provenance=ledger,
                capabilities=dict(scheduler_compatible=True,analysis_only=True,timing_compatible=False,instrumentation_compatible=False),
                basic_extension=dict(source_condition_count=len(source_analysis.regions),helper_call_contexts=len(call_contexts),max_lock_paths=MAX_LOCK_PATHS))
    return dict(copy.deepcopy(common),view='struct',segments=rows),struct_dag,dict(common,view='sched',segments=final),sched_dag,dict(rule_name='composition_c2b',level='level2',analysis_only=True,scheduler_compatible=True)
