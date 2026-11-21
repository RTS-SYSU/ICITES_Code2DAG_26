"""C2b local path candidates for one complete Condition inside a mutex region."""
import copy,hashlib,json,re
from collections import Counter
from .composition_model import CONTROL,require,_mask,_project,_ordered_unique,digest
from .condition_model import _sanitize_c,analyze_conditionals,build_condition_views,_match_pair,_skip_ws,ConditionalRegion
from .loop_nested_model import build_loop_nested_views
from ..core.func_ranges import _strip_comments_and_strings,is_ignored_compiler_fn,normalize_func_name
from .algo.common import topo_sort
from .source_syntax import mask_supported_enum_typedefs, direct_thread_calls, SourceSyntaxError

RULE='composition_c2b'
MAX_LOCK_REGIONS = 8



def _condition_region_spans(regions):
    spans=[]
    for region in regions:
        require(isinstance(region,ConditionalRegion),'C2b requires formal ConditionalRegion objects')
        require(type(getattr(region,'start_offset',None)) is int and type(getattr(region,'end_offset',None)) is int,
                'C2b ConditionalRegion requires integer offset attributes')
        spans.append((region.start_offset,region.end_offset))
    return spans



def _source_matched_regions(source_text):
    """The same local matched-region analyzer feeds dispatch and C2b modeling."""
    from ..core.func_ranges import _find_definition_signature,_scan_to_body_open,_match_brace_block
    clean=_sanitize_c(source_text);lines=clean.splitlines();line=lambda p:clean.count('\n',0,p)+1
    try:
        creates=direct_thread_calls(source_text,'create')
    except SourceSyntaxError:
        require(False,'C2b requires unique direct thread entries')
    entries=[call.entry for call in creates]
    require(entries or not re.search(r'\bpthread_create\b',clean), 'C2b requires direct thread entries')
    require(len({call.handle for call in creates})==len(creates),'C2b requires distinct create handles')
    ranges={}
    # Match each source function once; execution instances come from the actual
    # generator metadata in build_lock_path_views, never from guessed names.
    for fn in dict.fromkeys(entries+['main']):
        cursor=0;definitions=[]
        while True:
            found=_find_definition_signature(clean,fn,cursor)
            if found is None:break
            a,paren=found;opening=_scan_to_body_open(clean,paren);cursor=paren+1
            if opening is None:continue
            closing=_match_brace_block(clean,opening)
            require(closing is not None,'C2b incomplete thread function')
            definitions.append((line(a),line(closing)))
        require(len(definitions)==1,'C2b ambiguous thread function: '+fn)
        ranges[fn]=definitions[0]
    # main is an execution context too; helpers may not own synchronization.
    from .lock_regions import match_lock_regions,LockRegionError
    try:
        regions=match_lock_regions(source_text,ranges)
    except LockRegionError as exc:
        message=str(exc)
        if message in ('lock must belong to one formal function','lock call has no formal function owner'):message='lock must belong to one direct thread function'
        require(False,'C2b '+message)
    compatible=[]
    for region in regions:
        original={}
        for key,value in region.items():
            if key=='critical_range':continue
            original[key]=value
            if key=='function':original['thread_entry_function']=value
        compatible.append(original)
    return ranges,compatible


def build_lock_path_views(*,base_name,source_text,base_segments_json,base_dag_json,_allow_basic_extension=True):
    raw=copy.deepcopy(base_segments_json);dag=copy.deepcopy(base_dag_json);bases=raw['segments'];by_id={r['seg_id']:r for r in bases}
    require(len(by_id)==len(bases) and set(by_id)==set(dag['nodes']),'C2b invalid raw node identity')
    topo_sort(dag['nodes'],[(e['src'],e['dst']) for e in dag['edges']])
    clean=_sanitize_c(source_text);lines=clean.splitlines();directive=_strip_comments_and_strings(source_text)
    require(not re.search(r'^\s*#\s*(?!include\b)\w+',directive,re.M),'C2b rejects active directives')
    require(not re.search(r'\b(?:typedef|sem_\w+|pthread_mutexattr\w*|pthread_rwlock\w*)\b',mask_supported_enum_typedefs(clean)),'C2b rejects aliases and unsupported synchronization')
    from .composition_c2b_basic_model import needs_basic_extension, build_basic_views
    if _allow_basic_extension and needs_basic_extension(source_text):
        return build_basic_views(base_name=base_name,source_text=source_text,
                                 base_segments_json=raw,base_dag_json=dag)
    ranges={}
    for r in bases:
        a,b=ranges.get(r['function'],(r['start_line'],r['end_line']));ranges[r['function']]=(min(a,r['start_line']),max(b,r['end_line']))
    analysis=analyze_conditionals(source_text,ranges)
    _condition_region_spans(analysis.regions)
    roots=[r for r in analysis.regions if r.parent_id is None]
    require(not analysis.diagnostics and len(roots)==1 and 1<=len(analysis.regions)<=2 and len(analysis.regions)==len(re.findall(r'\bif\b',clean)),'C2b requires one bounded Condition tree, not sequential Conditions')
    root=roots[0];line=lambda p:clean.count('\n',0,p)+1
    declarations=list(re.finditer(r'^\s*(?:static\s+)?pthread_mutex_t\s+(\w+)\s*=\s*PTHREAD_MUTEX_INITIALIZER\s*;',clean,re.M));mutexes=[m.group(1) for m in declarations]
    require(1<=len(mutexes)<=2 and len(set(mutexes))==len(mutexes) and len(mutexes)==len(re.findall(r'\bpthread_mutex_t\b',clean)),'C2b requires direct statically initialized mutexes')
    _,source_regions=_source_matched_regions(source_text);remainder=clean
    creates=direct_thread_calls(source_text,'create')
    create_counts=Counter(call.entry for call in creates)
    repeated={fn:count for fn,count in create_counts.items() if count>1}
    if repeated:
        try:joins=direct_thread_calls(source_text,'join')
        except SourceSyntaxError:
            require(False,'C2b repeated workers require direct joins with NULL/0 result pointers')
        require(Counter(call.handle for call in joins)==Counter(call.handle for call in creates),
                'C2b repeated workers require one direct join per create handle')
        require(not re.search(r'\b(?:for|while|do)\b',clean),
                'C2b repeated worker entries with Loop are unsupported')
        # A repeated source entry must have every execution instance, with one
        # distinct create carrier per instance. Do not silently reuse one row.
        for fn,count in repeated.items():
            instances={name for name in ranges if normalize_func_name(name)==fn}
            require(len(instances)==count and instances<=set(raw.get('thread_entry_functions',[])),
                    'C2b repeated worker execution instances do not match creates')
            created=Counter(by_id[e['dst']]['function'] for e in dag['edges']
                            if e['kind']=='create' and by_id[e['dst']]['function'] in instances)
            require(created==Counter({name:1 for name in instances}),
                    'C2b repeated worker create edges do not match execution instances')
        from .lock_regions import match_lock_regions,LockRegionError
        try:regions=match_lock_regions(source_text,ranges)
        except LockRegionError as exc:require(False,'C2b '+str(exc))
        for matched in regions:
            matched.pop('critical_range',None)
            matched['thread_entry_function']=normalize_func_name(matched['function'])
    else:
        regions=source_regions
    # Exactly one region owns the bounded Condition tree below. Other complete
    # straight-line regions keep their original vertices/edges and mutex identity;
    # their number does not increase the number of branch path candidates.
    require(1<=len(regions)<=MAX_LOCK_REGIONS,'C2b requires paired locks and at most eight complete regions')
    for matched in regions:
        fn=matched['function'];start=matched['lock_range'][0];n=matched['unlock_range'][0]
        require(fn in raw.get('thread_entry_functions',[]),'C2b worker not confirmed by base thread metadata')
        owners=[r for r in bases if r['function']==fn and r['kind']=='mutex_cs' and r['start_line']==start and r['end_line']==n]
        require(len(owners)==1,'C2b critical region must be one complete old mutex segment')
        matched['member_segment_id']=owners[0]['seg_id']
    for m in reversed(declarations):remainder=remainder[:m.start()]+(' '*(m.end()-m.start()))+remainder[m.end():]
    remainder=re.sub(r'pthread_mutex_(?:lock|unlock)\s*\(\s*&\s*\w+\s*\)\s*;','',remainder)
    require(not any(re.search(r'\b'+re.escape(m)+r'\b',remainder) for m in mutexes),'C2b mutex aliases or extra uses unsupported')
    target=[r for r in regions if normalize_func_name(r['function'])==normalize_func_name(root.function)
            and r['lock_range'][0]<root.entry_line and root.exit_line<r['unlock_range'][0]]
    expected_targets=create_counts[normalize_func_name(root.function)] if repeated else 1
    require(expected_targets>=1 and len(target)==expected_targets and len({r['function'] for r in target})==expected_targets,
            'C2b Condition must stay inside one matched lock/unlock per execution instance')
    for r in regions:
        body='\n'.join(lines[r['lock_range'][0]:r['unlock_range'][0]-1])
        require(not re.search(r'\b(?:for|while|do|switch|return|goto|break|continue|pthread_\w+|sem_\w+)\b',body),'C2b rejects Loop, synchronization or control transfer inside lock')
    cs,cd,ce,_,cm=build_condition_views(base_name=base_name,source_text=source_text,base_segments_json=raw,base_dag_json=dag)
    require(cm['scheduler_compatible'],'C2b incompatible Condition: '+str(cm['blocked_reasons']))
    membership={r['seg_id']:r.get('condition_memberships',[]) for r in ce['segments']}
    for r in cs['segments']:
        if membership.get(r['seg_id']):r['condition_memberships']=copy.deepcopy(membership[r['seg_id']])
    # Attach the complete original dependency witnesses before any projection.
    ledger=[]
    for i,e in enumerate(dag['edges']):
        def ports(sid):
            b=by_id[sid];return sorted([r for r in cs['segments'] if r['function']==b['function'] and max(r['start_line'],b['start_line'])<=min(r['end_line'],b['end_line'])],key=lambda r:(r['start_line'],0 if r['kind']=='condition_entry' else 2 if r['kind']=='condition_exit' else 1,r['end_line']))
        left,right=ports(e['src']),ports(e['dst']);require(left and right,'C2b cannot trace raw edge endpoints')
        matches=[x for x in cd['edges'] if x['src']==left[-1]['seg_id'] and x['dst']==right[0]['seg_id'] and x['kind']==e['kind']]
        require(matches or e['kind']=='intra','C2b cannot preserve external dependency')
        for x in matches:
            for key,value in e.items():
                if key not in ('src','dst','kind'):x.setdefault(key,copy.deepcopy(value))
            x.setdefault('provenance',[]).append(dict(base_edge_index=i,edge=copy.deepcopy(e)))
        ledger.append(dict(base_edge_index=i,edge=copy.deepcopy(e),disposition='projected' if matches else 'condition_lexical_replacement'))
    spans=[]
    for m in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,m.end()),'(',')');end=_match_pair(clean,_skip_ws(clean,close+1),'{','}')+1
        require(not any(max(line(m.start()),r['lock_range'][0])<=min(line(end-1),r['unlock_range'][0]) for r in regions),'C2b Loop overlaps lock')
        spans.append((m.start(),end))
        owner=[r for r in cs['segments'] if not r.get('synthetic') and r['start_line']<=line(m.start())<=r['end_line']]
        require(len(owner)==1,'C2b Loop owner unclear');owner[0]['end_line']=max(owner[0]['end_line'],line(end-1))
    if spans:
        offsets=[0]+[m.end() for m in re.finditer('\n',source_text)]
        masks=_condition_region_spans(roots)
        masks += [(offsets[i],offsets[i+1] if i+1<len(offsets) else len(source_text)) for i,l in enumerate(lines) if re.search(r'\bpthread_\w+\b',l)]
        loop_source=_mask(source_text,masks);payload=copy.deepcopy(raw);payload['segments']=cs['segments']
        st,sd,_,_,_=build_loop_nested_views(base_name=base_name,source_text=loop_source,base_segments_json=payload,base_dag_json=cd)
    else:st,sd=cs,cd;st.update(loops=[],templates=[])
    rows=st['segments'];execution=[r for r in rows if r['kind'] not in CONTROL]
    templates={t['template_id']:copy.deepcopy(t) for t in st.get('templates',[])}
    for r in execution:
        r['source_segments']=[b['seg_id'] for b in bases if b['function']==r['function'] and max(b['start_line'],r['start_line'])<=min(b['end_line'],r['end_line'])]
        effective=[i for i in range(r['start_line'],r['end_line']+1) if lines[i-1].strip() and lines[i-1].strip() not in ('{','}')]
        r['source_position_ids']=['LOOPSRC:'+r['function']+':line:'+str(i) for i in effective]
        r['generation_origin']=[n for n,l in raw.get('node_locations',{}).items() if not is_ignored_compiler_fn(n)
                                and (l['function']==r['function'] or
                                     (normalize_func_name(r['function']) in repeated and normalize_func_name(l['function'])==normalize_func_name(r['function'])))
                                and r['start_line']<=l['line']<=r['end_line']] or None
        ordinary=[i for i in effective if not re.search(r'\bpthread_mutex_(?:lock|unlock)\b',lines[i-1])]
        inside=[region for region in target if r['function']==region['function'] and region['lock_range'][0]<=r['start_line']<=r['end_line']<=region['unlock_range'][0]]
        require(len(inside)<=1,'C2b ambiguous mutex execution region')
        if inside:r['mutex_id']=inside[0]['mutex_id'];r['lock_region']=inside[0]['member_segment_id']
        if not ordinary and inside:r['template_id']=None;continue
        tid=r.get('template_id') or 'C2BTPL:'+r['seg_id'];r['template_id']=tid
        templates.setdefault(tid,dict(template_id=tid,function=r['function'],start_line=min(ordinary or effective),end_line=max(ordinary or effective),role='execution'))
    candidates=[];absorbed=set();local_owner={};by_struct={r['seg_id']:r for r in rows}
    for region in target:
        local=[r for r in rows if r['function']==region['function'] and region['lock_range'][0]<=r['start_line']<=r['end_line']<=region['unlock_range'][0]]
        start=[r for r in local if r['start_line']==region['lock_range'][0]];end=[r for r in local if r['end_line']==region['unlock_range'][0]]
        require(len(start)==len(end)==1,'C2b missing mutex boundary port')
        local_ids={r['seg_id'] for r in local};adj={n:[] for n in local_ids}
        for e in sd['edges']:
            if e['src'] in local_ids and e['dst'] in local_ids:adj[e['src']].append(e)
        pending=[(start[0]['seg_id'],[],[])];leaves=set();first_candidate=len(candidates)
        group_id='LOCKGROUP:'+region['mutex_id']
        while pending:
            node,path,choices=pending.pop();path=path+[node]
            if node!=end[0]['seg_id']:
                for e in reversed(adj[node]):
                    choice=[{k:e[k] for k in ('exclusive_group','branch_id','guard')}] if e['kind']=='condition_branch' else []
                    pending.append((e['dst'],path,choices+choice))
                continue
            key='|'.join(m['exclusive_group']+'='+m['branch_id'] for m in choices);require(choices and key not in leaves,'C2b duplicate or missing leaf identity');leaves.add(key)
            parts=[by_struct[n] for n in path if by_struct[n]['kind'] not in CONTROL];weighted=[r for r in parts if r.get('template_id')]
            candidates.append(dict(seg_id='LOCKPATH:'+region['member_segment_id']+'@'+key,kind='lock_path_candidate',function=region['function'],start_line=region['lock_range'][0],end_line=region['unlock_range'][0],path_id=key,condition_memberships=choices,mutex_id=region['mutex_id'],lock_group=dict(group_id=group_id,mutex_id=region['mutex_id'],source_region=copy.deepcopy(region)),member_segment_ids=[r['seg_id'] for r in weighted],member_template_ids=[r['template_id'] for r in weighted],source_structural_nodes=path,source_segments=_ordered_unique(s for r in parts for s in r['source_segments']),source_position_ids=_ordered_unique(s for r in parts for s in r['source_position_ids']),generation_origin=_ordered_unique(n for r in parts for n in (r['generation_origin'] or [])) or None))
        require(2<=len(candidates)-first_candidate<=3,'C2b unsupported Condition leaf count')
        for sid in local_ids:
            require(sid not in local_owner,'C2b overlapping mutex execution regions')
            local_owner[sid]=region['member_segment_id']
        absorbed.update(r['seg_id'] for r in execution if r['seg_id'] in local_ids)
    final=[copy.deepcopy(r) for r in execution if r['seg_id'] not in absorbed]+candidates
    projection={r['seg_id']:[c['seg_id'] for c in candidates if r['seg_id'] in c['source_structural_nodes']] if r['seg_id'] in absorbed else [r['seg_id']] for r in execution}
    edges={};internal=[]
    for i,e in enumerate(_project(rows,sd)):
        witness=dict(base_edge_index=i,edge=copy.deepcopy(e))
        if e['src'] in absorbed and e['dst'] in absorbed and local_owner[e['src']]==local_owner[e['dst']]:internal.append(witness);continue
        for a in projection[e['src']]:
            for b in projection[e['dst']]:
                out=copy.deepcopy(e);out.update(src=a,dst=b);key=json.dumps(out,sort_keys=True)
                if key not in edges:edges[key]=dict(out,provenance=[])
                edges[key]['provenance'].append(witness)
    dd=dict(base_name=base_name,nodes=[r['seg_id'] for r in final],edges=list(edges.values()),removed_internal_edges=internal,exclusive_groups=copy.deepcopy(cd['exclusive_groups']))
    topo_sort(dd['nodes'],[(e['src'],e['dst']) for e in dd['edges']])
    oldmap={b['seg_id']:_ordered_unique(r['seg_id'] for r in final if b['seg_id'] in r['source_segments']) for b in bases}
    gen={n:(_ordered_unique(r['seg_id'] for r in final if n in (r['generation_origin'] or [])) if n in raw.get('node_locations',{}) else oldmap[sid].copy()) for n,sid in raw.get('node_to_segment',{}).items() if not is_ignored_compiler_fn(n)}
    positions={}
    for key,targets in raw.get('mappings',{}).get('source_position_to_execution_segments',{}).items():
        positions[key]=_ordered_unique(r['seg_id'] for r in final if any(s in r['source_segments'] for s in targets) and (not key.startswith('LOOPSRC:') or key in r['source_position_ids']))
    for r in final:
        for key in r['source_position_ids']:positions.setdefault(key,[]).append(r['seg_id'])
    for loop in st.get('loops',[]):
        for a,b in loop.get('statements',[]):
            for i in range(a,b+1):positions.setdefault('LOOPSRC:'+loop['function']+':line:'+str(i),[])
    mappings=copy.deepcopy(raw.get('mappings',{}));mappings.update(generation_node_to_execution_segments=gen,original_segment_to_execution_segments=oldmap,source_position_to_execution_segments={k:_ordered_unique(v) for k,v in positions.items()},structural_node_to_execution_segments=projection,grouped_node_to_member_segments={c['seg_id']:c['member_segment_ids'] for c in candidates},grouped_node_to_mutex_and_source={c['seg_id']:c['lock_group'] for c in candidates})
    if repeated:
        # Legacy generation node names are shared by clones. Keep their aggregate
        # one-to-many map and also publish an unambiguous map per execution.
        mappings['generation_node_to_execution_segments_by_instance']={
            fn:{node:_ordered_unique(r['seg_id'] for r in final if r['function']==fn and node in (r['generation_origin'] or []))
                for node,loc in raw.get('node_locations',{}).items()
                if not is_ignored_compiler_fn(node) and normalize_func_name(loc['function'])==normalize_func_name(fn)}
            for fn in sorted(ranges) if normalize_func_name(fn) in repeated}
    common=dict(base_name=base_name,rule_name=RULE,level='level2',source_file=raw['source_file'],source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),condition_regions=cs['condition_regions'],loops=st.get('loops',[]),templates=list(templates.values()),lock_groups=[dict(group_id='LOCKGROUP:'+m,mutex_id=m,members=[copy.deepcopy(r) for r in regions if r['mutex_id']==m],candidate_segment_ids=[c['seg_id'] for c in candidates if c['mutex_id']==m]) for m in mutexes],mappings=mappings,composition_edge_provenance=ledger,composition_input_sha256=digest([raw,dag]),capabilities=dict(scheduler_compatible=True,timing_compatible=False,instrumentation_compatible=False,analysis_only=True))
    sd['exclusive_groups']=copy.deepcopy(cd['exclusive_groups'])
    return dict(copy.deepcopy(common),view='struct',segments=rows),sd,dict(common,view='sched',segments=final),dd,dict(rule_name=RULE,level='level2',scheduler_compatible=True,analysis_only=True)
