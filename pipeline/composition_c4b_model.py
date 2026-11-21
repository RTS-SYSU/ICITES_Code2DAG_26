"""C4b: a static Loop wholly inside one matched lock/unlock critical region."""
import copy,hashlib,json,re
from .composition_model import CONTROL,require,_mask,_ordered_unique,digest
from .condition_model import _sanitize_c,_match_pair,_skip_ws,analyze_conditionals,build_condition_views
from .lock_regions import match_lock_regions,LockRegionError
from .loop_nested_model import build_loop_nested_views
from .composition_c4a_model import (_formal_function_spans,_condition_function_ranges,
    _helper_condition_views,_restore_condition_dependencies)
from ..core.func_ranges import _strip_comments_and_strings,is_ignored_compiler_fn
from .source_syntax import mask_supported_enum_typedefs
from .algo.common import topo_sort
RULE='composition_c4b'


def _syntactic_loops(clean):
    spans=[]
    for match in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,match.end()),'(',')')
        brace=_skip_ws(clean,close+1)
        if brace>=len(clean) or clean[brace]!='{':continue
        spans.append((match.start(),_match_pair(clean,brace,'{','}')))
    return spans


def loop_inside_lock_overlap(text):
    """Route a complete Loop enclosed by one direct lock/unlock pair to C4b."""
    clean=_sanitize_c(text);stack=[];regions=[]
    for match in re.finditer(r'\bpthread_mutex_(lock|unlock)\s*\(\s*&\s*\w+\s*\)\s*;',clean):
        if match.group(1)=='lock':stack.append(match.start())
        elif stack:regions.append((stack.pop(),match.end()))
    loops=_syntactic_loops(clean)
    return any(a<loop_start and loop_end<b for a,b in regions for loop_start,loop_end in loops)


def _direct_mutexes(clean):
    declarations=list(re.finditer(r'^\s*(?:static\s+)?pthread_mutex_t\s+(\w+)\s*=\s*PTHREAD_MUTEX_INITIALIZER\s*;',clean,re.M))
    names=[m.group(1) for m in declarations]
    require(names and len(set(names))==len(names) and len(names)==len(re.findall(r'\bpthread_mutex_t\b',clean)),
            'C4b requires direct statically initialized mutex declarations')
    remainder=_mask(clean,[(m.start(),m.end()) for m in declarations])
    remainder=re.sub(r'pthread_mutex_(?:lock|unlock)\s*\(\s*&\s*\w+\s*\)\s*;','',remainder)
    require(not any(re.search(r'\b'+re.escape(name)+r'\b',remainder) for name in names),'C4b rejects mutex aliases or extra uses')
    return names


def _loop_spans(source_text,ranges):
    clean=_sanitize_c(source_text);lines=clean.splitlines();line=lambda p:clean.count('\n',0,p)+1
    require(not re.search(r'\b(?:while|do)\b',clean),'C4b supports only static for Loops')
    spans=[]
    for match in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,match.end()),'(',')');brace=_skip_ws(clean,close+1)
        require(brace<len(clean) and clean[brace]=='{','C4b requires braced static Loops')
        end=_match_pair(clean,brace,'{','}')
        owners=[fn for fn,(a,b) in ranges.items() if a<=line(match.start())<=line(end)<=b]
        require(len(owners)==1,'C4b Loop has no formal function owner')
        spans.append(dict(start=match.start(),brace=brace,end=end,header=line(match.start()),exit=line(end),function=owners[0]))
    require(1<=len(spans)<=2,'C4b supports only Loop L1/L2/L3')
    return spans


def _loops_in_regions(loops,regions):
    for loop in loops:
        containing=[r for r in regions if r['function']==loop['function'] and r['start_offset']<loop['start'] and loop['end']<r['end_offset']]
        require(len(containing)==1,'C4b Loop must stay completely inside one matched lock/unlock')
        for region in regions:
            overlap=max(region['start_offset'],loop['start'])<=min(region['end_offset'],loop['end'])
            if region is not containing[0]:
                require(not overlap,'C4b rejects partial Loop/lock boundary overlap')
            require(not (loop['start']<=region['start_offset']<=loop['end'] or loop['start']<=region['end_offset']<=loop['end']),
                    'C4b Lock inside Loop belongs to C4a')
        loop['region']=containing[0]
    return loops


def _region_owner_segments(regions,bases):
    for region in regions:
        lock,unlock=region['lock_range'][0],region['unlock_range'][0]
        owners=[row for row in bases if row['function']==region['function'] and row['kind']=='mutex_cs'
                and row['start_line']<=lock and unlock<=row['end_line']]
        require(len(owners)==1,'C4b critical region must be owned by one complete old mutex segment')
        region['owner_segment_id']=owners[0]['seg_id']
        region['original_segment_id']=owners[0]['seg_id']
        region['member_segment_id']=owners[0]['seg_id']
        region['region_id']=region['function']+'@'+str(lock)
    return regions


def _split_owner_segments(regions,bases,dag,node_to_segment,node_locations):
    """Split Level-2's absorbed post/create/wait/join tails away from the critical window."""
    by_owner={region['owner_segment_id']:region for region in regions}
    work=[];split_map={};parts_by_old={}
    for base in bases:
        region=by_owner.get(base['seg_id'])
        if region is None:work.append(copy.deepcopy(base));continue
        lock,unlock=region['lock_range'][0],region['unlock_range'][0]
        if base['start_line']==lock and base['end_line']==unlock:
            parts=[copy.deepcopy(base)];region['member_segment_id']=base['seg_id'];work.append(parts[0]);continue
        parts=[]
        if base['start_line']<lock:
            parts.append(dict(seg_id='C4BSPLIT:'+base['seg_id']+':pre',function=base['function'],kind='compute',
                              start_line=base['start_line'],end_line=lock-1,split_source=base['seg_id']))
        critical=dict(seg_id='C4BSPLIT:'+base['seg_id']+':crit',function=base['function'],kind='mutex_cs',
                      start_line=lock,end_line=unlock,split_source=base['seg_id'])
        parts.append(critical)
        if unlock<base['end_line']:
            parts.append(dict(seg_id='C4BSPLIT:'+base['seg_id']+':post',function=base['function'],kind='compute',
                              start_line=unlock+1,end_line=base['end_line'],split_source=base['seg_id']))
        split_map[base['seg_id']]=parts;parts_by_old[base['seg_id']]=parts
        region['member_segment_id']=critical['seg_id']
        work.extend(parts)
    nodes=[]
    for base in bases:
        if base['seg_id'] in parts_by_old:nodes.extend(part['seg_id'] for part in parts_by_old[base['seg_id']])
        else:nodes.append(base['seg_id'])
    edges=[]
    for edge in dag['edges']:
        src=edge['src'];dst=edge['dst']
        if src in split_map:src=split_map[src][-1]['seg_id']
        if dst in split_map:dst=split_map[dst][0]['seg_id']
        out=copy.deepcopy(edge);out.update(src=src,dst=dst);edges.append(out)
    for old,parts in parts_by_old.items():
        for left,right in zip(parts,parts[1:]):
            edges.append(dict(src=left['seg_id'],dst=right['seg_id'],kind='intra',source='c4b_owner_split'))
    mapped_nodes={}
    for node,sid in node_to_segment.items():
        if sid in split_map:
            line=node_locations.get(node,{}).get('line');parts=split_map[sid]
            owner=next((part for part in parts if isinstance(line,int) and part['start_line']<=line<=part['end_line']),parts[0])
            mapped_nodes[node]=owner['seg_id']
        else:mapped_nodes[node]=sid
    result=dict(base_name=dag.get('base_name'),nodes=nodes,edges=edges)
    return work,result,split_map,mapped_nodes


def _condition_regions(source_text,ranges):
    analyses=[analyze_conditionals(source_text,{fn:bounds}) for fn,bounds in ranges.items()]
    require(not any(analysis.diagnostics for analysis in analyses),'C4b invalid Condition')
    return [region for analysis in analyses for region in analysis.regions]


def _reject_conditions_in_combination(conditions,regions,loops):
    for condition in conditions:
        for region in regions:
            require(not (max(condition.start_offset,region['start_offset'])<=min(condition.end_offset,region['end_offset'])),
                    'C4b Condition inside Lock is unsupported')
        for loop in loops:
            require(not (max(condition.start_offset,loop['start'])<=min(condition.end_offset,loop['end'])),
                    'C4b Condition inside Loop is unsupported')


def _reject_sync_inside(clean_lines,regions,loops):
    banned=r'\b(?:pthread_create|pthread_join|sem_\w+|if|else|switch|return|goto|break|continue)\b'
    for region in regions:
        body='\n'.join(clean_lines[region['lock_range'][0]:region['unlock_range'][0]-1])
        require(not re.search(banned,body),'C4b unsupported control/synchronization inside Lock')
    for loop in loops:
        body='\n'.join(clean_lines[loop['header']-1:loop['exit']])
        require(not re.search(r'\b(?:pthread_create|pthread_join|sem_\w+)\b',body),'C4b unsupported synchronization inside Loop')


def _annotate_edges(dag):
    result=copy.deepcopy(dag);edges=[]
    for index,edge in enumerate(dag['edges']):
        annotated=copy.deepcopy(edge)
        annotated.setdefault('provenance',[]).append(dict(base_edge_index=index,edge=copy.deepcopy(edge)))
        edges.append(annotated)
    result['edges']=edges
    return result


def _row_region(row,regions):
    matches=[r for r in regions if row.get('function')==r['function']
             and r['lock_range'][0]<=row['start_line'] and row['end_line']<=r['unlock_range'][0]]
    require(len(matches)<=1,'C4b node belongs to multiple critical regions')
    if not matches:
        for region in regions:
            overlap=max(region['lock_range'][0],row['start_line'])<=min(region['unlock_range'][0],row['end_line'])
            require(not overlap,'C4b node crosses a Lock boundary')
        return None
    for region in regions:
        if region is matches[0]:continue
        overlap=max(region['lock_range'][0],row['start_line'])<=min(region['unlock_range'][0],row['end_line'])
        require(not overlap,'C4b node crosses a Lock boundary')
    return matches[0]


def _loop_masks(source_text,clean_lines,conditions,loops,current):
    line_offsets=[];cursor=0
    for line in source_text.splitlines(keepends=True):
        line_offsets.append(cursor);cursor+=len(line)
    spans=[]
    for index,line in enumerate(clean_lines):
        if re.search(r'\b(?:pthread_|sem_)\w*\b',line):
            start=line_offsets[index];end=start+len(clean_lines[index])
            spans.append((start,end))
    spans.extend((region.start_offset,region.end_offset) for region in conditions)
    spans.extend((loop['start'],loop['end']+1) for loop in loops if loop not in current)
    return spans


def _is_internal_witness(item):
    """Formal internal witnesses are identified by the base-edge index field."""
    return isinstance(item,dict) and 'base_edge_index' in item


def _validated_provenance(edge,context=''):
    """Opaque base provenance records stay opaque; formal witnesses must be well formed."""
    provenance=edge.get('provenance',[])
    require(isinstance(provenance,list),'C4b provenance must be a list'+context)
    for item in provenance:
        if not _is_internal_witness(item):continue
        require(type(item['base_edge_index']) is int,'C4b malformed internal dependency witness: base_edge_index'+context)
        require(isinstance(item.get('edge'),dict),'C4b malformed internal dependency witness: edge'+context)
        require(all(key in item['edge'] for key in ('src','dst','kind')),'C4b malformed internal dependency witness: edge fields'+context)
    return provenance


def _dedupe_edges(edges):
    result=[];seen={}
    for edge in edges:
        _validated_provenance(edge,' in projected edge')
        key=json.dumps({k:v for k,v in edge.items() if k!='provenance'},sort_keys=True)
        if key not in seen:
            seen[key]=copy.deepcopy(edge);result.append(seen[key]);continue
        existing=seen[key]
        chain=existing.setdefault('provenance',[])
        for item in edge.get('provenance',[]):
            chain.append(copy.deepcopy(item))
    return result


def _contract_and_project(rows,graph,node_group,group_ids):
    for edge in graph['edges']:_validated_provenance(edge,' in dependency graph')
    contracted={}
    for row in rows:
        target=node_group.get(row['seg_id'],row['seg_id'])
        if target not in contracted:contracted[target]=None
    for group_id in group_ids:contracted.setdefault(group_id,None)
    nodes=list(contracted)
    edges=[];removed=[]
    for edge in graph['edges']:
        src=node_group.get(edge['src'],edge['src']);dst=node_group.get(edge['dst'],edge['dst'])
        if src==dst:
            if src in group_ids:removed.append(dict(edge=copy.deepcopy(edge)));continue
            require(False,'C4b contraction creates a self dependency')
        out=copy.deepcopy(edge);out.update(src=src,dst=dst);edges.append(out)
    edges=_dedupe_edges(edges)
    outside=[row['seg_id'] for row in rows if row['kind'] not in CONTROL and not row.get('source_only') and row['seg_id'] not in node_group]
    sched_nodes=_ordered_unique(outside+sorted(group_ids))
    active=set(sched_nodes)
    adj={node:[] for node in nodes}
    for edge in edges:adj[edge['src']].append(edge)
    projected=[]
    for node in nodes:
        if node not in active:continue
        pending=[(edge['dst'],[edge],()) for edge in adj[node]]
        seen=set()
        while pending:
            target,path,transitions=pending.pop()
            state=(target,tuple(id(edge) for edge in path))
            if state in seen:continue
            seen.add(state)
            next_transitions=transitions
            for edge in path:
                if edge.get('kind') in ('condition_branch','condition_merge'):
                    next_transitions=next_transitions+((
                        edge.get('kind'),edge.get('exclusive_group',''),edge.get('branch_id',''),edge.get('guard','')),)
            if target not in active:
                for edge in adj.get(target,[]):pending.append((edge['dst'],path+[edge],next_transitions))
                continue
            witnesses=[item for edge in path for item in _validated_provenance(edge) if _is_internal_witness(item)]
            if witnesses:
                chain=[]
                for edge in path:
                    for item in _validated_provenance(edge):chain.append(copy.deepcopy(item))
                for witness in witnesses:
                    out=copy.deepcopy(witness['edge']);out.update(src=node,dst=target)
                    out['provenance']=copy.deepcopy(chain)
                    if next_transitions:
                        kind,group,branch,guard=next_transitions[-1]
                        out.update(kind=witness['edge'].get('kind',kind),exclusive_group=group,branch_id=branch,guard=guard)
                        if len(next_transitions)>1:out['condition_transitions']=[dict(kind=k,exclusive_group=g,branch_id=br,guard=gu) for k,g,br,gu in next_transitions]
                    projected.append(out)
            else:
                out=copy.deepcopy(path[0]);out.update(src=node,dst=target)
                if len(path)>1:out['control_projection']=copy.deepcopy(path)
                if next_transitions:
                    kind,group,branch,guard=next_transitions[-1]
                    out.update(exclusive_group=group,branch_id=branch,guard=guard)
                    if len(next_transitions)>1:out['condition_transitions']=[dict(kind=k,exclusive_group=g,branch_id=br,guard=gu) for k,g,br,gu in next_transitions]
                projected.append(out)
    projected=_dedupe_edges(projected)
    require(all(edge['src']!=edge['dst'] for edge in projected),'C4b projected graph contains a self loop')
    topo_sort(sched_nodes,[(edge['src'],edge['dst']) for edge in projected])
    return nodes,edges,projected,removed


def build_loop_in_lock_views(*,base_name,source_text,base_segments_json,base_dag_json):
    raw=copy.deepcopy(base_segments_json);dag=copy.deepcopy(base_dag_json);original_bases=copy.deepcopy(raw['segments'])
    byid={r['seg_id']:r for r in raw['segments']}
    require(len(byid)==len(raw['segments']) and set(byid)==set(dag['nodes']),'C4b raw node identity conflict')
    topo_sort(dag['nodes'],[(e['src'],e['dst']) for e in dag['edges']])
    for edge in dag['edges']:_validated_provenance(edge,' in base DAG')
    clean=_sanitize_c(source_text);lines=clean.splitlines()
    require(not re.search(r'^\s*#\s*(?!include\b)\w+',_strip_comments_and_strings(source_text),re.M),'C4b rejects active directives')
    require(not re.search(r'\b(?:typedef|pthread_mutexattr\w*|pthread_rwlock\w*)\b',mask_supported_enum_typedefs(clean)),'C4b rejects aliases and unsupported synchronization')
    formal,sources=_formal_function_spans(raw)
    ranges=_condition_function_ranges(source_text,raw['segments'],formal)
    _direct_mutexes(clean)
    try:regions=match_lock_regions(source_text,ranges)
    except LockRegionError as exc:require(False,'C4b '+str(exc))
    require(regions,'C4b missing matched Lock')
    for region in regions:
        region['thread_entry_function']=region['function'] if region['function'] in raw.get('thread_entry_functions',[]) else None
    regions=_region_owner_segments(regions,raw['segments'])
    loops=_loops_in_regions(_loop_spans(source_text,ranges),regions)
    conditions=_condition_regions(source_text,ranges)
    _reject_conditions_in_combination(conditions,regions,loops)
    _reject_sync_inside(lines,regions,loops)
    annotated=_annotate_edges(dag)
    work_bases,work_dag,split_map,work_node_to_segment=_split_owner_segments(
        regions,raw['segments'],annotated,raw.get('node_to_segment',{}),raw.get('node_locations',{}))
    bases=work_bases
    payload_raw=copy.deepcopy(raw);payload_raw['segments']=work_bases;payload_raw['node_to_segment']=work_node_to_segment
    topo_sort(work_dag['nodes'],[(edge['src'],edge['dst']) for edge in work_dag['edges']])
    condition_struct,condition_dag,condition_exec,_,meta=build_condition_views(
        base_name=base_name,source_text=source_text,base_segments_json=payload_raw,base_dag_json=work_dag)
    require(meta['scheduler_compatible'],'C4b incompatible outside Condition')
    condition_rows={r['source_structural_node']:r for r in condition_exec['segments']}
    rows=copy.deepcopy(condition_struct)['segments']
    rows=[copy.deepcopy(condition_rows[r['seg_id']]) if r['kind'] not in CONTROL else copy.deepcopy(r) for r in rows]
    cd=_restore_condition_dependencies(payload_raw,work_dag,condition_struct,condition_dag)
    graph=dict(base_name=base_name,nodes=[r['seg_id'] for r in rows],edges=copy.deepcopy(cd['edges']),
               exclusive_groups=copy.deepcopy(cd.get('exclusive_groups',{})))
    # Only the owner row of a Loop-bearing region is normalized to compute for
    # the Loop parser; the whole critical region remains one Lock group later.
    by_region={}
    for loop in loops:by_region.setdefault(loop['region']['region_id'],[]).append(loop)
    for region in regions:
        owners=[r for r in rows if r['kind'] not in CONTROL and region['member_segment_id'] in r.get('source_segments',[])
                and r['start_line']==region['lock_range'][0] and r['end_line']==region['unlock_range'][0]]
        require(len(owners)==1,'C4b missing unique critical owner row')
        if region['region_id'] in by_region:
            owners[0]['kind']='compute';owners[0]['lock_owner']=True;owners[0]['mutex_id']=region['mutex_id'];owners[0]['lock_region']=region['region_id']
    all_loops=[];all_templates=[]
    for region in sorted(regions,key=lambda r:(r['function'],r['lock_range'][0])):
        current=by_region.get(region['region_id'])
        if not current:continue
        mask=_loop_masks(source_text,lines,conditions,loops,current)
        loop_source=_mask(source_text,mask)
        payload=copy.deepcopy(raw);payload.update(segments=copy.deepcopy(rows),node_to_segment={},mappings={})
        st,sd,_,_,_=build_loop_nested_views(base_name=base_name,source_text=loop_source,base_segments_json=payload,base_dag_json=graph)
        rows=st['segments'];graph=sd
        for loop in st['loops']:loop['lock_region']=region['region_id'];loop['mutex_id']=region['mutex_id'];all_loops.append(loop)
        for template in st['templates']:all_templates.append(template)
    # Conditions wholly outside the combination keep their existing views.
    segment_functions={r['function'] for r in bases}
    helper_functions=sorted({r.function for r in conditions if r.function not in segment_functions})
    helper_views=_helper_condition_views(base_name=base_name,source_text=source_text,raw=raw,sources=sources,
                                         functions=helper_functions,prefix='C4BHELPER') if helper_functions else None
    helper_rows=[];helper_edges=[];helper_conditions=[];helper_groups={};helper_segments=[]
    helper_segment_to_struct={};helper_position_to_struct={}
    if helper_views is not None:
        helper_struct,helper_dag,helper_exec,helper_segments=helper_views
        helper_exec_by_id={r['seg_id']:r for r in helper_exec['segments']}
        helper_segment_rows=[dict(copy.deepcopy(r),source_only=True,analysis_only=True) for r in helper_segments]
        for original in helper_struct['segments']:
            row=copy.deepcopy(original);row.update(source_only=True,analysis_only=True)
            if row['kind'] not in CONTROL:
                members=helper_exec_by_id.get(row['seg_id'],{}).get('condition_memberships')
                if members:row['condition_memberships']=copy.deepcopy(members)
                fn=row['function'];row['source_position_ids']=['C4BHELPER:'+fn+':line:'+str(n) for n in range(row['start_line'],row['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
            helper_rows.append(row)
            for sid in row.get('source_segments',[]):
                if sid.startswith('C4BHELPER:'):helper_segment_to_struct.setdefault(sid,[]).append(row['seg_id'])
            for key in row.get('source_position_ids',[]):helper_position_to_struct.setdefault(key,[]).append(row['seg_id'])
        helper_edges=[copy.deepcopy(edge) for edge in helper_dag['edges']]
        helper_conditions=[copy.deepcopy(r) for r in helper_struct['condition_regions']]
        helper_groups=copy.deepcopy(helper_dag.get('exclusive_groups',{}))
    rows.extend(copy.deepcopy(helper_rows))
    graph['edges']=_dedupe_edges(list(graph['edges'])+copy.deepcopy(helper_edges))
    graph['exclusive_groups']=copy.deepcopy(cd.get('exclusive_groups',{}));graph['exclusive_groups'].update(helper_groups)
    graph['nodes']=[r['seg_id'] for r in rows]
    # Assign stable execution templates and complete source identities.
    templates={template['template_id']:copy.deepcopy(template) for template in all_templates}
    tids=set(templates)
    for row in rows:
        if row['kind'] in CONTROL or row.get('source_only'):continue
        fn=row['function']
        if 'source_segments' not in row:row['source_segments']=[b['seg_id'] for b in bases if b['function']==fn and max(b['start_line'],row['start_line'])<=min(b['end_line'],row['end_line'])]
        if 'source_position_ids' not in row:row['source_position_ids']=['LOOPSRC:'+fn+':line:'+str(n) for n in range(row['start_line'],row['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
        if 'generation_origin' not in row:row['generation_origin']=[n for n,l in raw.get('node_locations',{}).items() if not is_ignored_compiler_fn(n) and l['function']==fn and row['start_line']<=l['line']<=row['end_line']] or None
        if not row.get('template_id'):
            tid='C4BTPL:'+row['seg_id'];require(tid not in tids,'C4b duplicate template');tids.add(tid);row['template_id']=tid
            templates[tid]=dict(template_id=tid,function=fn,role='execution',start_line=row['start_line'],end_line=row['end_line'])
    topo_sort(graph['nodes'],[(edge['src'],edge['dst']) for edge in graph['edges']])
    # Group every node of a critical region, including Loop control ports, into
    # its one mutex group. This also keeps an N=0 critical region connected.
    node_region={}
    for row in rows:
        if row['kind'] in CONTROL and row.get('source_only'):continue
        region=_row_region(row,regions)
        if region is not None:node_region[row['seg_id']]=region
    groups=[];node_group={};group_rows=[]
    regions_by_mutex={}
    for region in regions:regions_by_mutex.setdefault(region['mutex_id'],[]).append(region)
    for mutex in sorted(regions_by_mutex):
        group_id='LOCKGROUP:'+mutex;group_regions=sorted(regions_by_mutex[mutex],key=lambda r:(r['function'],r['lock_range'][0]))
        member_nodes=sorted([row for row in rows if node_region.get(row['seg_id']) in group_regions],key=lambda r:(r['function'],r['start_line'],r['seg_id']))
        member_rows=[row for row in member_nodes if row['kind'] not in CONTROL and not row.get('source_only')]
        for row in member_nodes:
            node_group[row['seg_id']]=group_id
            row['mutex_id']=mutex;row['lock_group_id']=group_id;row['lock_source']=copy.deepcopy(next(r for r in group_regions if r is node_region[row['seg_id']]));row['lock_region']=node_region[row['seg_id']]['region_id']
        member_templates=_ordered_unique(row['template_id'] for row in member_rows)
        for template in templates.values():
            if template['template_id'] in member_templates:
                template['mutex_id']=mutex;template['lock_group_id']=group_id
        occurrences=[dict(segment_id=row['seg_id'],template_id=row['template_id'],loop_id=row.get('loop_id'),
                          iteration_index=row.get('iteration_index'),outer_iteration_index=row.get('outer_iteration_index'),
                          inner_iteration_index=row.get('inner_iteration_index'),source_position_ids=copy.deepcopy(row.get('source_position_ids',[])))
                     for row in member_rows]
        group=dict(group_id=group_id,seg_id=group_id,mutex_id=mutex,function='<lockgroup>',kind='lock_group',role='lock_group',
                   start_line=min(r['lock_range'][0] for r in group_regions),end_line=max(r['unlock_range'][0] for r in group_regions),
                   member_segment_ids=[row['seg_id'] for row in member_rows],member_node_ids=[row['seg_id'] for row in member_nodes],
                   member_template_ids=member_templates,members=[copy.deepcopy(row) for row in member_rows],
                   occurrences=occurrences,regions=copy.deepcopy(group_regions),
                   aggregation_method='fieldwise_sum_of_member_weights')
        groups.append(group);group_rows.append(copy.deepcopy(group))
    for loop in all_loops:
        if loop.get('lock_region'):loop['lock_group_id']='LOCKGROUP:'+loop['mutex_id']
    nodes,contracted,projected,removed=_contract_and_project(rows,graph,node_group,{g['group_id'] for g in groups})
    sched_rows=[copy.deepcopy(row) for row in rows if row['kind'] not in CONTROL and not row.get('source_only') and row['seg_id'] not in node_group]
    sched_rows.extend(group_rows)
    sched_nodes=[row['seg_id'] for row in sched_rows]
    require(len(sched_nodes)==len(set(sched_nodes)),'C4b duplicate scheduling node')
    topo_sort(sched_nodes,[(edge['src'],edge['dst']) for edge in projected])
    # Mappings: every iteration and critical ordinary row is many-to-one to its group.
    structural_to_sched={};original_to_sched={}
    for row in rows:
        if row.get('source_only'):structural_to_sched[row['seg_id']]=[];continue
        if row['seg_id'] in node_group:structural_to_sched[row['seg_id']]=[node_group[row['seg_id']]]
        elif row['kind'] not in CONTROL:structural_to_sched[row['seg_id']]=[row['seg_id']]
        else:structural_to_sched[row['seg_id']]=[]
    for base in bases:
        region=next((r for r in regions if r['function']==base['function'] and r['member_segment_id']==base['seg_id']),None)
        if region is not None:original_to_sched[base['seg_id']]=['LOCKGROUP:'+region['mutex_id']];continue
        targets=[]
        for row in rows:
            if row['kind'] in CONTROL or row.get('source_only'):continue
            if base['seg_id'] in row.get('source_segments',[]) and row['seg_id'] not in node_group:
                if row['seg_id'] not in targets:targets.append(row['seg_id'])
        original_to_sched[base['seg_id']]=_ordered_unique(targets)
    for base in original_bases:
        if base['seg_id'] in original_to_sched:continue
        targets=[]
        for part in split_map.get(base['seg_id'],[dict(seg_id=base['seg_id'])]):
            for item in original_to_sched.get(part['seg_id'],[]):
                if item not in targets:targets.append(item)
        original_to_sched[base['seg_id']]=_ordered_unique(targets)
    positions={}
    for key,targets in raw.get('mappings',{}).get('source_position_to_execution_segments',{}).items():
        mapped=[]
        for target in targets:
            items=original_to_sched.get(target) or structural_to_sched.get(target,[])
            for item in items:
                if item not in mapped:mapped.append(item)
        positions[key]=mapped
    for row in rows:
        if row['kind'] in CONTROL or row.get('source_only'):continue
        target=[node_group[row['seg_id']]] if row['seg_id'] in node_group else [row['seg_id']]
        for key in row.get('source_position_ids',[]):
            values=positions.setdefault(key,[])
            for item in target:
                if item not in values:values.append(item)
    for template in templates.values():
        if 'source_position_ids' in template:
            for key in template['source_position_ids']:positions.setdefault(key,[])
    for loop in all_loops:
        for start,end in loop.get('statements',[]):
            for line in range(start,end+1):positions.setdefault('LOOPSRC:'+loop['function']+':line:'+str(line),[])
    for row in helper_rows:
        for key in row.get('source_position_ids',[]):positions.setdefault(key,[])
    group_member_occurrences={group['group_id']:copy.deepcopy(group['occurrences']) for group in groups}
    template_targets={}
    for tid in templates:
        targets=[]
        for row in rows:
            if row['kind'] in CONTROL or row.get('source_only') or row.get('template_id')!=tid:continue
            target=node_group.get(row['seg_id'],row['seg_id'])
            if target not in targets:targets.append(target)
        template_targets[tid]=targets
    gen={}
    for node,location in raw.get('node_locations',{}).items():
        if is_ignored_compiler_fn(node):continue
        region=next((r for r in regions if r['function']==location['function'] and r['lock_range'][0]<=location['line']<=r['unlock_range'][0]),None)
        if region is not None:gen[node]=['LOCKGROUP:'+region['mutex_id']];continue
        targets=[]
        for row in rows:
            if row['kind'] in CONTROL or row.get('source_only'):continue
            if node in (row.get('generation_origin') or []):targets.append(row['seg_id'])
        gen[node]=_ordered_unique(targets)
    for node,sid in raw.get('node_to_segment',{}).items():
        if node in gen or is_ignored_compiler_fn(node):continue
        targets=original_to_sched.get(sid,[])
        gen[node]=_ordered_unique(targets)
    mappings=copy.deepcopy(raw.get('mappings',{}))
    mappings.update(generation_node_to_execution_segments=gen,original_segment_to_execution_segments=original_to_sched,
                    source_position_to_execution_segments={k:_ordered_unique(v) for k,v in positions.items()},
                    template_to_execution_segments=template_targets,structural_node_to_execution_segments=structural_to_sched,
                    grouped_node_to_member_segments={group['group_id']:group['member_segment_ids'] for group in groups},
                    grouped_node_to_member_templates={group['group_id']:group['member_template_ids'] for group in groups},
                    grouped_node_to_member_occurrences=group_member_occurrences,
                    source_lock_region_to_group={r['region_id']:'LOCKGROUP:'+r['mutex_id'] for r in regions},
                    iteration_to_group={row['seg_id']:'LOCKGROUP:'+row['mutex_id'] for row in rows if row['kind'] not in CONTROL and row.get('role')=='iteration'})
    if helper_conditions:
        mappings['source_only_segment_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_segment_to_struct.items())}
        mappings['source_position_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_position_to_struct.items())}
    condition_regions=copy.deepcopy(condition_struct['condition_regions'])
    if helper_conditions:condition_regions=sorted(condition_regions+helper_conditions,key=lambda r:(r['function'],r['entry_line'],r['exit_line'],r['condition_id']))
    iteration_instances=[dict(loop_id=row['loop_id'],iteration_index=row.get('iteration_index'),
                              outer_iteration_index=row.get('outer_iteration_index'),inner_iteration_index=row.get('inner_iteration_index'),
                              segment_id=row['seg_id'],lock_group_id=row.get('lock_group_id'),
                              template_id=row['template_id'],source_position_ids=copy.deepcopy(row.get('source_position_ids',[])))
                         for row in rows if row['kind'] not in CONTROL and row.get('role')=='iteration']
    common=dict(base_name=base_name,rule_name=RULE,level='level2',source_file=raw['source_file'],
                source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),
                thread_entry_functions=copy.deepcopy(raw.get('thread_entry_functions',[])),
                loops=all_loops,condition_regions=condition_regions,templates=sorted(templates.values(),key=lambda t:t['template_id']),
                lock_groups=groups,critical_regions=copy.deepcopy(regions),iteration_instances=iteration_instances,
                mappings=mappings,composition_edge_provenance=[dict(base_edge_index=i,edge=copy.deepcopy(e)) for i,e in enumerate(dag['edges'])],
                composition_input_sha256=digest([raw,dag]),
                capabilities=dict(scheduler_compatible=True,timing_compatible=False,instrumentation_compatible=False,analysis_only=True))
    if helper_conditions:common['source_only_segments']=copy.deepcopy(helper_segment_rows)
    struct_dag=dict(base_name=base_name,nodes=[row['seg_id'] for row in rows],edges=copy.deepcopy(graph['edges']),
                    exclusive_groups=copy.deepcopy(graph.get('exclusive_groups',{})))
    sched_dag=dict(base_name=base_name,nodes=sched_nodes,edges=projected,
                   removed_internal_edges=[dict(edge=item['edge']) for item in removed],
                   exclusive_groups=copy.deepcopy(graph.get('exclusive_groups',{})))
    topo_sort(struct_dag['nodes'],[(edge['src'],edge['dst']) for edge in struct_dag['edges']])
    return dict(copy.deepcopy(common),view='struct',segments=rows),struct_dag,dict(common,view='sched',segments=sched_rows),sched_dag,dict(rule_name=RULE,level='level2',scheduler_compatible=True,analysis_only=True)
