"""C5e: one outer matched Lock contains a Condition whose leaf branches own static Loops."""
import copy,hashlib,json,re
from .composition_model import CONTROL,require,_mask,_ordered_unique,_project,digest
from .condition_model import _sanitize_c,_match_pair,_skip_ws,analyze_conditionals
from .lock_regions import match_lock_regions,LockRegionError
from .composition_c5e_inner_model import build_branch_loop_views
from .composition_c5b_model import _normalize_cluster,_external_loop_source
from .loop_nested_model import build_loop_nested_views
from .composition_c4a_model import _formal_function_spans,_condition_function_ranges,_helper_condition_views
from .composition_c5c_model import _validate_base_provenance
from ..core.func_ranges import _strip_comments_and_strings
from .algo.common import topo_sort
RULE='composition_c5e'
_SEM_RE=re.compile(r'\bsem_(?:post|wait)\s*\(')

def _topo_sem(nodes,edges):
    topo_sort(nodes,[(e['src'],e['dst']) for e in edges if not (e.get('kind')=='sem_dep' and e.get('src')==e.get('dst'))])

def _sem_calls(clean):
    out=[]
    for m in _SEM_RE.finditer(clean):
        name=clean[m.start():m.end()].split('(',1)[0].strip()
        opening=clean.find('(',m.start(),m.end());close=_match_pair(clean,opening,'(',')')
        out.append(dict(name=name,start=m.start(),end=close+1,line=clean.count('\n',0,m.start())+1))
    return out

def _call_on_path(call,pathset,conds):
    for region in conds:
        hits=[b for b in region.branches if b.start_offset is not None and b.end_offset is not None and b.start_offset<=call['start'] and call['end']<=b.end_offset]
        require(len(hits)<=1,'C5e sem call falls in ambiguous Condition branches')
        if hits and (region.condition_id,hits[0].branch_id) not in pathset:return False
    return True



def _loops(clean):
    spans=[]
    for m in re.finditer(r'\bfor\b',clean):
        try:
            close=_match_pair(clean,_skip_ws(clean,m.end()),'(',')');brace=_skip_ws(clean,close+1);end=_match_pair(clean,brace,'{','}')
        except Exception:continue
        spans.append(dict(start=m.start(),end=end,header=clean.count('\n',0,m.start())+1,exit=clean.count('\n',0,end)+1))
    return spans


def _leaves(regions):
    result=[]
    def walk(region,path):
        for branch in region.branches:
            nxt=path+[(region,branch)]
            children=[child for child in regions if child.parent_id==region.condition_id and child.parent_branch_id==branch.branch_id]
            if children:
                for child in children:walk(child,nxt)
            else:result.append(nxt)
    for root in [r for r in regions if r.parent_id is None]:walk(root,[])
    return result


def lock_condition_loop_overlap(text):
    clean=_sanitize_c(text);regions=analyze_conditionals(text,{'<source>':(1,len(clean.splitlines()))}).regions;loops=_loops(clean)
    try:locks=match_lock_regions(text,{'<source>':(1,len(clean.splitlines()))})
    except Exception:return False
    for lock in locks:
        conds=[r for r in regions if lock['start_offset']<=r.start_offset and r.end_offset<=lock['end_offset']]
        if not conds:continue
        for path in _leaves(conds):
            region,branch=path[-1]
            for loop in loops:
                if branch.start_offset is not None and branch.end_offset is not None and branch.start_offset<=loop['start'] and loop['end']<=branch.end_offset:return True
    return False


def _target_locks(source_text,clean,raw):
    formal,_=_formal_function_spans(raw);ranges=_condition_function_ranges(source_text,raw['segments'],formal)
    regions=analyze_conditionals(source_text,ranges).regions
    try:locks=match_lock_regions(source_text,ranges)
    except LockRegionError as exc:require(False,'C5e '+str(exc))
    targets=[];target_locks=[]
    for lock in locks:
        inside=[r for r in regions if lock['start_offset']<=r.start_offset and r.end_offset<=lock['end_offset']]
        overlap=[r for r in regions if max(r.start_offset,lock['start_offset'])<=min(r.end_offset,lock['end_offset'])]
        require(all(r in inside for r in overlap),'C5e Condition crosses Lock boundary')
        if not inside:continue
        roots=[r for r in inside if r.parent_id is None];require(len(roots)==1,'C5e one Condition tree per target Lock')
        loops=_loops(clean);selected=[]
        for loop in loops:
            if lock['start_offset']<loop['start'] and loop['end']<lock['end_offset']:
                owners=[path[-1] for path in _leaves(inside) if path[-1][1].start_offset is not None and path[-1][1].end_offset is not None and path[-1][1].start_offset<=loop['start'] and loop['end']<=path[-1][1].end_offset]
                require(len(owners)==1,'C5e Loop must stay completely inside one Condition leaf')
                selected.append((loop,owners[0]))
        if selected:
            targets.append((lock,inside,selected));target_locks.append(lock)
        else:
            require(False,'C5e Condition inside Lock must own a Loop leaf')
    require(1<=len(targets)<=2,'C5e supports one or two target Locks')
    for i,left in enumerate(target_locks):
        for right in target_locks[i+1:]:
            overlap=max(left['start_offset'],right['start_offset'])<=min(left['end_offset'],right['end_offset'])
            require(not overlap,'C5e overlapping/nested target Locks are unsupported')
    for lock in locks:
        if lock in target_locks:continue
        for target in target_locks:
            overlap=max(lock['start_offset'],target['start_offset'])<=min(lock['end_offset'],target['end_offset'])
            require(not overlap,'C5e external Lock overlaps a target Lock')
    for loop in _loops(clean):
        containing=[t for t in targets if t[0]['start_offset']<loop['start'] and loop['end']<t[0]['end_offset']]
        if containing:
            require(len(containing)==1,'C5e Loop crosses target Lock boundary')
            lock,conds,selected=containing[0]
            require(any(item[0]['start']==loop['start'] and item[0]['end']==loop['end'] for item in selected),'C5e Loop inside Lock must stay inside a Condition leaf')
        else:
            for lock,conds,selected in targets:
                for cond in conds:
                    overlap=max(loop['start'],cond.start_offset)<=min(loop['end'],cond.end_offset)
                    require(not overlap,'C5e Loop crosses a Condition boundary')
                require(not (loop['start']<=lock['start_offset'] and lock['end_offset']<=loop['end']),'C5e Lock inside Loop is unsupported')
    target_conditions=[r for _,conds,_ in targets for r in conds]
    for region in regions:
        if region in target_conditions:continue
        for lock in target_locks:
            require(not (max(region.start_offset,lock['start_offset'])<=min(region.end_offset,lock['end_offset'])),'C5e Condition overlaps target Lock')
    return targets,target_conditions,regions

def _mask_lock(source_text,lock):
    locks=lock if isinstance(lock,list) else [lock];spans=[];clean=_sanitize_c(source_text)
    for m in re.finditer(r'\bpthread_mutex_(?:lock|unlock)\s*\(\s*&\s*\w+\s*\)\s*;',clean):
        line=clean.count('\n',0,m.start())+1
        if any(item['lock_range'][0]<=line<=item['unlock_range'][0] for item in locks):spans.append((m.start(),m.end()))
    return _mask(source_text,spans)


def _paths(regions):
    leaf_paths=_leaves(regions);result=[]
    for path in leaf_paths:
        memberships=[dict(exclusive_group=region.condition_id,branch_id=branch.branch_id,guard=branch.guard) for region,branch in path]
        key='|'.join(m['exclusive_group']+'='+m['branch_id'] for m in memberships)
        result.append((key,memberships,path))
    require(2<=len(result)<=4,'C5e unsupported Condition leaf count')
    return result


def _compatible(row,pathset):
    for m in row.get('condition_memberships',[]):
        if (m.get('exclusive_group'),m.get('branch_id')) not in pathset:return False
    return True


def _build_candidates(lock,conds,s,dd,target_region_rows,group_id=None):
    paths=_paths(conds);critical=set(target_region_rows)
    critical_nodes=[r['seg_id'] for r in s['segments'] if r['seg_id'] in critical]
    edges=[(e['src'],e['dst']) for e in dd['edges'] if e['src'] in critical and e['dst'] in critical]
    topo_sort(critical_nodes,edges)
    order={n:i for i,n in enumerate(topo_sort(critical_nodes,edges))}
    candidates=[];projection={}
    for key,memberships,path in paths:
        pathset={(m['exclusive_group'],m['branch_id']) for m in memberships}
        members=[r for r in s['segments'] if r['seg_id'] in critical and _compatible(r,pathset)]
        members.sort(key=lambda r:(order.get(r['seg_id'],10**9),r['start_line'],r['seg_id']))
        require(members,'C5e empty Condition path')
        gid=group_id or ('LOCKGROUP:'+lock['mutex_id'])
        cid='LOCKPATH:'+gid.split(':',1)[1]+'@'+key
        member_templates=[r['template_id'] for r in members if r.get('template_id')]
        candidate=dict(seg_id=cid,kind='lock_path_candidate',function=lock['function'],start_line=lock['lock_range'][0],end_line=lock['unlock_range'][0],
                       path_id=key,condition_memberships=copy.deepcopy(memberships),mutex_id=lock['mutex_id'],
                       lock_group=dict(group_id=gid,mutex_id=lock['mutex_id'],source_region=copy.deepcopy(lock)),
                       member_segment_ids=[r['seg_id'] for r in members],member_template_ids=member_templates,
                       source_structural_nodes=copy.deepcopy([r['seg_id'] for r in members]),
                       source_segments=_ordered_unique(sid for r in members for sid in r.get('source_segments',[])),
                       source_position_ids=_ordered_unique(sid for r in members for sid in r.get('source_position_ids',[])),
                       generation_origin=_ordered_unique(n for r in members for n in (r.get('generation_origin') or [])) or None)
        candidates.append(candidate)
        for r in members:projection.setdefault(r['seg_id'],[]).append(cid)
    return candidates,projection


def build_lock_condition_loop_views(*,base_name,source_text,base_segments_json,base_dag_json):
    raw=copy.deepcopy(base_segments_json);dag=copy.deepcopy(base_dag_json);bases=raw['segments'];byid={r['seg_id']:r for r in bases}
    require(len(byid)==len(bases) and set(byid)==set(dag['nodes']),'C5e raw node identity conflict')
    _topo_sem(dag['nodes'],dag['edges'])
    _validate_base_provenance(dag)
    clean=_sanitize_c(source_text);lines=clean.splitlines()
    require(not re.search(r'^\s*#\s*(?!include\b)\w+',_strip_comments_and_strings(source_text),re.M),'C5e rejects active directives')
    targets,target_conditions,all_conditions=_target_locks(source_text,clean,raw)
    target_locks=[lock for lock,_,_ in targets]
    all_loops=_loops(clean)
    formal,sources=_formal_function_spans(raw);ranges=_condition_function_ranges(source_text,bases,formal)
    segment_functions={r['function'] for r in bases};helper_regions=[r for r in all_conditions if r.function not in segment_functions]
    outside_loops=[loop for loop in all_loops if not any(t[0]['start_offset']<loop['start'] and loop['end']<t[0]['end_offset'] for t in targets) and not any(cond.start_offset<=loop['start'] and loop['end']<=cond.end_offset for cond in all_conditions if cond not in helper_regions)]
    raw_sem_edges=[copy.deepcopy(e) for e in dag['edges'] if e.get('kind')=='sem_dep'];sem_calls=_sem_calls(clean)
    masked=_mask_lock(source_text,target_locks);raw_local=copy.deepcopy(raw)
    for lock in target_locks:
        target_mu=[r for r in raw_local['segments'] if r['kind']=='mutex_cs' and r['function']==lock['function'] and r['start_line']==lock['lock_range'][0] and r['end_line']==lock['unlock_range'][0]]
        require(len(target_mu)==1,'C5e outer Lock must own one complete Level-2 mutex segment')
        target_mu[0]['kind']='compute'
    local_dag=copy.deepcopy(dag);local_dag['edges']=[e for e in local_dag['edges'] if e.get('kind')!='sem_dep']
    masked=_mask(masked,[(r.start_offset,r.end_offset) for r in helper_regions])
    masked=_mask(masked,[(call['start'],call['end']+1 if call['end']<len(masked) and masked[call['end']]==';' else call['end']) for call in sem_calls])
    masked=_mask(masked,[(loop['start'],loop['end']+1) for loop in outside_loops])
    try:st,sd,s,dd,meta=build_branch_loop_views(base_name=base_name,source_text=masked,base_segments_json=raw_local,base_dag_json=local_dag)
    except Exception as exc:require(False,'C5e inner Condition/Loop rejected: '+str(exc))
    require(meta['scheduler_compatible'],'C5e incompatible Condition/Loop view')
    target_of={};critical_ids=set();projection={};candidates=[];target_groups=[];group_by_target={}
    multiple=len(target_locks)>1 or len({lock['mutex_id'] for lock in target_locks})>1
    for index,(lock,conds,selected) in enumerate(targets):
        gid='LOCKGROUP:'+lock['mutex_id']+('@'+lock['function']+':'+str(lock['lock_range'][0]) if multiple else '')
        group_by_target[index]=gid
        critical=[r for r in s['segments'] if r.get('function')==lock['function'] and lock['lock_range'][0]<=r['start_line'] and r['end_line']<=lock['unlock_range'][0] and r['kind'] not in CONTROL and r['kind']!='lock_group']
        require(critical,'C5e missing critical execution rows')
        for row in critical:target_of[row['seg_id']]=index;critical_ids.add(row['seg_id'])
        c,pr=_build_candidates(lock,conds,s,dd,{r['seg_id'] for r in critical},gid);candidates.extend(c)
        for key,values in pr.items():projection.setdefault(key,[]).extend(values)
        target_groups.append(dict(group_id=gid,mutex_id=lock['mutex_id'],members=[copy.deepcopy(lock)],candidate_segment_ids=[x['seg_id'] for x in c],loop_id=None))
    absorbed=[]
    if raw_sem_edges:
        posts=[c for c in sem_calls if c['name']=='sem_post'];waits=[c for c in sem_calls if c['name']=='sem_wait']
        require(posts and waits,'C5e sem_dep requires formal semaphore calls')
        for edge in raw_sem_edges:
            matched=[]
            for candidate in candidates:
                pathset={(m['exclusive_group'],m['branch_id']) for m in candidate['condition_memberships']}
                if any(_call_on_path(c,pathset,target_conditions) for c in posts) and any(_call_on_path(c,pathset,target_conditions) for c in waits):matched.append(candidate)
            require(matched,'C5e sem dependency crosses a Condition path')
            for candidate in matched:absorbed.append(dict(edge=copy.deepcopy(edge),projected_src=candidate['seg_id'],projected_dst=candidate['seg_id']))
    external=[r for r in s['segments'] if r['seg_id'] not in critical_ids]
    final=copy.deepcopy(external)+copy.deepcopy(candidates)
    projected=[]
    for edge in dd['edges']:
        if edge['src'] in critical_ids and edge['dst'] in critical_ids and target_of.get(edge['src'])==target_of.get(edge['dst']):continue
        srcs=projection.get(edge['src'],[edge['src']]);dsts=projection.get(edge['dst'],[edge['dst']])
        for a in srcs:
            for b in dsts:
                if a==b:continue
                out=copy.deepcopy(edge);out.update(src=a,dst=b);projected.append(out)
    seen={};result_edges=[]
    for e in projected:
        key=json.dumps({k:v for k,v in e.items() if k!='provenance'},sort_keys=True)
        if key not in seen:seen[key]=e;result_edges.append(e)
        else:seen[key].setdefault('provenance',[]).extend(copy.deepcopy(e.get('provenance',[])))
    final_dag=dict(base_name=base_name,nodes=[r['seg_id'] for r in final],edges=result_edges,exclusive_groups=copy.deepcopy(dd.get('exclusive_groups',{})))
    topo_sort(final_dag['nodes'],[(e['src'],e['dst']) for e in result_edges])
    loops=copy.deepcopy(s.get('loops',[]))
    # External independent Loops outside all Conditions are expanded after target candidates.
    ext_absorbed=[];ext_templates=[]
    if outside_loops:
        by_fn={}
        for loop in outside_loops:by_fn.setdefault(loop.get('function') or target_locks[0]['function'],[]).append(loop)
        for index,(fn,items) in enumerate(sorted(by_fn.items())):
            items=sorted(items,key=lambda x:x['start']);require(len(items)<=2,'C5e supports only external Loop L1/L2/L3 per function')
            low=min(x['header'] for x in items);high=max(x['exit'] for x in items)
            if not any(r.get('function')==fn and max(r['start_line'],low)<=min(r['end_line'],high) for r in final):
                before=[r for r in final if r.get('function')==fn and r['end_line']<low and r['kind'] not in CONTROL and r['kind']!='lock_group']
                after=[r for r in final if r.get('function')==fn and r['start_line']>high and r['kind'] not in CONTROL and r['kind']!='lock_group']
                require(before and after,'C5e external Loop has no surrounding execution owner')
                left=max(before,key=lambda r:r['end_line']);right=min(after,key=lambda r:r['start_line'])
                raw_ids=[b['seg_id'] for b in bases if b['function']==fn and max(b['start_line'],low)<=min(b['end_line'],high)]
                holder=dict(seg_id='C5EEXT:'+fn+':'+str(index)+':body',kind='compute',function=fn,start_line=low,end_line=high,source_segments=raw_ids,source_position_ids=['LOOPSRC:'+fn+':line:'+str(n) for n in range(low,high+1)],role='external_loop_placeholder')
                final.append(holder);final_dag['nodes'].append(holder['seg_id'])
                prefix=[e for e in final_dag['edges'] if e['src']==left['seg_id'] and e['dst']==right['seg_id']]
                final_dag['edges']=[e for e in final_dag['edges'] if not (e['src']==left['seg_id'] and e['dst']==right['seg_id'])]
                for e in prefix:
                    a=copy.deepcopy(e);a['dst']=holder['seg_id'];final_dag['edges'].append(a)
                    b=copy.deepcopy(e);b['src']=holder['seg_id'];b['dst']=right['seg_id'];final_dag['edges'].append(b)
                if not prefix:
                    final_dag['edges'].append({'src':left['seg_id'],'dst':holder['seg_id'],'kind':'intra'})
                    final_dag['edges'].append({'src':holder['seg_id'],'dst':right['seg_id'],'kind':'intra'})
            final,final_dag=_normalize_cluster(final,final_dag,fn,low,high,'C5EEXT:'+fn+':'+str(index),ext_absorbed)
            loop_source=_external_loop_source(source_text,lines,all_conditions,target_locks,all_loops,items)
            payload=copy.deepcopy(raw);payload.update(segments=copy.deepcopy(final),node_to_segment={},mappings={})
            st2,sd2,_,_,_=build_loop_nested_views(base_name=base_name,source_text=loop_source,base_segments_json=payload,base_dag_json=final_dag)
            final=[r for r in st2['segments'] if r['kind'] not in CONTROL]
            final_dag=dict(nodes=[r['seg_id'] for r in final],edges=_project(st2['segments'],sd2),exclusive_groups=copy.deepcopy(sd2.get('exclusive_groups',{})))
            loops.extend(copy.deepcopy(st2.get('loops',[])));ext_templates.extend(copy.deepcopy(st2.get('templates',[])))
    mappings=copy.deepcopy(s.get('mappings',{}))
    grouped=dict(mappings.get('grouped_node_to_member_segments',{}));source_lock=dict(mappings.get('source_lock_region_to_group',{}));source_inst=dict(mappings.get('source_lock_region_to_instances',{}))
    for index,group in enumerate(target_groups):grouped[group['group_id']]=group['candidate_segment_ids']
    for lock,group in zip(target_locks,target_groups):
        key=lock['function']+'@'+str(lock['lock_range'][0]);source_lock[key]=[group['group_id']];source_inst[key]=[group['group_id']]
    iteration=dict(mappings.get('iteration_to_group',{}))
    for row in s['segments']:
        if row.get('role')=='iteration' and row['seg_id'] in projection:
            for cid in projection[row['seg_id']]:
                for index,(lock,_,_) in enumerate(targets):
                    if lock['lock_range'][0]<=row['start_line']<=row['end_line']<=lock['unlock_range'][0]:iteration[row['seg_id']]=group_by_target[index]
    template_map=copy.deepcopy(mappings.get('template_to_execution_segments',{}))
    for tid,targets_list in list(template_map.items()):
        mapped=[]
        for target in targets_list:mapped.extend(projection.get(target,[target]))
        template_map[tid]=_ordered_unique(mapped)
    original=copy.deepcopy(mappings.get('original_segment_to_execution_segments',{}))
    for base in bases:
        ts=[]
        for row in s['segments']:
            if base['seg_id'] in (row.get('source_segments') or []):ts.extend(projection.get(row['seg_id'],[row['seg_id']]))
        if ts:original[base['seg_id']]=_ordered_unique(ts)
    positions=copy.deepcopy(mappings.get('source_position_to_execution_segments',{}))
    for key,ts in list(positions.items()):
        mapped=[]
        for target in ts:mapped.extend(projection.get(target,[target]))
        positions[key]=_ordered_unique(mapped)
    for row in s['segments']:
        for key in row.get('source_position_ids',[]):
            if row['seg_id'] in projection:positions[key]=_ordered_unique(positions.get(key,[])+projection[row['seg_id']])
    generation=copy.deepcopy(mappings.get('generation_node_to_execution_segments',{}))
    for node,ts in list(generation.items()):
        mapped=[]
        for target in ts:mapped.extend(projection.get(target,[target]))
        generation[node]=_ordered_unique(mapped)
    candidate_ids={row['seg_id'] for row in candidates};local_ids={row['seg_id'] for row in s['segments']}
    for template in ext_templates:
        tid=template['template_id']
        mapped_targets=[row['seg_id'] for row in final if row.get('template_id')==tid and row['kind'] not in CONTROL]
        if tid in template_map and set(template_map[tid])!=set(mapped_targets):
            require(False,'C5e external template mapping conflict: '+tid)
        template_map[tid]=_ordered_unique(mapped_targets)
    for row in final:
        if row['seg_id'] in local_ids or row['seg_id'] in candidate_ids:continue
        for sid in row.get('source_segments',[]):original.setdefault(sid,[]);original[sid]=_ordered_unique(original[sid]+[row['seg_id']])
        for key in row.get('source_position_ids',[]):positions[key]=_ordered_unique(positions.get(key,[])+[row['seg_id']])
        for node in row.get('generation_origin') or []:generation[node]=_ordered_unique(generation.get(node,[])+[row['seg_id']])
    mappings.update(grouped_node_to_member_segments=grouped,source_lock_region_to_group=source_lock,source_lock_region_to_instances=source_inst,iteration_to_group=iteration,template_to_execution_segments=template_map,original_segment_to_execution_segments=original,source_position_to_execution_segments=positions,generation_node_to_execution_segments=generation,absorbed_internal_sem_dependencies=copy.deepcopy(absorbed))
    for loop in loops:
        for index,(lock,conds,selected) in enumerate(targets):
            if lock['lock_range'][0]<=loop['header_line'] and loop['exit_line']<=lock['unlock_range'][0]:loop['lock_group_id']=group_by_target[index];loop['mutex_id']=lock['mutex_id']
    iteration_instances=[];seen_instances=set()
    local_ids={r['seg_id'] for r in s['segments']};instance_rows=list(s['segments'])+[r for r in final if r['seg_id'] not in local_ids]
    for row in instance_rows:
        if row.get('role')!='iteration':continue
        if row['seg_id'] in seen_instances:continue
        seen_instances.add(row['seg_id']);gid=None
        for index,(lock,_,_) in enumerate(targets):
            if row.get('function')==lock['function'] and lock['lock_range'][0]<=row['start_line']<=row['end_line']<=lock['unlock_range'][0]:gid=group_by_target[index]
        iteration_instances.append(dict(loop_id=row['loop_id'],iteration_index=row.get('iteration_index'),outer_iteration_index=row.get('outer_iteration_index'),inner_iteration_index=row.get('inner_iteration_index'),segment_id=row['seg_id'],template_id=row.get('template_id'),condition_memberships=copy.deepcopy(row.get('condition_memberships',[])),lock_group_id=gid,source_position_ids=copy.deepcopy(row.get('source_position_ids',[]))))
    tags=copy.deepcopy(s.get('templates',[]));tags.extend(copy.deepcopy(ext_templates))
    tids={t['template_id'] for t in tags}
    for row in candidates:
        for tid in row['member_template_ids']:
            if tid not in tids:tags.append(dict(template_id=tid,function=row['function'],start_line=row['start_line'],end_line=row['end_line'],role='execution'));tids.add(tid)
    helper_functions=sorted({r.function for r in helper_regions});helper_views=_helper_condition_views(base_name=base_name,source_text=source_text,raw=raw,sources=sources,functions=helper_functions,prefix='C5EHELPER') if helper_functions else None
    helper_rows=[];helper_edges=[];helper_conditions=[];helper_segments=[];helper_segment_to_struct={};helper_position_to_struct={}
    if helper_views is not None:
        hs,hd,he,hseg=helper_views;he_by={r['seg_id']:r for r in he['segments']};helper_segments=[dict(copy.deepcopy(r),source_only=True,analysis_only=True) for r in hseg]
        for original_row in hs['segments']:
            row=copy.deepcopy(original_row);row.update(source_only=True,analysis_only=True)
            if row['kind'] not in CONTROL:
                members=he_by.get(row['seg_id'],{}).get('condition_memberships')
                if members:row['condition_memberships']=copy.deepcopy(members)
                fn=row['function'];row['source_position_ids']=['C5EHELPER:'+fn+':line:'+str(n) for n in range(row['start_line'],row['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
            helper_rows.append(row)
            for sid in row.get('source_segments',[]):
                if sid.startswith('C5EHELPER:'):helper_segment_to_struct.setdefault(sid,[]).append(row['seg_id'])
            for key in row.get('source_position_ids',[]):helper_position_to_struct.setdefault(key,[]).append(row['seg_id'])
        helper_edges=[copy.deepcopy(e) for e in hd['edges']];helper_conditions=[copy.deepcopy(r) for r in hs['condition_regions']]
        st['segments']=st['segments']+copy.deepcopy(helper_rows);sd['edges']=copy.deepcopy(list(sd['edges'])+helper_edges);sd['nodes']=[r['seg_id'] for r in st['segments']]
        s['condition_regions']=sorted(copy.deepcopy(s.get('condition_regions',[]))+helper_conditions,key=lambda r:(r['function'],r['entry_line'],r['exit_line'],r['condition_id']))
        mappings['source_only_segment_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_segment_to_struct.items())}
        mappings['source_position_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_position_to_struct.items())}
    external_groups=copy.deepcopy(s.get('lock_groups',[]));lock_groups=target_groups+external_groups;external_locks=[]
    for item in external_groups:external_locks.extend(copy.deepcopy(item.get('members',[])))
    common=dict(base_name=base_name,rule_name=RULE,level='level2',source_file=raw['source_file'],source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),
                thread_entry_functions=copy.deepcopy(raw.get('thread_entry_functions',[])),loops=loops,condition_regions=copy.deepcopy(s.get('condition_regions',[])),
                templates=tags,lock_groups=lock_groups,lock_templates=[copy.deepcopy(lock) for lock in target_locks]+external_locks,critical_regions=[copy.deepcopy(lock) for lock in target_locks],independent_lock_regions=external_locks,
                iteration_instances=iteration_instances,mappings=mappings,composition_edge_provenance=copy.deepcopy(s.get('composition_edge_provenance',[])),composition_input_sha256=digest([raw,dag]),
                capabilities=dict(scheduler_compatible=True,timing_compatible=False,instrumentation_compatible=False,analysis_only=True))
    if helper_conditions:common['source_only_segments']=copy.deepcopy(helper_segments)
    struct_dag=dict(base_name=base_name,nodes=[r['seg_id'] for r in st['segments']],edges=copy.deepcopy(sd['edges']),exclusive_groups=copy.deepcopy(sd.get('exclusive_groups',{})))
    return dict(copy.deepcopy(common),view='struct',segments=st['segments']),struct_dag,dict(common,view='sched',segments=final),final_dag,dict(rule_name=RULE,level='level2',scheduler_compatible=True,analysis_only=True)
