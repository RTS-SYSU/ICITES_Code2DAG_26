"""C5f: one outer Lock contains a static Loop whose iterations own independent Conditions."""
import copy,hashlib,json,re
from .composition_model import CONTROL,require,_mask,_ordered_unique,_project,digest
from .condition_model import _sanitize_c,_match_pair,_skip_ws,analyze_conditionals
from .lock_regions import match_lock_regions,LockRegionError
from .composition_c3b_model import build_loop_condition_views
from .composition_c5b_model import _normalize_cluster,_external_loop_source,_split_owner_segments
from .loop_nested_model import build_loop_nested_views
from .composition_c4b_model import _validated_provenance
from .composition_c5c_model import _validate_base_provenance
from .composition_c5d_model import (_map_external_condition_view,_collapsed_sem_edge,_topo_with_collapsed_sem_witnesses,_compatible_paths,_row_path,_SEM_CALL_RE,_formal_sem_spans)
from .composition_c4a_model import _formal_function_spans,_condition_function_ranges,_helper_condition_views
from ..core.func_ranges import _strip_comments_and_strings
from .algo.common import topo_sort
RULE='composition_c5f'


def _loops(clean):
    out=[]
    for m in re.finditer(r'\bfor\b',clean):
        try:
            close=_match_pair(clean,_skip_ws(clean,m.end()),'(',')');brace=_skip_ws(clean,close+1);end=_match_pair(clean,brace,'{','}')
        except Exception:continue
        out.append(dict(start=m.start(),end=end,header=clean.count('\n',0,m.start())+1,exit=clean.count('\n',0,end)+1))
    return out


def lock_loop_condition_overlap(text):
    clean=_sanitize_c(text);conds=analyze_conditionals(text,{'<source>':(1,len(clean.splitlines()))}).regions;loops=_loops(clean)
    try:locks=match_lock_regions(text,{'<source>':(1,len(clean.splitlines()))})
    except Exception:return False
    for lock in locks:
        for loop in loops:
            if lock['start_offset']<loop['start'] and loop['end']<lock['end_offset']:
                if any(loop['start']<r.start_offset and r.end_offset<loop['end'] for r in conds):return True
    return False


def _targets(source_text,clean,raw):
    formal,_=_formal_function_spans(raw);ranges=_condition_function_ranges(source_text,raw['segments'],formal);conds=analyze_conditionals(source_text,ranges).regions
    try:locks=match_lock_regions(source_text,ranges)
    except LockRegionError as exc:require(False,'C5f '+str(exc))
    loops=_loops(clean);targets=[]
    for loop in loops:
        owners=[fn for fn,(low,high) in ranges.items() if low<=loop['header'] and loop['exit']<=high]
        require(len(owners)<=1,'C5f Loop belongs to more than one formal function')
        loop['function']=owners[0] if owners else None
    for lock in locks:
        inside_loops=[loop for loop in loops if lock['start_offset']<loop['start'] and loop['end']<lock['end_offset']]
        if not inside_loops:continue
        contain_conds=[r for r in conds if lock['start_offset']<=r.start_offset and r.end_offset<=lock['end_offset']]
        require(contain_conds,'C5f target Lock must contain Conditions')
        for region in conds:
            overlap=max(region.start_offset,lock['start_offset'])<=min(region.end_offset,lock['end_offset'])
            require(not overlap or region in contain_conds,'C5f Condition crosses Lock boundary')
        loop_conditions=set()
        for loop in inside_loops:
            owners=[r for r in conds if loop['start']<r.start_offset and r.end_offset<loop['end']]
            require(owners,'C5f Loop body must contain Conditions')
            for region in conds:
                overlap=max(region.start_offset,loop['start'])<=min(region.end_offset,loop['end'])
                require(not overlap or region in owners,'C5f Condition crosses target Loop boundary')
            loop_conditions.update(r.condition_id for r in owners)
        # A Condition in the target Lock but outside every target Loop is not
        # an external Condition: the critical region would contain a Condition
        # that C5f does not own.  It must be rejected, never reclassified.
        require(all(r.condition_id in loop_conditions for r in contain_conds),'C5f target Lock Condition must stay inside Loop body')
        target_conds=[r for r in contain_conds if r.condition_id in loop_conditions]
        targets.append((lock,inside_loops,target_conds))
    require(1<=len(targets)<=2,'C5f supports one or two target Locks')
    for i,left in enumerate(targets):
        for right in targets[i+1:]:
            overlap=max(left[0]['start_offset'],right[0]['start_offset'])<=min(left[0]['end_offset'],right[0]['end_offset'])
            require(not overlap,'C5f overlapping/nested target Locks')
    target_conditions=[r for _,_,cs in targets for r in cs];external_conditions=[]
    for region in conds:
        if region in target_conditions:continue
        for lock,_target_loops,cs in targets:
            overlap=max(region.start_offset,lock['start_offset'])<=min(region.end_offset,lock['end_offset'])
            require(not overlap,'C5f Condition overlaps target Lock')
            for loop in _target_loops:
                overlap=max(region.start_offset,loop['start'])<=min(region.end_offset,loop['end'])
                require(not overlap,'C5f Condition crosses target Loop boundary')
        external_conditions.append(region)
    target_locks=[t[0] for t in targets];target_loops=[loop for _,loops,_ in targets for loop in loops]
    external_locks=[lock for lock in locks if not any(lock is target for target in target_locks)]
    for lock in external_locks:
        for target in target_locks:
            overlap=max(lock['start_offset'],target['start_offset'])<=min(lock['end_offset'],target['end_offset'])
            require(not overlap,'C5f external Lock overlaps target Lock')
        for other in external_locks:
            if other is lock:continue
            overlap=max(lock['start_offset'],other['start_offset'])<=min(lock['end_offset'],other['end_offset'])
            require(not overlap,'C5f overlapping/nested external Locks')
        for region in external_conditions:
            overlap=max(region.start_offset,lock['start_offset'])<=min(region.end_offset,lock['end_offset'])
            require(not overlap,'C5f external Condition overlaps external Lock')
    external_loops=[]
    for loop in loops:
        if any(loop is target for target in target_loops):continue
        for target in target_locks:
            overlap=max(loop['start'],target['start_offset'])<=min(loop['end'],target['end_offset'])
            require(not overlap,'C5f external Loop overlaps target Lock')
        for region in external_conditions:
            overlap=max(loop['start'],region.start_offset)<=min(loop['end'],region.end_offset)
            require(not overlap,'C5f external Loop overlaps external Condition')
        for lock in external_locks:
            overlap=max(loop['start'],lock['start_offset'])<=min(loop['end'],lock['end_offset'])
            require(not overlap,'C5f external Loop overlaps external Lock')
        require(loop.get('function'),'C5f external Loop has no formal function owner')
        external_loops.append(loop)
    return targets,target_conditions,external_conditions,ranges,loops,external_locks,external_loops


def _row_source_bases(row):
    return [sid for sid in (row.get('source_segments') or []) if isinstance(sid,str)]


def _base_overlaps_region(base,region):
    return base.get('function')==region.function and max(base['start_line'],region.entry_line)<=min(base['end_line'],region.exit_line)


def _affected_base_ids(bases,regions):
    return {base['seg_id'] for base in bases if any(_base_overlaps_region(base,region) for region in regions)}


def _external_region_ids_for_bases(bases,hit,payloads):
    selected=[]
    for payload in payloads:
        for base in bases:
            if base['seg_id'] not in hit:continue
            if base.get('function')==payload['function'] and max(base['start_line'],payload['entry_line'])<=min(base['end_line'],payload['exit_line']):
                selected.append(payload['condition_id']);break
    return set(selected)


def _protected_target_ids(rows,locks):
    return {row['seg_id'] for row in rows if row.get('function') and any(row['function']==lock['function'] and lock['lock_range'][0]<=row['start_line'] and row['end_line']<=lock['unlock_range'][0] for lock in locks)}


def _validate_collapsed_sem_witnesses(dag,raw,clean):
    """Only a formal post/wait pair collapsed into one Level-2 segment may be
    removed from the raw topo graph.  A synthetic sem_dep self loop without a
    real post/wait pair, or with a non-sem provenance witness, is rejected.
    """
    raw_by_id={row['seg_id']:row for row in raw.get('segments',[])}
    calls=[]
    for match in _SEM_CALL_RE.finditer(clean):
        name=match.group(0).split('(',1)[0]
        if name not in ('sem_post','sem_wait'):continue
        calls.append((name,clean.count('\n',0,match.start())+1))
    for edge in dag.get('edges',[]):
        if not _collapsed_sem_edge(edge):continue
        endpoint=edge.get('src');require(isinstance(endpoint,str) and endpoint,'C5f malformed collapsed sem witness')
        base=raw_by_id.get(endpoint);require(base is not None,'C5f collapsed sem witness has no source segment')
        names={name for name,line in calls if base['start_line']<=line<=base['end_line']}
        require('sem_post' in names and 'sem_wait' in names,'C5f collapsed sem witness has no formal post/wait pair')
        for item in edge.get('provenance',[]):
            if isinstance(item,dict) and 'base_edge_index' in item:
                require(item.get('edge',{}).get('kind')=='sem_dep','C5f collapsed sem witness has non-sem provenance')


def _target_entry_for_line(targets,function,line):
    for index,(lock,_target_loops,conditions) in enumerate(targets):
        if lock.get('function')==function and lock['lock_range'][0]<=line<=lock['unlock_range'][0]:
            return index,(lock,_target_loops,conditions)
    return None,None


def _sem_row_matches_target_call(row,line,regions,condition_regions):
    for region in regions:
        containing=[]
        for branch in region.branches:
            if branch.start_line is not None and branch.end_line is not None and branch.start_line<=line<=branch.end_line:
                containing.append(branch)
        require(len(containing)<=1,'C5f sem call falls in ambiguous Condition branches')
        clones=[clone for clone in condition_regions
                if clone.get('source_condition_id')==region.condition_id
                and clone.get('loop_iteration_id')==row.get('loop_iteration_id')]
        memberships=row.get('condition_memberships',[])
        if containing:
            if not clones:return False
            branch=containing[0]
            if not any(m.get('exclusive_group')==clone['condition_id'] and m.get('branch_id')==branch.branch_id
                       for clone in clones for m in memberships):
                return False
        else:
            if any(m.get('exclusive_group')==clone['condition_id'] for clone in clones for m in memberships):
                return False
    return True


def _sem_row_matches_external_call(row,line,payloads):
    relevant=False
    for payload in payloads:
        if payload.get('function')!=row.get('function'):continue
        if not (payload['entry_line']<=line<=payload['exit_line']):continue
        relevant=True
        containing=[branch for branch in payload.get('branches',[])
                    if branch.get('start_line') is not None and branch.get('end_line') is not None
                    and branch['start_line']<=line<=branch['end_line']]
        require(len(containing)<=1,'C5f sem call falls in ambiguous external Condition branches')
        memberships=row.get('condition_memberships',[])
        if containing:
            branch=containing[0]
            if not any(m.get('exclusive_group')==payload['condition_id'] and m.get('branch_id')==branch['branch_id']
                       for m in memberships):return False
        else:
            if any(m.get('exclusive_group')==payload['condition_id'] for m in memberships):return False
    return relevant


def _sem_endpoint_candidates(*,raw_id,call,rows,final_rows,targets,group_by_target,
                             target_conditions,condition_regions,external_payloads):
    """Return (formal endpoint descriptors, zero-instance flag)."""
    base=next((row for row in rows if raw_id in (row.get('source_segments') or [])),None)
    require(base is not None,'C5f sem_dep endpoint has no source row: '+str(raw_id))
    function=base.get('function')
    index,target=_target_entry_for_line(targets,function,call['line'])
    if target is not None:
        lock,loops,conditions=target
        inside_loops=[loop for loop in loops if loop['header']<=call['line']<=loop['exit']]
        if not inside_loops:
            gid=group_by_target[index]
            candidates=[row for row in rows if row.get('function')==function
                        and raw_id in (row.get('source_segments') or [])
                        and row.get('start_line') is not None
                        and row['start_line']<=call['line']<=row['end_line']]
            if not candidates:
                boundary=[row for row in rows if row.get('function')==function
                          and raw_id in (row.get('source_segments') or [])
                          and row.get('loop_iteration_id') is None and row.get('start_line') is not None]
                if call['line']<min(loop['header'] for loop in loops):
                    candidates=[row for row in boundary if row['end_line']<call['line']]
                    candidates=sorted(candidates,key=lambda row:row['end_line'],reverse=True)[:1]
                elif call['line']>max(loop['exit'] for loop in loops):
                    candidates=[row for row in boundary if row['start_line']>call['line']]
                    candidates=sorted(candidates,key=lambda row:row['start_line'])[:1]
            require(len(candidates)==1,'C5f sem_dep has no unique target execution row: '+str((raw_id,call['line'])))
            row=candidates[0]
            return [dict(node=row['seg_id'],iteration=None,path=_row_path(row),
                         lock=gid,raw_id=raw_id,call=call,row=row)],False
        candidates=[]
        for row in rows:
            if row.get('function')!=function or raw_id not in (row.get('source_segments') or []):continue
            if row.get('loop_iteration_id') is None:continue
            if not _sem_row_matches_target_call(row,call['line'],conditions,condition_regions):continue
            candidates.append(row)
        if not candidates:return [],True
        return [dict(node=row['seg_id'],iteration=row.get('loop_iteration_id'),path=_row_path(row),
                     lock=group_by_target[index],raw_id=raw_id,call=call,row=row) for row in candidates],False
    candidates=[]
    for row in rows:
        if row.get('function')!=function or raw_id not in (row.get('source_segments') or []):continue
        if row.get('start_line') is not None and row['start_line']<=call['line']<=row['end_line']:
            candidates.append(row);continue
        if _sem_row_matches_external_call(row,call['line'],external_payloads):
            candidates.append(row)
    if not candidates:
        for row in final_rows:
            if row.get('function')!=function or raw_id not in (row.get('source_segments') or []):continue
            if row.get('start_line') is not None and row['start_line']<=call['line']<=row['end_line']:candidates.append(row)
    if not candidates:return [],False
    result=[]
    for row in candidates:
        group=row.get('lock_group')
        lock=row.get('lock_group_id') or (group.get('group_id') if isinstance(group,dict) else None)
        result.append(dict(node=row['seg_id'],iteration=row.get('loop_iteration_id'),path=_row_path(row),
                           lock=lock,raw_id=raw_id,call=call,row=row))
    return result,False


def _project_c5f_sem_edges(*,sem_edges,rows,final_rows,group_by_target,
                           targets,target_conditions,condition_regions,external_payloads,raw,clean):
    if not sem_edges:return [], []
    raw_by_id={row['seg_id']:row for row in raw.get('segments',[])}
    final_ids={row['seg_id'] for row in final_rows}
    sem_calls=[]
    for match in _SEM_CALL_RE.finditer(clean):
        name=match.group(0).split('(',1)[0]
        if name not in ('sem_post','sem_wait'):continue
        opening=clean.find('(',match.start(),match.end())
        require(opening>=0,'C5f malformed semaphore call')
        close=_match_pair(clean,opening,'(',')')
        sem_calls.append(dict(name=name,start=match.start(),end=close+1,line=clean.count('\n',0,match.start())+1))
    def calls_in(raw_id,name):
        base=raw_by_id.get(raw_id)
        if base is None:return []
        return [call for call in sem_calls if call['name']==name and base['start_line']<=call['line']<=base['end_line']]
    def final_node(node,raw_id):
        if node in final_ids:return node
        candidates=[row['seg_id'] for row in final_rows
                    if raw_id in (row.get('source_segments') or [])
                    and row.get('function')==raw_by_id.get(raw_id,{}).get('function')]
        require(len(candidates)==1,'C5f sem_dep endpoint has no final formal node: '+str(node))
        return candidates[0]
    projected=[];absorbed=[]
    for edge in sem_edges:
        src_raw=edge.get('src');dst_raw=edge.get('dst')
        post_calls=calls_in(src_raw,'sem_post');wait_calls=calls_in(dst_raw,'sem_wait')
        require(post_calls and wait_calls,'C5f sem_dep requires formal post/wait endpoints: '+str(src_raw)+' -> '+str(dst_raw))
        src_descriptors=[];dst_descriptors=[];zero=False
        for call in post_calls:
            descriptors,is_zero=_sem_endpoint_candidates(raw_id=src_raw,call=call,rows=rows,final_rows=final_rows,
                targets=targets,group_by_target=group_by_target,target_conditions=target_conditions,
                condition_regions=condition_regions,external_payloads=external_payloads)
            src_descriptors.extend(descriptors);zero=zero or is_zero
        for call in wait_calls:
            descriptors,is_zero=_sem_endpoint_candidates(raw_id=dst_raw,call=call,rows=rows,final_rows=final_rows,
                targets=targets,group_by_target=group_by_target,target_conditions=target_conditions,
                condition_regions=condition_regions,external_payloads=external_payloads)
            dst_descriptors.extend(descriptors);zero=zero or is_zero
        if not src_descriptors and not dst_descriptors:
            if zero:continue
            require(False,'C5f sem_dep endpoint has no formal instance: '+str(src_raw)+' -> '+str(dst_raw))
        pairs=[]
        for source in src_descriptors:
            for destination in dst_descriptors:
                if source['call']['start']>=destination['call']['start']:continue
                if source['iteration'] is not None and destination['iteration'] is not None and source['iteration']!=destination['iteration']:continue
                if not _compatible_paths(source['path'],destination['path']):continue
                if source['lock'] is not None and destination['lock'] is not None and source['lock']!=destination['lock']:
                    require(False,'C5f sem_dep crosses mutex group')
                pairs.append((source,destination))
        if not pairs:
            if zero:continue
            require(False,'C5f sem_dep crosses Condition branch or has no formal projection')
        for source,destination in pairs:
            src_node=final_node(source['node'],src_raw);dst_node=final_node(destination['node'],dst_raw)
            if src_node==dst_node:
                absorbed.append(dict(edge=copy.deepcopy(edge),projected_src=src_node,projected_dst=dst_node,
                                     source_instance=source['node'],destination_instance=destination['node']))
                continue
            out=copy.deepcopy(edge);out.update(src=src_node,dst=dst_node);projected.append(out)
    return projected,absorbed


def _c5f_cond_get(region,name,default=None):
    if isinstance(region,dict):return region.get(name,default)
    return getattr(region,name,default)


def _c5f_cond_branches(region):
    result=[]
    for branch in _c5f_cond_get(region,'branches',[]) or []:
        result.append((str(_c5f_cond_get(branch,'branch_id','')),_c5f_cond_get(branch,'start_line'),_c5f_cond_get(branch,'end_line')))
    return tuple(sorted(result))


def _c5f_cond_base_key(region):
    return (str(_c5f_cond_get(region,'function','')),int(_c5f_cond_get(region,'entry_line',-1)),int(_c5f_cond_get(region,'exit_line',-1)),_c5f_cond_branches(region))


def _c5f_cond_match_key(region):
    branches=tuple(sorted(str(_c5f_cond_get(branch,'branch_id','')) for branch in _c5f_cond_get(region,'branches',[]) or []))
    return (str(_c5f_cond_get(region,'function','')),int(_c5f_cond_get(region,'entry_line',-1)),int(_c5f_cond_get(region,'exit_line',-1)),branches)


def _c5f_validate_local_branches(local,formal):
    for branch in _c5f_cond_get(local,'branches',[]) or []:
        bid=str(_c5f_cond_get(branch,'branch_id',''));ls=_c5f_cond_get(branch,'start_line');le=_c5f_cond_get(branch,'end_line')
        match=[b for b in _c5f_cond_get(formal,'branches',[]) or [] if str(_c5f_cond_get(b,'branch_id',''))==bid]
        require(len(match)==1,'C5f Condition branch identity conflict')
        fs=_c5f_cond_get(match[0],'start_line');fe=_c5f_cond_get(match[0],'end_line')
        if ls is None and le is None and fs is None and fe is None:continue
        require(ls is not None and le is not None and fs is not None and fe is not None and fs<=ls and le<=fe,'C5f Condition branch metadata conflict')


def _c5f_canonical_id(descriptor,source_sha,used):
    raw=json.dumps(descriptor,sort_keys=True,separators=(',',':'),ensure_ascii=False)
    base='C5FSRC:'+hashlib.sha256((source_sha+'\0'+raw).encode()).hexdigest()[:20]
    candidate=base;suffix=0
    while candidate in used:
        suffix+=1;candidate=base+':'+str(suffix)
    return candidate


def _c5f_replace_string(value,local,canonical):
    if isinstance(value,str):
        if value==local:return canonical
        if value.startswith(local+':'):return canonical+value[len(local):]
        return value
    if isinstance(value,list):return [_c5f_replace_string(item,local,canonical) for item in value]
    if isinstance(value,dict):return {key:_c5f_replace_string(item,local,canonical) for key,item in value.items()}
    return value


def _c5f_contains_local(value,local):
    if isinstance(value,str):return value==local or value.startswith(local+':')
    if isinstance(value,list):return any(_c5f_contains_local(item,local) for item in value)
    if isinstance(value,dict):return any(_c5f_contains_local(item,local) for item in value.values())
    return False


_C5F_CONDITION_ID_FIELDS=('condition_id','source_condition_id','exclusive_group','parent_id',
    'parent_condition_id','owner_condition_id','condition_owner_id','child_condition_id')
_C5F_CONDITION_LIST_FIELDS=('source_condition_ids','condition_ids','child_condition_ids')
_C5F_BRANCH_FIELDS=('branch_id','branch_scope_id','parent_branch_id')
_C5F_CONDITION_MAP_FIELDS=('source_condition_to_instances','condition_template_node_to_instances')


def _c5f_condition_parent_path(region,lookup):
    path=[];seen=set();parent_id=_c5f_cond_get(region,'parent_id')
    while parent_id is not None:
        require(parent_id not in seen,'C5f cyclic Condition parent path')
        parent=lookup.get(parent_id)
        require(parent is not None,'C5f Condition parent identity missing: '+str(parent_id))
        seen.add(parent_id);path.append(_c5f_cond_base_key(parent));parent_id=_c5f_cond_get(parent,'parent_id')
    return tuple(reversed(path))


def _c5f_external_view_alias_map(view,payloads):
    """Map external-view local Condition IDs to formal payload IDs by identity.

    The external view is built from a masked source, so its local numbering can
    differ from the formal source view.  Function/range/branch/parent identity
    is the only permitted basis for this mapping; aliases are never selected by
    traversal order or by the numeric suffix alone.
    """
    local={}
    for piece in (view[0],view[2]):
        for region in piece.get('condition_regions',[]) or []:
            cid=region.get('condition_id')
            require(isinstance(cid,str) and cid,'C5f external view Condition without ID')
            old=local.get(cid)
            if old is not None:
                require(_c5f_cond_base_key(old)==_c5f_cond_base_key(region),'C5f conflicting external view Condition alias: '+cid)
                require(_c5f_cond_get(old,'parent_id')==_c5f_cond_get(region,'parent_id'),'C5f conflicting external Condition parent alias: '+cid)
            else:local[cid]=region
    formal={p.get('condition_id'):p for p in payloads}
    require(len(formal)==len(payloads),'C5f duplicate external Condition payload ID')
    require(len(local)==len(formal),'C5f external view/payload Condition count mismatch')
    def signatures(regions):
        result={}
        for cid,region in regions.items():
            sig=(_c5f_cond_base_key(region),_c5f_condition_parent_path(region,regions))
            require(sig not in result,'C5f duplicate external Condition formal identity: '+cid)
            result[sig]=cid
        return result
    local_sig=signatures(local);formal_sig=signatures(formal)
    require(set(local_sig)==set(formal_sig),'C5f external view/payload formal identity mismatch')
    alias={}
    for sig,local_id in local_sig.items():
        formal_id=formal_sig[sig]
        prior=alias.get(local_id)
        require(prior is None or prior==formal_id,'C5f ambiguous external Condition alias: '+local_id)
        alias[local_id]=formal_id
    # A formal source discovered under two local aliases still maps both aliases
    # to one payload identity; that is deterministic and is audited below.
    require(len(set(alias.values()))==len(payloads),'C5f external Condition alias did not cover every formal source')
    return alias


def _c5f_remap_condition_ref(value,alias):
    """Rebind a single Condition reference; never rewrite unrelated strings."""
    if not isinstance(value,str):return value
    if value in alias:return alias[value]
    for local,formal in alias.items():
        if value.startswith(local+':'):
            return formal+value[len(local):]
        marker='::'+local
        at=value.find(marker)
        if at>=0:
            end=at+len(marker)
            if end==len(value) or value[end] in ':=':
                return value[:at+len('::')]+formal+value[end:]
    return value


def _c5f_rebind_condition_object(value,alias):
    """Field-aware recursive rebinding for Condition references in a view."""
    if isinstance(value,dict):
        result={}
        for key,item in value.items():
            if key in _C5F_CONDITION_MAP_FIELDS:
                mapped_key=_c5f_remap_condition_ref(key,alias)
                if isinstance(item,list):result[mapped_key]=[_c5f_remap_condition_ref(x,alias) for x in item]
                elif isinstance(item,dict):result[mapped_key]={_c5f_remap_condition_ref(k,alias):_c5f_rebind_condition_object(v,alias) for k,v in item.items()}
                else:result[mapped_key]=_c5f_rebind_condition_object(item,alias)
            elif key=='exclusive_groups' and isinstance(item,dict):
                result[key]={_c5f_remap_condition_ref(k,alias):[_c5f_remap_condition_ref(v,alias) for v in vals] if isinstance(vals,list) else _c5f_rebind_condition_object(vals,alias) for k,vals in item.items()}
            elif key in _C5F_CONDITION_ID_FIELDS or key in _C5F_BRANCH_FIELDS:
                result[key]=_c5f_remap_condition_ref(item,alias)
            elif key in _C5F_CONDITION_LIST_FIELDS and isinstance(item,list):
                result[key]=[_c5f_remap_condition_ref(x,alias) for x in item]
            else:
                result[key]=_c5f_rebind_condition_object(item,alias)
        return result
    if isinstance(value,list):return [_c5f_rebind_condition_object(item,alias) for item in value]
    return value


def _c5f_rebind_external_view(view,payloads):
    alias=_c5f_external_view_alias_map(view,payloads)
    return tuple(_c5f_rebind_condition_object(piece,alias) for piece in view[:4])+(view[4],),alias


def _c5f_validate_condition_references(*,rows,struct_rows,regions,source_instances,source_keys,
                                       target_source_ids,external_ids,target_group_ids,
                                       edges,struct_edges,templates):
    """Post-merge audit: no Condition reference may point at a stale alias."""
    region_by_id={r.get('condition_id'):r for r in regions}
    require(len(region_by_id)==len(regions),'C5f duplicate published Condition ID')
    branches={cid:{b.get('branch_id') for b in region.get('branches',[]) or []} for cid,region in region_by_id.items()}
    target_instance_ids={r.get('condition_id') for r in regions if r.get('loop_iteration_id')}
    external_ids=set(external_ids);target_source_ids=set(target_source_ids)
    require(not (external_ids & target_source_ids),'C5f target/external Condition source key collision')
    def membership_ok(row,where):
        for m in row.get('condition_memberships',[]) or []:
            group=m.get('exclusive_group');branch=m.get('branch_id')
            require(group in region_by_id,where+' membership references unknown Condition: '+str(group))
            require(branch in branches[group],where+' membership references unknown branch: '+str(branch))
            if group in external_ids:
                require(row.get('lock_group_id') not in target_group_ids,where+' external Condition inherited target Lock identity')
        cid=row.get('condition_id')
        if cid is not None:require(cid in region_by_id,where+' condition_id is not a published Condition: '+str(cid))
    for row in list(rows)+list(struct_rows):membership_ok(row,'row')
    for template in templates:membership_ok(template,'template')
    def edge_ok(edge):
        group=edge.get('exclusive_group')
        if group is not None:
            require(group in region_by_id,'edge references unknown Condition: '+str(group))
            branch=edge.get('branch_id')
            if branch is not None:require(branch in branches[group],'edge references unknown branch: '+str(branch))
        for key in ('condition_transitions','control_projection'):
            for item in edge.get(key,[]) or []:edge_ok(item)
    for edge in list(edges)+list(struct_edges):edge_ok(edge)
    for region in regions:
        parent=region.get('parent_id')
        if parent is not None:require(parent in region_by_id,'Condition parent is not published: '+str(parent))
        source=region.get('source_condition_id')
        if source is not None:require(source in source_keys or source in target_source_ids,'Condition source alias unresolved: '+str(source))
    for key,values in source_instances.items():
        require(isinstance(values,list),'source Condition mapping is not a list')
        if key in external_ids:
            require(all(value in external_ids for value in values),'external Condition mapping contains target instance')
        elif key in target_source_ids:
            require(all(value in target_instance_ids for value in values),'target Condition mapping contains external instance')
        else:
            require(False,'unresolved source Condition mapping key: '+str(key))
    require(set(source_instances)<=external_ids|target_source_ids,'source mapping contains unregistered Condition key')


def _canonicalize_condition_sources(*,source_text,base_name,target_regions,external_payloads,helper_payloads,
                                    target_conditions,external_conditions,loops,struct_loops,struct_rows,
                                    sched_rows,struct_edges,sched_edges):
    """Register formal Condition source identities before local views are merged.

    A local ``source_condition_id`` is only an alias for one formal source.  If
    the alias is globally unique it is preserved; otherwise a deterministic
    canonical ID is derived from function, entry/exit range, branch ranges and
    the formal parent path.  No traversal order or target/external label enters
    the canonical identity.
    """
    all_formal=[];seen_formal=set()
    for region in list(target_conditions)+list(external_conditions):
        key=id(region)
        if key in seen_formal:continue
        seen_formal.add(key);all_formal.append(region)
    formal_by_id={_c5f_cond_get(r,'condition_id'):r for r in all_formal}
    # Parent/base descriptors use source ranges only.
    base_by_region={id(r):_c5f_cond_base_key(r) for r in all_formal}
    parent_paths={}
    def parent_path(region):
        key=id(region)
        if key in parent_paths:return parent_paths[key]
        path=[];seen=set();parent=formal_by_id.get(_c5f_cond_get(region,'parent_id'))
        while parent is not None:
            pkey=id(parent)
            require(pkey not in seen,'C5f cyclic Condition source parent')
            seen.add(pkey);path.append(base_by_region[pkey]);parent=formal_by_id.get(_c5f_cond_get(parent,'parent_id'))
        parent_paths[key]=tuple(reversed(path));return parent_paths[key]
    for region in all_formal:parent_path(region)
    identities={id(r):(base_by_region[id(r)],parent_path(r)) for r in all_formal}
    identity_by_id={_c5f_cond_get(r,'condition_id'):identities[id(r)] for r in all_formal}
    target_ranges={};external_ranges={}
    for region in target_conditions:
        key=_c5f_cond_match_key(region)
        require(key not in target_ranges,'C5f duplicate formal target Condition range')
        target_ranges[key]=region
    for region in external_conditions:
        key=_c5f_cond_match_key(region)
        require(key not in external_ranges,'C5f duplicate formal external Condition range')
        external_ranges[key]=region
    records=[];record_by_kind_id={}
    def add_record(kind,obj,formal,local,parent_local=None):
        identity=identities[id(formal)]
        record=dict(kind=kind,obj=obj,formal=formal,identity=identity,local=local,parent_local=parent_local)
        records.append(record)
        if kind=='target':record_by_kind_id[('target',obj['condition_id'])]=record
        else:record_by_kind_id[(kind,local)]=record
        return record
    # Target iteration clones: match the formal source by range, never by local ID.
    target_clone_formal={}
    for clone in target_regions:
        key=_c5f_cond_match_key(clone)
        formal=target_ranges.get(key)
        require(formal is not None,'C5f target Condition has no formal source range')
        _c5f_validate_local_branches(clone,formal)
        local=clone.get('source_condition_id')
        require(isinstance(local,str) and local,'C5f target Condition has no local source ID')
        rec=add_record('target',clone,formal,local,clone.get('parent_id'))
        target_clone_formal[clone['condition_id']]=formal
    # External and helper views carry their own local IDs; identities still come from range.
    for payload in list(external_payloads)+list(helper_payloads):
        key=_c5f_cond_match_key(payload)
        formal=external_ranges.get(key)
        require(formal is not None,'C5f external Condition has no formal source range')
        _c5f_validate_local_branches(payload,formal)
        local=payload.get('condition_id')
        require(isinstance(local,str) and local,'C5f external Condition has no local source ID')
        add_record('helper' if payload in helper_payloads else 'external',payload,formal,local,payload.get('parent_id'))
    # A zero-iteration target Loop has no clone rows; its Loop metadata still
    # carries the formal local source ID when one exists.
    target_record_forms={rec['formal'].condition_id for rec in records if rec['kind']=='target'}
    unknown=[formal for formal in target_conditions if formal.condition_id not in target_record_forms]
    assigned=set()
    loop_order=sorted([loop for loop in loops if loop.get('function') in {formal.function for formal in unknown}],key=lambda loop:(loop.get('function'),loop.get('header_line'),loop.get('exit_line')))
    for loop in loop_order:
        ids=_ordered_unique(loop.get('source_condition_ids') or [])
        if not ids:continue
        candidates=[formal for formal in unknown if formal.condition_id not in assigned and formal.function==loop.get('function') and loop.get('header_line')<=formal.entry_line and formal.exit_line<=loop.get('exit_line')]
        if not candidates:continue
        if len(candidates)!=len(ids):require(False,'C5f ambiguous zero-Loop source Condition identity')
        numeric=lambda value:int(value.rsplit('#',1)[1]) if value.rsplit('#',1)[-1].isdigit() else value
        for formal,local in zip(sorted(candidates,key=lambda r:(r.entry_line,r.exit_line,r.condition_id)),sorted(ids,key=numeric)):
            add_record('target_zero',formal,formal,local,formal.parent_id);assigned.add(formal.condition_id)
    # Leftover source-only zero Conditions keep a deterministic canonical identity
    # without inventing an unrelated local alias.
    for formal in unknown:
        if formal.condition_id not in assigned:
            add_record('target_zero',formal,formal,formal.condition_id,formal.parent_id)
    # Parent metadata consistency, per view and against the formal parent path.
    for rec in records:
        formal=rec['formal'];parent=_c5f_cond_get(formal,'parent_id')
        parent_local=rec['parent_local']
        if parent is None:
            require(parent_local is None,'C5f Condition parent metadata conflict')
            continue
        require(parent_local is not None,'C5f Condition source parent missing')
        if rec['kind']=='target':
            parent_rec=next((x for x in records if x['kind']=='target' and x['obj'].get('condition_id')==parent_local),None)
        elif rec['kind']=='target_zero':
            parent_rec=next((x for x in records if x['kind'] in ('target','target_zero') and x['formal'].condition_id==parent),None)
        else:
            parent_rec=next((x for x in records if x['kind']==rec['kind'] and x['local']==parent_local),None)
        require(parent_rec is not None,'C5f Condition source parent is missing')
        require(parent_rec['formal'].condition_id==parent,'C5f Condition source parent identity conflict')
    # Local aliases and canonical ID assignment.
    formal_records={}
    for rec in records:formal_records.setdefault(rec['identity'],[]).append(rec)
    local_forms={}
    for identity,recs in formal_records.items():
        for local in {rec['local'] for rec in recs}:local_forms.setdefault(local,set()).add(identity)
    used_locals={local for local in local_forms}
    canonical={};changed=False
    for identity,recs in sorted(formal_records.items(),key=lambda item:json.dumps(item[0],sort_keys=True,default=str)):
        locals_={rec['local'] for rec in recs}
        if len(locals_)!=1 or any(len(local_forms[local])>1 for local in locals_):
            cid=_c5f_canonical_id(dict(identity=identity),hashlib.sha256(source_text.encode()).hexdigest(),used_locals)
            canonical[identity]=cid;used_locals.add(cid);changed=True
        else:
            canonical[identity]=next(iter(locals_))
    # Same formal source discovered twice under one alias is still one source;
    # duplicate aliases make the source canonical.
    if not changed:return None
    # Update target source aliases and Loop metadata.
    target_alias={}
    for rec in records:
        if rec['kind']=='target':
            target_alias.setdefault(rec['local'],set()).add(canonical[rec['identity']])
    for clone in target_regions:
        formal=target_clone_formal[clone['condition_id']]
        clone['source_condition_id']=canonical[identities[id(formal)]]
    def update_loop_sources(loop_list):
        for loop in loop_list:
            old_ids=loop.get('source_condition_ids') or []
            new=[]
            for old in old_ids:
                candidates=set()
                for rec in records:
                    if rec['kind'] not in ('target','target_zero'):continue
                    if rec['local']!=old:continue
                    formal=rec['formal']
                    if formal.function!=loop.get('function'):continue
                    if not (loop.get('header_line')<=formal.entry_line and formal.exit_line<=loop.get('exit_line')):continue
                    candidates.add(canonical[rec['identity']])
                if len(candidates)==1:new.append(next(iter(candidates)))
                elif not candidates:new.append(old)
                else:require(False,'C5f ambiguous target Loop source Condition alias')
            loop['source_condition_ids']=_ordered_unique(new)
    update_loop_sources(loops);update_loop_sources(struct_loops)
    # External/helper source aliases.  A single alias must resolve to one canonical source.
    external_alias={}
    for rec in records:
        if rec['kind'] in ('external','helper'):
            external_alias.setdefault(rec['local'],set()).add(canonical[rec['identity']])
    for rec in records:
        if rec['kind'] in ('external','helper'):
            payload=rec['obj'];cid=canonical[rec['identity']]
            payload['condition_id']=cid
            if rec['parent_local'] is not None:
                parent_rec=next(x for x in records if x['kind']==rec['kind'] and x['local']==rec['parent_local'])
                payload['parent_id']=canonical[parent_rec['identity']]
            # Branch labels themselves are then/else; any embedded source ref is remapped.
            for key,value in list(payload.items()):
                if key in ('condition_id','parent_id'):continue
                payload[key]=_c5f_replace_string(value,rec['local'],cid)
    def external_candidates(local):
        return sorted(external_alias.get(local,set()))
    def replace_external_object(obj,context=None):
        if isinstance(obj,str):
            for local,cids in external_alias.items():
                if obj==local or obj.startswith(local+':'):
                    if len(cids)!=1:
                        require(False,'C5f ambiguous external Condition local alias')
                    return _c5f_replace_string(obj,local,next(iter(cids)))
            return obj
        if isinstance(obj,list):return [replace_external_object(item,context) for item in obj]
        if isinstance(obj,dict):return {key:replace_external_object(value,context) for key,value in obj.items()}
        return obj
    # Replace dictionaries in place to retain container identity used by edges.
    def replace_in_place(obj):
        if isinstance(obj,dict):
            for key in list(obj):obj[key]=replace_in_place(obj[key])
        elif isinstance(obj,list):
            for i in range(len(obj)):obj[i]=replace_in_place(obj[i])
        elif isinstance(obj,str):return replace_external_object(obj)
        return obj
    for row in sched_rows+struct_rows:
        if any(_c5f_contains_local(row,local) for local in external_alias):replace_in_place(row)
    for edge in sched_edges+struct_edges:
        if any(_c5f_contains_local(edge,local) for local in external_alias):replace_in_place(edge)
    # Rebuild the formal source -> instances mapping from canonical regions only.
    source_instances={}
    for rec in records:
        if rec['kind'] in ('target','target_zero'):
            source_instances.setdefault(canonical[rec['identity']],[])
    for clone in target_regions:
        key=clone['source_condition_id'];source_instances.setdefault(key,[])
        if clone['condition_id'] not in source_instances[key]:source_instances[key].append(clone['condition_id'])
    for payload in list(external_payloads)+list(helper_payloads):
        key=payload['condition_id'];source_instances.setdefault(key,[])
        if key not in source_instances[key]:source_instances[key].append(key)
    return source_instances


def _merge_external_condition_rows(*,rows,edges,external_rows,external_edges,bases,regions,payloads,protected_ids,structural):
    """Replace each masked external-Condition host row by its formal Condition
    subgraph.  All other C5f rows and their dependencies stay untouched; cross
    dependencies are rewired to the subgraph entry/exit instead of being
    dropped.  The external subgraph keeps the independent Condition contract
    built by the existing C5d view helper.
    """
    affected=_affected_base_ids(bases,regions)
    if not affected:return rows,edges,0
    kept={row['seg_id']:row for row in rows}
    replacements={};used=set()
    for row in rows:
        rid=row['seg_id']
        if rid in protected_ids:continue
        row_bases=set(_row_source_bases(row));hit=row_bases & affected
        if not hit:continue
        # A masked host row must not also own target code.  Splitting such a
        # row would silently attach target metadata to an external Condition.
        if row_bases-affected:
            from .composition_model import require as _require
            _require(False,'C5f external Condition shares a scheduling segment with other code')
        region_ids=_external_region_ids_for_bases(bases,hit,payloads)
        selected=[]
        for ext in external_rows:
            ids={ext['seg_id']}
            if structural and ext.get('kind') in CONTROL and ext.get('condition_id') in region_ids:
                selected.append(ext);continue
            if set(_row_source_bases(ext)) & hit:selected.append(ext)
        require(selected,'C5f external Condition has no formal subgraph')
        selected_ids={ext['seg_id'] for ext in selected}
        require(not (selected_ids & used),'C5f external Condition subgraph reused by multiple host rows')
        require(not any(sid in kept and sid!=rid for sid in selected_ids),'C5f external Condition subgraph conflicts with a retained row')
        internal=[e for e in external_edges if e['src'] in selected_ids and e['dst'] in selected_ids]
        incoming={e['dst'] for e in internal};outgoing={e['src'] for e in internal}
        entry=[ext['seg_id'] for ext in selected if ext['seg_id'] not in incoming]
        exit_=[ext['seg_id'] for ext in selected if ext['seg_id'] not in outgoing]
        require(entry and exit_,'C5f external Condition subgraph has no entry or exit')
        replacements[rid]=dict(entries=entry,exits=exit_,rows=[copy.deepcopy(ext) for ext in selected],ids=selected_ids)
        used.update(selected_ids)
    result_rows=[copy.deepcopy(row) for row in rows if row['seg_id'] not in replacements]
    for replacement in replacements.values():result_rows.extend(replacement['rows'])
    result_edges=[]
    for edge in edges:
        left,right=edge['src'],edge['dst']
        if left in replacements and right in replacements and left==right:continue
        srcs=replacements[left]['exits'] if left in replacements else [left]
        dsts=replacements[right]['entries'] if right in replacements else [right]
        for src in srcs:
            for dst in dsts:
                out=copy.deepcopy(edge);out.update(src=src,dst=dst);result_edges.append(out)
    for edge in external_edges:
        if edge['src'] in used and edge['dst'] in used:result_edges.append(copy.deepcopy(edge))
    return result_rows,result_edges,len(replacements)


def _mask_locks(source_text,locks):
    clean=_sanitize_c(source_text);spans=[]
    for m in re.finditer(r'\bpthread_mutex_(?:lock|unlock)\s*\(\s*&\s*\w+\s*\)\s*;',clean):
        line=clean.count('\n',0,m.start())+1
        if any(l['lock_range'][0]<=line<=l['unlock_range'][0] for l in locks):spans.append((m.start(),m.end()))
    return _mask(source_text,spans)


def build_lock_loop_condition_views(*,base_name,source_text,base_segments_json,base_dag_json):
    raw=copy.deepcopy(base_segments_json);dag=copy.deepcopy(base_dag_json);bases=raw['segments'];byid={r['seg_id']:r for r in bases}
    require(len(byid)==len(bases) and set(byid)==set(dag['nodes']),'C5f raw node identity conflict')
    _validate_base_provenance(dag)
    clean=_sanitize_c(source_text);lines=clean.splitlines()
    require(not re.search(r'^\s*#\s*(?!include\b)\w+',_strip_comments_and_strings(source_text),re.M),'C5f rejects active directives')
    _validate_collapsed_sem_witnesses(dag,raw,clean)
    sem_edges=[copy.deepcopy(edge) for edge in dag['edges'] if edge.get('kind')=='sem_dep']
    dag_inner=copy.deepcopy(dag);dag_inner['edges']=[copy.deepcopy(edge) for edge in dag['edges'] if edge.get('kind')!='sem_dep']
    _topo_with_collapsed_sem_witnesses(dag['nodes'],dag['edges'])
    topo_sort(dag_inner['nodes'],[(edge['src'],edge['dst']) for edge in dag_inner['edges']])
    targets,target_conditions,external_conditions,ranges,all_loops,external_locks,external_loops=_targets(source_text,clean,raw);target_locks=[t[0] for t in targets]
    masked=_mask_locks(source_text,target_locks+external_locks);masked=_mask(masked,[(r.start_offset,r.end_offset) for r in external_conditions])
    masked=_mask(masked,[(loop['start'],min(loop['end']+1,len(masked))) for loop in external_loops])
    masked=_mask(masked,_formal_sem_spans(clean));raw_local=copy.deepcopy(raw);split_map={}
    if external_locks:
        regions=copy.deepcopy(external_locks)
        for region in regions:
            owners=[r for r in raw['segments'] if r['kind']=='mutex_cs' and r['function']==region['function'] and r['start_line']<=region['lock_range'][0] and region['unlock_range'][0]<=r['end_line']]
            require(len(owners)==1,'C5f external Lock must own one complete Level-2 mutex segment')
            region['owner_segment_id']=owners[0]['seg_id'];region['region_id']=region['function']+'@'+str(region['lock_range'][0])
        work,split_dag,split_map,split_nodes=_split_owner_segments(regions,raw['segments'],dag_inner,raw.get('node_to_segment',{}),raw.get('node_locations',{}))
        raw_local['segments']=work;raw_local['node_to_segment']=split_nodes;dag_inner=split_dag
        topo_sort(dag_inner['nodes'],[(edge['src'],edge['dst']) for edge in dag_inner['edges']])
    for lock in target_locks:
        owners=[r for r in raw_local['segments'] if r['kind']=='mutex_cs' and r['function']==lock['function'] and r['start_line']==lock['lock_range'][0] and r['end_line']==lock['unlock_range'][0]]
        require(len(owners)==1,'C5f target Lock must own one Level-2 mutex segment');owners[0]['kind']='compute'
    try:st,sd,s,dd,meta=build_loop_condition_views(base_name=base_name,source_text=masked,base_segments_json=raw_local,base_dag_json=dag_inner)
    except Exception as exc:require(False,'C5f inner Loop/Condition rejected: '+str(exc))
    require(meta['scheduler_compatible'],'C5f incompatible inner view')
    segment_functions={r['function'] for r in bases}
    external_regions=[r for r in external_conditions if r.function in segment_functions]
    helper_regions=[r for r in external_conditions if r.function not in segment_functions]
    external_view=None;external_conditions_payload=[]
    target_regions=copy.deepcopy(s.get('condition_regions',[]))
    s_protected=_protected_target_ids(s['segments'],target_locks)
    st_protected=_protected_target_ids(st['segments'],target_locks)
    external_condition_alias={}
    if external_regions:
        external_view=_map_external_condition_view(base_name=base_name,source_text=source_text,body_regions=target_conditions,raw=raw,dag=dag_inner,ranges=ranges,external_regions=external_regions)
        external_view,external_condition_alias=_c5f_rebind_external_view(external_view,external_view[4])
        external_conditions_payload=external_view[4]
        s['segments'],dd['edges'],_smerged=_merge_external_condition_rows(rows=s['segments'],edges=dd['edges'],external_rows=external_view[2]['segments'],external_edges=external_view[3]['edges'],bases=bases,regions=external_regions,payloads=external_conditions_payload,protected_ids=s_protected,structural=False)
        st['segments'],sd['edges'],_stmerged=_merge_external_condition_rows(rows=st['segments'],edges=sd['edges'],external_rows=external_view[0]['segments'],external_edges=external_view[1]['edges'],bases=bases,regions=external_regions,payloads=external_conditions_payload,protected_ids=st_protected,structural=True)
        require(_smerged==_stmerged,'C5f external Condition replacement differs between views')
    external_row_ids={r['seg_id'] for r in s['segments'] if r.get('function') in {x.function for x in external_regions} and r['seg_id'] not in s_protected}
    s['condition_regions']=target_regions+external_conditions_payload
    # An external Condition in a helper with no old Level-2 segment is a
    # source-only structural tree.  It never gets a scheduling row, a target
    # Lock/Loop identity, or a timing template.
    formal,sources=_formal_function_spans(raw)
    helper_rows=[];helper_edges=[];helper_conditions=[];helper_segment_rows=[];helper_segment_to_struct={};helper_position_to_struct={}
    if helper_regions:
        hs,hd,he,hseg=_helper_condition_views(base_name=base_name,source_text=source_text,raw=raw,sources=sources,functions=sorted({r.function for r in helper_regions}),prefix='C5FHELPER')
        helper_exec_by_id={r['seg_id']:r for r in he['segments']}
        helper_segment_rows=[dict(copy.deepcopy(r),source_only=True,analysis_only=True) for r in hseg]
        for original in hs['segments']:
            row=copy.deepcopy(original);row.update(source_only=True,analysis_only=True)
            if row['kind'] not in CONTROL:
                members=helper_exec_by_id.get(row['seg_id'],{}).get('condition_memberships')
                if members:row['condition_memberships']=copy.deepcopy(members)
                fn=row['function'];row['source_position_ids']=['C5FHELPER:'+fn+':line:'+str(n) for n in range(row['start_line'],row['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
            helper_rows.append(row)
            for sid in row.get('source_segments',[]):
                if sid.startswith('C5FHELPER:'):helper_segment_to_struct.setdefault(sid,[]).append(row['seg_id'])
            for key in row.get('source_position_ids',[]):helper_position_to_struct.setdefault(key,[]).append(row['seg_id'])
        helper_edges=[copy.deepcopy(e) for e in hd['edges']]
        helper_conditions=[copy.deepcopy(r) for r in hs['condition_regions']]
        st['segments'].extend(copy.deepcopy(helper_rows));sd['edges'].extend(copy.deepcopy(helper_edges))
        s['condition_regions']=sorted(copy.deepcopy(s.get('condition_regions',[]))+helper_conditions,key=lambda r:(r['function'],r['entry_line'],r['exit_line'],r['condition_id']))
    canonical_source_instances=_canonicalize_condition_sources(source_text=source_text,base_name=base_name,
        target_regions=target_regions,external_payloads=external_conditions_payload,helper_payloads=helper_conditions,
        target_conditions=target_conditions,external_conditions=external_conditions,loops=s.get('loops',[]),
        struct_loops=st.get('loops',[]),struct_rows=st['segments'],sched_rows=s['segments'],
        struct_edges=sd['edges'],sched_edges=dd['edges'])
    if canonical_source_instances is not None:
        combined=target_regions+external_conditions_payload+helper_conditions
        if helper_conditions:combined=sorted(combined,key=lambda r:(r['function'],r['entry_line'],r['exit_line'],r['condition_id']))
        s['condition_regions']=combined
    # External Condition rows get an isolated template namespace; target
    # per-iteration templates keep their C3B identities.  Assign before the
    # final scheduling rows are cloned so the clone carries the same binding.
    for row in s['segments']:
        if row['seg_id'] in external_row_ids and row['kind'] not in CONTROL and not row.get('source_only') and not row.get('template_id'):
            row['template_id']='C5FEXT:'+row['seg_id']
            row['source_position_ids']=['LOOPSRC:'+str(row['function'])+':line:'+str(n) for n in range(row['start_line'],row['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
    critical_by_target={};all_critical=set()
    for index,(lock,_target_loops,conds) in enumerate(targets):
        rows=[r for r in s['segments'] if r.get('function')==lock['function'] and lock['lock_range'][0]<=r['start_line'] and r['end_line']<=lock['unlock_range'][0] and r['kind'] not in CONTROL and r['kind']!='lock_group']
        require(rows,'C5f missing critical rows');critical_by_target[index]=rows;all_critical.update(r['seg_id'] for r in rows)
    groups=[];group_by_target={};multiple=len(targets)>1 or len({l['mutex_id'] for l in target_locks})>1
    for index,(lock,_target_loops,conds) in enumerate(targets):
        gid='LOCKGROUP:'+lock['mutex_id']+('@'+lock['function']+':'+str(lock['lock_range'][0]) if multiple else '');group_by_target[index]=gid
        members=critical_by_target[index]
        for member in members:member['lock_group_id']=gid
        tids=_ordered_unique(r.get('template_id') for r in members if r.get('template_id'))
        groups.append(dict(group_id=gid,seg_id=gid,kind='lock_group',role='lock_group',mutex_id=lock['mutex_id'],function='<lockgroup>',start_line=lock['lock_range'][0],end_line=lock['unlock_range'][0],members=[copy.deepcopy(r) for r in members],member_segment_ids=[r['seg_id'] for r in members],member_template_ids=tids,regions=[copy.deepcopy(lock)],source_segments=_ordered_unique(sid for r in members for sid in (r.get('source_segments') or [])),source_position_ids=_ordered_unique(k for r in members for k in (r.get('source_position_ids') or [])),generation_origin=_ordered_unique(n for r in members for n in (r.get('generation_origin') or [])) or None,aggregation_method='resource_membership_only'))
    # External Locks are masked from the target C3b call.  Their already
    # admitted execution rows are grouped once per mutex after that call; the
    # target group namespace is never reused.
    external_groups=[];external_group_by_row={};used_group_ids={g['group_id'] for g in groups}
    by_mutex={}
    for lock in external_locks:by_mutex.setdefault(lock['mutex_id'],[]).append(lock)
    for mutex,lock_regions in sorted(by_mutex.items()):
        members=[]
        for lock in lock_regions:
            rows=[r for r in s['segments'] if r.get('function')==lock['function'] and lock['lock_range'][0]<=r['start_line'] and r['end_line']<=lock['unlock_range'][0] and r['kind'] not in CONTROL and r['kind']!='lock_group']
            require(rows,'C5f external Lock has no execution rows')
            members.extend(rows)
        unique={r['seg_id']:r for r in members};members=[unique[key] for key in sorted(unique)]
        gid='LOCKGROUP:'+mutex
        if gid in used_group_ids:gid='LOCKGROUP:'+mutex+'@external:'+lock_regions[0]['function']+':'+str(lock_regions[0]['lock_range'][0])
        require(gid not in used_group_ids,'C5f external Lock group namespace conflict')
        used_group_ids.add(gid);tids=_ordered_unique(r.get('template_id') for r in members if r.get('template_id'))
        external_groups.append(dict(group_id=gid,seg_id=gid,kind='lock_group',role='lock_group',mutex_id=mutex,function='<lockgroup>',start_line=min(lock['lock_range'][0] for lock in lock_regions),end_line=max(lock['unlock_range'][0] for lock in lock_regions),members=[copy.deepcopy(r) for r in members],member_segment_ids=[r['seg_id'] for r in members],member_template_ids=tids,regions=[copy.deepcopy(lock) for lock in lock_regions],source_segments=_ordered_unique(sid for r in members for sid in (r.get('source_segments') or [])),source_position_ids=_ordered_unique(k for r in members for k in (r.get('source_position_ids') or [])),generation_origin=_ordered_unique(n for r in members for n in (r.get('generation_origin') or [])) or None,aggregation_method='fieldwise_sum_of_member_weights'))
        for row in members:external_group_by_row[row['seg_id']]=gid
    if external_group_by_row:s['segments']=[r for r in s['segments'] if r['seg_id'] not in external_group_by_row]
    external=[r for r in s['segments'] if r['seg_id'] not in all_critical]+copy.deepcopy(external_groups)
    final=copy.deepcopy(external)+[copy.deepcopy(row) for rows in critical_by_target.values() for row in rows]
    projected=[]
    for edge in dd['edges']:
        a=external_group_by_row.get(edge['src'],edge['src'])
        b=external_group_by_row.get(edge['dst'],edge['dst'])
        if a==b:continue
        out=copy.deepcopy(edge);out.update(src=a,dst=b);projected.append(out)
    sem_projected,sem_absorbed=_project_c5f_sem_edges(sem_edges=sem_edges,rows=s['segments'],final_rows=final,
        group_by_target=group_by_target,targets=targets,
        target_conditions=target_conditions,condition_regions=s['condition_regions'],
        external_payloads=external_conditions_payload,raw=raw,clean=clean)
    projected.extend(sem_projected)
    seen={};edges=[]
    for e in projected:
        key=json.dumps({k:v for k,v in e.items() if k!='provenance'},sort_keys=True)
        if key not in seen:seen[key]=e;edges.append(e)
        else:seen[key].setdefault('provenance',[]).extend(copy.deepcopy(e.get('provenance',[])))
    final_dag=dict(base_name=base_name,nodes=[r['seg_id'] for r in final],edges=edges,exclusive_groups=copy.deepcopy(dd.get('exclusive_groups',{})));topo_sort(final_dag['nodes'],[(e['src'],e['dst']) for e in edges])
    loops=copy.deepcopy(s.get('loops',[]));ext_templates=[];ext_absorbed=[];external_loop_owner_map={};external_loop_base_map={};external_zero_template_ids=set()
    if external_loops:
        # External Loops are completely masked from the target C3b call.  Build
        # them from the already admitted final cluster with the existing L1/L2/L3
        # Loop contract, one cluster per contiguous external-loop family.
        tops=[loop for loop in external_loops
              if not any(other is not loop and other.get('function')==loop.get('function')
                         and other['start']<loop['start'] and loop['end']<other['end'] for other in external_loops)]
        by_fn={}
        for loop in tops:by_fn.setdefault(loop.get('function'),[]).append(loop)
        clusters=[]
        for fn,items in sorted(by_fn.items()):
            items=sorted(items,key=lambda loop:loop['start']);current=[]
            def separated(previous,nxt):
                for lock in target_locks+external_locks:
                    if previous['exit']<lock['lock_range'][0] and lock['unlock_range'][0]<nxt['header']:return True
                for region in target_conditions+external_conditions:
                    if previous['exit']<region.entry_line and region.exit_line<nxt['header']:return True
                return False
            for top in items:
                if current and (len(current)>=2 or separated(current[-1],top)):
                    clusters.append((fn,current));current=[]
                current.append(top)
            if current:clusters.append((fn,current))
        for index,(fn,top_items) in enumerate(clusters):
            low=min(loop['header'] for loop in top_items);high=max(loop['exit'] for loop in top_items)
            items=[loop for loop in external_loops if loop.get('function')==fn and low<=loop['header'] and loop['exit']<=high]
            if not any(row.get('function')==fn and max(row['start_line'],low)<=min(row['end_line'],high) for row in final):
                before=[row for row in final if row.get('function')==fn and row['end_line']<low and row['kind'] not in CONTROL and row['kind']!='lock_group']
                after=[row for row in final if row.get('function')==fn and row['start_line']>high and row['kind'] not in CONTROL and row['kind']!='lock_group']
                require(before and after,'C5f external Loop has no surrounding execution owner')
                left=max(before,key=lambda row:row['end_line']);right=min(after,key=lambda row:row['start_line'])
                raw_ids=[base['seg_id'] for base in bases if base['function']==fn and max(base['start_line'],low)<=min(base['end_line'],high)]
                holder=dict(seg_id='C5FEXT:'+fn+':'+str(index)+':body',kind='compute',function=fn,start_line=low,end_line=high,source_segments=raw_ids,source_position_ids=['LOOPSRC:'+fn+':line:'+str(n) for n in range(low,high+1)],role='external_loop_placeholder')
                final.append(holder);final_dag['nodes'].append(holder['seg_id'])
                prefix=[edge for edge in final_dag['edges'] if edge['src']==left['seg_id'] and edge['dst']==right['seg_id']]
                final_dag['edges']=[edge for edge in final_dag['edges'] if not (edge['src']==left['seg_id'] and edge['dst']==right['seg_id'])]
                for edge in prefix:
                    a=copy.deepcopy(edge);a['dst']=holder['seg_id'];final_dag['edges'].append(a)
                    b=copy.deepcopy(edge);b['src']=holder['seg_id'];b['dst']=right['seg_id'];final_dag['edges'].append(b)
                if not prefix:
                    final_dag['edges'].append({'src':left['seg_id'],'dst':holder['seg_id'],'kind':'intra'})
                    final_dag['edges'].append({'src':holder['seg_id'],'dst':right['seg_id'],'kind':'intra'})
            final,final_dag=_normalize_cluster(final,final_dag,fn,low,high,'C5FEXT:'+fn+':'+str(index),ext_absorbed)
            loop_source=_external_loop_source(source_text,lines,target_conditions+external_conditions,target_locks+external_locks,all_loops,items)
            payload=copy.deepcopy(raw);payload.update(segments=copy.deepcopy(final),node_to_segment={},mappings={})
            st2,sd2,_,_,_=build_loop_nested_views(base_name=base_name,source_text=loop_source,base_segments_json=payload,base_dag_json=final_dag)
            final=[row for row in st2['segments'] if row['kind'] not in CONTROL]
            final_dag=dict(nodes=[row['seg_id'] for row in final],edges=_project(st2['segments'],sd2),exclusive_groups=copy.deepcopy(sd2.get('exclusive_groups',{})))
            loops.extend(copy.deepcopy(st2.get('loops',[])));ext_templates.extend(copy.deepcopy(st2.get('templates',[])))
            loop_meta={loop['loop_id']:loop for loop in st2.get('loops',[])}
            zero_loop_ids=set()
            for loop in st2.get('loops',[]):
                effective=1;cursor=loop;seen=set()
                while cursor is not None:
                    require(cursor['loop_id'] not in seen,'C5f cyclic external Loop parent')
                    seen.add(cursor['loop_id']);effective*=cursor.get('iteration_count',0);cursor=loop_meta.get(cursor.get('parent_loop_id'))
                if effective==0:zero_loop_ids.add(loop['loop_id'])
            for template in st2.get('templates',[]):
                tid=str(template.get('template_id',''))
                if template.get('loop_id') in zero_loop_ids or any(tid==str(lid)+':body_template' or tid.startswith(str(lid)+':') for lid in zero_loop_ids):
                    external_zero_template_ids.add(tid)
            cluster_rows=[row for row in final if row.get('function')==fn and row['kind'] not in CONTROL and row['kind']!='lock_group' and max(row['start_line'],low)<=min(row['end_line'],high)]
            external_loop_owner_map['C5FEXT:'+fn+':'+str(index)]=_ordered_unique(row['seg_id'] for row in cluster_rows if 'C5FEXT:'+fn+':'+str(index) in (row.get('source_segments') or [])) or _ordered_unique(row['seg_id'] for row in cluster_rows if row.get('role')=='iteration')
            for base in bases:
                if base.get('function')==fn and max(base['start_line'],low)<=min(base['end_line'],high):
                    mapped=[row['seg_id'] for row in final if row.get('function')==fn and row['kind'] not in CONTROL and row['kind']!='lock_group'
                            and max(row['start_line'],base['start_line'])<=min(row['end_line'],base['end_line'])]
                    external_loop_base_map[base['seg_id']]=_ordered_unique(mapped)
        topo_sort(final_dag['nodes'],[(edge['src'],edge['dst']) for edge in final_dag['edges']])
    lock_groups=groups+external_groups
    mappings=copy.deepcopy(s.get('mappings',{}));grouped={g['group_id']:g['member_segment_ids'] for g in lock_groups};source_lock=dict(mappings.get('source_lock_region_to_group',{}));source_inst=dict(mappings.get('source_lock_region_to_instances',{}))
    for index,(lock,_target_loops,conds) in enumerate(targets):
        key=lock['function']+'@'+str(lock['lock_range'][0]);source_lock[key]=[group_by_target[index]];source_inst[key]=[group_by_target[index]]
    for lock in external_locks:
        key=lock['function']+'@'+str(lock['lock_range'][0])
        gid=external_group_by_row.get(next((row['seg_id'] for row in s['segments'] if row.get('source_segments') and row['function']==lock['function'] and lock['lock_range'][0]<=row['start_line'] and row['end_line']<=lock['unlock_range'][0]),''),None)
        if gid is None:
            gid=next((group['group_id'] for group in external_groups if group['mutex_id']==lock['mutex_id']),None)
        require(gid is not None,'C5f external Lock has no group mapping')
        source_lock[key]=[gid];source_inst[key]=[gid]
    # Rebuild execution mappings on the final scheduled endpoints only.
    final_ids={r['seg_id'] for r in final}
    template_map={tid:_ordered_unique(x for x in values if x in final_ids) for tid,values in copy.deepcopy(mappings.get('template_to_execution_segments',{})).items()}
    for tid in external_zero_template_ids:template_map.setdefault(tid,[])
    original=copy.deepcopy(mappings.get('original_segment_to_execution_segments',{}))
    for base in bases:
        if base['seg_id'] in external_loop_base_map:
            original[base['seg_id']]=_ordered_unique(external_loop_base_map[base['seg_id']])
        else:
            hit=[r['seg_id'] for r in final if base['seg_id'] in (r.get('source_segments') or [])]
            if hit:original[base['seg_id']]=_ordered_unique(hit)
    for owner,rows in external_loop_owner_map.items():
        if rows:original[owner]=_ordered_unique(rows)
    for old_base,parts in split_map.items():
        mapped=[]
        for part in parts:
            if part['seg_id'] in external_group_by_row:mapped.append(external_group_by_row[part['seg_id']])
            else:mapped.extend(row['seg_id'] for row in final if part['seg_id'] in (row.get('source_segments') or []) or row['seg_id']==part['seg_id'])
        if mapped:original[old_base]=_ordered_unique(mapped)
        for part in parts:
            mapped_targets=[external_group_by_row[part['seg_id']]] if part['seg_id'] in external_group_by_row else [row['seg_id'] for row in final if part['seg_id'] in (row.get('source_segments') or []) or row['seg_id']==part['seg_id']]
            if mapped_targets:original[part['seg_id']]=_ordered_unique(mapped_targets)
    positions={}
    for row in final:
        for key in row.get('source_position_ids',[]):positions.setdefault(key,[]).append(row['seg_id'])
    for key,values in positions.items():positions[key]=_ordered_unique(values)
    generation={}
    for row in final:
        for node in (row.get('generation_origin') or []):generation.setdefault(node,[]).append(row['seg_id'])
    for node,sid in (raw.get('node_to_segment') or {}).items():
        if sid in original:
            for target_id in original[sid]:
                if target_id not in generation.setdefault(node,[]):generation[node].append(target_id)
    for tid,values in template_map.items():template_map[tid]=_ordered_unique(values)
    for row in final:
        if row['kind']=='lock_group':
            for member in row.get('members',[]):
                tid=member.get('template_id') or (member.get('execution_segment') or {}).get('template_id')
                if tid:template_map.setdefault(tid,[]);
                if tid and row['seg_id'] not in template_map[tid]:template_map[tid].append(row['seg_id'])
        elif row.get('template_id'):
            template_map.setdefault(row['template_id'],[])
            if row['seg_id'] not in template_map[row['template_id']]:template_map[row['template_id']].append(row['seg_id'])
    iteration=dict(mappings.get('iteration_to_group',{}))
    for index,(lock,_target_loops,conds) in enumerate(targets):
        for row in critical_by_target[index]:
            if row.get('role')=='iteration':iteration[row['seg_id']]=group_by_target[index]
    for loop in loops:
        for index,(lock,_,_) in enumerate(targets):
            if lock['lock_range'][0]<=loop['header_line'] and loop['exit_line']<=lock['unlock_range'][0]:loop['lock_group_id']=group_by_target[index];loop['mutex_id']=lock['mutex_id']
    instances=copy.deepcopy(s.get('iteration_instances',[]))
    loop_group={l['loop_id']:l.get('lock_group_id') for l in loops}
    for item in instances:
        item['lock_group_id']=loop_group.get(item.get('loop_id'))
    known_instance_rows={item.get('segment_id') or item.get('original_iteration_segment') for item in instances}
    for row in final:
        if row.get('role')!='iteration' or row['seg_id'] in known_instance_rows:continue
        if not row.get('loop_id'):continue
        instances.append(dict(loop_id=row['loop_id'],iteration_index=row.get('iteration_index'),
            outer_iteration_index=row.get('outer_iteration_index'),inner_iteration_index=row.get('inner_iteration_index'),
            loop_iteration_id=row.get('loop_iteration_id'),segment_id=row['seg_id'],template_id=row.get('template_id'),
            condition_memberships=copy.deepcopy(row.get('condition_memberships',[])),
            lock_group_id=loop_group.get(row['loop_id']),source_position_ids=copy.deepcopy(row.get('source_position_ids',[]))))
    source_instances=copy.deepcopy(canonical_source_instances) if canonical_source_instances is not None else dict(mappings.get('source_condition_to_instances',{}))
    for payload in external_conditions_payload+helper_conditions:
        cid=payload.get('condition_id')
        if cid is None:continue
        source_instances.setdefault(cid,[])
        if cid not in source_instances[cid]:source_instances[cid].append(cid)
    # Structural mappings for external rows: remove the masked host line
    # bindings and bind the formal branch/control nodes instead.  A masked
    # Level-2 host segment is replaced by the published struct rows whose
    # source_segments name that host; stale host IDs are never guessed or
    # borrowed from adjacent nodes.
    base_ids={row['seg_id'] for row in bases}
    struct_by_id={row['seg_id']:row for row in st['segments']}
    struct_ids=set(struct_by_id)
    replacement_by_base={}
    for row in st['segments']:
        for sid in row.get('source_segments') or []:
            if sid in base_ids:replacement_by_base.setdefault(sid,[]).append(row['seg_id'])
    structural_positions=copy.deepcopy(mappings.get('source_position_to_structural_nodes',{}))
    external_function_names={r.function for r in external_regions}
    external_condition_ids={p.get('condition_id') for p in external_conditions_payload}
    for region in external_regions:
        for number in range(region.entry_line,region.exit_line+1):
            structural_positions.pop('LOOPSRC:'+region.function+':line:'+str(number),None)
    for row in st['segments']:
        if row.get('function') in external_function_names and row['kind'] not in CONTROL and row['seg_id'] not in st_protected:
            for number in range(row['start_line'],row['end_line']+1):
                if number-1<len(lines) and lines[number-1].strip() and lines[number-1].strip() not in ('{','}'):
                    structural_positions.setdefault('LOOPSRC:'+row['function']+':line:'+str(number),[]).append(row['seg_id'])
        if row['kind'] in CONTROL and row.get('condition_id') in external_condition_ids:
            payload=next((p for p in external_conditions_payload if p.get('condition_id')==row['condition_id']),None)
            if payload:
                for number in (payload['entry_line'],payload['exit_line']):
                    structural_positions.setdefault('LOOPSRC:'+row['function']+':line:'+str(number),[]).append(row['seg_id'])
    def source_position_line(key):
        match=re.match(r'^(?:LOOPSRC|C5FHELPER):([^:]+):line:(\d+)$',str(key))
        return (match.group(1),int(match.group(2))) if match else None
    for key,values in list(structural_positions.items()):
        clean=[];extra=[]
        for value in values:
            if value in struct_ids:clean.append(value);continue
            require(value in base_ids,'C5f structural source mapping references unknown node: '+str(value))
            parsed=source_position_line(key)
            require(parsed is not None,'C5f masked host has no source-position identity: '+str(key))
            line=parsed[1]
            candidates=[sid for sid in replacement_by_base.get(value,[]) if struct_by_id[sid].get('start_line',line)<=line<=struct_by_id[sid].get('end_line',line)]
            # A control row at the exact source boundary is a formal replacement
            # even though it has no source_segments list.
            if not candidates:
                candidates=[sid for sid,row in struct_by_id.items() if row.get('function')==parsed[0] and row.get('kind') in CONTROL and (row.get('start_line')==line or row.get('end_line')==line)]
            if candidates:extra.extend(candidates)
            else:
                # The source position has no formal structural node; removing
                # the stale host is the only permitted empty result.
                continue
        merged=_ordered_unique(clean+extra)
        # Preserve a legitimate empty key only when it was empty before repair.
        if merged or not values:structural_positions[key]=merged
        else:del structural_positions[key]
    for key,values in helper_position_to_struct.items():structural_positions[key]=_ordered_unique(values)
    structural_original={}
    for base in bases:
        hit=[row['seg_id'] for row in st['segments'] if base['seg_id'] in (row.get('source_segments') or [])]
        if hit:structural_original[base['seg_id']]=_ordered_unique(hit)
    structural_generation={}
    for node,sid in (raw.get('node_to_segment') or {}).items():
        if sid in structural_original:structural_generation[node]=copy.deepcopy(structural_original[sid])
    mappings.update(grouped_node_to_member_segments=grouped,source_lock_region_to_group=source_lock,source_lock_region_to_instances=source_inst,iteration_to_group=iteration,template_to_execution_segments=template_map,original_segment_to_execution_segments=original,source_position_to_execution_segments=positions,generation_node_to_execution_segments=generation,source_condition_to_instances=source_instances,source_position_to_structural_nodes=structural_positions,original_segment_to_structural_nodes=structural_original,generation_node_to_structural_nodes=structural_generation,absorbed_internal_sem_dependencies=copy.deepcopy(sem_absorbed))
    if helper_conditions:
        mappings['source_only_segment_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_segment_to_struct.items())}
    tags=copy.deepcopy(s.get('templates',[]));tags.extend(copy.deepcopy(ext_templates));tids={t['template_id'] for t in tags}
    for row in s['segments']:
        if row.get('template_id') and row['template_id'] not in tids:
            tags.append(dict(template_id=row['template_id'],function=row.get('function'),start_line=row.get('start_line'),end_line=row.get('end_line'),role='execution'));tids.add(row['template_id'])
    for row in final:
        if row['kind']=='lock_group':
            for member in row.get('members',[]):
                tid=member.get('template_id') or (member.get('execution_segment') or {}).get('template_id')
                if tid and tid not in tids:
                    tags.append(dict(template_id=tid,function=member.get('function'),start_line=member.get('start_line'),end_line=member.get('end_line'),role='execution'));tids.add(tid)
    used_tids=set()
    for row in final:
        if row['kind']=='lock_group':
            for member in row.get('members',[]):
                tid=member.get('template_id') or (member.get('execution_segment') or {}).get('template_id')
                if tid:used_tids.add(tid)
        elif row.get('template_id'):used_tids.add(row['template_id'])
    used_tids.update(external_zero_template_ids)
    tags=[tag for tag in tags if tag['template_id'] in used_tids]
    template_map={tid:values for tid,values in template_map.items() if tid in used_tids}
    mappings['template_to_execution_segments']=template_map
    # Condition-reference audit before publication.  This catches stale local
    # aliases, target/external cross-wiring, unknown branches, and unresolved
    # source mapping keys without rewriting ordinary strings.
    target_source_ids={r.get('source_condition_id') for r in target_regions if r.get('source_condition_id') is not None}
    target_source_ids.update(x for loop in loops for x in (loop.get('source_condition_ids') or []))
    external_ids={p.get('condition_id') for p in external_conditions_payload+helper_conditions if p.get('condition_id') is not None}
    _c5f_validate_condition_references(rows=final,struct_rows=st['segments'],regions=s['condition_regions'],
        source_instances=source_instances,source_keys=set(source_instances),target_source_ids=target_source_ids,
        external_ids=external_ids,target_group_ids=set(group_by_target.values()),edges=final_dag['edges'],
        struct_edges=sd['edges'],templates=tags)
    # Publication guard: every execution-node reference must name a node that
    # is actually being published.  Empty lists are legal only for zero-Loop
    # template/original-segment keys.
    final_ids={row['seg_id'] for row in final};struct_ids={row['seg_id'] for row in st['segments']}
    for key,value in mappings.items():
        if key.endswith('_to_execution_segments'):
            require(isinstance(value,dict),'C5f malformed execution mapping '+key)
            for map_key,map_values in value.items():
                require(isinstance(map_values,list),'C5f malformed execution mapping '+key)
                for node in map_values:require(node in final_ids,'C5f dangling execution mapping '+key+': '+str(node))
        elif key.endswith('_to_execution_segment'):
            require(isinstance(value,dict),'C5f malformed execution mapping '+key)
            for map_key,map_value in value.items():
                require(map_value in final_ids,'C5f dangling execution mapping '+key+': '+str(map_value))
    for field in ('source_position_to_structural_nodes','original_segment_to_structural_nodes',
                  'generation_node_to_structural_nodes','original_node_to_structural_nodes'):
        for map_key,map_values in mappings.get(field,{}).items():
            require(isinstance(map_values,list),'C5f malformed structural mapping '+field)
            for node in map_values:require(node in struct_ids,'C5f dangling structural mapping '+field+': '+str(node))
    for map_key,map_values in mappings.get('structural_node_to_execution_segments',{}).items():
        require(map_key in struct_ids,'C5f dangling structural source node: '+str(map_key))
        require(isinstance(map_values,list),'C5f malformed structural-to-execution mapping')
        for node in map_values:require(node in final_ids,'C5f structural-to-execution type mismatch: '+str(node))
    for edge in final_dag['edges']:
        require(edge['src'] in final_ids and edge['dst'] in final_ids,'C5f dangling final edge')
    external_lock_templates=[copy.deepcopy(lock) for lock in external_locks]
    common=dict(base_name=base_name,rule_name=RULE,level='level2',source_file=raw['source_file'],source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),thread_entry_functions=copy.deepcopy(raw.get('thread_entry_functions',[])),loops=loops,condition_regions=copy.deepcopy(s.get('condition_regions',[])),templates=tags,lock_groups=lock_groups,critical_regions=[copy.deepcopy(l) for l in target_locks],lock_templates=[copy.deepcopy(l) for l in target_locks]+external_lock_templates,independent_lock_regions=external_lock_templates,iteration_instances=instances,mappings=mappings,composition_edge_provenance=copy.deepcopy(s.get('composition_edge_provenance',[])),composition_input_sha256=digest([raw,dag]),capabilities=dict(scheduler_compatible=True,timing_compatible=False,instrumentation_compatible=False,analysis_only=True))
    if helper_conditions:common['source_only_segments']=copy.deepcopy(helper_segment_rows)
    struct_dag=dict(base_name=base_name,nodes=[r['seg_id'] for r in st['segments']],edges=copy.deepcopy(sd['edges']),exclusive_groups=copy.deepcopy(sd.get('exclusive_groups',{})))
    return dict(copy.deepcopy(common),view='struct',segments=st['segments']),struct_dag,dict(common,view='sched',segments=final),final_dag,dict(rule_name=RULE,level='level2',scheduler_compatible=True,analysis_only=True)
