"""C5d: static outer Loop iterations contain independent Condition instances with branch-local Locks."""
import copy,hashlib,json,re
from types import SimpleNamespace
from .composition_model import CONTROL,require,_mask,_project,_ordered_unique,digest
from .condition_model import _sanitize_c,analyze_conditionals,build_condition_views,_match_pair,_skip_ws
from .composition_c2a_model import flatten_condition_headers,contract_branch_groups
from .lock_regions import match_lock_regions,LockRegionError
from .composition_c3a_model import _template_targets
from .composition_c2b_model import build_lock_path_views
from .composition_c4a_model import _formal_function_spans,_condition_function_ranges,_helper_condition_views
from .composition_c4b_model import _dedupe_edges
from .loop_nested_model import build_loop_nested_views,_check_induction
from ..core.func_ranges import _strip_comments_and_strings,is_ignored_compiler_fn
from .algo.common import topo_sort
RULE='composition_c5d'


def loop_condition_lock_overlap(text):
    """Route Loop body -> complete Lock -> contained Condition to C5d only."""
    clean=_sanitize_c(text);regions=analyze_conditionals(text,{'<source>':(1,len(clean.splitlines()))}).regions
    loops=[]
    for match in re.finditer(r'\bfor\b',clean):
        try:
            close=_match_pair(clean,_skip_ws(clean,match.end()),'(',')');end=_match_pair(clean,_skip_ws(clean,close+1),'{','}')
        except Exception:continue
        loops.append((match.start(),end))
    stack=[];locks=[]
    for match in re.finditer(r'\bpthread_mutex_(lock|unlock)\s*\(\s*&\s*\w+\s*\)\s*;',clean):
        if match.group(1)=='lock':stack.append(match.start())
        elif stack:locks.append((stack.pop(),match.end()))
    for ls,le in locks:
        for loop_start,loop_end in loops:
            if not (loop_start<ls and le<loop_end):continue
            for region in regions:
                if ls<=region.start_offset and region.end_offset<=le:
                    if not (region.start_offset<ls and le<region.end_offset):return True
    return False




def condition_inside_loop(text):
    clean=_sanitize_c(text);conditions=analyze_conditionals(text,{'<source>':(1,len(text.splitlines()))}).regions
    for m in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,m.end()),'(',')');brace=_skip_ws(clean,close+1);end=_match_pair(clean,brace,'{','}')
        if any(brace<r.start_offset and r.end_offset<=end for r in conditions):return True
    return False



def _condition_leaves(regions):
    leaves=[]
    for region in regions:
        for branch in region.branches:
            if branch.start_offset is None:continue
            if not any(child.parent_id==region.condition_id and child.parent_branch_id==branch.branch_id for child in regions):
                leaves.append((region,branch))
    return leaves


def _condition_trees(regions):
    roots=[region for region in regions if region.parent_id is None]
    for root in roots:
        tree=[];pending=[root]
        while pending:
            item=pending.pop();tree.append(item);pending.extend(child for child in regions if child.parent_id==item.condition_id)
        require(len(tree)<=2,'C5d supports Condition C1-C4 only: '+root.condition_id)
    return roots


def _lock_leaf(lock,regions):
    leaves=[(region,branch) for region,branch in _condition_leaves(regions)
            if branch.start_offset<=lock['start_offset'] and lock['end_offset']<=branch.end_offset]
    if leaves:
        require(len(leaves)==1,'C5d Lock must stay inside one Condition leaf')
        return leaves[0]
    for region in regions:
        overlap=max(lock['start_offset'],region.start_offset)<=min(lock['end_offset'],region.end_offset)
        require(not overlap,'C5d Lock crosses Condition branch boundary')
    return None


def _classify_locks(locks,regions,loops):
    inside=[];outside=[]
    for lock in locks:
        leaf=_lock_leaf(lock,regions)
        if leaf is None:
            for loop in loops:
                overlap=max(lock['start_offset'],loop['start'])<=min(lock['end_offset'],loop['end'])
                require(not overlap,'C5d Lock inside a Loop without Condition is unsupported')
            outside.append(lock);continue
        region,branch=leaf
        # A nested Loop inside the Condition or Lock is a different arrangement.
        for loop in loops:
            if loop['start']<lock['start_offset'] and lock['end_offset']<=loop['end']:
                continue  # the outer Loop containing this Condition/Lock
            if lock['start_offset']<loop['start'] and loop['end']<lock['end_offset']:
                require(False,'C5d Loop inside Lock is unsupported')
        containers=[]
        for loop in loops:
            if loop['start']<region.start_offset and region.end_offset<=loop['end']:
                containers.append(loop);continue
            overlap=max(region.start_offset,loop['start'])<=min(region.end_offset,loop['end'])
            if overlap:
                require(not (region.start_offset<loop['start'] and loop['end']<region.end_offset),
                        'C5d Loop inside Condition is unsupported')
                require(False,'C5d Loop crosses Condition boundary')
        require(containers,'C5d Condition Lock must stay inside one Loop body')
        owner=max(containers,key=lambda item:item['start'])
        lock['condition_region_id']=region.condition_id;lock['condition_branch_id']=branch.branch_id;lock['loop_header']=owner['header']
        inside.append(lock)
    require(inside,'C5d missing Lock inside a Loop-body Condition leaf')
    return inside,outside




_SEM_CALLS=('sem_wait','sem_post','sem_trywait','sem_timedwait','sem_clockwait','sem_init','sem_destroy','sem_getvalue','sem_open','sem_close','sem_unlink')
_SEM_CALL_RE=re.compile(r'\b(?:'+'|'.join(_SEM_CALLS)+r')\s*\(')
_SEM_TYPE_RE=re.compile(r'\bsem_t\b')
_SEM_DECL_RE=re.compile(r'\bsem_t\b[^;]*;')


def _formal_sem_spans(clean):
    """Locate formal semaphore syntax in the sanitized analysis copy.

    The returned offsets address the original source without changing it.
    Comments and string literals are already masked by _sanitize_c, so a fake
    sem call there cannot be selected. Only registered semaphore API calls,
    the sem_t type and sem_t declarations are removed from the Loop-L3 parser
    copy; other identifiers, including similarly named ordinary functions,
    remain visible to the old parser.
    """
    spans=[]
    for match in _SEM_CALL_RE.finditer(clean):
        opening=clean.find('(',match.start(),match.end())
        if opening<0:continue
        try:closing=_match_pair(clean,opening,'(',')')
        except Exception:continue
        end=closing+1;cursor=_skip_ws(clean,end)
        if cursor<len(clean) and clean[cursor]==';':end=cursor+1
        spans.append((match.start(),end))
    for match in _SEM_TYPE_RE.finditer(clean):spans.append((match.start(),match.end()))
    for match in _SEM_DECL_RE.finditer(clean):spans.append((match.start(),match.end()))
    return spans


def _validate_base_provenance(dag):
    """Reject malformed formal witnesses before any C5d projection or publication.

    Opaque dictionaries without base_edge_index remain valid source annotations.
    Their order and nested content are preserved untouched.
    """
    nodes={str(node) for node in dag.get('nodes',[])}
    edge_count=len(dag.get('edges',[]))
    for edge in dag.get('edges',[]):
        provenance=edge.get('provenance',[])
        require(isinstance(provenance,list),'C5d provenance must be a list')
        for item in provenance:
            require(isinstance(item,dict),'C5d provenance entries must be dictionaries')
            if 'base_edge_index' not in item:continue
            index=item['base_edge_index']
            require(type(index) is int,'C5d malformed internal dependency witness: base_edge_index')
            require(0<=index<edge_count,'C5d internal dependency witness index out of range')
            witness=item.get('edge')
            require(isinstance(witness,dict),'C5d malformed internal dependency witness: edge')
            require(all(key in witness for key in ('src','dst','kind')),'C5d malformed internal dependency witness: edge fields')
            require(isinstance(witness.get('kind'),str) and bool(witness['kind']),'C5d malformed internal dependency witness: kind')
            require(all(isinstance(witness[key],str) and witness[key] in nodes for key in ('src','dst')),
                    'C5d malformed internal dependency witness: endpoints')


def _basis_node_map(raw,bases):
    """Map every compiler node to the structural row that owns its source line."""
    locations=raw.get('node_locations',{});byid={row['seg_id']:row for row in bases};result={}
    for node,sid in raw.get('node_to_segment',{}).items():
        location=locations.get(node);chosen=None
        if isinstance(location,dict):
            candidates=[row for row in bases if row.get('function')==location.get('function')
                        and row['start_line']<=location.get('line',-1)<=row['end_line']]
            exact=[row for row in candidates if sid in (row.get('source_segments') or []) or row['seg_id']==sid]
            if exact:candidates=exact
            if candidates:candidates.sort(key=lambda row:(row['end_line']-row['start_line'],row['seg_id']));chosen=candidates[0]['seg_id']
        result[node]=chosen if chosen is not None else sid
    return result


def _normalize_loop_owners(raw,bases,dag,spans,node_to_segment,original_sem_edges=()):
    """Give each function's connected Loop family a complete local owner row."""
    byid={row['seg_id']:row for row in bases};byfn={}
    for span in spans:byfn.setdefault(span['function'],[]).append(span)
    owner_of={};owner_sources={};owners={};normalized=[];owner_rows=[]
    for index,function in enumerate(sorted(byfn)):
        family=byfn[function];low=min(span['header'] for span in family);high=max(span['exit'] for span in family)
        selected=[row for row in bases if row['function']==function and max(row['start_line'],low)<=min(row['end_line'],high)]
        require(selected,'C5d Loop family has no Level-2 owner: '+function)
        owner=dict(seg_id='C5DOWNER:'+function+':'+str(index),function=function,kind='compute',
                   start_line=min(row['start_line'] for row in selected),end_line=max(row['end_line'] for row in selected))
        owner_rows.append(owner)
        for row in selected:
            owner_of[row['seg_id']]=owner['seg_id']
            for sid in row.get('source_segments') or []:owner_sources[sid]=owner['seg_id']
        for base in raw['segments']:
            if base['function']==function and max(base['start_line'],low)<=min(base['end_line'],high):
                owner_sources[base['seg_id']]=owner['seg_id']
    for row in bases:
        if row['seg_id'] not in owner_of:normalized.append(copy.deepcopy(row))
    normalized.extend(owner_rows)
    edges=[];absorbed=[];internal_sem=[]
    # Raw semantic edges inside one owner must be projected per concrete loop
    # iteration later; structural Condition views may have rewritten their
    # endpoints, so use the original Level-2 sem edges for this classification.
    for edge in original_sem_edges:
        left=owner_sources.get(edge['src']);right=owner_sources.get(edge['dst'])
        if left is not None and left==right:internal_sem.append(copy.deepcopy(edge))
    for edge in dag['edges']:
        src=owner_of.get(edge['src'],edge['src']);dst=owner_of.get(edge['dst'],edge['dst'])
        if src==dst:
            kind=edge.get('kind')
            if kind=='sem_dep':continue
            require(kind=='intra','C5d Loop owner absorbs non-intra dependency');absorbed.append(copy.deepcopy(edge));continue
        out=copy.deepcopy(edge);out.update(src=src,dst=dst);edges.append(out)
    mapped={n:owner_of.get(sid,owner_sources.get(sid,sid)) for n,sid in node_to_segment.items()}
    result=dict(base_name=raw.get('base_name'),nodes=[row['seg_id'] for row in normalized],edges=edges)
    return normalized,result,mapped,owner_of,internal_sem


def _payload_memberships(regions,function,start,end):
    result=[]
    for region in regions:
        if region['function']!=function:continue
        for branch in region['branches']:
            if branch['start_line'] is not None and branch['start_line']<=start<=end<=branch['end_line']:
                result.append(dict(exclusive_group=region['condition_id'],branch_id=branch['branch_id'],guard=branch['guard']))
    return result


def _unique_custom(values):
    unique=[]
    for value in values:
        if not any(value==existing for existing in unique):unique.append(copy.deepcopy(value))
    return unique


def _inherit_source_custom(rows,raw):
    """Carry opaque source node annotations through generated structural rows."""
    bases=[base for base in raw.get('segments',[]) if isinstance(base,dict)];byid={base['seg_id']:base for base in bases}
    for row in rows:
        if not isinstance(row,dict) or 'custom' in row or row.get('kind') in CONTROL or row.get('synthetic'):continue
        source_ids=list(row.get('source_segments') or []);values=[]
        for sid in source_ids:
            base=byid.get(sid)
            if base is not None and 'custom' in base:values.append(base['custom'])
        if not values and (not source_ids or all(str(sid).startswith('C5DOWNER:') for sid in source_ids)):
            values=[base['custom'] for base in bases if base.get('custom') is not None
                    and base.get('function')==row.get('function')
                    and max(base['start_line'],row['start_line'])<=min(base['end_line'],row['end_line'])]
        unique=_unique_custom(values)
        if len(unique)==1:row['custom']=copy.deepcopy(unique[0])


def _condition_region_payload(region,*,source_condition_id=None,condition_text=None):
    row=dict(condition_id=region.condition_id,function=region.function,entry_line=region.entry_line,exit_line=region.exit_line,
             parent_id=region.parent_id,parent_branch_id=region.parent_branch_id,
             branches=[dict(branch_id=b.branch_id,guard=b.guard,start_line=b.start_line,end_line=b.end_line) for b in region.branches])
    if source_condition_id is not None:row['source_condition_id']=source_condition_id
    if condition_text is not None:row['condition_text']=condition_text
    return row


def _map_external_condition_view(*,base_name,source_text,body_regions,raw,dag,ranges,external_regions):
    """Build the preserved branch view for Conditions outside every Loop."""
    if not external_regions:return None
    external_source=_mask(flatten_condition_headers(source_text,SimpleNamespace(regions=body_regions)),[])
    # Ownership is judged only for functions that the executable payload
    # actually publishes. Conditions in no-old-segment helpers are handled by
    # _helper_condition_views as source_only/analysis_only structure.
    payload_functions={row['function'] for row in raw['segments']}
    payload_ranges={function:bounds for function,bounds in ranges.items() if function in payload_functions}
    external_analysis=analyze_conditionals(external_source,payload_ranges)
    require(not external_analysis.diagnostics,'C5d invalid external Condition')
    expected={(r.function,r.entry_line,r.exit_line):r for r in external_regions}
    actual={(r.function,r.entry_line,r.exit_line):r for r in external_analysis.regions}
    require(set(expected)==set(actual),'C5d external Condition ownership conflict')
    id_map={};branch_map={}
    for key,original in expected.items():
        region=actual[key];id_map[region.condition_id]=original.condition_id
        require(len(region.branches)==len(original.branches),'C5d external Condition branch conflict')
        for left,right in zip(region.branches,original.branches):
            require((left.start_line,left.end_line)==(right.start_line,right.end_line),'C5d external Condition branch range conflict')
            branch_map[(region.condition_id,left.branch_id)]=right.branch_id
    cs,cd,ce,ced,meta=build_condition_views(base_name=base_name,source_text=external_source,
                                            base_segments_json=raw,base_dag_json=dag)
    require(meta['scheduler_compatible'],'C5d incompatible external Condition: '+str(meta['blocked_reasons']))
    require(len(cs['condition_regions'])==len(external_analysis.regions),'C5d external Condition view is incomplete')
    def branch(value,condition_id):
        if not value:return value
        prefix=str(condition_id)+':'
        composite=value.startswith(prefix)
        suffix=value[len(prefix):] if composite else value
        mapped=branch_map.get((condition_id,suffix),suffix)
        return (id_map.get(condition_id,condition_id)+':'+mapped) if composite else mapped
    def remap_branch_value(value):
        if not isinstance(value,str):return value
        for source in id_map:
            if value.startswith(source+':'):return branch(value,source)
        return value
    for row in cs['segments']:
        condition=row.get('condition_id')
        if condition in id_map:
            row['source_condition_id']=condition;row['condition_id']=id_map[condition]
        if row.get('branch_id'):row['branch_id']=remap_branch_value(row['branch_id'])
        for membership in row.get('condition_memberships',[]):
            source=membership.get('exclusive_group')
            if source in id_map:
                membership['exclusive_group']=id_map[source]
                membership['branch_id']=branch(membership.get('branch_id'),source)
    for edge in cd['edges']:
        source=edge.get('exclusive_group')
        if source in id_map:
            edge['branch_id']=branch(edge.get('branch_id'),source);edge['exclusive_group']=id_map[source]
    remapped_groups={}
    for group,values in cd.get('exclusive_groups',{}).items():
        target=id_map.get(group,group)
        remapped_groups[target]=[target+':'+branch(value[len(group)+1:],group) for value in values]
    cd['exclusive_groups']=remapped_groups
    _inherit_source_custom(cs['segments'],raw)
    regions=[]
    for region in external_analysis.regions:
        original=expected[(region.function,region.entry_line,region.exit_line)]
        payload=_condition_region_payload(original,condition_text=original.condition_text)
        payload['parent_id']=id_map.get(region.parent_id,region.parent_id)
        payload['parent_branch_id']=original.parent_branch_id
        regions.append(payload)
    return cs,cd,ce,ced,regions

def _iteration_fields(row):
    i=row['iteration_index'];require(type(i) is int,'C3b iteration index must be an exact integer')
    two=type(row.get('outer_iteration_index')) is int
    identity='iteration:('+str(row['outer_iteration_index'])+','+str(row['inner_iteration_index'])+')' if two else 'iteration:'+str(i)
    result=dict(loop_id=row['loop_id'],iteration_index=i,iteration_identity=identity,loop_iteration_id=row['loop_id']+':'+identity)
    if two:result.update(outer_iteration_index=row['outer_iteration_index'],inner_iteration_index=row['inner_iteration_index'],parent_loop_id=row.get('parent_loop_id'))
    return result



def _condition_body_function(loop,conditions):
    function=loop.get('function')
    require(type(function) is str and bool(function.strip()),'C3b missing Loop function owner')
    require(conditions and all(r.function==function for r in conditions),'C3b conflicting Loop/Condition function owners')
    return function


def _row_path(row):
    return tuple(m.get('branch_id') for m in row.get('condition_memberships',[]))


def _compatible_paths(left,right):
    return left==right or left[:len(right)]==right or right[:len(left)]==left


def _collapsed_sem_edge(edge):
    """A raw Level-2 sem_dep whose endpoints were merged into one segment."""
    return edge.get('kind')=='sem_dep' and edge.get('src')==edge.get('dst')


def _topo_with_collapsed_sem_witnesses(nodes,edges):
    """Keep the formal graph check, but do not turn a collapsed sem witness
    into a self-loop before C5d has a chance to project it.  Every other edge,
    including a genuine multi-node sem cycle, remains in the checked graph.
    """
    topo_sort(nodes,[(e['src'],e['dst']) for e in edges if not _collapsed_sem_edge(e)])


def _project_internal_sem_edges(sem_edges,execution,raw,clean,regions,source_regions):
    """Project an owner-internal sem dependency onto concrete iteration nodes.

    A native sem_dep may connect two Level-2 rows that both fall inside the
    same normalized Loop owner.  It must become one edge per concrete iteration
    (L1) or per (outer,inner) coordinate (L3), never a cross-iteration edge.
    """
    by_raw={};raw_by_id={row['seg_id']:row for row in raw.get('segments',[])}
    sem_calls=[]
    for match in _SEM_CALL_RE.finditer(clean):
        name=match.group(0).split('(',1)[0]
        if name not in ('sem_post','sem_wait'):continue
        opening=clean.find('(',match.start(),match.end())
        require(opening>=0,'C5d malformed semaphore call')
        close=_match_pair(clean,opening,'(',')')
        sem_calls.append(dict(name=name,start=match.start(),end=close+1,line=clean.count('\n',0,match.start())+1))
    for row in execution:
        for sid in row.get('source_segments') or []:by_raw.setdefault(sid,[]).append(row)
    def focus_calls(raw_id,name):
        base=raw_by_id.get(raw_id)
        if base is None:return []
        return [call for call in sem_calls if call['name']==name and base['start_line']<=call['line']<=base['end_line']]
    def lock_identity(row):
        group=row.get('lock_group')
        return row.get('lock_group_id') or (group.get('group_id') if isinstance(group,dict) else None)
    region_by_id={region['condition_id']:region for region in regions}
    source_region_by_id={region.condition_id:region for region in source_regions}
    def row_matches_call(row,call):
        memberships=row.get('condition_memberships') or []
        for membership in memberships:
            clone=region_by_id.get(membership.get('exclusive_group'))
            require(clone is not None,'C5d sem membership has no local ConditionRegion')
            source_id=clone.get('source_condition_id') or clone.get('condition_id')
            source=source_region_by_id.get(source_id)
            require(source is not None,'C5d sem membership has no source ConditionRegion')
            containing=[]
            for branch in source.branches:
                if branch.start_offset is None or branch.end_offset is None:continue
                overlap=max(call['start'],branch.start_offset)<=min(call['end'],branch.end_offset)
                if overlap and not (branch.start_offset<=call['start'] and call['end']<=branch.end_offset):
                    require(False,'C5d sem call crosses Condition branch boundary')
                if branch.start_offset<=call['start'] and call['end']<=branch.end_offset:
                    containing.append(branch)
            require(len(containing)<=1,'C5d sem call falls in ambiguous Condition branches')
            if containing and containing[0].branch_id!=membership.get('branch_id'):return False
        return True
    def endpoint_rows(rows,calls):
        if not calls:return rows
        return [row for row in rows if any(row_matches_call(row,call) for call in calls)]
    projected=[];absorbed=[]
    for edge in sem_edges:
        src_raw=edge.get('src');dst_raw=edge.get('dst')
        sources=by_raw.get(src_raw,[]);destinations=by_raw.get(dst_raw,[])
        if not sources and not destinations:continue
        # A zero-trip Loop has no iteration rows at all; an external row that
        # happens to overlap the raw segment does not make this edge inhabited.
        sources=[row for row in sources if row.get('loop_iteration_id') is not None]
        destinations=[row for row in destinations if row.get('loop_iteration_id') is not None]
        if not sources and not destinations:continue
        require(sources and destinations,'C5d sem_dep endpoint has no iteration instance: '+str(src_raw)+' -> '+str(dst_raw))
        src_calls=focus_calls(src_raw,'sem_post');dst_calls=focus_calls(dst_raw,'sem_wait')
        require(src_calls and dst_calls,'C5d sem_dep requires formal semaphore call endpoints')
        source_candidates=endpoint_rows(sources,src_calls)
        destination_candidates=endpoint_rows(destinations,dst_calls)
        require(source_candidates and destination_candidates,'C5d sem_dep endpoint has no Condition component instance')
        src_ids={row.get('loop_iteration_id') for row in source_candidates}
        dst_ids={row.get('loop_iteration_id') for row in destination_candidates}
        shared=src_ids&dst_ids
        if not shared:
            require(False,'C5d sem_dep crosses Loop iteration identity')
        for identity in sorted(shared,key=str):
            left=[row for row in source_candidates if row.get('loop_iteration_id')==identity]
            right=[row for row in destination_candidates if row.get('loop_iteration_id')==identity]
            pairs=[];seen_pairs=set()
            for source in left:
                for destination in right:
                    pair=(source['seg_id'],destination['seg_id'])
                    if pair in seen_pairs:continue
                    if not _compatible_paths(_row_path(source),_row_path(destination)):continue
                    src_lock=lock_identity(source);dst_lock=lock_identity(destination)
                    if src_lock is not None and dst_lock is not None:
                        require(src_lock==dst_lock,'C5d sem_dep crosses mutex group')
                    pairs.append((source,destination));seen_pairs.add(pair)
            require(pairs,'C5d sem_dep crosses Condition branch')
            for source,destination in pairs:
                if source['seg_id']==destination['seg_id']:
                    absorbed.append(dict(edge=copy.deepcopy(edge),projected_src=source['seg_id'],projected_dst=destination['seg_id']))
                    continue
                out=copy.deepcopy(edge);out.update(src=source['seg_id'],dst=destination['seg_id'])
                projected.append(out)
    # Stable de-duplication while retaining every source witness.  The
    # direction is always post->wait; duplicate edges only gain provenance.
    seen={}
    for edge in projected:
        key=json.dumps({k:v for k,v in edge.items() if k!='provenance'},sort_keys=True)
        if key not in seen:seen[key]=edge
        else:
            existing=seen[key]
            existing.setdefault('provenance',[]).extend(copy.deepcopy(edge.get('provenance',[])))
    return list(seen.values()),absorbed



def _c5d_synthetic_body_source(source_text,clean,bases_body,loop):
    lines=clean.splitlines();a=loop['body_start'];b=loop['body_end'];spans=[]
    for m in re.finditer(r'\bfor\b',clean):spans.append((m.start(),m.end()))
    for m in re.finditer(r'\bpthread_create\s*\(',clean):
        try:
            opening=clean.find('(',m.start(),m.end());close=_match_pair(clean,opening,'(',')');end=close+1;cur=_skip_ws(clean,end)
            if cur<len(clean) and clean[cur]==';':end=cur+1
            spans.append((m.start(),end))
        except Exception:pass
    spans += _formal_sem_spans(clean)
    for m in re.finditer(r'\bif\b',clean):
        line=clean.count('\n',0,m.start())+1
        if not (a<=line<=b):spans.append((m.start(),m.end()))
    for m in re.finditer(r'\bpthread_mutex_(?:lock|unlock)\s*\(',clean):
        line=clean.count('\n',0,m.start())+1
        if a<=line<=b:continue
        try:
            opening=clean.find('(',m.start(),m.end());close=_match_pair(clean,opening,'(',')');end=close+1;cur=_skip_ws(clean,end)
            if cur<len(clean) and clean[cur]==';':end=cur+1
            spans.append((m.start(),end))
        except Exception:pass
    masked=_mask(source_text,spans)
    dummy="\nvoid __c5d_body_entry(void){pthread_t t; pthread_create(&t,NULL,"+loop['function']+",NULL);}\n"
    return masked+dummy


def _c5d_body_model(loop,spans,bases,raw,dag,source_text,clean,line,base_name):
    a,b=loop['body_start'],loop['body_end']
    while a<=b and not clean.splitlines()[a-1].strip():a+=1
    while b>=a and not clean.splitlines()[b-1].strip():b-=1
    body_raw=[base for base in bases if base['function']==loop['function'] and max(base['start_line'],a)<=min(base['end_line'],b)]
    require(body_raw,'C5d Loop body has no source segment')
    body_bases=[]
    for base in body_raw:
        clipped=copy.deepcopy(base);clipped['start_line']=max(base['start_line'],a);clipped['end_line']=min(base['end_line'],b);body_bases.append(clipped)
    body_ids={base['seg_id'] for base in body_bases}
    locations={n:l for n,l in raw.get('node_locations',{}).items() if l['function']==loop['function'] and a<=l['line']<=b}
    node_map={n:sid for n,sid in raw.get('node_to_segment',{}).items() if sid in body_ids}
    payload=copy.deepcopy(raw);payload.update(segments=body_bases,node_locations=locations,node_to_segment=node_map,thread_entry_functions=[loop['function']])
    body_edges=[copy.deepcopy(e) for e in dag['edges'] if e['src'] in body_ids and e['dst'] in body_ids]
    # The synthetic C2b body masks formal semaphore calls; raw sem_dep rows are
    # projected once, later, from the original Level-2 graph.  They must not be
    # fed to C2b's ordinary graph builder (a collapsed raw sem pair is a legal
    # witness here, not an intra edge cycle).
    c2b_edges=[copy.deepcopy(e) for e in body_edges if e.get('kind')!='sem_dep']
    synthetic=_c5d_synthetic_body_source(source_text,clean,body_bases,loop)
    st,sd,s,dd,meta=build_lock_path_views(base_name=base_name,source_text=synthetic,base_segments_json=payload,base_dag_json=dict(nodes=list(body_ids),edges=c2b_edges))
    # A body view can contain a source Lock from another sibling Loop; C2b
    # represents that non-owner Lock as an empty group.  It is not a Lock
    # instance of this owner and must never be cloned into this iteration.
    s['lock_groups']=[group for group in s.get('lock_groups',[])
                      if group.get('members') and group.get('candidate_segment_ids')]
    return st,sd,s,dd,dict(loop_id=loop['loop_id'],function=loop['function'],a=a,b=b,body_ids=body_ids)


def _remap_condition_record(value,cidmap):
    if isinstance(value,dict):
        out={}
        for k,v in value.items():
            if k=='exclusive_group' and v in cidmap:out[k]=cidmap[v]
            elif k=='path_id' and isinstance(v,str):
                out[k]=v
                for old,new in cidmap.items():out[k]=out[k].replace(old,new)
            else:out[k]=_remap_condition_record(v,cidmap)
        return out
    if isinstance(value,list):return [_remap_condition_record(v,cidmap) for v in value]
    return value


def _clone_body_iteration(loop_meta,body_st,body_sd,body_s,body_dd,fields,scope):
    cid={r['condition_id']:scope+'::'+r['condition_id'] for r in body_s['condition_regions']}
    tmap={t['template_id']:'C5DTPL:'+scope+':'+t['template_id'] for t in body_s.get('templates',[])}
    def rid(x):return scope+'::'+str(x)
    sched=[];schedmap={}
    for original in body_s['segments']:
        new=copy.deepcopy(original);new.update(fields,seg_id=rid(original['seg_id']))
        new=_remap_condition_record(new,cid)
        if new.get('template_id') and new['template_id'] in tmap:new['template_id']=tmap[new['template_id']]
        if new.get('kind')=='lock_path_candidate':
            new['path_id']=rid(new.get('path_id',''))
            new['member_segment_ids']=[rid(x) for x in new.get('member_segment_ids',[])]
            new['member_template_ids']=[tmap.get(x,x) for x in new.get('member_template_ids',[])]
            new['source_structural_nodes']=[rid(x) for x in new.get('source_structural_nodes',[])]
            if new.get('lock_group'):
                g=copy.deepcopy(new['lock_group']);g['group_id']=g.get('group_id','LOCKGROUP')+'@'+scope;g.update(fields);new['lock_group']=g;new['lock_group_id']=g['group_id']
        sched.append(new);schedmap[original['seg_id']]=new['seg_id']
    sedges=[]
    for original in body_dd['edges']:
        new=copy.deepcopy(original);new.update(src=rid(new['src']),dst=rid(new['dst']))
        new=_remap_condition_record(new,cid);new['loop_iteration_id']=scope
        sedges.append(new)
    sends=set(new['seg_id'] for new in sched)
    indeg={n:0 for n in sends};outdeg={n:0 for n in sends}
    for e in sedges:
        if e['src'] in indeg:outdeg[e['src']]+=1
        if e['dst'] in indeg:indeg[e['dst']]+=1
    entries=[n for n in sends if indeg[n]==0];exits=[n for n in sends if outdeg[n]==0]
    # Struct view clone (condition controls included).
    struct=[];smap={}
    for original in body_st['segments']:
        new=copy.deepcopy(original);new.update(fields,seg_id=rid(original['seg_id']))
        new=_remap_condition_record(new,cid)
        if new.get('condition_id') and new['condition_id'] in cid:new['condition_id']=cid[new['condition_id']]
        if new.get('branch_id'):
            for old,newid in cid.items():
                if new['branch_id'].startswith(old+':'):new['branch_id']=newid+new['branch_id'][len(old):];break
        struct.append(new);smap[original['seg_id']]=new['seg_id']
    structedges=[]
    for original in body_sd['edges']:
        new=copy.deepcopy(original);new.update(src=rid(new['src']),dst=rid(new['dst']));new=_remap_condition_record(new,cid);new['loop_iteration_id']=scope;structedges.append(new)
    snodes=set(new['seg_id'] for new in struct);sind={n:0 for n in snodes};sout={n:0 for n in snodes}
    for e in structedges:
        if e['src'] in sind:sout[e['src']]+=1
        if e['dst'] in sind:sind[e['dst']]+=1
    sentries=[n for n in snodes if sind[n]==0];sexits=[n for n in snodes if sout[n]==0]
    regions=[]
    for original in body_s['condition_regions']:
        new=copy.deepcopy(original);new.update(fields,condition_id=cid[original['condition_id']],source_condition_id=original['condition_id'])
        new['parent_id']=cid.get(new['parent_id'],new['parent_id']);regions.append(new)
    groups=[]
    for group in body_s.get('lock_groups',[]):
        g=copy.deepcopy(group);g['group_id']=(g.get('group_id') or ('LOCKGROUP:'+g.get('mutex_id','')))+'@'+scope;g.update(fields)
        g['candidate_segment_ids']=[rid(x) for x in g.get('candidate_segment_ids',[])]
        g['members']=_remap_condition_record(g.get('members',[]),cid)
        groups.append(g)
    templates=[]
    for original in body_s.get('templates',[]):
        t=copy.deepcopy(original);t['template_id']=tmap[original['template_id']];t.update(fields)
        templates.append(t)
    return dict(fields=fields,scope=scope,sched=sched,sedges=sedges,entries=entries,exits=exits,
                struct=struct,structedges=structedges,sentries=sentries,sexits=sexits,regions=regions,groups=groups,templates=templates,
                cid=cid,tmap=tmap,struct_map=smap,loop_id=loop_meta['loop_id'])


def build_loop_condition_views(*,base_name,source_text,base_segments_json,base_dag_json):
    raw=copy.deepcopy(base_segments_json);dag=copy.deepcopy(base_dag_json);bases=raw['segments'];rawids={r['seg_id']:r for r in bases}
    require(len(rawids)==len(bases) and set(rawids)==set(dag['nodes']),'C5d invalid raw IDs')
    _validate_base_provenance(dag)
    _topo_with_collapsed_sem_witnesses(dag['nodes'],dag['edges'])
    clean=_sanitize_c(source_text);lines=clean.splitlines();line=lambda o:clean.count('\n',0,o)+1
    require(not re.search(r'^\s*#\s*(?!include\b)\w+',_strip_comments_and_strings(source_text),re.M),'C5d rejects active directives')
    formal,sources=_formal_function_spans(raw);ranges=_condition_function_ranges(source_text,bases,formal)
    analysis=analyze_conditionals(source_text,ranges);require(not analysis.diagnostics and len(analysis.regions)==len(re.findall(r'\bif\b',clean)),'C5d invalid or unowned Condition')
    _condition_trees(analysis.regions)
    spans=[]
    for m in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,m.end()),'(',')');brace=_skip_ws(clean,close+1);end=_match_pair(clean,brace,'{','}')
        header=line(m.start());exit_line=line(end);owners=[fn for fn,(lo,hi) in ranges.items() if lo<=header<=exit_line<=hi]
        require(len(owners)==1,'C5d Loop has no formal function owner')
        spans.append(dict(start=m.start(),brace=brace,end=end,header=header,exit=exit_line,function=owners[0],body_low=line(brace)+1,body_high=exit_line-1))
    require(1<=len(spans)<=2,'C5d supports only Loop L1/L2/L3')
    assignments={};external_all=[];segment_functions={r['function'] for r in bases}
    for region in analysis.regions:
        containers=[p for p in spans if p['brace']<region.start_offset and region.end_offset<=p['end']]
        if not containers:
            for p in spans:
                require(not (max(region.start_offset,p['start'])<=min(region.end_offset,p['end'])),'C5d Condition crosses Loop body boundary')
            external_all.append(region);continue
        owner=max(containers,key=lambda p:p['start']);assignments.setdefault(owner['header'],[]).append(region)
    external_regions=[r for r in external_all if r.function in segment_functions];helper_regions=[r for r in external_all if r.function not in segment_functions]
    try:all_locks=match_lock_regions(source_text,ranges)
    except LockRegionError as exc:require(False,'C5d '+str(exc))
    inside_locks=[];outside_locks=[]
    for lock in all_locks:
        if lock['function'] not in {p['function'] for p in spans}:outside_locks.append(lock);continue
        containers=[p for p in spans if p['function']==lock['function'] and p['brace']<lock['start_offset'] and lock['end_offset']<=p['end']]
        if not containers:outside_locks.append(lock);continue
        owner=max(containers,key=lambda p:p['start'])
        conds=[r for r in analysis.regions if r.function==lock['function'] and lock['start_offset']<=r.start_offset and r.end_offset<=lock['end_offset']]
        for region in analysis.regions:
            require(not (region.start_offset<lock['start_offset'] and lock['end_offset']<region.end_offset),'C5d Lock inside Condition is unsupported')
        if conds:
            require(len([r for r in conds if r.parent_id is None])==1,'C5d one Condition tree per Lock')
            lock['condition_region_ids']=[r.condition_id for r in conds];lock['loop_header']=owner['header'];inside_locks.append(lock)
        else:
            lock['loop_header']=owner['header'];inside_locks.append(lock)
    require(inside_locks,'C5d missing Lock inside a Loop body')
    target_by_loop={}
    for h,conditions in assignments.items():
        require(1<=len(conditions)<=2 and len([r for r in conditions if r.parent_id is None])==1,'C5d one bounded Condition tree per Loop')
        containing=[lock for lock in inside_locks if lock.get('condition_region_ids') and all(lock['start_offset']<=r.start_offset and r.end_offset<=lock['end_offset'] for r in conditions)]
        require(len(containing)==1,'C5d Condition must stay inside one Lock critical region')
        target=containing[0];target_by_loop[h]=target
        for lock in inside_locks:
            if lock is target:continue
            overlap=max(lock['start_offset'],target['start_offset'])<=min(lock['end_offset'],target['end_offset'])
            require(not overlap,'C5d overlapping Lock regions are unsupported')
    external_conditions=[];external_view=None
    if external_regions:
        external_view=_map_external_condition_view(base_name=base_name,source_text=source_text,
            body_regions=[r for values in assignments.values() for r in values],raw=raw,dag=dag,ranges=ranges,external_regions=external_regions)
        external_conditions=external_view[4]
    basis_rows=raw['segments'];basis_dag=dag
    if external_view is not None:
        basis_rows=external_view[0]['segments'];basis_dag=external_view[1]
    # top-level loop structure, all Conditions/Locks masked for the loop parser
    offsets=[0]+[m.end() for m in re.finditer('\n',source_text)]
    pthread_lines=[(offsets[i],offsets[i+1] if i+1<len(offsets) else len(source_text)) for i,text in enumerate(lines) if re.search(r'\bpthread_\w+\b',text)]
    flat=_mask(flatten_condition_headers(source_text,analysis),pthread_lines+_formal_sem_spans(clean))
    original_sem_edges=[copy.deepcopy(e) for e in dag['edges'] if e.get('kind')=='sem_dep']
    basis_node_map=_basis_node_map(raw,basis_rows)
    loop_bases,loop_dag,loop_node_to_segment,loop_owner_of,internal_sem_edges=_normalize_loop_owners(raw,basis_rows,basis_dag,spans,basis_node_map,original_sem_edges)
    loop_payload=copy.deepcopy(raw);loop_payload['segments']=loop_bases;loop_payload['node_to_segment']=loop_node_to_segment
    top_st,top_sd,_,_,_=build_loop_nested_views(base_name=base_name,source_text=flat,base_segments_json=loop_payload,base_dag_json=loop_dag)
    loops=copy.deepcopy(top_st['loops']);loopby={l['loop_id']:l for l in loops}
    for row in top_st['segments']:
        if row['kind'] in CONTROL:continue
        memberships=_payload_memberships(external_conditions,row['function'],row['start_line'],row['end_line'])
        if memberships:row['condition_memberships']=memberships
    body_models={};body_meta={}
    for loop in loops:
        target=target_by_loop.get(loop['header_line'])
        if target is None:continue
        span=next(p for p in spans if p['header']==loop['header_line']);loop['body_start']=span['body_low'];loop['body_end']=span['body_high']
        stb,sdb,sb,ddb,meta=_c5d_body_model(loop,spans,bases,raw,dag,source_text,clean,line,base_name)
        body_models[loop['loop_id']]=(stb,sdb,sb,ddb);body_meta[loop['loop_id']]=meta
        loop['source_condition_ids']=[r.condition_id for r in assignments.get(loop['header_line'],[])]
    regions=copy.deepcopy(external_conditions);instances=[];groups_all=[];templates_all=[];source_to_instances={r.condition_id:[] for r in analysis.regions};source_component_templates=[];component_targets={};lock_templates=[];source_lock_map={}
    # Source Lock templates are static source identities, independent of N=0 instances.
    for lid,(stb,sdb,sb,ddb) in body_models.items():
        for group in sb.get('lock_groups',[]):
            for lock in group.get('members',[]):
                key=lock.get('function','')+'@'+str(lock.get('lock_range',[0])[0])
                source_lock_map.setdefault(key,[])
                lt=copy.deepcopy(lock);lt.update(loop_id=lid,analysis_only=True)
                if not any(x.get('function')==lt.get('function') and x.get('lock_range')==lt.get('lock_range') and x.get('mutex_id')==lt.get('mutex_id') for x in lock_templates):lock_templates.append(lt)
    # Clone body per iteration and replace iteration rows.
    sched_rows=[];struct_rows=[];sched_edges=[];struct_edges=[];replacement={};struct_replacement={}
    for row in top_st['segments']:
        if row.get('role')=='iteration' and row.get('loop_id') in body_models:
            stb,sdb,sb,ddb=body_models[row['loop_id']];fields=_iteration_fields(row);scope=fields['loop_iteration_id']
            clone=_clone_body_iteration(body_meta[row['loop_id']],stb,sdb,sb,ddb,fields,scope)
            sched_rows.extend(clone['sched']);struct_rows.extend(clone['struct']);sched_edges.extend(clone['sedges']);struct_edges.extend(clone['structedges'])
            replacement[row['seg_id']]=(clone['entries'],clone['exits']);struct_replacement[row['seg_id']]=(clone['sentries'],clone['sexits'])
            instances.append(dict(fields,original_iteration_segment=row['seg_id'],entry=clone['entries'],exit=clone['exits'],lock_groups=[g['group_id'] for g in clone['groups']]))
            for r in clone['regions']:
                regions.append(r)
                if r.get('source_condition_id') in source_to_instances:source_to_instances[r['source_condition_id']].append(r['condition_id'])
            # source components and instance templates
            for t in clone['templates']:templates_all.append(t)
            for src_t in sb.get('templates',[]):
                sid='C5DSRC:'+row['loop_id']+'::'+src_t['template_id'];component_targets.setdefault(sid,[])
                clone_tid='C5DTPL:'+scope+':'+src_t['template_id']
                if clone_tid in [t['template_id'] for t in clone['templates']]:
                    component_targets[sid].append(clone_tid)
            for g in clone['groups']:
                g['function']='<lockgroup>';groups_all.append(g)
                for lock in g.get('members',[]):
                    key=lock.get('function','')+'@'+str(lock.get('lock_range',[0])[0]);source_lock_map.setdefault(key,[])
                    if g['group_id'] not in source_lock_map[key]:source_lock_map[key].append(g['group_id'])
            continue
        sched_rows.append(copy.deepcopy(row));struct_rows.append(copy.deepcopy(row))
    # source components for every body template with zero/effective targets
    seen_src=set()
    for lid,(stb,sdb,sb,ddb) in body_models.items():
        for t in sb.get('templates',[]):
            sid='C5DSRC:'+lid+'::'+t['template_id']
            if sid in seen_src:continue
            seen_src.add(sid)
            source_component_templates.append(dict(template_id=sid,loop_id=lid,function=t.get('function'),role='condition_component_source',analysis_only=True,start_line=t.get('start_line'),end_line=t.get('end_line')))
            component_targets.setdefault(sid,[])
    for edge in top_sd['edges']:
        srcs=replacement.get(edge['src'],([edge['src']],[edge['src']]))[1] if edge['src'] in replacement else [edge['src']]
        dsts=replacement.get(edge['dst'],([edge['dst']],[edge['dst']]))[0] if edge['dst'] in replacement else [edge['dst']]
        for a in srcs:
            for b in dsts:
                out=copy.deepcopy(edge);out.update(src=a,dst=b);sched_edges.append(out)
        ssrcs=struct_replacement.get(edge['src'],([edge['src']],[edge['src']]))[1] if edge['src'] in struct_replacement else [edge['src']]
        sdsts=struct_replacement.get(edge['dst'],([edge['dst']],[edge['dst']]))[0] if edge['dst'] in struct_replacement else [edge['dst']]
        for a in ssrcs:
            for b in sdsts:
                out=copy.deepcopy(edge);out.update(src=a,dst=b);struct_edges.append(out)
    for e in sched_edges:
        if e.get('exclusive_group'):pass
    # Assign stable execution templates to top-level ordinary rows (main/create/join/before/after).
    for r in [x for x in sched_rows if x['kind'] not in CONTROL and not x.get('source_only')]:
        if r['kind']=='lock_path_candidate' or r.get('template_id'):continue
        tid='C5DTPL:'+r['seg_id']
        r['template_id']=tid
        if tid not in {t['template_id'] for t in templates_all}:
            templates_all.append(dict(template_id=tid,role='execution',function=r['function'],start_line=r['start_line'],end_line=r['end_line']))
    # Top-level loop control templates (before/after and any untouched Loop body).
    existing_tids={t['template_id'] for t in templates_all}
    for t in top_st.get('templates',[]):
        if t['template_id'] not in existing_tids:
            templates_all.append(copy.deepcopy(t));existing_tids.add(t['template_id'])
    for region in regions:
        if region.get('loop_iteration_id'):continue
        source_id=region.get('source_condition_id')
        if source_id is None and not source_to_instances.get(region['condition_id']):source_to_instances[region['condition_id']]=[region['condition_id']]
    # lock-group membership maps
    grouped_segments={g['group_id']:g.get('candidate_segment_ids',[]) for g in groups_all}
    iteration_to_group={cand:g['group_id'] for g in groups_all for cand in g.get('candidate_segment_ids',[])}
    # sem projection on final scheduled execution rows
    exec_rows=[r for r in sched_rows if r['kind'] not in CONTROL and not r.get('source_only')]
    projected_sem,absorbed_sem=_project_internal_sem_edges(internal_sem_edges,exec_rows,raw,clean,regions,analysis.regions)
    sched_edges.extend(projected_sem)
    # project control nodes for scheduling
    active={r['seg_id']:r for r in sched_rows}
    graph=dict(base_name=base_name,nodes=list(active),edges=sched_edges,exclusive_groups={r['condition_id']:[r['condition_id']+':'+b['branch_id'] for b in r['branches']] for r in regions})
    proj=_project(sched_rows,graph)
    final=[r for r in sched_rows if r['kind'] not in CONTROL and not r.get('source_only')]
    topo_sort([r['seg_id'] for r in final],[(e['src'],e['dst']) for e in proj])
    # Conditions in helpers with no old Level-2 segment remain struct-only.
    helper_functions=sorted({r.function for r in helper_regions})
    helper_views=_helper_condition_views(base_name=base_name,source_text=source_text,raw=raw,sources=sources,functions=helper_functions,prefix='C5DHELPER') if helper_functions else None
    helper_rows=[];helper_edges=[];helper_conditions=[];helper_segment_rows=[];helper_segment_to_struct={};helper_position_to_struct={}
    if helper_views is not None:
        helper_struct,helper_dag,helper_exec,helper_segments=helper_views
        helper_exec_by_id={r['seg_id']:r for r in helper_exec['segments']}
        helper_segment_rows=[dict(copy.deepcopy(r),source_only=True,analysis_only=True) for r in helper_segments]
        for original in helper_struct['segments']:
            row=copy.deepcopy(original);row.update(source_only=True,analysis_only=True)
            if row['kind'] not in CONTROL:
                members=helper_exec_by_id.get(row['seg_id'],{}).get('condition_memberships')
                if members:row['condition_memberships']=copy.deepcopy(members)
                fn=row['function'];row['source_position_ids']=['C5DHELPER:'+fn+':line:'+str(n) for n in range(row['start_line'],row['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
            helper_rows.append(row)
            for sid in row.get('source_segments',[]):
                if sid.startswith('C5DHELPER:'):helper_segment_to_struct.setdefault(sid,[]).append(row['seg_id'])
            for key in row.get('source_position_ids',[]):helper_position_to_struct.setdefault(key,[]).append(row['seg_id'])
        helper_edges=[copy.deepcopy(e) for e in helper_dag['edges']]
        helper_conditions=[copy.deepcopy(r) for r in helper_struct['condition_regions']]
        struct_rows.extend(copy.deepcopy(helper_rows));struct_edges.extend(_dedupe_edges(list(struct_edges)+copy.deepcopy(helper_edges)))
        regions=sorted(regions+helper_conditions,key=lambda r:(r['function'],r['entry_line'],r['exit_line'],r['condition_id']))
        for region in helper_conditions:
            if region['condition_id'] in source_to_instances:source_to_instances[region['condition_id']].append(region['condition_id'])
    # mappings
    oldmap={}
    for b in bases:
        oldmap[b['seg_id']]=_ordered_unique(r['seg_id'] for r in final if b['seg_id'] in (r.get('source_segments') or []))
    generation={n:oldmap[sid] for n,sid in raw.get('node_to_segment',{}).items() if not is_ignored_compiler_fn(n)}
    positions={}
    for r in final:
        for key in r.get('source_position_ids',[]):positions.setdefault(key,[]).append(r['seg_id'])
    template_targets={t['template_id']:[] for t in templates_all}
    for r in final:
        if r.get('kind')=='lock_path_candidate':
            for tid in r.get('member_template_ids',[]):
                if tid in template_targets and r['seg_id'] not in template_targets[tid]:template_targets[tid].append(r['seg_id'])
        elif r.get('template_id') in template_targets and r['seg_id'] not in template_targets[r['template_id']]:template_targets[r['template_id']].append(r['seg_id'])
    for sid,targets in component_targets.items():template_targets[sid]=targets
    # per-iteration conditions namespaced; source condition maps
    mappings=copy.deepcopy(raw.get('mappings',{}));mappings.update(generation_node_to_execution_segments=generation,original_segment_to_execution_segments=oldmap,source_position_to_execution_segments={k:_ordered_unique(v) for k,v in positions.items()},template_to_execution_segments=template_targets,iteration_to_group=iteration_to_group,grouped_node_to_member_segments=grouped_segments,source_lock_region_to_group=source_lock_map,source_lock_region_to_instances=source_lock_map,source_condition_to_instances=source_to_instances,absorbed_internal_sem_dependencies=copy.deepcopy(absorbed_sem))
    if helper_conditions:
        mappings['source_only_segment_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_segment_to_struct.items())}
        mappings['source_position_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_position_to_struct.items())}
    common=dict(base_name=base_name,rule_name=RULE,level='level2',source_file=raw['source_file'],source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),thread_entry_functions=copy.deepcopy(raw.get('thread_entry_functions',[])),loops=loops,condition_regions=regions,condition_templates=[],iteration_instances=instances,templates=sorted(templates_all,key=lambda t:t['template_id']),source_component_templates=sorted(source_component_templates,key=lambda t:t['template_id']),lock_groups=groups_all,lock_templates=lock_templates,independent_lock_regions=copy.deepcopy(outside_locks),mappings=mappings,composition_edge_provenance=[dict(base_edge_index=i,edge=copy.deepcopy(e)) for i,e in enumerate(dag['edges'])],composition_input_sha256=digest([raw,dag]),capabilities=dict(scheduler_compatible=True,timing_compatible=False,instrumentation_compatible=False,analysis_only=True))
    if helper_conditions:common['source_only_segments']=copy.deepcopy(helper_segment_rows)
    struct_graph=dict(base_name=base_name,nodes=[r['seg_id'] for r in struct_rows],edges=struct_edges,exclusive_groups=dict(common['mappings'].get('exclusive_groups',{})))
    dd=dict(base_name=base_name,nodes=[r['seg_id'] for r in final],edges=proj,exclusive_groups=common['exclusive_groups'] if 'exclusive_groups' in common else {r['condition_id']:[r['condition_id']+':'+b['branch_id'] for b in r['branches']] for r in regions})
    return dict(copy.deepcopy(common),view='struct',segments=struct_rows),struct_graph,dict(common,view='sched',segments=final),dd,dict(rule_name=RULE,level='level2',scheduler_compatible=True,analysis_only=True)
