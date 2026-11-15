"""C5c: static outer Loop iterations contain independent Condition instances with branch-local Locks."""
import copy,hashlib,json,re
from types import SimpleNamespace
from .composition_model import CONTROL,require,_mask,_project,_ordered_unique,digest
from .condition_model import _sanitize_c,analyze_conditionals,build_condition_views,_match_pair,_skip_ws
from .composition_c2a_model import flatten_condition_headers,contract_branch_groups
from .lock_regions import match_lock_regions,LockRegionError
from .composition_c3a_model import _template_targets
from .composition_c4a_model import _formal_function_spans,_condition_function_ranges,_helper_condition_views
from .composition_c4b_model import _dedupe_edges
from .loop_nested_model import build_loop_nested_views,_check_induction
from ..core.func_ranges import _strip_comments_and_strings,is_ignored_compiler_fn
from .algo.common import topo_sort
RULE='composition_c5c'


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
        require(len(tree)<=2,'C5c supports Condition C1-C4 only: '+root.condition_id)
    return roots


def _lock_leaf(lock,regions):
    leaves=[(region,branch) for region,branch in _condition_leaves(regions)
            if branch.start_offset<=lock['start_offset'] and lock['end_offset']<=branch.end_offset]
    if leaves:
        require(len(leaves)==1,'C5c Lock must stay inside one Condition leaf')
        return leaves[0]
    for region in regions:
        overlap=max(lock['start_offset'],region.start_offset)<=min(lock['end_offset'],region.end_offset)
        require(not overlap,'C5c Lock crosses Condition branch boundary')
    return None


def _classify_locks(locks,regions,loops):
    inside=[];outside=[]
    for lock in locks:
        leaf=_lock_leaf(lock,regions)
        if leaf is None:
            for loop in loops:
                overlap=max(lock['start_offset'],loop['start'])<=min(lock['end_offset'],loop['end'])
                require(not overlap,'C5c Lock inside a Loop without Condition is unsupported')
            outside.append(lock);continue
        region,branch=leaf
        # A nested Loop inside the Condition or Lock is a different arrangement.
        for loop in loops:
            if loop['start']<lock['start_offset'] and lock['end_offset']<=loop['end']:
                continue  # the outer Loop containing this Condition/Lock
            if lock['start_offset']<loop['start'] and loop['end']<lock['end_offset']:
                require(False,'C5c Loop inside Lock is unsupported')
        containers=[]
        for loop in loops:
            if loop['start']<region.start_offset and region.end_offset<=loop['end']:
                containers.append(loop);continue
            overlap=max(region.start_offset,loop['start'])<=min(region.end_offset,loop['end'])
            if overlap:
                require(not (region.start_offset<loop['start'] and loop['end']<region.end_offset),
                        'C5c Loop inside Condition is unsupported')
                require(False,'C5c Loop crosses Condition boundary')
        require(containers,'C5c Condition Lock must stay inside one Loop body')
        owner=max(containers,key=lambda item:item['start'])
        lock['condition_region_id']=region.condition_id;lock['condition_branch_id']=branch.branch_id;lock['loop_header']=owner['header']
        inside.append(lock)
    require(inside,'C5c missing Lock inside a Loop-body Condition leaf')
    return inside,outside


def loop_condition_lock_overlap(text):
    """Route Loop->Condition leaf->complete Lock to C5c, without C5a/C5b ambiguity."""
    clean=_sanitize_c(text);regions=analyze_conditionals(text,{'<source>':(1,len(clean.splitlines()))}).regions
    leaves=_condition_leaves(regions);loops=[]
    for match in re.finditer(r'\bfor\b',clean):
        try:
            close=_match_pair(clean,_skip_ws(clean,match.end()),'(',')');end=_match_pair(clean,_skip_ws(clean,close+1),'{','}')
        except Exception:continue
        loops.append((match.start(),end))
    stack=[];locks=[]
    for match in re.finditer(r'\bpthread_mutex_(lock|unlock)\s*\(\s*&\s*\w+\s*\)\s*;',clean):
        if match.group(1)=='lock':stack.append(match.start())
        elif stack:locks.append((stack.pop(),match.end()))
    for lock_start,lock_end in locks:
        for region,branch in leaves:
            if not (branch.start_offset<=lock_start and lock_end<=branch.end_offset):continue
            for loop_start,loop_end in loops:
                if not (loop_start<region.start_offset and region.end_offset<=loop_end):continue
                if lock_start<loop_start and loop_end<lock_end:continue
                if region.start_offset<loop_start and loop_end<region.end_offset:continue
                return True
    return False


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
    """Reject malformed formal witnesses before any C5c projection or publication.

    Opaque dictionaries without base_edge_index remain valid source annotations.
    Their order and nested content are preserved untouched.
    """
    nodes={str(node) for node in dag.get('nodes',[])}
    edge_count=len(dag.get('edges',[]))
    for edge in dag.get('edges',[]):
        provenance=edge.get('provenance',[])
        require(isinstance(provenance,list),'C5c provenance must be a list')
        for item in provenance:
            require(isinstance(item,dict),'C5c provenance entries must be dictionaries')
            if 'base_edge_index' not in item:continue
            index=item['base_edge_index']
            require(type(index) is int,'C5c malformed internal dependency witness: base_edge_index')
            require(0<=index<edge_count,'C5c internal dependency witness index out of range')
            witness=item.get('edge')
            require(isinstance(witness,dict),'C5c malformed internal dependency witness: edge')
            require(all(key in witness for key in ('src','dst','kind')),'C5c malformed internal dependency witness: edge fields')
            require(isinstance(witness.get('kind'),str) and bool(witness['kind']),'C5c malformed internal dependency witness: kind')
            require(all(isinstance(witness[key],str) and witness[key] in nodes for key in ('src','dst')),
                    'C5c malformed internal dependency witness: endpoints')


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
        require(selected,'C5c Loop family has no Level-2 owner: '+function)
        owner=dict(seg_id='C5COWNER:'+function+':'+str(index),function=function,kind='compute',
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
            require(kind=='intra','C5c Loop owner absorbs non-intra dependency');absorbed.append(copy.deepcopy(edge));continue
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
        if not values and (not source_ids or all(str(sid).startswith('C5COWNER:') for sid in source_ids)):
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
    require(not external_analysis.diagnostics,'C5c invalid external Condition')
    expected={(r.function,r.entry_line,r.exit_line):r for r in external_regions}
    actual={(r.function,r.entry_line,r.exit_line):r for r in external_analysis.regions}
    require(set(expected)==set(actual),'C5c external Condition ownership conflict')
    id_map={};branch_map={}
    for key,original in expected.items():
        region=actual[key];id_map[region.condition_id]=original.condition_id
        require(len(region.branches)==len(original.branches),'C5c external Condition branch conflict')
        for left,right in zip(region.branches,original.branches):
            require((left.start_line,left.end_line)==(right.start_line,right.end_line),'C5c external Condition branch range conflict')
            branch_map[(region.condition_id,left.branch_id)]=right.branch_id
    cs,cd,ce,ced,meta=build_condition_views(base_name=base_name,source_text=external_source,
                                            base_segments_json=raw,base_dag_json=dag)
    require(meta['scheduler_compatible'],'C5c incompatible external Condition: '+str(meta['blocked_reasons']))
    require(len(cs['condition_regions'])==len(external_analysis.regions),'C5c external Condition view is incomplete')
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


def _project_internal_sem_edges(sem_edges,execution,raw,clean):
    """Project an owner-internal sem dependency onto concrete iteration nodes.

    A native sem_dep may connect two Level-2 rows that both fall inside the
    same normalized Loop owner.  It must become one edge per concrete iteration
    (L1) or per (outer,inner) coordinate (L3), never a cross-iteration edge.
    """
    by_raw={};raw_by_id={row['seg_id']:row for row in raw.get('segments',[])}
    sem_lines=[]
    for match in _SEM_CALL_RE.finditer(clean):
        name=match.group(0).split('(',1)[0]
        if name in ('sem_post','sem_wait'):sem_lines.append((name,clean.count('\n',0,match.start())+1))
    for row in execution:
        for sid in row.get('source_segments') or []:by_raw.setdefault(sid,[]).append(row)
    def focus(raw_id,name):
        base=raw_by_id.get(raw_id)
        if base is None:return None
        lines=[line for call,line in sem_lines if call==name and base['start_line']<=line<=base['end_line']]
        return lines[0] if len(lines)==1 else None
    projected=[];absorbed=[]
    for edge in sem_edges:
        sources=by_raw.get(edge.get('src'),[]);destinations=by_raw.get(edge.get('dst'),[])
        if not sources and not destinations:continue
        # A zero-trip Loop has no iteration rows at all; an external row that
        # happens to overlap the raw segment does not make this edge inhabited.
        sources=[row for row in sources if row.get('loop_iteration_id') is not None]
        destinations=[row for row in destinations if row.get('loop_iteration_id') is not None]
        if not sources and not destinations:continue
        require(sources and destinations,'C5c sem_dep endpoint has no iteration instance: '+str(edge.get('src'))+' -> '+str(edge.get('dst')))
        src_focus=focus(edge.get('src'),'sem_post');dst_focus=focus(edge.get('dst'),'sem_wait')
        source_candidates=[row for row in sources if src_focus is None or row['start_line']<=src_focus<=row['end_line']]
        destination_candidates=[row for row in destinations if dst_focus is None or row['start_line']<=dst_focus<=row['end_line']]
        require(source_candidates and destination_candidates,'C5c sem_dep endpoint has no Condition component instance')
        src_ids={row.get('loop_iteration_id') for row in source_candidates}
        dst_ids={row.get('loop_iteration_id') for row in destination_candidates}
        shared=src_ids&dst_ids
        if not shared:
            require(False,'C5c sem_dep crosses Loop iteration identity')
        for identity in sorted(shared,key=str):
            left=[row for row in source_candidates if row.get('loop_iteration_id')==identity]
            right=[row for row in destination_candidates if row.get('loop_iteration_id')==identity]
            pairs=[]
            for source in left:
                for destination in right:
                    if _compatible_paths(_row_path(source),_row_path(destination)):pairs.append((source,destination))
            require(pairs,'C5c sem_dep crosses Condition branch')
            require(len(pairs)==1,'C5c sem_dep projection is ambiguous')
            source,destination=pairs[0]
            if source['seg_id']==destination['seg_id']:
                absorbed.append(dict(edge=copy.deepcopy(edge),projected_src=source['seg_id'],projected_dst=destination['seg_id']))
                continue
            out=copy.deepcopy(edge);out.update(src=source['seg_id'],dst=destination['seg_id'])
            projected.append(out)
    # Stable de-duplication while retaining every source witness.
    seen={}
    for edge in projected:
        key=json.dumps({k:v for k,v in edge.items() if k!='provenance'},sort_keys=True)
        if key not in seen:seen[key]=edge
        else:
            seen[key].setdefault('provenance',[]).extend(copy.deepcopy(edge.get('provenance',[])))
    return list(seen.values()),absorbed


def build_loop_condition_views(*,base_name,source_text,base_segments_json,base_dag_json):
    raw=copy.deepcopy(base_segments_json);dag=copy.deepcopy(base_dag_json);bases=raw['segments'];rawids={r['seg_id']:r for r in bases}
    require(len(rawids)==len(bases) and set(rawids)==set(dag['nodes']),'C3b invalid raw IDs')
    _validate_base_provenance(dag)
    topo_sort(dag['nodes'],[(e['src'],e['dst']) for e in dag['edges']])
    clean=_sanitize_c(source_text);lines=clean.splitlines();line=lambda offset:clean.count('\n',0,offset)+1
    require(not re.search(r'^\s*#\s*(?!include\b)\w+',_strip_comments_and_strings(source_text),re.M),'C3b rejects active directives')
    formal,sources=_formal_function_spans(raw)
    ranges=_condition_function_ranges(source_text,bases,formal)
    analysis=analyze_conditionals(source_text,ranges);require(not analysis.diagnostics and len(analysis.regions)==len(re.findall(r'\bif\b',clean)),'C5c invalid or unowned Condition')
    _condition_trees(analysis.regions)
    spans=[]
    for m in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,m.end()),'(',')');brace=_skip_ws(clean,close+1);end=_match_pair(clean,brace,'{','}')
        header=line(m.start());exit_line=line(end)
        owners=[fn for fn,(lo,hi) in ranges.items() if lo<=header<=exit_line<=hi]
        require(len(owners)==1,'C5c Loop has no formal function owner')
        spans.append(dict(start=m.start(),brace=brace,end=end,header=header,exit=exit_line,function=owners[0],body_low=line(brace)+1,body_high=exit_line-1))
    require(1<=len(spans)<=2,'C5c supports only Loop L1/L2/L3')
    assignments={};external_all=[];segment_functions={r['function'] for r in bases}
    for region in analysis.regions:
        containers=[p for p in spans if p['brace']<region.start_offset and region.end_offset<=p['end']]
        if not containers:
            for p in spans:
                overlap=max(region.start_offset,p['start'])<=min(region.end_offset,p['end'])
                require(not overlap,'C5c Condition crosses Loop body boundary')
            external_all.append(region);continue
        owner=max(containers,key=lambda p:p['start']);assignments.setdefault(owner['header'],[]).append(region)
    external_regions=[r for r in external_all if r.function in segment_functions]
    helper_regions=[r for r in external_all if r.function not in segment_functions]
    require(assignments,'C5c missing Condition body')
    for h,conditions in assignments.items():
        require(1<=len(conditions)<=2 and len([r for r in conditions if r.parent_id is None])==1,'C5c one bounded Condition tree per Loop, not sequential trees')
        require(all(r.parent_id is None or any(p.condition_id==r.parent_id for p in conditions) for r in conditions),'C5c cross-body Condition parent')
    try:all_locks=match_lock_regions(source_text,ranges)
    except LockRegionError as exc:require(False,'C5c '+str(exc))
    inside_locks,outside_locks=_classify_locks(all_locks,analysis.regions,spans)
    for h,conditions in assignments.items():
        require(any(lock['loop_header']==h for lock in inside_locks),'C5c Condition inside Loop must contain a Lock')
    offsets=[0]+[m.end() for m in re.finditer('\n',source_text)]
    pthread_lines=[(offsets[i],offsets[i+1] if i+1<len(offsets) else len(source_text)) for i,text in enumerate(lines) if re.search(r'\bpthread_\w+\b',text)]
    flat=_mask(flatten_condition_headers(source_text,analysis),pthread_lines+_formal_sem_spans(clean))
    original_sem_edges=[copy.deepcopy(e) for e in dag['edges'] if e.get('kind')=='sem_dep']
    external_conditions=[];external_view=None
    if external_regions:
        external_view=_map_external_condition_view(base_name=base_name,source_text=source_text,
            body_regions=[r for values in assignments.values() for r in values],raw=raw,dag=dag,
            ranges=ranges,external_regions=external_regions)
        external_conditions=external_view[4]
    basis_rows=raw['segments'];basis_dag=dag
    if external_view is not None:
        basis_rows=external_view[0]['segments'];basis_dag=external_view[1]
    basis_node_map=_basis_node_map(raw,basis_rows)
    loop_bases,loop_dag,loop_node_to_segment,loop_owner_of,internal_sem_edges=_normalize_loop_owners(raw,basis_rows,basis_dag,spans,basis_node_map,original_sem_edges)
    loop_payload=copy.deepcopy(raw);loop_payload['segments']=loop_bases;loop_payload['node_to_segment']=loop_node_to_segment
    st,sd,_,_,_=build_loop_nested_views(base_name=base_name,source_text=flat,base_segments_json=loop_payload,base_dag_json=loop_dag)
    _inherit_source_custom(st['segments'],raw)
    loops=copy.deepcopy(st['loops']);loopby={r['loop_id']:r for r in loops};body_templates={};source_conditions=[];source_nodes={};source_to_instances={r.condition_id:[] for r in analysis.regions};condition_nodes={};source_components={};source_component_templates=[];templates=copy.deepcopy(st['templates'])
    for row in st['segments']:
        if row['kind'] in CONTROL:continue
        memberships=_payload_memberships(external_conditions,row['function'],row['start_line'],row['end_line'])
        if memberships:row['condition_memberships']=memberships
    for loop in loops:
        conditions=assignments.get(loop['header_line'],[]);loop['source_condition_ids']=[r.condition_id for r in conditions]
        if not conditions:continue
        function=_condition_body_function(loop,conditions)
        span=next(p for p in spans if p['header']==loop['header_line']);a,b=span['body_low'],span['body_high']
        while a<=b and not lines[a-1].strip():a+=1
        while b>=a and not lines[b-1].strip():b-=1
        variables=[p['variable'] for p in loops if p['header_line']<=loop['header_line'] and loop['exit_line']<=p['exit_line']]
        for region in conditions:_check_induction(region.condition_text,[region.condition_text+';'],variables)
        body_raw=[base for base in bases if base['function']==loop['function'] and max(base['start_line'],a)<=min(base['end_line'],b)]
        require(body_raw,'C5c Loop body has no source segment')
        body_bases=[]
        for base in body_raw:
            clipped=copy.deepcopy(base);clipped['start_line']=max(base['start_line'],a);clipped['end_line']=min(base['end_line'],b);body_bases.append(clipped)
        body_ids={base['seg_id'] for base in body_bases}
        locations={n:l for n,l in raw.get('node_locations',{}).items() if l['function']==loop['function'] and a<=l['line']<=b}
        node_map={n:sid for n,sid in raw.get('node_to_segment',{}).items() if sid in body_ids}
        payload=dict(base_name=base_name,source_file=raw['source_file'],segments=copy.deepcopy(body_bases),node_locations=locations,node_to_segment=node_map)
        cs,cd,ce,_,cm=build_condition_views(base_name=base_name,source_text=source_text,base_segments_json=payload,base_dag_json=dict(nodes=[base['seg_id'] for base in body_bases],edges=[]))
        require(cm['scheduler_compatible'],'C3b incompatible Condition body: '+str(cm['blocked_reasons']))
        _inherit_source_custom(cs['segments'],raw)
        local_analysis=analyze_conditionals(source_text,{loop['function']:(a,b)})
        originals={r.entry_line:r.condition_id for r in conditions};origmap={r['condition_id']:originals[r['entry_line']] for r in cs['condition_regions']}
        for region in local_analysis.regions:
            for row in cs['segments']:
                if row['kind'] in CONTROL and row.get('condition_id')==region.condition_id:continue
                for branch in region.branches:
                    if branch.start_line is not None and branch.start_line<=row['start_line']<=row['end_line']<=branch.end_line:
                        row.setdefault('condition_memberships',[]).append(dict(exclusive_group=region.condition_id,branch_id=branch.branch_id,guard=branch.guard))
        inbound={n:0 for n in cd['nodes']};outbound={n:0 for n in cd['nodes']}
        for e in cd['edges']:inbound[e['dst']]+=1;outbound[e['src']]+=1
        entry=[n for n in cd['nodes'] if inbound[n]==0];exit=[n for n in cd['nodes'] if outbound[n]==0]
        require(len(entry)==len(exit)==1,'C3b body has no unique entry/exit ports')
        old=[t for t in templates if t['role']=='body_template' and t['start_line']==loop['body_start'] and t['end_line']==loop['body_end']]
        require(len(old)==1,'C3b cannot identify source body template')
        loop['source_body_template']=dict(copy.deepcopy(old[0]),analysis_only=True);templates=[t for t in templates if t['template_id']!=old[0]['template_id']]
        loop.update(body_start=a,body_end=b)
        body_templates[loop['loop_id']]=(cs,cd,origmap,entry[0],exit[0])
        source_conditions.append(dict(loop_id=loop['loop_id'],function=function,source_range=[a,b],analysis_only=True,condition_regions=[dict(copy.deepcopy(r),source_condition_id=origmap[r['condition_id']]) for r in cs['condition_regions']]))
        for row in cs['segments']:
            sourceid='C3BSRC:'+loop['loop_id']+'::'+row['seg_id'];source_nodes[sourceid]=copy.deepcopy(row);condition_nodes[sourceid]=[]
            if row['kind'] not in CONTROL:
                source_components[sourceid]=[]
                source_component_templates.append(dict(template_id='C5CSRC:'+sourceid,loop_id=loop['loop_id'],function=function,
                    role='condition_component_source',analysis_only=True,start_line=row['start_line'],end_line=row['end_line'],
                    source_condition_node_id=sourceid,condition_memberships=copy.deepcopy(row.get('condition_memberships',[]))))
    # The old whole-body iteration row is a container, replaced by a complete fork/join graph.
    rows=[];replacement={};edges=[];regions=copy.deepcopy(external_conditions);instances=[];exclusive={}
    if external_view is not None:exclusive.update(copy.deepcopy(external_view[1].get('exclusive_groups',{})))
    for row in st['segments']:
        if row.get('role')!='iteration' or row.get('loop_id') not in body_templates:rows.append(copy.deepcopy(row));continue
        cs,cd,origmap,entry,exit=body_templates[row['loop_id']];fields=_iteration_fields(row);scope=fields['loop_iteration_id'];nid={n:scope+'::'+n for n in cd['nodes']};cid={k:scope+'::'+v for k,v in origmap.items()};replacement[row['seg_id']]=(nid[entry],nid[exit])
        instances.append(dict(fields,original_iteration_segment=row['seg_id'],entry=nid[entry],exit=nid[exit]))
        for original in cs['segments']:
            new=copy.deepcopy(original);sourceid='C3BSRC:'+row['loop_id']+'::'+original['seg_id'];new.update(fields,seg_id=nid[original['seg_id']],role='iteration',source_condition_node_id=sourceid)
            condition_nodes[sourceid].append(new['seg_id'])
            if new.get('condition_id'):new['source_condition_id']=origmap[new['condition_id']];new['condition_id']=cid[new['condition_id']]
            new['condition_memberships']=[dict(m,exclusive_group=cid[m['exclusive_group']]) for m in new.get('condition_memberships',[])]
            if new.get('branch_id'):
                for key in cid:
                    if new['branch_id'].startswith(key+':'):new['branch_id']=cid[key]+new['branch_id'][len(key):];break
            if new['kind'] not in CONTROL:
                tid='C3BTPL:'+new['seg_id'];new['template_id']=tid;new['source_template_id']=sourceid;source_components[sourceid].append(tid)
                templates.append(dict(fields,template_id=tid,source_template_id=sourceid,role='condition_component',function=new['function'],start_line=new['start_line'],end_line=new['end_line'],condition_memberships=copy.deepcopy(new['condition_memberships'])))
            rows.append(new)
        for original in cd['edges']:
            new=copy.deepcopy(original);new.update(src=nid[new['src']],dst=nid[new['dst']],loop_iteration_id=scope)
            if new.get('exclusive_group'):new['exclusive_group']=cid[new['exclusive_group']]
            edges.append(new)
        for original in cs['condition_regions']:
            new=copy.deepcopy(original);local=original['condition_id'];new.update(fields,condition_id=cid[local],source_condition_id=origmap[local],condition_text=next(r.condition_text for r in analysis.regions if r.condition_id==origmap[local]))
            new['parent_id']=cid[new['parent_id']] if new['parent_id'] else None;regions.append(new);source_to_instances[origmap[local]].append(cid[local]);exclusive[cid[local]]=[cid[local]+':'+b['branch_id'] for b in new['branches']]
    for original in sd['edges']:
        new=copy.deepcopy(original)
        if new['src'] in replacement:new['src']=replacement[new['src']][1]
        if new['dst'] in replacement:new['dst']=replacement[new['dst']][0]
        edges.append(new)
    # Conditions in helpers without an old segment stay source-only and outside the combination.
    helper_functions=sorted({r.function for r in helper_regions})
    helper_views=_helper_condition_views(base_name=base_name,source_text=source_text,raw=raw,sources=sources,
                                         functions=helper_functions,prefix='C5CHELPER') if helper_functions else None
    helper_rows=[];helper_edges=[];helper_conditions=[];helper_groups={};helper_segment_rows=[]
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
                fn=row['function'];row['source_position_ids']=['C5CHELPER:'+fn+':line:'+str(n) for n in range(row['start_line'],row['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
            helper_rows.append(row)
            for sid in row.get('source_segments',[]):
                if sid.startswith('C5CHELPER:'):helper_segment_to_struct.setdefault(sid,[]).append(row['seg_id'])
            for key in row.get('source_position_ids',[]):helper_position_to_struct.setdefault(key,[]).append(row['seg_id'])
        helper_edges=[copy.deepcopy(edge) for edge in helper_dag['edges']]
        helper_conditions=[copy.deepcopy(r) for r in helper_struct['condition_regions']]
        helper_groups=copy.deepcopy(helper_dag.get('exclusive_groups',{}))
    rows.extend(copy.deepcopy(helper_rows));edges=_dedupe_edges(list(edges)+copy.deepcopy(helper_edges))
    exclusive.update(copy.deepcopy(helper_groups))
    execution_for_sem=[r for r in rows if r['kind'] not in CONTROL and not r.get('source_only')]
    projected_sem,absorbed_sem=_project_internal_sem_edges(internal_sem_edges,execution_for_sem,raw,clean)
    edges.extend(projected_sem)
    graph=dict(base_name=base_name,nodes=[r['seg_id'] for r in rows],edges=edges,exclusive_groups=exclusive)
    require(len(graph['nodes'])==len(set(graph['nodes'])),'C3b duplicate instance node')
    topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in edges])
    execution=[r for r in rows if r['kind'] not in CONTROL and not r.get('source_only')];tids={t['template_id'] for t in templates};require(len(tids)==len(templates),'C3b duplicate template identity')
    for r in execution:
        r['source_segments']=[b['seg_id'] for b in bases if b['function']==r['function'] and max(b['start_line'],r['start_line'])<=min(b['end_line'],r['end_line'])]
        r['source_position_ids']=['LOOPSRC:'+r['function']+':line:'+str(i) for i in range(r['start_line'],r['end_line']+1) if lines[i-1].strip() and lines[i-1].strip() not in ('{','}')]
        r['generation_origin']=[n for n,l in raw.get('node_locations',{}).items() if not is_ignored_compiler_fn(n) and l['function']==r['function'] and r['start_line']<=l['line']<=r['end_line']] or None
        if not r.get('template_id'):
            r['template_id']='C3BTPL:'+r['seg_id'];require(r['template_id'] not in tids,'C3b ordinary template conflict');tids.add(r['template_id']);templates.append(dict(template_id=r['template_id'],role='execution',function=r['function'],start_line=r['start_line'],end_line=r['end_line']))
    # Per-iteration, branch-local Lock groups. A group never spans two
    # iterations, two Condition branches, or two mutexes.
    loop_id_by_header={loop['header_line']:loop['loop_id'] for loop in loops}
    loop_by_id={loop['loop_id']:loop for loop in loops}
    def loop_effective(loop):
        count=1;cursor=loop
        while cursor is not None:
            count*=cursor['iteration_count'];parent=cursor.get('parent_loop_id');cursor=loop_by_id.get(parent) if parent else None
        return count
    def covers_lock(row,lock):
        return row['start_line']<=lock['lock_range'][0] and lock['unlock_range'][0]<=row['end_line']
    def overlaps_other(row,lock):
        for other in all_locks:
            if other is lock or other['function']!=lock['function']:continue
            if max(row['start_line'],other['lock_range'][0])<=min(row['end_line'],other['unlock_range'][0]):return True
        return False
    def branch_owner(row,lock):
        target=lock.get('condition_region_id');branch=lock.get('condition_branch_id')
        for membership in row.get('condition_memberships',[]):
            group=membership.get('exclusive_group')
            if membership.get('branch_id')==branch and (group==target or str(group).endswith('::'+str(target))):return True
        return False
    buckets={};lock_instances={}
    for lock in inside_locks:
        lid=loop_id_by_header.get(lock['loop_header'])
        require(lid is not None,'C5c Lock has no instantiated Loop')
        candidates=[r for r in execution if r.get('role')=='iteration' and r.get('loop_id')==lid
              and r['kind']=='mutex_cs' and r['function']==lock['function']
              and covers_lock(r,lock) and not overlaps_other(r,lock) and branch_owner(r,lock)]
        effective=loop_effective(loop_by_id[lid])
        if effective==0:
            require(not candidates,'C5c zero-trip Lock has an execution instance')
            continue
        require(candidates,'C5c Lock has no per-iteration execution instance')
        by_iteration={}
        for row in candidates:
            identity=row.get('loop_iteration_id')
            require(identity is not None,'C5c Lock instance has no Loop iteration identity')
            require(identity not in by_iteration,'C5c Lock has ambiguous per-iteration instances')
            by_iteration[identity]=row
        require(len(by_iteration)==effective,'C5c Lock instance count does not match Loop expansion')
        lock_rows=list(by_iteration.values())
        for row in lock_rows:
            path=copy.deepcopy(row.get('condition_memberships',[]))
            key=(row.get('loop_iteration_id'),lock['mutex_id'],json.dumps(path,sort_keys=True))
            buckets.setdefault(key,[]).append((lock,row))
    groups=[];node_group={};group_rows=[];group_id_by_key={};counts={}
    for (scope,mutex,pathkey) in buckets:counts[(scope,mutex)]=counts.get((scope,mutex),0)+1
    sequences={}
    for scope in {key[0] for key in buckets}:
        sequences[scope]=[row['seg_id'] for row in sorted((row for row in execution if row.get('loop_iteration_id')==scope),
                                                           key=lambda row:(row['start_line'],row['end_line'],row['seg_id']))]
    for key in sorted(buckets,key=lambda item:(str(item[1]),str(item[0]),item[2])):
        scope,mutex,pathkey=key;items=sorted(buckets[key],key=lambda item:(item[1]['start_line'],item[1]['end_line'],item[1]['seg_id']))
        path=json.loads(pathkey);order=sequences[scope];positions={sid:index for index,sid in enumerate(order)}
        runs=[];current=[]
        for item in items:
            if current and positions[item[1]['seg_id']]!=positions[current[-1][1]['seg_id']]+1:
                runs.append(current);current=[]
            current.append(item)
        if current:runs.append(current)
        for run_index,run in enumerate(runs):
            gid='LOCKGROUP:'+mutex+'@'+str(scope)
            if counts[(scope,mutex)]>1:
                suffix='|'.join(m['exclusive_group']+':'+m['branch_id'] for m in path) or 'unconditional';gid+='@'+suffix
            if len(runs)>1:gid+=':run:'+str(run_index)
            member_rows=[row for _,row in run];locks_here=[lock for lock,_ in run]
            group_custom=_unique_custom([row['custom'] for row in member_rows if 'custom' in row])
            members=[]
            for lock,row in run:
                row['mutex_id']=mutex;row['lock_group_id']=gid;row['lock_source']=copy.deepcopy(lock)
                node_group[row['seg_id']]=gid
                members.append(dict(copy.deepcopy(lock),member_node_id=row['seg_id'],member_segment_id=row['seg_id'],
                                    execution_segment=copy.deepcopy(row),condition_memberships=copy.deepcopy(path)))
            member_templates=_ordered_unique(row['template_id'] for row in member_rows)
            for template in templates:
                if template['template_id'] in member_templates:
                    template['mutex_id']=mutex;template['lock_group_id']=gid
            occurrences=[dict(segment_id=row['seg_id'],template_id=row['template_id'],mutex_id=mutex,loop_id=row.get('loop_id'),
                              iteration_index=row.get('iteration_index'),outer_iteration_index=row.get('outer_iteration_index'),
                              inner_iteration_index=row.get('inner_iteration_index'),condition_memberships=copy.deepcopy(row.get('condition_memberships',[])),
                              source_position_ids=copy.deepcopy(row.get('source_position_ids',[]))) for row in member_rows]
            group=dict(group_id=gid,seg_id=gid,mutex_id=mutex,function='<lockgroup>',kind='lock_group',role='lock_group',
                       start_line=min(row['start_line'] for row in member_rows),end_line=max(row['end_line'] for row in member_rows),
                       member_segment_ids=[row['seg_id'] for row in member_rows],member_node_ids=[row['seg_id'] for row in member_rows],
                       member_template_ids=member_templates,members=members,
                       occurrences=occurrences,regions=[copy.deepcopy(lock) for lock in locks_here],
                       region_ids=_ordered_unique(lock.get('region_id',lock['function']+'@'+str(lock['lock_range'][0])) for lock in locks_here),
                       condition_memberships=path,aggregation_method='fieldwise_sum_of_member_weights')
            if path:group['branch_scope_id']=path[0]['exclusive_group']+':'+path[0]['branch_id']
            if len(group_custom)==1:group['custom']=copy.deepcopy(group_custom[0])
            # Keep the Loop instance identity on the group.
            for field in ('loop_id','iteration_index','outer_iteration_index','inner_iteration_index','loop_iteration_id','iteration_identity'):
                if member_rows[0].get(field) is not None:group[field]=member_rows[0][field]
            groups.append(group);group_rows.append(copy.deepcopy(group));group_id_by_key[(scope,mutex,pathkey,run_index)]=gid
    # Outside independent Locks retain C3b's one-group-per-mutex behavior.
    for mutex in sorted({r['mutex_id'] for r in outside_locks}):
        members=[]
        for lock in [r for r in outside_locks if r['mutex_id']==mutex]:
            found=[r for r in execution if r['kind']=='mutex_cs' and r.get('role')!='iteration' and r['function']==lock['function'] and [r['start_line'],r['end_line']]==lock['segment_range']]
            require(len(found)==1,'C5c lost complete outside mutex region');r=found[0]
            r['mutex_id']=mutex;r['condition_memberships']=[];r['lock_group']=dict(group_id='LOCKGROUP:'+mutex,mutex_id=mutex,source_region=copy.deepcopy(lock));r['lock_group_id']='LOCKGROUP:'+mutex
            node_group[r['seg_id']]='LOCKGROUP:'+mutex
            members.append(dict(copy.deepcopy(lock),member_node_id=r['seg_id'],member_segment_id=r['seg_id'],execution_segment=copy.deepcopy(r),condition_memberships=[]))
        gid='LOCKGROUP:'+mutex
        group=dict(group_id=gid,seg_id=gid,mutex_id=mutex,function='<lockgroup>',kind='lock_group',role='lock_group',
                   member_node_ids=[m['member_node_id'] for m in members],member_segment_ids=[m['member_segment_id'] for m in members],
                   member_template_ids=_ordered_unique(r['template_id'] for r in [m['execution_segment'] for m in members]),
                   members=members,regions=copy.deepcopy([lock for lock in outside_locks if lock['mutex_id']==mutex]),
                   condition_memberships=[],aggregation_method='fieldwise_sum_of_member_weights')
        group['occurrences']=[dict(segment_id=m['execution_segment']['seg_id'],template_id=m['execution_segment']['template_id'],mutex_id=mutex,loop_id=None,
                                   iteration_index=None,outer_iteration_index=None,inner_iteration_index=None,
                                   condition_memberships=[],source_position_ids=copy.deepcopy(m['execution_segment'].get('source_position_ids',[]))) for m in members]
        outside_custom=_unique_custom([m['execution_segment']['custom'] for m in members if 'custom' in m['execution_segment']])
        if len(outside_custom)==1:group['custom']=copy.deepcopy(outside_custom[0])
        groups.append(group);group_rows.append(copy.deepcopy(group))
    # Dependency edge witnesses are retained through C2a contraction.
    pre=dict(base_name=base_name,nodes=[r['seg_id'] for r in execution],edges=_project(rows,graph))
    groups,projection,dd=contract_branch_groups(pre,groups);topo_sort(dd['nodes'],[(e['src'],e['dst']) for e in dd['edges']]);dd['exclusive_groups']=copy.deepcopy(exclusive)
    absorbed={sid for g in groups for sid in g['member_segment_ids']};final=[copy.deepcopy(r) for r in execution if r['seg_id'] not in absorbed]
    for group in groups:
        parts=[m['execution_segment'] for m in group['members']];final.append(dict(copy.deepcopy(group),seg_id=group['group_id'],kind='lock_group',function='<lockgroup>',start_line=min(r['start_line'] for r in parts),end_line=max(r['end_line'] for r in parts),source_segments=_ordered_unique(s for r in parts for s in r['source_segments']),source_position_ids=_ordered_unique(s for r in parts for s in r['source_position_ids']),generation_origin=_ordered_unique(n for r in parts for n in (r['generation_origin'] or [])) or None))
    oldmap={b['seg_id']:_ordered_unique(projection[r['seg_id']] for r in execution if b['seg_id'] in r['source_segments']) for b in bases}
    generation={n:(_ordered_unique(projection[r['seg_id']] for r in execution if n in (r['generation_origin'] or [])) if n in raw.get('node_locations',{}) else oldmap[sid].copy()) for n,sid in raw.get('node_to_segment',{}).items() if not is_ignored_compiler_fn(n)}
    positions={}
    for key,targets in raw.get('mappings',{}).get('source_position_to_execution_segments',{}).items():positions[key]=_ordered_unique(projection[r['seg_id']] for r in execution if any(t in r['source_segments'] for t in targets) and (not key.startswith('LOOPSRC:') or key in r['source_position_ids']))
    for r in execution:
        for key in r['source_position_ids']:positions.setdefault(key,[]).append(projection[r['seg_id']])
    for body in source_conditions:
        for i in range(body['source_range'][0],body['source_range'][1]+1):
            if lines[i-1].strip():positions.setdefault('LOOPSRC:'+body['function']+':line:'+str(i),[])
    structural_positions={}
    for r in rows:
        if r.get('source_position_ids'):
            for key in r['source_position_ids']:structural_positions.setdefault(key,[]).append(r['seg_id'])
        else:
            for i in range(r['start_line'],r['end_line']+1):
                if lines[i-1].strip():structural_positions.setdefault('LOOPSRC:'+r['function']+':line:'+str(i),[]).append(r['seg_id'])
    inside_region_to_groups={};outside_region_to_groups={}
    for lock in inside_locks:
        key=lock.get('region_id',lock['function']+'@'+str(lock['lock_range'][0]))
        inside_region_to_groups.setdefault(key,[])
        for g in groups:
            if any(m.get('function')==lock['function'] and m.get('lock_range')==lock['lock_range'] and m.get('mutex_id')==lock['mutex_id'] for m in g.get('regions',[])):
                if g['group_id'] not in inside_region_to_groups[key]:inside_region_to_groups[key].append(g['group_id'])
    for lock in outside_locks:
        key=lock.get('region_id',lock['function']+'@'+str(lock['lock_range'][0]))
        outside_region_to_groups[key]=['LOCKGROUP:'+lock['mutex_id']]
    source_lock_map=dict(inside_region_to_groups);source_lock_map.update(outside_region_to_groups)
    if helper_conditions:
        regions=sorted(regions+helper_conditions,key=lambda r:(r['function'],r['entry_line'],r['exit_line'],r['condition_id']))
        for region in helper_conditions:
            if region['condition_id'] in source_to_instances:source_to_instances[region['condition_id']].append(region['condition_id'])
    for region in regions:
        if region.get('loop_iteration_id'):continue
        source_id=region.get('source_condition_id')
        if source_id is None and not source_to_instances.get(region['condition_id']):
            source_to_instances[region['condition_id']]=[region['condition_id']]
    lock_templates=copy.deepcopy(inside_locks+outside_locks)
    grouped_templates={g['group_id']:g['member_template_ids'] for g in groups}
    grouped_occurrences={g['group_id']:copy.deepcopy(g.get('occurrences',[])) for g in groups}
    iteration_to_group={r['seg_id']:r['lock_group_id'] for r in execution if r.get('role')=='iteration' and r.get('lock_group_id')}
    mappings=copy.deepcopy(raw.get('mappings',{}));mappings.update(generation_node_to_execution_segments=generation,original_segment_to_execution_segments=oldmap,source_position_to_execution_segments={k:_ordered_unique(v) for k,v in positions.items()},source_position_to_structural_nodes={k:_ordered_unique(v) for k,v in structural_positions.items()},structural_node_to_execution_segments={r['seg_id']:([] if r['kind'] in CONTROL or r.get('source_only') else [projection[r['seg_id']]]) for r in rows},generation_node_to_structural_nodes={n:[r['seg_id'] for r in rows if r['function']==l['function'] and r['start_line']<=l['line']<=r['end_line']] for n,l in raw.get('node_locations',{}).items() if not is_ignored_compiler_fn(n)},source_condition_to_instances=source_to_instances,condition_template_node_to_instances=condition_nodes,source_component_to_templates=source_components,source_component_templates=source_component_templates,template_to_execution_segments=_template_targets(templates,execution,projection,set(dd['nodes'])),grouped_node_to_member_segments={g['group_id']:g['member_segment_ids'] for g in groups},grouped_node_to_member_templates=grouped_templates,grouped_node_to_member_occurrences=grouped_occurrences,source_lock_region_to_group=source_lock_map,source_lock_region_to_instances=source_lock_map,iteration_to_group=iteration_to_group,absorbed_internal_sem_dependencies=copy.deepcopy(absorbed_sem))
    component_targets={}
    for component in source_component_templates:
        targets=[]
        for tid in source_components.get(component['source_condition_node_id'],[]):
            matches=[r for r in execution if r.get('template_id')==tid]
            require(len(matches)==1,'C5c source component instance has no unique row: '+component['template_id'])
            target=projection[matches[0]['seg_id']]
            if target not in targets:targets.append(target)
        component_targets[component['template_id']]=targets
    require(not (set(component_targets)&{t['template_id'] for t in templates}),'C5c source component template identity conflict')
    mappings['template_to_execution_segments'].update(component_targets)
    if helper_conditions:
        mappings['source_only_segment_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_segment_to_struct.items())}
        mappings['source_position_to_structural_nodes'].update({k:_ordered_unique(v) for k,v in sorted(helper_position_to_struct.items())})
    common=dict(base_name=base_name,rule_name=RULE,level='level2',source_file=raw['source_file'],source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),thread_entry_functions=copy.deepcopy(raw.get('thread_entry_functions',[])),loops=loops,condition_regions=regions,condition_templates=source_conditions,iteration_instances=instances,templates=templates,source_component_templates=sorted(source_component_templates,key=lambda t:t['template_id']),lock_groups=groups,lock_templates=lock_templates,independent_lock_regions=copy.deepcopy(outside_locks),mappings=mappings,composition_edge_provenance=[dict(base_edge_index=i,edge=copy.deepcopy(e)) for i,e in enumerate(dag['edges'])],composition_input_sha256=digest([raw,dag]),capabilities=dict(scheduler_compatible=True,timing_compatible=False,instrumentation_compatible=False,analysis_only=True))
    if helper_conditions:common['source_only_segments']=copy.deepcopy(helper_segment_rows)
    return dict(copy.deepcopy(common),view='struct',segments=rows),graph,dict(common,view='sched',segments=final),dd,dict(rule_name=RULE,level='level2',scheduler_compatible=True,analysis_only=True)
