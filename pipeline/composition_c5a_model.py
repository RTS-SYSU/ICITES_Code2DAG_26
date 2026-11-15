"""C5a: a Condition leaf contains a static Loop whose body contains matched Lock(s)."""
import copy,hashlib,json,re
from .composition_model import CONTROL,require,_mask,_project,_ordered_unique,digest
from .condition_model import _sanitize_c,analyze_conditionals,build_condition_views,_match_pair,_skip_ws
from .loop_nested_model import build_loop_nested_views
from .composition_c2a_model import boundary_edge_replacements,contract_branch_groups
from .composition_c3b_model import _iteration_fields
from .composition_c4a_model import (_formal_function_spans,_condition_function_ranges,
    _helper_condition_views,_restore_condition_dependencies)
from .composition_c4b_model import _annotate_edges,_validated_provenance,_is_internal_witness,_dedupe_edges
from .lock_regions import match_lock_regions,LockRegionError
from ..core.func_ranges import _strip_comments_and_strings,is_ignored_compiler_fn
from .algo.common import topo_sort
RULE='composition_c5a'


def condition_leaf_loop_lock_overlap(text):
    clean=_sanitize_c(text);regions=analyze_conditionals(text,{'<source>':(1,len(clean.splitlines()))}).regions
    leaves=[]
    for region in regions:
        for branch in region.branches:
            if branch.start_offset is None:continue
            if not any(child.parent_id==region.condition_id and child.parent_branch_id==branch.branch_id for child in regions):leaves.append(branch)
    loops=[]
    for match in re.finditer(r'\bfor\b',clean):
        try:
            close=_match_pair(clean,_skip_ws(clean,match.end()),'(',')');brace=_skip_ws(clean,close+1);end=_match_pair(clean,brace,'{','}')
        except Exception:continue
        loops.append((match.start(),end))
    stack=[];locks=[]
    for match in re.finditer(r'\bpthread_mutex_(lock|unlock)\s*\(\s*&\s*\w+\s*\)\s*;',clean):
        if match.group(1)=='lock':stack.append(match.start())
        elif stack:locks.append((stack.pop(),match.end()))
    return any(loop_start<lock_start and lock_end<=loop_end and any(leaf.start_offset<=loop_start and loop_end<=leaf.end_offset for leaf in leaves)
               for lock_start,lock_end in locks for loop_start,loop_end in loops)


def _memberships(regions,fn,a,b):
    return [dict(exclusive_group=region.condition_id,branch_id=branch.branch_id,guard=branch.guard)
            for region in regions if region.function==fn for branch in region.branches
            if branch.start_line is not None and branch.start_line<=a<=b<=branch.end_line]


def _loop_spans(source_text,ranges):
    clean=_sanitize_c(source_text);lines=clean.splitlines();line=lambda p:clean.count('\n',0,p)+1
    require(not re.search(r'\b(?:while|do)\b',clean),'C5a supports only static for Loops')
    spans=[]
    for match in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,match.end()),'(',')');brace=_skip_ws(clean,close+1)
        require(brace<len(clean) and clean[brace]=='{','C5a requires braced static Loops')
        end=_match_pair(clean,brace,'{','}')
        owners=[fn for fn,(a,b) in ranges.items() if a<=line(match.start())<=line(end)<=b]
        require(len(owners)==1,'C5a Loop has no formal function owner')
        fn=owners[0];span=dict(start=match.start(),brace=brace,end=end,header=line(match.start()),exit=line(end),function=fn)
        span['loop_id']='LOOP:'+fn+'@'+str(span['header']);spans.append(span)
    require(spans,'C5a missing Loop')
    return spans


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
        require(len(tree)<=2,'C5a supports Condition C1-C4 only: '+root.condition_id)
    return roots


def _normalize_cluster(rows,graph,fn,low,high,owner_id,absorbed):
    grouped=[row for row in rows if row.get('function')==fn and row['kind'] not in CONTROL and not row.get('synthetic') and row['start_line']<=high and low<=row['end_line']]
    require(grouped,'C5a cluster has no execution owner')
    start=min(row['start_line'] for row in grouped);end=max(high,max(row['end_line'] for row in grouped))
    owner=dict(seg_id=owner_id,function=fn,kind='compute',start_line=start,end_line=end,
               source_segments=_ordered_unique(s for row in grouped for s in row.get('source_segments',[])),
               source_position_ids=_ordered_unique(s for row in grouped for s in row.get('source_position_ids',[])),cluster_owner=True)
    owner_map={row['seg_id']:owner_id for row in grouped}
    new_rows=[];inserted=False
    for row in rows:
        if row['seg_id'] in owner_map:
            if not inserted:new_rows.append(owner);inserted=True
            continue
        new_rows.append(row)
    require(inserted,'C5a cluster owner insertion failed')
    nodes=_ordered_unique(row['seg_id'] for row in new_rows)
    edges=[]
    for edge in graph['edges']:
        src=owner_map.get(edge['src'],edge['src']);dst=owner_map.get(edge['dst'],edge['dst'])
        if src==dst==owner_id:
            require(edge.get('kind')=='intra','C5a cluster absorbs non-intra dependency')
            witness=dict(edge=copy.deepcopy(edge))
            indices=_ordered_unique(item.get('base_edge_index') for item in edge.get('provenance',[]) if type(item.get('base_edge_index')) is int)
            if len(indices)==1:witness['base_edge_index']=indices[0]
            elif indices:witness['base_edge_indices']=indices
            absorbed.append(witness);continue
        out=copy.deepcopy(edge);out.update(src=src,dst=dst);edges.append(out)
    edges=_dedupe_edges(edges)
    return new_rows,dict(base_name=graph.get('base_name'),nodes=nodes,edges=edges,exclusive_groups=copy.deepcopy(graph.get('exclusive_groups',{})))


def _line_offsets(source_text):
    result=[];cursor=0
    for line in source_text.splitlines(keepends=True):result.append(cursor);cursor+=len(line)
    return result


def _cluster_source(source_text,clean_lines,offsets,cluster,regions,loops):
    spans=[]
    for index,line in enumerate(clean_lines):
        if re.search(r'\b(?:pthread_|sem_)\w*\b',line):spans.append((offsets[index],offsets[index]+len(clean_lines[index])))
    if cluster['kind']=='leaf':
        branch=cluster['branch'];spans.extend([(0,branch.start_offset),(branch.end_offset,len(source_text))])
    else:
        spans.extend((region.start_offset,region.end_offset) for region in regions)
        current=set(id(loop) for loop in cluster['loops'])
        spans.extend((loop['start'],loop['end']+1) for loop in loops if id(loop) not in current)
    return _mask(source_text,spans)


def _source_component(loop,index,piece,lines):
    tid='C5ASRC:'+loop['loop_id']+':part:'+str(index)
    return dict(template_id=tid,loop_id=loop['loop_id'],function=loop['function'],role='loop_part_template',loop_part_role=piece['role'],
                analysis_only=True,start_line=piece['start_line'],end_line=piece['end_line'],
                source_position_ids=['LOOPSRC:'+loop['function']+':line:'+str(n) for n in range(piece['start_line'],piece['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')])


def _split_iteration_locks(rows,graph,st_loops,st_templates,lock_map,lines,all_templates,all_groups,source_components,lock_templates):
    local={};templates=[copy.deepcopy(template) for template in st_templates]
    for loop in st_loops:
        selected=sorted(lock_map.get(loop['loop_id'],[]),key=lambda lock:lock['lock_range'][0])
        if not selected:continue
        old=[template for template in templates if template.get('role')=='body_template' and template.get('start_line')==loop['body_start'] and template.get('end_line')==loop['body_end']]
        require(len(old)==1,'C5a Loop body template identity conflict')
        loop['source_body_template']=dict(copy.deepcopy(old[0]),analysis_only=True);templates=[template for template in templates if template['template_id']!=old[0]['template_id']]
        pieces=[];cursor=loop['body_start']
        def ordinary(a,b):
            effective=[n for n in range(a,b+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
            if effective:pieces.append(dict(role='ordinary',start_line=effective[0],end_line=effective[-1]))
        for lock in selected:
            ordinary(cursor,lock['lock_range'][0]-1);pieces.append(dict(role='critical',start_line=lock['lock_range'][0],end_line=lock['unlock_range'][0],lock=copy.deepcopy(lock)));cursor=lock['unlock_range'][0]+1
        ordinary(cursor,loop['body_end']);local[loop['loop_id']]=pieces;loop['lock_inside']=True
        for index,piece in enumerate(pieces):piece['source_template_id']=_source_component(loop,index,piece,lines)['template_id'];source_components.append(_source_component(loop,index,piece,lines))
        lock_templates.extend(dict(copy.deepcopy(lock),loop_id=loop['loop_id'],base_group_id='LOCKGROUP:'+lock['mutex_id']) for lock in selected)
    for loop in st_loops:
        if loop['loop_id'] not in local:loop['lock_inside']=False
    new_rows=[];replacement={}
    for row in rows:
        if row.get('role')!='iteration' or row.get('loop_id') not in local:new_rows.append(row);continue
        fields=_iteration_fields(row);scope=fields['loop_iteration_id'];chain=[];runs=[];active=None
        for index,piece in enumerate(local[row['loop_id']]):
            sid=scope+':part:'+str(index);tid='C5ATPL:'+sid
            new=dict(fields,seg_id=sid,template_id=tid,source_template_id=piece['source_template_id'],function=row['function'],
                     kind='mutex_cs' if piece['role']=='critical' else 'compute',role='iteration',start_line=piece['start_line'],end_line=piece['end_line'],loop_part_role=piece['role'])
            if piece['role']=='critical':
                lock=piece['lock'];new.update(mutex_id=lock['mutex_id'],base_group_id='LOCKGROUP:'+lock['mutex_id'],lock_source=copy.deepcopy(lock))
                if active is None or active['mutex_id']!=lock['mutex_id']:
                    active=dict(mutex_id=lock['mutex_id'],first_index=index,parts=[]);runs.append(active)
                active['parts'].append(new)
            else:
                active=None
            new_rows.append(new);chain.append(sid);templates.append(dict(fields,template_id=tid,function=row['function'],role='iteration_component',start_line=new['start_line'],end_line=new['end_line'],mutex_id=new.get('mutex_id')))
        require(chain,'C5a empty iteration components');replacement[row['seg_id']]=(chain[0],chain[-1])
        for left,right in zip(chain,chain[1:]):graph['edges'].append(dict(src=left,dst=right,kind='intra',source='c5a_loop_lock_lexical_order'))
        # A Lock group is one maximal adjacent run of critical pieces for one
        # mutex.  Ordinary work or another mutex ends the run; merging across
        # it would create a scheduling self-cycle and falsely move ordinary
        # work into the critical-region timing sum.
        counts={}
        for run in runs:counts[run['mutex_id']]=counts.get(run['mutex_id'],0)+1
        for run in runs:
            mutex=run['mutex_id'];parts=run['parts']
            gid='LOCKGROUP:'+mutex+'@'+scope
            if counts[mutex]>1:gid+=':run:'+str(run['first_index'])
            for part in parts:part['lock_group_id']=gid
            all_groups.append(dict(fields,group_id=gid,base_group_id='LOCKGROUP:'+mutex,mutex_id=mutex,member_node_ids=[part['seg_id'] for part in parts],
                                   member_segment_ids=[part['seg_id'] for part in parts],members=[],aggregation_method='fieldwise_sum_of_member_weights'))
    if replacement:
        edges=[]
        for edge in graph['edges']:
            out=copy.deepcopy(edge)
            if out['src'] in replacement:out['src']=replacement[out['src']][1]
            if out['dst'] in replacement:out['dst']=replacement[out['dst']][0]
            edges.append(out)
        graph['edges']=_dedupe_edges(edges)
    for template in templates:
        if template not in all_templates:all_templates.append(template)
    return new_rows,graph


def _template_targets(templates,execution,projection,final_ids):
    targets={template['template_id']:[] for template in templates}
    for row in execution:
        tid=row.get('template_id');target=projection.get(row['seg_id'])
        require(tid in targets and type(target) is str and target in final_ids,'C5a used template has no unique legal final target: '+str(tid))
        if target not in targets[tid]:targets[tid].append(target)
    return targets


def build_branch_loop_lock_views(*,base_name,source_text,base_segments_json,base_dag_json):
    raw=copy.deepcopy(base_segments_json);dag=copy.deepcopy(base_dag_json);bases=raw['segments'];by_raw={r['seg_id']:r for r in bases}
    require(len(by_raw)==len(bases) and set(by_raw)==set(dag['nodes']),'C5a raw identities must match uniquely')
    topo_sort(dag['nodes'],[(e['src'],e['dst']) for e in dag['edges']])
    for edge in dag['edges']:_validated_provenance(edge,' in base DAG')
    clean=_sanitize_c(source_text);lines=clean.splitlines();offsets=_line_offsets(source_text)
    require(not re.search(r'^\s*#\s*(?!include\b)\w+',_strip_comments_and_strings(source_text),re.M),'C5a rejects active directives')
    require(not re.search(r'\b(?:typedef|pthread_mutexattr\w*|pthread_rwlock\w*)\b',clean),'C5a rejects aliases and unsupported synchronization')
    from .composition_c4b_model import _direct_mutexes
    _direct_mutexes(clean)
    formal,sources=_formal_function_spans(raw);ranges=_condition_function_ranges(source_text,bases,formal)
    try:locks=match_lock_regions(source_text,ranges)
    except LockRegionError as exc:require(False,'C5a '+str(exc))
    require(locks,'C5a missing matched Lock')
    for lock in locks:
        body=[line for line in lines[lock['lock_range'][0]:lock['unlock_range'][0]-1] if line.strip()]
        require(body,'C5a empty critical region')
        lock['critical_range']=[lock['lock_range'][0]+1,lock['unlock_range'][0]-1]
        require(not re.search(r'\b(?:for|while|do|if|else|switch|case|return|goto|break|continue|pthread_\w+|sem_\w+)\b','\n'.join(body)),
                'C5a unsupported control or synchronization inside a Lock body')
    loops=_loop_spans(source_text,ranges)
    require(all(loop['function'] in {r['function'] for r in bases} for loop in loops),'C5a Loop requires Level-2 ownership')
    analysis=analyze_conditionals(source_text,ranges)
    require(not analysis.diagnostics,'C5a invalid Condition')
    regions=analysis.regions;_condition_trees(regions)
    leaves=_condition_leaves(regions)
    # Condition must not sit inside a Loop/Lock; the approved nesting is the reverse.
    for region in regions:
        for loop in loops:
            require(not (loop['brace']<region.start_offset and region.end_offset<loop['end']),'C5a Condition inside Loop is unsupported')
        for lock in locks:
            require(not (lock['start_offset']<region.start_offset and region.end_offset<lock['end_offset']),'C5a Condition inside Lock is unsupported')
    clusters=[];leaf_clusters={};loop_cluster={}
    for loop in loops:
        containing=[(region,branch) for region,branch in leaves if branch.start_offset<=loop['start'] and loop['end']<=branch.end_offset]
        overlapping=[region for region in regions if max(loop['start'],region.start_offset)<=min(loop['end'],region.end_offset)]
        if containing:
            require(len(containing)==1,'C5a Loop must stay inside one Condition leaf')
            region,branch=containing[0];key=(region.condition_id,branch.branch_id)
            cluster=leaf_clusters.get(key)
            if cluster is None:
                cluster=dict(kind='leaf',region=region,branch=branch,function=loop['function'],loops=[]);leaf_clusters[key]=cluster;clusters.append(cluster)
            require(cluster['function']==loop['function'],'C5a Condition leaf Loops must share one function')
            cluster['loops'].append(loop);loop_cluster[id(loop)]=cluster
        elif overlapping:
            require(False,'C5a Loop crosses Condition branch boundary')
        else:
            cluster=None
            for item in clusters:
                if item['kind']=='independent' and item['function']==loop['function']:cluster=item;break
            if cluster is None:
                cluster=dict(kind='independent',function=loop['function'],loops=[]);clusters.append(cluster)
            cluster['loops'].append(loop);loop_cluster[id(loop)]=cluster
    for cluster in clusters:
        require(1<=len(cluster['loops'])<=2,'C5a supports only Loop L1/L2/L3 per cluster')
        cluster['loops'].sort(key=lambda loop:loop['start']);cluster['low']=min(loop['header'] for loop in cluster['loops']);cluster['high']=max(loop['exit'] for loop in cluster['loops'])
    locks_by_loop={};outside_locks=[]
    for lock in locks:
        containing=[loop for loop in loops if loop['function']==lock['function'] and loop['brace']<lock['start_offset'] and lock['end_offset']<=loop['end']]
        if containing:
            inner=max(containing,key=lambda loop:loop['brace']);cluster=loop_cluster[id(inner)]
            require(cluster['kind']=='leaf','C5a Lock inside an independent Loop is not this composition')
            locks_by_loop.setdefault(inner['loop_id'],[]).append(lock)
        else:
            for loop in loops:
                require(not (max(lock['start_offset'],loop['start'])<=min(lock['end_offset'],loop['end'])),'C5a Lock crosses Loop boundary')
            for region in regions:
                require(not (max(lock['start_offset'],region.start_offset)<=min(lock['end_offset'],region.end_offset)),'C5a Lock inside Condition without Loop is unsupported')
            outside_locks.append(lock)
    require(locks_by_loop,'C5a missing Lock inside a Condition leaf Loop')
    annotated=_annotate_edges(dag)
    condition_struct,condition_dag,condition_exec,_,meta=build_condition_views(base_name=base_name,source_text=source_text,base_segments_json=raw,base_dag_json=annotated)
    require(meta['scheduler_compatible'],'C5a incompatible Condition: '+str(meta['blocked_reasons']))
    rows=copy.deepcopy(condition_struct['segments']);graph=_restore_condition_dependencies(raw,annotated,condition_struct,condition_dag)
    graph['exclusive_groups']=copy.deepcopy(condition_dag.get('exclusive_groups',{}))
    absorbed=[]
    for index,cluster in enumerate(clusters):
        rows,graph=_normalize_cluster(rows,graph,cluster['function'],cluster['low'],cluster['high'],'C5AOWNER:'+cluster['function']+':'+str(index),absorbed)
    all_loops=[];all_templates=[];all_groups=[];source_components=[];lock_templates=[]
    for cluster in clusters:
        loop_source=_cluster_source(source_text,lines,offsets,cluster,regions,loops)
        payload=copy.deepcopy(raw);payload['segments']=copy.deepcopy(rows);payload['node_to_segment']={}
        for node,location in raw.get('node_locations',{}).items():
            targets=[row for row in rows if not row.get('synthetic') and row['function']==location['function'] and row['start_line']<=location['line']<=row['end_line']]
            if targets:payload['node_to_segment'][node]=targets[0]['seg_id']
        st,sd,_,_,_=build_loop_nested_views(base_name=base_name,source_text=loop_source,base_segments_json=payload,base_dag_json=graph)
        rows,sd=_split_iteration_locks(st['segments'],sd,st['loops'],st['templates'],locks_by_loop,lines,all_templates,all_groups,source_components,lock_templates)
        sd['nodes']=_ordered_unique(row['seg_id'] for row in rows);graph=sd
        for loop in st['loops']:all_loops.append(loop)
    # Conditions in helpers without an old segment stay source-only and outside the combination.
    segment_functions={r['function'] for r in bases};helper_functions=sorted({r.function for r in regions if r.function not in segment_functions})
    helper_views=_helper_condition_views(base_name=base_name,source_text=source_text,raw=raw,sources=sources,functions=helper_functions,prefix='C5AHELPER') if helper_functions else None
    helper_rows=[];helper_edges=[];helper_conditions=[];helper_groups={};helper_segment_rows=[];helper_segment_to_struct={};helper_position_to_struct={}
    if helper_views is not None:
        helper_struct,helper_dag,helper_exec,helper_segments=helper_views
        helper_exec_by_id={row['seg_id']:row for row in helper_exec['segments']}
        helper_segment_rows=[dict(copy.deepcopy(row),source_only=True,analysis_only=True) for row in helper_segments]
        for original in helper_struct['segments']:
            row=copy.deepcopy(original);row.update(source_only=True,analysis_only=True)
            if row['kind'] not in CONTROL:
                members=helper_exec_by_id.get(row['seg_id'],{}).get('condition_memberships')
                if members:row['condition_memberships']=copy.deepcopy(members)
                fn=row['function'];row['source_position_ids']=['C5AHELPER:'+fn+':line:'+str(n) for n in range(row['start_line'],row['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
            helper_rows.append(row)
            for sid in row.get('source_segments',[]):
                if sid.startswith('C5AHELPER:'):helper_segment_to_struct.setdefault(sid,[]).append(row['seg_id'])
            for key in row.get('source_position_ids',[]):helper_position_to_struct.setdefault(key,[]).append(row['seg_id'])
        helper_edges=[copy.deepcopy(edge) for edge in helper_dag['edges']];helper_conditions=[copy.deepcopy(r) for r in helper_struct['condition_regions']];helper_groups=copy.deepcopy(helper_dag.get('exclusive_groups',{}))
    rows.extend(copy.deepcopy(helper_rows));graph['edges']=_dedupe_edges(list(graph['edges'])+copy.deepcopy(helper_edges));graph['nodes']=_ordered_unique(row['seg_id'] for row in rows)
    graph['exclusive_groups']=copy.deepcopy(condition_dag.get('exclusive_groups',{}));graph['exclusive_groups'].update(helper_groups)
    for row in rows:row['condition_memberships']=_memberships(regions,row.get('function'),row['start_line'],row['end_line'])
    for loop in all_loops:
        loop['condition_memberships']=_memberships(regions,loop['function'],loop['header_line'],loop['exit_line'])
        for cluster in clusters:
            if cluster['kind']=='leaf' and loop['loop_id'] in {item['loop_id'] for item in cluster['loops']}:
                loop['branch_scope_id']=cluster['region'].condition_id+':'+cluster['branch'].branch_id
                break
    tids={template['template_id'] for template in all_templates}
    execution=[row for row in rows if row['kind'] not in CONTROL and not row.get('source_only')]
    for row in execution:
        fn=row['function'];row['source_segments']=[b['seg_id'] for b in bases if b['function']==fn and max(b['start_line'],row['start_line'])<=min(b['end_line'],row['end_line'])]
        row['source_position_ids']=['LOOPSRC:'+fn+':line:'+str(i) for i in range(row['start_line'],row['end_line']+1) if lines[i-1].strip() and lines[i-1].strip() not in ('{','}')]
        row['generation_origin']=[n for n,l in raw.get('node_locations',{}).items() if not is_ignored_compiler_fn(n) and l['function']==fn and row['start_line']<=l['line']<=row['end_line']] or None
        if not row.get('template_id'):
            tid='C5ATPL:'+row['seg_id'];require(tid not in tids,'C5a duplicate template');tids.add(tid);row['template_id']=tid
            all_templates.append(dict(template_id=tid,function=fn,role='execution',start_line=row['start_line'],end_line=row['end_line']))
    loop_by_id={loop['loop_id']:loop for loop in all_loops}
    for template in all_templates:
        loop=loop_by_id.get(template.get('loop_id'))
        if loop is None and template.get('function') is None:
            # The shared loop builder names plain body templates without a
            # loop_id/function.  Bind them back by their published source range
            # so a no-Lock Loop inside a Condition leaf still inherits scope.
            start=template.get('start_line');end=template.get('end_line')
            owners=[candidate for candidate in all_loops
                    if type(start) is int and type(end) is int
                    and candidate['body_start']<=start<=end<=candidate['body_end']]
            if len(owners)==1:loop=owners[0]
        if loop is not None:
            template['condition_memberships']=copy.deepcopy(loop.get('condition_memberships',[]))
            if loop.get('branch_scope_id'):template['branch_scope_id']=loop['branch_scope_id']
        else:template['condition_memberships']=_memberships(regions,template.get('function'),template.get('start_line',0),template.get('end_line',0))
    execution_by_id={row['seg_id']:row for row in execution}
    for component in source_components:
        loop=loop_by_id.get(component.get('loop_id'));component['condition_memberships']=copy.deepcopy(loop.get('condition_memberships',[])) if loop else []
    # Independent Lock groups outside every Condition stay one group per mutex.
    for mutex in sorted({lock['mutex_id'] for lock in outside_locks}):
        members=[]
        for lock in [item for item in outside_locks if item['mutex_id']==mutex]:
            found=[row for row in execution if row['function']==lock['function'] and row['kind']=='mutex_cs' and row['start_line']==lock['lock_range'][0] and row['end_line']==lock['unlock_range'][0]]
            require(len(found)==1,'C5a lost independent Lock region')
            row=found[0];require(not row.get('condition_memberships'),'C5a independent Lock must stay outside Conditions')
            members.append(dict(copy.deepcopy(lock),member_node_id=row['seg_id'],member_segment_id=row['seg_id'],execution_segment=copy.deepcopy(row)))
        all_groups.append(dict(group_id='LOCKGROUP:'+mutex,mutex_id=mutex,function='<lockgroup>',member_node_ids=[m['member_node_id'] for m in members],member_segment_ids=[m['member_segment_id'] for m in members],members=members,aggregation_method='fieldwise_sum_of_member_weights'))
    for group in all_groups:
        parts=[execution_by_id[sid] for sid in group['member_segment_ids']]
        if not group.get('members'):
            group['members']=[dict(copy.deepcopy(part.get('lock_source',{})),member_node_id=part['seg_id'],member_segment_id=part['seg_id'],execution_segment=copy.deepcopy(part)) for part in parts]
        if parts:
            group['function']=parts[0]['function']
            group['condition_memberships']=_memberships(regions,parts[0]['function'],min(part['start_line'] for part in parts),max(part['end_line'] for part in parts))
        else:group['condition_memberships']=[]
        if group['condition_memberships']:
            member=group['condition_memberships'][0];group['branch_scope_id']=member['exclusive_group']+':'+member['branch_id']
        group['member_template_ids']=_ordered_unique(part['template_id'] for part in parts)
    groups,projection,dd=contract_branch_groups(dict(base_name=base_name,nodes=[row['seg_id'] for row in execution],edges=_project(rows,graph)),all_groups)
    topo_sort(dd['nodes'],[(edge['src'],edge['dst']) for edge in dd['edges']])
    grouped={sid for group in groups for sid in group['member_segment_ids']}
    final=[copy.deepcopy(row) for row in execution if row['seg_id'] not in grouped]
    for group in groups:
        parts=[execution_by_id[sid] for sid in group['member_segment_ids']]
        final.append(dict(copy.deepcopy(group),seg_id=group['group_id'],kind='lock_group',function='<lockgroup>',start_line=min(row['start_line'] for row in parts),end_line=max(row['end_line'] for row in parts),source_segments=_ordered_unique(s for row in parts for s in row['source_segments']),source_position_ids=_ordered_unique(s for row in parts for s in row['source_position_ids']),generation_origin=_ordered_unique(n for row in parts for n in (row['generation_origin'] or [])) or None))
    oldmap={row['seg_id']:_ordered_unique(projection[item['seg_id']] for item in execution if row['seg_id'] in item['source_segments']) for row in bases}
    generation={n:(_ordered_unique(projection[row['seg_id']] for row in execution if n in (row['generation_origin'] or [])) if n in raw.get('node_locations',{}) else oldmap[sid].copy()) for n,sid in raw.get('node_to_segment',{}).items() if not is_ignored_compiler_fn(n)}
    positions={}
    for key,targets in raw.get('mappings',{}).get('source_position_to_execution_segments',{}).items():
        positions[key]=_ordered_unique(projection[row['seg_id']] for row in execution if any(t in row['source_segments'] for t in targets) and (not key.startswith('LOOPSRC:') or key in row['source_position_ids']))
    for row in execution:
        for key in row['source_position_ids']:positions.setdefault(key,[]).append(projection[row['seg_id']])
    for loop in all_loops:
        for a,b in loop.get('statements',[]):
            for i in range(a,b+1):positions.setdefault('LOOPSRC:'+loop['function']+':line:'+str(i),[])
    for template in source_components:
        for key in template['source_position_ids']:positions.setdefault(key,[])
    for row in helper_rows:
        for key in row.get('source_position_ids',[]):positions.setdefault(key,[])
    template_targets=_template_targets(all_templates,execution,projection,set(dd['nodes']))
    source_targets={component['template_id']:[] for component in source_components}
    require(len(source_targets)==len(source_components),'C5a duplicate source component template')
    for row in execution:
        if 'source_template_id' not in row:continue
        tid=row['source_template_id'];target=projection[row['seg_id']]
        require(tid in source_targets and target in dd['nodes'],'C5a unknown source component or final target')
        if target not in source_targets[tid]:source_targets[tid].append(target)
    require(not set(template_targets)&set(source_targets),'C5a conflicting template identities')
    template_targets.update(source_targets)
    iteration_instances=[dict(loop_id=row.get('loop_id'),loop_iteration_id=row.get('loop_iteration_id'),iteration_index=row.get('iteration_index'),outer_iteration_index=row.get('outer_iteration_index'),inner_iteration_index=row.get('inner_iteration_index'),segment_id=row['seg_id'],lock_group_id=row.get('lock_group_id'),template_id=row.get('template_id'),condition_memberships=copy.deepcopy(row.get('condition_memberships',[])),source_position_ids=copy.deepcopy(row.get('source_position_ids',[]))) for row in execution if row.get('role')=='iteration']
    group_templates={group['group_id']:group['member_template_ids'] for group in groups}
    group_occurrences={group['group_id']:[dict(segment_id=member['member_segment_id'],template_id=member['execution_segment']['template_id'],mutex_id=group['mutex_id'],loop_id=member['execution_segment'].get('loop_id'),iteration_index=member['execution_segment'].get('iteration_index'),outer_iteration_index=member['execution_segment'].get('outer_iteration_index'),inner_iteration_index=member['execution_segment'].get('inner_iteration_index'),condition_memberships=copy.deepcopy(member['execution_segment'].get('condition_memberships',[])),source_position_ids=copy.deepcopy(member['execution_segment'].get('source_position_ids',[]))) for member in group['members']] for group in groups}
    source_lock_region_to_group={}
    for lock in locks:
        key=lock['function']+':'+str(lock['lock_range'][0])
        source_lock_region_to_group[key]=_ordered_unique(group['group_id'] for group in groups if any(member.get('function')==lock['function'] and member.get('lock_range')==lock['lock_range'] for member in group.get('members',[])))
    mappings=copy.deepcopy(raw.get('mappings',{}));mappings.update(generation_node_to_execution_segments=generation,original_segment_to_execution_segments=oldmap,source_position_to_execution_segments={k:_ordered_unique(v) for k,v in positions.items()},template_to_execution_segments=template_targets,structural_node_to_execution_segments={row['seg_id']:([] if row.get('source_only') or row['kind'] in CONTROL else [projection[row['seg_id']]]) for row in rows},grouped_node_to_member_segments={group['group_id']:group['member_segment_ids'] for group in groups},grouped_node_to_member_templates=group_templates,grouped_node_to_member_occurrences=group_occurrences,iteration_to_group={row['seg_id']:projection[row['seg_id']] for row in execution if row.get('role')=='iteration' and row.get('lock_group_id')},source_lock_region_to_group=source_lock_region_to_group,source_lock_region_to_instances=copy.deepcopy(source_lock_region_to_group))
    if helper_conditions:
        mappings['source_only_segment_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_segment_to_struct.items())}
        mappings['source_position_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_position_to_struct.items())}
    condition_regions=copy.deepcopy(condition_struct['condition_regions'])
    if helper_conditions:condition_regions=sorted(condition_regions+helper_conditions,key=lambda r:(r['function'],r['entry_line'],r['exit_line'],r['condition_id']))
    dd['exclusive_groups']=copy.deepcopy(graph.get('exclusive_groups',{}))
    common=dict(base_name=base_name,rule_name=RULE,level='level2',source_file=raw['source_file'],source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),thread_entry_functions=copy.deepcopy(raw.get('thread_entry_functions',[])),condition_regions=condition_regions,loops=all_loops,iteration_instances=iteration_instances,templates=sorted(all_templates,key=lambda t:t['template_id']),source_component_templates=source_components,lock_groups=groups,lock_templates=lock_templates,independent_lock_regions=copy.deepcopy(outside_locks),absorbed_intra_dependencies=copy.deepcopy(absorbed),mappings=mappings,composition_edge_provenance=[dict(base_edge_index=i,edge=copy.deepcopy(e)) for i,e in enumerate(dag['edges'])],composition_input_sha256=digest([raw,dag]),capabilities=dict(scheduler_compatible=True,timing_compatible=False,instrumentation_compatible=False,analysis_only=True))
    if helper_conditions:common['source_only_segments']=copy.deepcopy(helper_segment_rows)
    graph['exclusive_groups']=copy.deepcopy(graph.get('exclusive_groups',{}))
    return dict(copy.deepcopy(common),view='struct',segments=rows),graph,dict(common,view='sched',segments=final),dd,dict(rule_name=RULE,level='level2',scheduler_compatible=True,analysis_only=True)


# Public aliases keep the C5a entry point discoverable without a second model.
condition_loop_lock_overlap = condition_leaf_loop_lock_overlap
build_condition_loop_lock_views = build_branch_loop_lock_views
