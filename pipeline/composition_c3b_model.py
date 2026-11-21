"""C3b: replace each static iteration by its independently named Condition body."""
import copy,hashlib,re
from .composition_model import CONTROL,require,_mask,_project,_ordered_unique,digest
from .condition_model import _sanitize_c,analyze_conditionals,build_condition_views,_match_pair,_skip_ws
from .composition_c2a_model import flatten_condition_headers,contract_branch_groups
from .composition_c2b_model import _source_matched_regions
from .composition_c3a_model import _template_targets
from .loop_nested_model import build_loop_nested_views,_check_induction
from ..core.func_ranges import _strip_comments_and_strings,is_ignored_compiler_fn
from .algo.common import topo_sort
RULE='composition_c3b'


def condition_inside_loop(text):
    clean=_sanitize_c(text);conditions=analyze_conditionals(text,{'<source>':(1,len(text.splitlines()))}).regions
    for m in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,m.end()),'(',')');brace=_skip_ws(clean,close+1);end=_match_pair(clean,brace,'{','}')
        if any(brace<r.start_offset and r.end_offset<=end for r in conditions):return True
    return False


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


def build_loop_condition_views(*,base_name,source_text,base_segments_json,base_dag_json):
    raw=copy.deepcopy(base_segments_json);dag=copy.deepcopy(base_dag_json);bases=raw['segments'];rawids={r['seg_id']:r for r in bases}
    require(len(rawids)==len(bases) and set(rawids)==set(dag['nodes']),'C3b invalid raw IDs')
    topo_sort(dag['nodes'],[(e['src'],e['dst']) for e in dag['edges']])
    clean=_sanitize_c(source_text);lines=clean.splitlines();line=lambda offset:clean.count('\n',0,offset)+1
    require(not re.search(r'^\s*#\s*(?!include\b)\w+',_strip_comments_and_strings(source_text),re.M),'C3b rejects active directives')
    ranges={}
    for r in bases:
        require(type(r.get('function')) is str and bool(r['function'].strip()),'C3b missing original segment function owner')
        a,b=ranges.get(r['function'],(r['start_line'],r['end_line']));ranges[r['function']]=(min(a,r['start_line']),max(b,r['end_line']))
    analysis=analyze_conditionals(source_text,ranges);require(not analysis.diagnostics and len(analysis.regions)==len(re.findall(r'\bif\b',clean)),'C3b invalid or unowned Condition')
    spans=[]
    for m in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,m.end()),'(',')');brace=_skip_ws(clean,close+1);end=_match_pair(clean,brace,'{','}')
        spans.append(dict(start=m.start(),brace=brace,end=end,header=line(m.start()),body_low=line(brace)+1,body_high=line(end)-1))
    require(1<=len(spans)<=2,'C3b supports only Loop L1/L2/L3')
    assignments={}
    for region in analysis.regions:
        containers=[p for p in spans if p['brace']<region.start_offset and region.end_offset<=p['end']]
        require(containers,'C3b Condition must stay entirely inside one Loop body')
        owner=max(containers,key=lambda p:p['start']);assignments.setdefault(owner['header'],[]).append(region)
    require(assignments,'C3b missing Condition body')
    for h,conditions in assignments.items():
        require(1<=len(conditions)<=2 and len([r for r in conditions if r.parent_id is None])==1,'C3b one bounded Condition tree per iteration, not sequential trees')
        require(all(r.parent_id is None or any(p.condition_id==r.parent_id for p in conditions) for r in conditions),'C3b cross-body Condition parent')
    locks=[]
    if re.search(r'\bpthread_mutex_(?!t\b)\w+\s*\(',clean):
        _,locks=_source_matched_regions(source_text)
        require(all(not any(max(r['start_offset'],p['start'])<min(r['end_offset'],p['end']+1) for p in spans) for r in locks),'C3b Lock must stay outside every Loop and Condition')
    offsets=[0]+[m.end() for m in re.finditer('\n',source_text)]
    pthread_lines=[(offsets[i],offsets[i+1] if i+1<len(offsets) else len(source_text)) for i,text in enumerate(lines) if re.search(r'\bpthread_\w+\b',text)]
    flat=_mask(flatten_condition_headers(source_text,analysis),pthread_lines)
    st,sd,_,_,_=build_loop_nested_views(base_name=base_name,source_text=flat,base_segments_json=raw,base_dag_json=dag)
    loops=copy.deepcopy(st['loops']);loopby={r['loop_id']:r for r in loops};body_templates={};source_conditions=[];source_nodes={};source_to_instances={r.condition_id:[] for r in analysis.regions};condition_nodes={};source_components={};templates=copy.deepcopy(st['templates'])
    for loop in loops:
        conditions=assignments.get(loop['header_line'],[]);loop['source_condition_ids']=[r.condition_id for r in conditions]
        if not conditions:continue
        function=_condition_body_function(loop,conditions)
        span=next(p for p in spans if p['header']==loop['header_line']);a,b=span['body_low'],span['body_high']
        while a<=b and not lines[a-1].strip():a+=1
        while b>=a and not lines[b-1].strip():b-=1
        variables=[p['variable'] for p in loops if p['header_line']<=loop['header_line'] and loop['exit_line']<=p['exit_line']]
        for region in conditions:_check_induction(region.condition_text,[region.condition_text+';'],variables)
        sid='C3BSRC:BODY:'+loop['loop_id'];window=dict(seg_id=sid,function=loop['function'],kind='compute',start_line=a,end_line=b)
        locations={n:l for n,l in raw.get('node_locations',{}).items() if l['function']==loop['function'] and a<=l['line']<=b}
        payload=dict(base_name=base_name,source_file=raw['source_file'],segments=[window],node_locations=locations,node_to_segment={n:sid for n in locations})
        cs,cd,ce,_,cm=build_condition_views(base_name=base_name,source_text=source_text,base_segments_json=payload,base_dag_json=dict(nodes=[sid],edges=[]))
        require(cm['scheduler_compatible'],'C3b incompatible Condition body: '+str(cm['blocked_reasons']))
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
            if row['kind'] not in CONTROL:source_components[sourceid]=[]
    # The old whole-body iteration row is a container, replaced by a complete fork/join graph.
    rows=[];replacement={};edges=[];regions=[];instances=[];exclusive={}
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
    graph=dict(base_name=base_name,nodes=[r['seg_id'] for r in rows],edges=edges,exclusive_groups=exclusive)
    require(len(graph['nodes'])==len(set(graph['nodes'])),'C3b duplicate instance node')
    topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in edges])
    execution=[r for r in rows if r['kind'] not in CONTROL];tids={t['template_id'] for t in templates};require(len(tids)==len(templates),'C3b duplicate template identity')
    for r in execution:
        r['source_segments']=[b['seg_id'] for b in bases if b['function']==r['function'] and max(b['start_line'],r['start_line'])<=min(b['end_line'],r['end_line'])]
        r['source_position_ids']=['LOOPSRC:'+r['function']+':line:'+str(i) for i in range(r['start_line'],r['end_line']+1) if lines[i-1].strip() and lines[i-1].strip() not in ('{','}')]
        r['generation_origin']=[n for n,l in raw.get('node_locations',{}).items() if not is_ignored_compiler_fn(n) and l['function']==r['function'] and r['start_line']<=l['line']<=r['end_line']] or None
        if not r.get('template_id'):
            r['template_id']='C3BTPL:'+r['seg_id'];require(r['template_id'] not in tids,'C3b ordinary template conflict');tids.add(r['template_id']);templates.append(dict(template_id=r['template_id'],role='execution',function=r['function'],start_line=r['start_line'],end_line=r['end_line']))
    groups=[]
    for mutex in sorted({r['mutex_id'] for r in locks}):
        members=[]
        for lock in [r for r in locks if r['mutex_id']==mutex]:
            found=[r for r in execution if r['kind']=='mutex_cs' and r['function']==lock['function'] and [r['start_line'],r['end_line']]==lock['segment_range']]
            require(len(found)==1,'C3b lost complete outside mutex region');r=found[0];r['lock_group']=dict(group_id='LOCKGROUP:'+mutex,mutex_id=mutex,source_region=copy.deepcopy(lock));members.append(dict(copy.deepcopy(lock),member_node_id=r['seg_id'],member_segment_id=r['seg_id'],execution_segment=copy.deepcopy(r)))
        groups.append(dict(group_id='LOCKGROUP:'+mutex,mutex_id=mutex,members=members,member_node_ids=[m['member_node_id'] for m in members],member_segment_ids=[m['member_segment_id'] for m in members],aggregation_method='fieldwise_sum_of_member_weights'))
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
        for i in range(r['start_line'],r['end_line']+1):
            if lines[i-1].strip():structural_positions.setdefault('LOOPSRC:'+r['function']+':line:'+str(i),[]).append(r['seg_id'])
    mappings=copy.deepcopy(raw.get('mappings',{}));mappings.update(generation_node_to_execution_segments=generation,original_segment_to_execution_segments=oldmap,source_position_to_execution_segments={k:_ordered_unique(v) for k,v in positions.items()},source_position_to_structural_nodes={k:_ordered_unique(v) for k,v in structural_positions.items()},generation_node_to_structural_nodes={n:[r['seg_id'] for r in rows if r['function']==l['function'] and r['start_line']<=l['line']<=r['end_line']] for n,l in raw.get('node_locations',{}).items() if not is_ignored_compiler_fn(n)},source_condition_to_instances=source_to_instances,condition_template_node_to_instances=condition_nodes,source_component_to_templates=source_components,template_to_execution_segments=_template_targets(templates,execution,projection,set(dd['nodes'])),grouped_node_to_member_segments={g['group_id']:g['member_segment_ids'] for g in groups})
    common=dict(base_name=base_name,rule_name=RULE,level='level2',source_file=raw['source_file'],source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),thread_entry_functions=copy.deepcopy(raw.get('thread_entry_functions',[])),loops=loops,condition_regions=regions,condition_templates=source_conditions,iteration_instances=instances,templates=templates,lock_groups=groups,mappings=mappings,composition_edge_provenance=[dict(base_edge_index=i,edge=copy.deepcopy(e)) for i,e in enumerate(dag['edges'])],composition_input_sha256=digest([raw,dag]),capabilities=dict(scheduler_compatible=True,timing_compatible=False,instrumentation_compatible=False,analysis_only=True))
    return dict(copy.deepcopy(common),view='struct',segments=rows),graph,dict(common,view='sched',segments=final),dd,dict(rule_name=RULE,level='level2',scheduler_compatible=True,analysis_only=True)
