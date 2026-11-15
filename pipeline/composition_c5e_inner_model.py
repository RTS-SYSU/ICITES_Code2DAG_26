"""Local C3a: expand existing Loop families independently inside Condition leaves."""
import copy,hashlib,json,re
from .composition_model import CONTROL,require,_mask,_project,_ordered_unique,digest
from .condition_model import _sanitize_c,analyze_conditionals,build_condition_views,_match_pair,_skip_ws
from .loop_nested_model import build_loop_nested_views
from .composition_c2a_model import boundary_edge_replacements,contract_branch_groups
from .composition_c2b_model import _source_matched_regions
from ..core.func_ranges import _strip_comments_and_strings,is_ignored_compiler_fn
from .algo.common import topo_sort
RULE='composition_c5e_inner'


def condition_loop_overlap(text):
    clean=_sanitize_c(text);regions=analyze_conditionals(text,{'<source>':(1,len(text.splitlines()))}).regions
    for match in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,match.end()),'(',')');end=_match_pair(clean,_skip_ws(clean,close+1),'{','}')+1
        if any(max(match.start(),r.start_offset)<min(end,r.end_offset) for r in regions):return True
    return False



def _template_targets(templates,execution,projection,final_ids):
    targets={t['template_id']:[] for t in templates}
    for row in execution:
        tid=row.get('template_id');target=projection.get(row['seg_id'])
        require(tid in targets and type(target) is str and target in final_ids,
                'C3a used template has no unique legal final target: '+str(tid))
        if target not in targets[tid]:targets[tid].append(target)
    return targets


def build_branch_loop_views(*,base_name,source_text,base_segments_json,base_dag_json):
    raw=copy.deepcopy(base_segments_json);dag=copy.deepcopy(base_dag_json);bases=raw['segments'];by_raw={r['seg_id']:r for r in bases}
    require(len(by_raw)==len(bases) and set(by_raw)==set(dag['nodes']),'C3a raw identities must match uniquely')
    topo_sort(dag['nodes'],[(e['src'],e['dst']) for e in dag['edges']])
    clean=_sanitize_c(source_text);lines=clean.splitlines();line=lambda p:clean.count('\n',0,p)+1
    require(not re.search(r'^\s*#\s*(?!include\b)\w+',_strip_comments_and_strings(source_text),re.M),'C3a rejects active directives')
    ranges={}
    for r in bases:
        a,b=ranges.get(r['function'],(r['start_line'],r['end_line']));ranges[r['function']]=(min(a,r['start_line']),max(b,r['end_line']))
    analysis=analyze_conditionals(source_text,ranges);roots=[r for r in analysis.regions if r.parent_id is None]
    require(not analysis.diagnostics and 1<=len(roots)<=4 and 1<=len(analysis.regions)<=8 and len(analysis.regions)==len(re.findall(r'\bif\b',clean)),'C3a requires bounded Condition trees')
    leaves=[]
    for region in analysis.regions:
        for branch in region.branches:
            if branch.start_offset is not None and not any(r.parent_id==region.condition_id and r.parent_branch_id==branch.branch_id for r in analysis.regions):leaves.append((region,branch))
    selected={};spans=[]
    for match in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,match.end()),'(',')');end=_match_pair(clean,_skip_ws(clean,close+1),'{','}')+1
        owners=[(r,b) for r,b in leaves if b.start_offset<=match.start() and end<=b.end_offset]
        require(len(owners)==1,'C3a Loop must stay completely inside one Condition leaf; reverse/partial nesting unsupported')
        region,branch=owners[0];selected.setdefault((region.condition_id,branch.branch_id),(region,branch,[]))[2].append((match.start(),end));spans.append((match.start(),end))
    require(spans and not re.search(r'\b(?:while|do)\b',clean),'C3a requires supported static for families')
    outside_locks=[]
    if re.search(r'\bpthread_mutex_(?!t\b)\w+\s*\(',clean):
        _,outside_locks=_source_matched_regions(source_text)
        for lock in outside_locks:
            require(not any(max(lock['start_offset'],r.start_offset)<min(lock['end_offset'],r.end_offset) for r in roots),'C3a Lock must stay outside the Condition/Loop combination')
            require(lock['function'] in raw.get('thread_entry_functions',[]),'C3a lock worker not confirmed')
    cs,cd,ce,_,cm=build_condition_views(base_name=base_name,source_text=source_text,base_segments_json=raw,base_dag_json=dag)
    require(cm['scheduler_compatible'],'C3a incompatible Condition: '+str(cm['blocked_reasons']))
    # Retain true closing braces dropped by Condition's executable-line trimming.
    for a,b in spans:
        h,x=line(a),line(b-1);owners=[r for r in cs['segments'] if not r.get('synthetic') and r['start_line']<=h<=r['end_line']]
        require(len(owners)==1 and owners[0]['kind']=='compute','C3a Loop has no unique compute owner')
        owners[0]['end_line']=max(owners[0]['end_line'],x)
    ledger=[]
    for index,edge in enumerate(dag['edges']):
        def ports(sid):
            b=by_raw[sid];return sorted([r for r in cs['segments'] if r['function']==b['function'] and max(r['start_line'],b['start_line'])<=min(r['end_line'],b['end_line'])],key=lambda r:(r['start_line'],0 if r['kind']=='condition_entry' else 2 if r['kind']=='condition_exit' else 1,r['end_line']))
        left,right=ports(edge['src']),ports(edge['dst']);boundary=not(left and right)
        if boundary:matches=boundary_edge_replacements(edge,left,right,by_raw,cs['segments'],cd,analysis,lines)
        else:matches=[e for e in cd['edges'] if e['src']==left[-1]['seg_id'] and e['dst']==right[0]['seg_id'] and e['kind']==edge['kind']]
        require(matches or edge['kind']=='intra','C3a cannot preserve external dependency')
        for e in matches:
            for key,value in edge.items():
                if key not in ('src','dst','kind'):e.setdefault(key,copy.deepcopy(value))
            e.setdefault('provenance',[]).append(dict(base_edge_index=index,edge=copy.deepcopy(edge)))
        ledger.append(dict(base_edge_index=index,edge=copy.deepcopy(edge),disposition='condition_boundary_replacement' if boundary else 'projected' if matches else 'condition_lexical_replacement'))
    rows=cs['segments'];graph=cd;loops=[];templates=[]
    def memberships(fn,a,b):
        return [dict(exclusive_group=r.condition_id,branch_id=br.branch_id,guard=br.guard) for r in analysis.regions if r.function==fn for br in r.branches if br.start_line is not None and br.start_line<=a<=b<=br.end_line]
    for _,(region,branch,_) in sorted(selected.items(),key=lambda item:item[1][1].start_offset):
        local=_mask(source_text,[(0,branch.start_offset),(branch.end_offset,len(source_text))])
        payload=copy.deepcopy(raw);payload['segments']=rows;payload['node_to_segment']={}
        for n,loc in raw.get('node_locations',{}).items():
            targets=[r for r in rows if not r.get('synthetic') and r['function']==loc['function'] and r['start_line']<=loc['line']<=r['end_line']]
            if targets:payload['node_to_segment'][n]=targets[0]['seg_id']
        child=build_loop_nested_views(base_name=base_name,source_text=local,base_segments_json=payload,base_dag_json=graph)
        rows,graph=child[0]['segments'],child[1]
        scope=memberships(region.function,branch.start_line,branch.end_line)
        for loop in child[0]['loops']:
            loop=copy.deepcopy(loop);loop['condition_owner_segment']=loop['original_segment']
            owners=[r for r in bases if r['function']==loop['function'] and r['start_line']<=loop['header_line'] and loop['exit_line']<=r['end_line']]
            require(len(owners)==1,'C3a complete Loop must stay inside one original Level-2 owner')
            loop.update(original_segment=owners[0]['seg_id'],condition_memberships=copy.deepcopy(scope),branch_scope_id=region.condition_id+':'+branch.branch_id);loops.append(loop)
        for template in child[0]['templates']:
            t=copy.deepcopy(template);t.update(function=region.function,condition_memberships=copy.deepcopy(scope),branch_scope_id=region.condition_id+':'+branch.branch_id);templates.append(t)
    execution=[r for r in rows if r['kind'] not in CONTROL]
    tids={t['template_id'] for t in templates};require(len(tids)==len(templates),'C3a branch template ID conflict')
    for r in rows:r['condition_memberships']=memberships(r['function'],r['start_line'],r['end_line'])
    for r in execution:
        r['source_segments']=[b['seg_id'] for b in bases if b['function']==r['function'] and max(b['start_line'],r['start_line'])<=min(b['end_line'],r['end_line'])]
        r['source_position_ids']=['LOOPSRC:'+r['function']+':line:'+str(i) for i in range(r['start_line'],r['end_line']+1) if lines[i-1].strip() and lines[i-1].strip() not in ('{','}')]
        r['generation_origin']=[n for n,l in raw.get('node_locations',{}).items() if not is_ignored_compiler_fn(n) and l['function']==r['function'] and r['start_line']<=l['line']<=r['end_line']] or None
        if not r.get('template_id'):
            r['template_id']='C3ATPL:'+r['seg_id'];require(r['template_id'] not in tids,'C3a ordinary template conflict');tids.add(r['template_id']);templates.append(dict(template_id=r['template_id'],function=r['function'],start_line=r['start_line'],end_line=r['end_line'],role='execution',condition_memberships=copy.deepcopy(r['condition_memberships'])))
    groups=[]
    for mutex in sorted({r['mutex_id'] for r in outside_locks}):
        members=[]
        for lock in [r for r in outside_locks if r['mutex_id']==mutex]:
            a,b=lock['segment_range'];old=[r for r in bases if r['function']==lock['function'] and r['kind']=='mutex_cs' and r['start_line']==a and r['end_line']==b]
            found=[r for r in execution if r['function']==lock['function'] and r['kind']=='mutex_cs' and r['start_line']==a and r['end_line']==b]
            require(len(old)==len(found)==1 and not found[0]['condition_memberships'],'C3a lost independent Lock region')
            r=found[0];r['lock_group']=dict(group_id='LOCKGROUP:'+mutex,mutex_id=mutex,source_region=copy.deepcopy(lock))
            members.append(dict(copy.deepcopy(lock),member_node_id=r['seg_id'],member_segment_id=r['seg_id'],execution_segment=copy.deepcopy(r)))
        groups.append(dict(group_id='LOCKGROUP:'+mutex,mutex_id=mutex,member_node_ids=[m['member_node_id'] for m in members],member_segment_ids=[m['member_segment_id'] for m in members],members=members,aggregation_method='fieldwise_sum_of_member_weights'))
    pre=dict(base_name=base_name,nodes=[r['seg_id'] for r in execution],edges=_project(rows,graph))
    groups,projection,dd=contract_branch_groups(pre,groups);topo_sort(dd['nodes'],[(e['src'],e['dst']) for e in dd['edges']])
    grouped={sid for g in groups for sid in g['member_segment_ids']};final=[copy.deepcopy(r) for r in execution if r['seg_id'] not in grouped]
    for g in groups:
        parts=[m['execution_segment'] for m in g['members']]
        final.append(dict(copy.deepcopy(g),seg_id=g['group_id'],kind='lock_group',function='<lockgroup>',start_line=min(r['start_line'] for r in parts),end_line=max(r['end_line'] for r in parts),condition_memberships=[],source_segments=_ordered_unique(s for r in parts for s in r['source_segments']),source_position_ids=_ordered_unique(s for r in parts for s in r['source_position_ids']),generation_origin=_ordered_unique(n for r in parts for n in (r['generation_origin'] or [])) or None))
    oldmap={r['seg_id']:_ordered_unique(projection[e['seg_id']] for e in execution if r['seg_id'] in e['source_segments']) for r in bases}
    generation={n:(_ordered_unique(projection[r['seg_id']] for r in execution if n in (r['generation_origin'] or [])) if n in raw.get('node_locations',{}) else oldmap[sid].copy()) for n,sid in raw.get('node_to_segment',{}).items() if not is_ignored_compiler_fn(n)}
    positions={}
    for key,targets in raw.get('mappings',{}).get('source_position_to_execution_segments',{}).items():
        positions[key]=_ordered_unique(projection[r['seg_id']] for r in execution if any(t in r['source_segments'] for t in targets) and (not key.startswith('LOOPSRC:') or key in r['source_position_ids']))
    for r in execution:
        for key in r['source_position_ids']:positions.setdefault(key,[]).append(projection[r['seg_id']])
    for loop in loops:
        for a,b in loop.get('statements',[]):
            for i in range(a,b+1):positions.setdefault('LOOPSRC:'+loop['function']+':line:'+str(i),[])
    mappings=copy.deepcopy(raw.get('mappings',{}));mappings.update(generation_node_to_execution_segments=generation,original_segment_to_execution_segments=oldmap,source_position_to_execution_segments={k:_ordered_unique(v) for k,v in positions.items()},template_to_execution_segments=_template_targets(templates,execution,projection,set(dd['nodes'])),grouped_node_to_member_segments={g['group_id']:g['member_segment_ids'] for g in groups})
    common=dict(base_name=base_name,rule_name=RULE,level='level2',source_file=raw['source_file'],source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),condition_regions=cs['condition_regions'],loops=loops,templates=templates,lock_groups=groups,mappings=mappings,composition_edge_provenance=ledger,composition_input_sha256=digest([raw,dag]),capabilities=dict(scheduler_compatible=True,timing_compatible=False,instrumentation_compatible=False,analysis_only=True))
    graph['exclusive_groups']=copy.deepcopy(cd['exclusive_groups']);dd['exclusive_groups']=copy.deepcopy(cd['exclusive_groups'])
    return dict(copy.deepcopy(common),view='struct',segments=rows),graph,dict(common,view='sched',segments=final),dd,dict(rule_name=RULE,level='level2',scheduler_compatible=True,analysis_only=True)
