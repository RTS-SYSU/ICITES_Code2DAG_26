"""Import explicit C5a templates; aggregate only real iteration Lock contributions."""
import argparse
import hashlib
import json
from pathlib import Path
from ..level2.segment_dag_level2 import build_level2_segments_and_dag
from .composition_model import CONTROL, require
from .composition_c5a_model import RULE, build_branch_loop_lock_views as build_composition_views
from .loop_timing import _read_loop_timing_json
from .io_utils import write_json

FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')


def _require_integer(value,field):
    require(type(value) is int,'C5a '+field+' must be an exact integer')


def _identity(item):
    outer=item.get('outer_iteration_index');inner=item.get('inner_iteration_index')
    if outer is not None or inner is not None:return (outer,inner)
    return item.get('iteration_index')


def _validate_loop_template_contract(segments,template_map,group_ids):
    """Validate published Loop templates by source range, hierarchy and identity.

    A unit iteration is never invented for a zero-trip Loop.  The only legal
    empty template targets are a zero-trip independent body or a Loop source
    component whose effective iteration product is zero.
    """
    loops=list(segments.get('loops',[]));templates=list(segments.get('templates',[]))
    components=list(segments.get('source_component_templates',[]));instances=list(segments.get('iteration_instances',[]))
    groups=list(segments.get('lock_groups',[]));group_ids=set(group_ids)
    template_ids={template['template_id'] for template in templates}
    require(set(template_map)==template_ids|{component['template_id'] for component in components},
            'C5a template mapping must cover exactly the published templates and source components')
    for template in templates:
        require(template['template_id'] not in {component['template_id'] for component in components},
                'C5a template/source-component identity conflict: '+template['template_id'])
    by_id={}
    for loop in loops:
        lid=loop.get('loop_id')
        require(type(lid) is str and bool(lid) and lid not in by_id,'C5a duplicate or invalid Loop identity')
        _require_integer(loop.get('iteration_count'),'iteration_count: '+lid)
        require(loop['iteration_count']>=0,'C5a negative Loop iteration count: '+lid)
        by_id[lid]=loop
    for loop in loops:
        parent=loop.get('parent_loop_id')
        if parent is not None:require(parent in by_id,'C5a Loop hierarchy names an unknown parent: '+loop['loop_id'])
    for loop in loops:
        seen=set();cursor=loop['loop_id']
        while cursor is not None:
            require(cursor not in seen,'C5a Loop hierarchy contains a cycle');seen.add(cursor)
            cursor=by_id[cursor].get('parent_loop_id')
    children={lid:[] for lid in by_id}
    for loop in loops:
        if loop.get('parent_loop_id'):children[loop['parent_loop_id']].append(loop['loop_id'])
    def effective(loop):
        count=1;cursor=loop
        while cursor is not None:
            count*=cursor['iteration_count']
            parent=cursor.get('parent_loop_id');cursor=by_id[parent] if parent is not None else None
        return count
    def loop_templates(lid):
        return [template for template in templates if template.get('loop_id')==lid]
    def source_components(lid):
        return [component for component in components if component.get('loop_id')==lid]
    def body_templates(loop):
        body=[]
        for template in templates:
            if template.get('role')!='body_template':continue
            if template.get('loop_id') is not None:
                if template['loop_id']==loop['loop_id']:body.append(template)
                continue
            if template.get('function') is not None and template['function']!=loop.get('function'):continue
            if template.get('start_line')==loop.get('body_start') and template.get('end_line')==loop.get('body_end'):
                body.append(template)
        return body
    matched_body=set();zero_targets=set();zero_sources=set()
    for loop in loops:
        lid=loop['loop_id'];count=effective(loop);has_child=bool(children[lid])
        mine_groups=[group for group in groups if group.get('loop_id')==lid]
        mine_instances=[item for item in instances if item.get('loop_id')==lid]
        mine_templates=loop_templates(lid);mine_sources=source_components(lid)
        if loop.get('lock_inside'):
            require(not has_child,'C5a Lock Loop must be the innermost Loop: '+lid)
            require(mine_sources,'C5a Lock Loop must publish source component templates: '+lid)
            require(not body_templates(loop),'C5a Lock Loop body template must be replaced by source components: '+lid)
            if count==0:
                require(not mine_groups,'C5a zero-trip Loop published Lock groups: '+lid)
                require(not mine_instances,'C5a zero-trip Loop published iteration instances: '+lid)
                for component in mine_sources:
                    require(template_map[component['template_id']]==[],
                            'C5a zero-trip Loop source component must keep empty targets: '+component['template_id'])
                    zero_sources.add(component['template_id'])
                for template in mine_templates:
                    require(template_map[template['template_id']]==[],
                            'C5a zero-trip Loop iteration template must keep empty targets: '+template['template_id'])
                continue
            require(mine_templates,'C5a nonempty Lock Loop must publish iteration templates: '+lid)
            identities={_identity(group) for group in mine_groups}
            require(len(identities)==count,'C5a Loop Lock group iteration count mismatch: '+lid)
            require(all(type(group.get('iteration_index')) is int or _identity(group)!=(None,None)
                        for group in mine_groups),'C5a Lock group has no iteration identity: '+lid)
            for component in mine_sources:
                targets=template_map[component['template_id']]
                require(targets,'C5a nonempty Loop source component has empty targets: '+component['template_id'])
                if component.get('loop_part_role')=='critical':
                    require(len(targets)==count,'C5a critical source component iteration count mismatch: '+component['template_id'])
                    require(all(target in group_ids for target in targets),
                            'C5a critical source component must target Lock groups: '+component['template_id'])
                else:
                    require(len(targets)==count,'C5a ordinary source component iteration count mismatch: '+component['template_id'])
                    require(all(target not in group_ids for target in targets),
                            'C5a ordinary source component must not target a Lock group: '+component['template_id'])
            require(len(mine_templates)==count*len(mine_sources),
                    'C5a Lock Loop iteration template count mismatch: '+lid)
            for template in mine_templates:
                targets=template_map[template['template_id']]
                require(len(targets)==1,'C5a each iteration component must have one final target: '+template['template_id'])
                if template.get('role')=='iteration_component' and template.get('mutex_id') is not None:
                    require(targets[0] in group_ids,
                            'C5a critical iteration template must target a Lock group: '+template['template_id'])
                else:
                    require(targets[0] not in group_ids,
                            'C5a ordinary iteration template must not target a Lock group: '+template['template_id'])
            for group in mine_groups:
                require(group['group_id'] in group_ids,'C5a source Lock group is not a final node: '+group['group_id'])
            for identity in identities:
                for key in ('iteration_index','outer_iteration_index','inner_iteration_index'):
                    values={item.get(key) for item in mine_groups if _identity(item)==identity}
                    require(len(values)<=1,'C5a Lock group iteration fields conflict: '+lid)
            instance_identities={_identity(item) for item in mine_instances}
            require(instance_identities==identities,'C5a Loop iteration instance identities mismatch: '+lid)
            for identity in identities:
                require(len([item for item in mine_instances if _identity(item)==identity])==len(mine_sources),
                        'C5a Loop iteration part count mismatch: '+lid)
            require(len(mine_instances)==count*len(mine_sources),'C5a Loop iteration instance count mismatch: '+lid)
            continue
        if has_child:
            require(not body_templates(loop),'C5a nested outer Loop must not publish a body template: '+lid)
            require(not mine_instances,'C5a nested outer Loop must not publish iteration instances: '+lid)
            require(not mine_groups,'C5a nested outer Loop must not publish Lock groups: '+lid)
            continue
        body=body_templates(loop)
        require(len(body)==1,'C5a leaf Loop must publish exactly one body template: '+lid)
        template=body[0];tid=template['template_id']
        require(tid not in matched_body,'C5a body template matched to multiple Loops: '+tid);matched_body.add(tid)
        targets=template_map[tid]
        if count==0:
            require(targets==[],'C5a zero-trip Loop body template must keep empty targets: '+tid)
            zero_targets.add(tid)
        else:
            require(targets and all(target not in group_ids for target in targets),
                    'C5a nonempty independent Loop body template must target ordinary rows: '+tid)
            require(len(targets)==count,'C5a independent Loop body template count mismatch: '+tid)
        require(not mine_groups,'C5a independent Loop must not publish Lock groups: '+lid)
        if count==0:require(not mine_instances,'C5a zero-trip independent Loop published iterations: '+lid)
        else:
            require(len(mine_instances)==count,'C5a independent Loop iteration instance count mismatch: '+lid)
            require({_identity(item) for item in mine_instances}=={item.get('iteration_index') for item in mine_instances},
                    'C5a independent Loop iteration identity mismatch: '+lid)
        for item in mine_instances:
            require(item.get('template_id')==tid,'C5a independent Loop iteration template mismatch: '+lid)
    unmatched=[template['template_id'] for template in templates
               if template.get('role')=='body_template' and template['template_id'] not in matched_body]
    require(not unmatched,'C5a body template has no matching leaf Loop: '+','.join(sorted(unmatched)))
    legal_source_zero={component['template_id'] for component in components
                       if by_id.get(component.get('loop_id')) is not None
                       and effective(by_id[component['loop_id']])==0}
    for tid,targets in template_map.items():
        if targets==[]:
            require(tid in zero_targets or tid in legal_source_zero,
                    'C5a unused template must belong to a zero-trip Loop: '+tid)
    # A Lock Loop with zero effective iterations has no iteration templates at
    # all; its source components are the only legal empty mappings.
    return {loop['loop_id']:effective(loop) for loop in loops}


def import_branch_loop_lock_timing(*,base_dir,base_name,input_timing):
    root=Path(base_dir)/'intermediate_results'/base_name/'pipeline'
    blocks=root/'blocks/level2'/RULE
    s=_read_loop_timing_json(blocks/'segments.json')
    source=Path(s['source_file'])
    require(hashlib.sha256(source.read_bytes()).hexdigest()==s['source_sha256'],'C5a source changed')
    raw,dag=build_level2_segments_and_dag(base_dir=Path(base_dir),base_name=base_name,source_file=source)
    st,sd,expected,dd,_=build_composition_views(base_name=base_name,source_text=source.read_bytes().decode('utf-8'),
                                                 base_segments_json=raw,base_dag_json=dag)
    expected['view']='single'
    expected=json.loads(json.dumps(expected,ensure_ascii=False,indent=2))
    dd=json.loads(json.dumps(dd,ensure_ascii=False,indent=2))
    for loop in s.get('loops',[]):_require_integer(loop.get('iteration_count'),'iteration_count: '+str(loop.get('loop_id')))
    for row in s.get('segments',[]):
        for key in ('iteration_index','outer_iteration_index','inner_iteration_index'):
            if row.get(key) is not None:_require_integer(row[key],key+': '+str(row.get('seg_id')))
    for instance in s.get('iteration_instances',[]):
        for key in ('iteration_index','outer_iteration_index','inner_iteration_index'):
            if instance.get(key) is not None:_require_integer(instance[key],key+': '+str(instance.get('segment_id')))
    require(json.dumps(s,sort_keys=True)==json.dumps(expected,sort_keys=True),'C5a segments, mappings or template binding changed')
    published_dag=_read_loop_timing_json(blocks/'dag_seg.json')
    if 'view' in published_dag:
        require(type(published_dag['view']) is str and published_dag['view'] in ('sched','single'),'C5a conflicting scheduling DAG view')
        published_dag=dict(published_dag);published_dag.pop('view')
    require(json.dumps(published_dag,sort_keys=True)==json.dumps(dd,sort_keys=True),'C5a scheduling DAG changed')
    input_path=Path(input_timing);data=_read_loop_timing_json(input_path)
    templates={t['template_id']:t for t in s['templates']}
    weights=data.get('weights',{})
    require(isinstance(weights,dict) and set(weights)==set(templates),'C5a requires exactly the explicit templates; no whole-segment or iteration weights')
    for tid,w in weights.items():
        require(isinstance(w,dict) and set(w)==set(FIELDS),'C5a template requires five timing fields: '+tid)
        require(all(type(v) is int and v>=0 for v in w.values()) and w['min_ns']<=w['avg_ns']<=w['max_ns'],'C5a invalid template metrics: '+tid)
    groups={group['group_id']:group for group in s['lock_groups']}
    require(len(groups)==len(s['lock_groups']),'C5a duplicate lock group')
    mappings=s['mappings'];member_map=mappings['grouped_node_to_member_segments'];template_map=mappings['template_to_execution_segments']
    require(set(template_map)==set(templates)|{c['template_id'] for c in s['source_component_templates']},
            'C5a template mapping coverage mismatch')
    final_ids={row['seg_id'] for row in s['segments']}
    require(all(type(target) is str and target in final_ids for targets in template_map.values() for target in targets),
            'C5a template mapping names an unknown final node')
    _validate_loop_template_contract(s,template_map,set(groups))
    result_weights={};contributions={}
    for row in s['segments']:
        sid=row['seg_id']
        if row['kind']=='lock_group':
            group=groups.get(sid);require(group is not None,'C5a unknown lock group row: '+sid)
            members=group['members'];ids=[member['member_segment_id'] for member in members]
            require(ids==member_map.get(sid,[]),'C5a lock group member mapping mismatch: '+sid)
            require(ids==group['member_segment_ids'],'C5a lock group member list mismatch: '+sid)
            require(len(ids)==len(set(ids)),'C5a duplicate lock group member: '+sid)
            require(all(member.get('mutex_id')==group['mutex_id'] for member in members),'C5a cross-mutex timing member: '+sid)
            require([member['execution_segment']['template_id'] for member in members]==group.get('member_template_ids'),
                    'C5a lock group member template trace mismatch: '+sid)
            require(group['group_id'] in mappings['grouped_node_to_member_templates']
                    and mappings['grouped_node_to_member_templates'][group['group_id']]==group['member_template_ids'],
                    'C5a lock group template mapping mismatch: '+sid)
            require(mappings['grouped_node_to_member_occurrences'].get(sid)==[
                dict(segment_id=member['member_segment_id'],template_id=member['execution_segment']['template_id'],
                     mutex_id=group['mutex_id'],loop_id=member['execution_segment'].get('loop_id'),
                     iteration_index=member['execution_segment'].get('iteration_index'),
                     outer_iteration_index=member['execution_segment'].get('outer_iteration_index'),
                     inner_iteration_index=member['execution_segment'].get('inner_iteration_index'),
                     condition_memberships=member['execution_segment'].get('condition_memberships',[]),
                     source_position_ids=member['execution_segment'].get('source_position_ids',[]))
                for member in members],'C5a lock group occurrence mapping mismatch: '+sid)
            for member in members:
                tid=member['execution_segment'].get('template_id');require(tid in weights,'C5a lock member has no explicit template: '+member['member_segment_id'])
                require(weights[tid]['count']>0,'C5a missing lock member samples: '+member['member_segment_id'])
                require(sid in template_map.get(tid,[]),'C5a lock member template does not project to group: '+tid)
            result_weights[sid]={field:sum(weights[member['execution_segment']['template_id']][field] for member in members) for field in FIELDS}
            contributions[sid]=[dict(segment_id=member['member_segment_id'],template_id=member['execution_segment']['template_id'],
                                     loop_id=member.get('loop_id'),iteration_index=member.get('iteration_index'),
                                     outer_iteration_index=member.get('outer_iteration_index'),
                                     inner_iteration_index=member.get('inner_iteration_index'),
                                     condition_memberships=member.get('condition_memberships',[]),
                                     source_position_ids=member.get('source_position_ids',[])) for member in members]
        else:
            require(row['kind'] not in CONTROL,'C5a control row in scheduling view')
            tid=row.get('template_id');require(tid in weights,'C5a execution row has no explicit template: '+sid)
            result_weights[sid]=dict(weights[tid])
    for group_id,group in groups.items():
        require(group_id in result_weights,'C5a missing lock group weight: '+group_id)
        require(group.get('member_segment_ids'),'C5a empty Lock group timing contribution: '+group_id)
    for key,targets in mappings['source_lock_region_to_group'].items():
        require(all(target in groups for target in targets),'C5a source Lock region names an unknown group: '+key)
    structural_ids={row['seg_id'] for row in st['segments']}
    for sid,target in mappings['iteration_to_group'].items():
        require(target in groups,'C5a iteration names an unknown group: '+sid)
        require(sid in structural_ids,'C5a iteration_to_group names a missing structural row: '+sid)
    require(set(result_weights)=={row['seg_id'] for row in s['segments']},'C5a final timing coverage mismatch')
    result=dict(base_name=base_name,rule_name=RULE,view='sched',weights=result_weights,
                timing_origin='explicit synthetic templates; Condition-leaf Loop copies then per-iteration Lock sums; no waits or WCET',
                lock_aggregation=dict(method='fieldwise_sum_of_actual_member_occurrences',
                                      count_semantics='sum of contributing iteration and independent member samples; zero when no iteration exists',
                                      avg_ns_semantics='sum of member means, not total divided by count',
                                      min_max_semantics='sums of member extrema, not measured group extrema or proven WCET',
                                      contributions=contributions,source_sha256=s['source_sha256']),
                input_timing_sha256=hashlib.sha256(input_path.read_bytes()).hexdigest(),
                segments_sha256=hashlib.sha256((blocks/'segments.json').read_bytes()).hexdigest())
    write_json(root/'timing/level2'/RULE/'timing.json',result)
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base-dir',type=Path,required=True);p.add_argument('--base-name',required=True);p.add_argument('--input-timing',type=Path,required=True)
    print(json.dumps(import_branch_loop_lock_timing(**vars(p.parse_args())),indent=2))


if __name__=='__main__':main()


# Public alias for callers that name the three-layer relation explicitly.
import_condition_loop_lock_timing = import_branch_loop_lock_timing
