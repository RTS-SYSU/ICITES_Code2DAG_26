"""Import explicit C4b templates; aggregate only real critical-region contributions."""
import argparse
import hashlib
import json
from pathlib import Path
from ..level2.segment_dag_level2 import build_level2_segments_and_dag
from .composition_model import CONTROL, require
from .composition_c4b_model import RULE, build_loop_in_lock_views as build_composition_views
from .loop_timing import _read_loop_timing_json
from .io_utils import write_json

FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')


def _require_integer(value,field):
    require(type(value) is int,'C4b '+field+' must be an exact integer')


def _validate_loop_template_contract(segments,template_map,group_ids):
    """Validate published Loop templates by source range and hierarchy, never by ID text."""
    loops=list(segments.get('loops',[]));templates=list(segments.get('templates',[]));instances=list(segments.get('iteration_instances',[]))
    template_ids={template['template_id'] for template in templates}
    require(set(template_map)==template_ids,'C4b template mapping must cover exactly the published templates')
    by_id={}
    for loop in loops:
        lid=loop.get('loop_id')
        require(type(lid) is str and bool(lid) and lid not in by_id,'C4b duplicate or invalid Loop identity')
        _require_integer(loop.get('iteration_count'),'iteration_count: '+lid)
        by_id[lid]=loop
    for loop in loops:
        parent=loop.get('parent_loop_id')
        if parent is not None:require(parent in by_id,'C4b Loop hierarchy names an unknown parent: '+loop['loop_id'])
    for loop in loops:
        seen=set();cursor=loop['loop_id']
        while cursor is not None:
            require(cursor not in seen,'C4b Loop hierarchy contains a cycle');seen.add(cursor)
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
    matched=set();zero_body=set()
    for loop in loops:
        lid=loop['loop_id'];has_child=bool(children[lid])
        def is_body(template):
            if template.get('role')!='body_template':return False
            if template.get('loop_id') is not None:return template['loop_id']==lid
            if template.get('function') is not None and template['function']!=loop.get('function'):return False
            return template.get('start_line')==loop.get('body_start') and template.get('end_line')==loop.get('body_end')
        body=[template for template in templates if is_body(template)]
        if has_child:
            require(not body,'C4b nested outer Loop must not publish a body template: '+lid)
            require(not any(item.get('loop_id')==lid for item in instances),'C4b nested outer Loop must not publish iteration instances: '+lid)
            continue
        require(len(body)==1,'C4b leaf Loop must publish exactly one body template: '+lid)
        tid=body[0]['template_id']
        require(tid not in matched,'C4b body template matched to multiple Loops: '+tid);matched.add(tid)
        targets=template_map[tid];count=effective(loop)
        if count==0:
            require(targets==[],'C4b zero-iteration Loop body template must keep empty targets: '+tid)
            zero_body.add(tid)
        else:
            require(len(targets)==1 and targets[0] in group_ids,'C4b nonempty Loop body template must target its Lock group: '+tid)
        mine=[item for item in instances if item.get('loop_id')==lid]
        if count==0:require(not mine,'C4b zero-iteration Loop published iteration instances: '+lid)
        else:
            require(len(mine)==count,'C4b Loop iteration instance count mismatch: '+lid)
            require(all(item.get('template_id')==tid for item in mine),'C4b Loop iteration template mismatch: '+lid)
    unmatched=[template['template_id'] for template in templates
               if template.get('role')=='body_template' and template['template_id'] not in matched]
    require(not unmatched,'C4b body template has no matching leaf Loop: '+','.join(sorted(unmatched)))
    for template in templates:
        tid=template['template_id']
        if template_map[tid]==[]:require(tid in zero_body,'C4b unused template must be a zero-iteration Loop body template: '+tid)


def import_loop_in_lock_timing(*,base_dir,base_name,input_timing):
    root=Path(base_dir)/'intermediate_results'/base_name/'pipeline'
    blocks=root/'blocks/level2'/RULE
    s=_read_loop_timing_json(blocks/'segments.json')
    source=Path(s['source_file'])
    require(hashlib.sha256(source.read_bytes()).hexdigest()==s['source_sha256'],'C4b source changed')
    raw,dag=build_level2_segments_and_dag(base_dir=Path(base_dir),base_name=base_name,source_file=source)
    st,sd,expected,dd,_=build_composition_views(base_name=base_name,source_text=source.read_bytes().decode('utf-8'),
                                                 base_segments_json=raw,base_dag_json=dag)
    expected['view']='single'
    expected=json.loads(json.dumps(expected,ensure_ascii=False,indent=2))
    dd=json.loads(json.dumps(dd,ensure_ascii=False,indent=2))
    require(json.dumps(s,sort_keys=True)==json.dumps(expected,sort_keys=True),'C4b segments, mappings or template binding changed')
    published_dag=_read_loop_timing_json(blocks/'dag_seg.json')
    if 'view' in published_dag:
        require(type(published_dag['view']) is str and published_dag['view'] in ('sched','single'),'C4b conflicting scheduling DAG view')
        published_dag=dict(published_dag);published_dag.pop('view')
    require(json.dumps(published_dag,sort_keys=True)==json.dumps(dd,sort_keys=True),'C4b scheduling DAG changed')
    for loop in s.get('loops',[]):_require_integer(loop.get('iteration_count'),'iteration_count: '+str(loop.get('loop_id')))
    for row in s.get('segments',[]):
        for key in ('iteration_index','outer_iteration_index','inner_iteration_index'):
            if row.get(key) is not None:_require_integer(row[key],key+': '+str(row.get('seg_id')))
    if s.get('iteration_instances'):
        for instance in s['iteration_instances']:
            for key in ('iteration_index','outer_iteration_index','inner_iteration_index'):
                if instance.get(key) is not None:_require_integer(instance[key],key+': '+str(instance.get('segment_id')))
    input_path=Path(input_timing);data=_read_loop_timing_json(input_path)
    templates={t['template_id']:t for t in s['templates']}
    weights=data.get('weights',{})
    require(isinstance(weights,dict) and set(weights)==set(templates),'C4b requires exactly the explicit templates; no whole-segment or iteration weights')
    for tid,w in weights.items():
        require(isinstance(w,dict) and set(w)==set(FIELDS),'C4b template requires five timing fields: '+tid)
        require(all(type(v) is int and v>=0 for v in w.values()) and w['min_ns']<=w['avg_ns']<=w['max_ns'],'C4b invalid template metrics: '+tid)
    groups={group['group_id']:group for group in s['lock_groups']}
    require(len(groups)==len(s['lock_groups']),'C4b duplicate lock group')
    mappings=s['mappings'];member_map=mappings['grouped_node_to_member_segments']
    template_map=mappings['template_to_execution_segments']
    result_weights={};contributions={}
    for row in s['segments']:
        sid=row['seg_id']
        if row['kind']=='lock_group':
            group=groups.get(sid);require(group is not None,'C4b unknown lock group row: '+sid)
            members=group['members'];ids=[member['seg_id'] for member in members]
            require(ids==member_map.get(sid,[]),'C4b lock group member mapping mismatch: '+sid)
            require(ids==group['member_segment_ids'],'C4b lock group member list mismatch: '+sid)
            require(len(ids)==len(set(ids)),'C4b duplicate lock group member: '+sid)
            require(all(member.get('mutex_id')==group['mutex_id'] for member in members),'C4b cross-mutex timing member: '+sid)
            for member in members:
                tid=member.get('template_id');require(tid in weights,'C4b lock member has no explicit template: '+member['seg_id'])
                require(sid in template_map.get(tid,[]),'C4b lock member template does not project to group: '+tid)
            result_weights[sid]={field:sum(weights[member['template_id']][field] for member in members) for field in FIELDS}
            contributions[sid]=[dict(segment_id=member['seg_id'],template_id=member['template_id'],
                                     loop_id=member.get('loop_id'),iteration_index=member.get('iteration_index'),
                                     outer_iteration_index=member.get('outer_iteration_index'),
                                     inner_iteration_index=member.get('inner_iteration_index'),
                                     source_position_ids=member.get('source_position_ids',[])) for member in members]
        else:
            require(row['kind'] not in CONTROL,'C4b control row in scheduling view')
            tid=row.get('template_id');require(tid in weights,'C4b execution row has no explicit template: '+sid)
            result_weights[sid]=dict(weights[tid])
    for group_id,group in groups.items():
        require(group_id in result_weights,'C4b missing lock group weight: '+group_id)
        expected_templates=group['member_template_ids']
        actual_templates=[]
        for member in group['members']:
            if member['template_id'] not in actual_templates:actual_templates.append(member['template_id'])
        require(actual_templates==expected_templates,'C4b lock group template trace mismatch: '+group_id)
    _validate_loop_template_contract(s,template_map,set(groups))
    for key,targets in mappings['source_lock_region_to_group'].items():
        require(targets in groups,'C4b source Lock region names an unknown group: '+key)
    structural_ids={row['seg_id'] for row in st['segments']}
    for sid,target in mappings['iteration_to_group'].items():
        require(target in groups,'C4b iteration names an unknown group: '+sid)
        require(sid in structural_ids,'C4b iteration_to_group names a missing structural row: '+sid)
    require(set(result_weights)=={row['seg_id'] for row in s['segments']},'C4b final timing coverage mismatch')
    result=dict(base_name=base_name,rule_name=RULE,view='sched',weights=result_weights,
                timing_origin='explicit synthetic templates; static Loop occurrence sums inside one Lock critical region; no waits/WCET',
                lock_aggregation=dict(method='fieldwise_sum_of_actual_member_occurrences',
                                      count_semantics='sum of contributing iteration and ordinary member samples; zero for an empty critical region',
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
    print(json.dumps(import_loop_in_lock_timing(**vars(p.parse_args())),indent=2))


if __name__=='__main__':main()
