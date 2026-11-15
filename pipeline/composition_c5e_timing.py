"""Import C5e component templates; path-local candidate sums, no branch maximum."""
import argparse,copy,hashlib,json
from pathlib import Path
from ..level2.segment_dag_level2 import build_level2_segments_and_dag
from .composition_model import CONTROL,require
from .composition_c5e_model import RULE,build_lock_condition_loop_views
from .loop_timing import _read_loop_timing_json
from .io_utils import write_json
FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')


def _integer(value,field):require(type(value) is int,'C5e '+field+' must be an exact integer')


def import_lock_condition_loop_timing(*,base_dir,base_name,input_timing):
    root=Path(base_dir)/'intermediate_results'/base_name/'pipeline';blocks=root/'blocks/level2'/RULE
    s=_read_loop_timing_json(blocks/'segments.json');source=Path(s['source_file'])
    require(hashlib.sha256(source.read_bytes()).hexdigest()==s['source_sha256'],'C5e source changed')
    raw,dag=build_level2_segments_and_dag(base_dir=Path(base_dir),base_name=base_name,source_file=source)
    st,sd,expected,dd,_=build_lock_condition_loop_views(base_name=base_name,source_text=source.read_bytes().decode(),base_segments_json=raw,base_dag_json=dag)
    expected['view']='single'
    expected=json.loads(json.dumps(expected,ensure_ascii=False,indent=2));dd=json.loads(json.dumps(dd,ensure_ascii=False,indent=2))
    require(json.dumps(s,sort_keys=True)==json.dumps(expected,sort_keys=True),'C5e segments, mappings or templates changed')
    published=_read_loop_timing_json(blocks/'dag_seg.json')
    if 'view' in published:
        require(type(published['view']) is str and published['view'] in ('single','sched'),'C5e conflicting DAG view');published=dict(published);published.pop('view')
    require(json.dumps(published,sort_keys=True)==json.dumps(dd,sort_keys=True),'C5e scheduling DAG changed')
    for loop in s.get('loops',[]):_integer(loop.get('iteration_count'),'iteration_count: '+str(loop.get('loop_id')))
    for row in s.get('segments',[]):
        for key in ('iteration_index','outer_iteration_index','inner_iteration_index'):
            if row.get(key) is not None:_integer(row[key],key+': '+str(row.get('seg_id')))
    data=_read_loop_timing_json(Path(input_timing));weights=data.get('weights');templates={t['template_id']:t for t in s['templates']}
    require(isinstance(weights,dict) and set(weights)==set(templates),'C5e requires exactly the explicit templates')
    for tid,w in weights.items():
        require(isinstance(w,dict) and set(w)==set(FIELDS) and all(type(v) is int and v>=0 for v in w.values()) and w['min_ns']<=w['avg_ns']<=w['max_ns'],'C5e invalid template metrics: '+tid)
    groups={g['group_id']:g for g in s.get('lock_groups',[])};rows_by={r['seg_id']:r for r in s['segments']}
    mapping=s['mappings'].get('template_to_execution_segments',{});component_ids={c['template_id'] for c in s.get('source_component_templates',[])}
    require(set(mapping)==set(templates)|component_ids,'C5e template mapping coverage')
    for tid,targets in mapping.items():
        require(all(target in rows_by for target in targets),'C5e template mapping names a missing final segment: '+tid)
        for target in targets:
            row=rows_by[target]
            if row['kind']=='lock_path_candidate':require(tid in row.get('member_template_ids',[]),'C5e template maps to wrong candidate: '+tid)
            elif row['kind']=='lock_group':
                group=groups.get(target);require(group is not None,'C5e template maps to unknown group: '+tid)
                members=group.get('member_template_ids') or [m.get('template_id') or (m.get('execution_segment') or {}).get('template_id') for m in group.get('members',[])]
                require(tid in members,'C5e template maps to wrong group: '+tid)
            else:require(row.get('template_id')==tid,'C5e template maps to wrong execution segment: '+tid)
    for tid in templates:
        used=any(row.get('template_id')==tid for row in s['segments']) or any(tid in row.get('member_template_ids',[]) for row in s['segments'] if row['kind']=='lock_path_candidate')
        if used:require(mapping.get(tid),'C5e used template has empty mapping: '+tid)
    result={}
    for row in s['segments']:
        if row['kind']=='lock_path_candidate':
            members=row.get('member_template_ids',[]);require(members and all(t in weights for t in members),'C5e candidate has unpublished template')
            result[row['seg_id']]={f:sum(weights[t][f] for t in members) for f in FIELDS}
        elif row['kind']=='lock_group':
            group=groups.get(row['seg_id']);require(group is not None,'C5e unknown lock group row')
            members=group.get('member_template_ids') or [m.get('template_id') or (m.get('execution_segment') or {}).get('template_id') for m in group.get('members',[])]
            require(members and all(t in weights for t in members),'C5e group has unpublished template')
            result[row['seg_id']]={f:sum(weights[t][f] for t in members) for f in FIELDS}
        else:
            require(row.get('template_id') in weights,'C5e execution row has no published template')
            result[row['seg_id']]=copy.deepcopy(weights[row['template_id']])
    require(set(result)=={r['seg_id'] for r in s['segments']},'C5e final timing coverage mismatch')
    out=dict(base_name=base_name,rule_name=RULE,view='sched',weights=result,
             timing_origin='explicit component templates; path-local Condition-leaf sums inside one outer Lock; no waits or WCET',
             input_timing_sha256=hashlib.sha256(Path(input_timing).read_bytes()).hexdigest(),
             segments_sha256=hashlib.sha256((blocks/'segments.json').read_bytes()).hexdigest())
    write_json(root/'timing/level2'/RULE/'timing.json',out);return out


import_condition_lock_loop_timing=import_lock_condition_loop_timing


def main():
    p=argparse.ArgumentParser();p.add_argument('--base-dir',type=Path,required=True);p.add_argument('--base-name',required=True);p.add_argument('--input-timing',type=Path,required=True)
    print(json.dumps(import_lock_condition_loop_timing(**vars(p.parse_args())),indent=2))
if __name__=='__main__':main()
