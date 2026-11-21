"""Import explicit C1 templates; no source instrumentation or execution."""
import argparse
import hashlib
import json
from pathlib import Path
from ..level2.segment_dag_level2 import build_level2_segments_and_dag
from .composition_model import CONTROL, CompositionError, require
from .composition_c2a_model import RULE, build_branch_lock_views as build_composition_views
from .loop_timing import _read_loop_timing_json
from .lock_model import aggregate_lock_timing, LockModelError
from .io_utils import write_json



def _require_integer(value, field):
    require(type(value) is int, 'C1 '+field+' must be an exact integer')


def import_branch_lock_timing(*, base_dir, base_name, input_timing):
    root=Path(base_dir)/'intermediate_results'/base_name/'pipeline'
    blocks=root/'blocks/level2'/RULE
    s=_read_loop_timing_json(blocks/'segments.json')
    source=Path(s['source_file'])
    require(hashlib.sha256(source.read_bytes()).hexdigest()==s['source_sha256'],'C1 source changed')
    raw,dag=build_level2_segments_and_dag(base_dir=Path(base_dir),base_name=base_name,source_file=source)
    st,sd,expected,dd,_=build_composition_views(base_name=base_name,source_text=source.read_bytes().decode('utf-8'),base_segments_json=raw,base_dag_json=dag)
    expected['view']='single'
    # Compare the complete payload in the same representation as write_json.
    expected=json.loads(json.dumps(expected,ensure_ascii=False,indent=2))
    dd=json.loads(json.dumps(dd,ensure_ascii=False,indent=2))
    for row in s.get('segments',[]):
        if row.get('role') == 'iteration':
            _require_integer(row.get('iteration_index'),'iteration_index: '+str(row.get('seg_id')))
    for loop in s.get('loops',[]):
        _require_integer(loop.get('iteration_count'),'iteration_count: '+str(loop.get('loop_id')))
    # JSON integer, Boolean and floating-point values are distinct, even when
    # Python's scalar equality says True == 1 == 1.0. Keep the entire binding.
    require(json.dumps(s,sort_keys=True)==json.dumps(expected,sort_keys=True),'C1 segments, mappings or template binding changed')
    published_dag=_read_loop_timing_json(blocks/'dag_seg.json')
    # The legacy publisher does not inject a DAG view. If supplied, it must
    # identify this scheduling alias, never a structural or unknown view.
    if 'view' in published_dag:
        require(type(published_dag['view']) is str and published_dag['view'] in ('sched','single'), 'C1 conflicting scheduling DAG view')
        published_dag=dict(published_dag)
        published_dag.pop('view')
    require(json.dumps(published_dag,sort_keys=True)==json.dumps(dd,sort_keys=True),'C1 scheduling DAG changed')
    input_path=Path(input_timing);data=_read_loop_timing_json(input_path)
    templates={t['template_id']:t for t in s['templates']}
    weights=data.get('weights',{})
    require(isinstance(weights,dict) and set(weights)==set(templates), 'C1 requires exactly the explicit templates; no whole-segment weights')
    rows=[r for r in st['segments'] if r['kind'] not in CONTROL]
    copied={r['seg_id']:weights[r['template_id']] for r in rows}
    projection={r['seg_id']:r['seg_id'] for r in rows}
    for g in s['lock_groups']:
        for member in g['member_segment_ids']: projection[member]=g['group_id']
    # Reuse the frozen Lock sum contract only after exact source/model binding.
    adapter=dict(base_name=base_name,capabilities={'scheduler_compatible':True},source_sha256=s['source_sha256'],
                 lock_groups=s['lock_groups'],mappings={'original_segment_to_grouped_node':projection})
    # Validate even zero-trip templates, which do not appear in copied weights.
    for tid,w in weights.items():
        require(isinstance(w,dict) and set(w)=={'total_ns','count','avg_ns','min_ns','max_ns'}, 'C1 template requires five timing fields: '+tid)
        require(all(type(v) is int and v>=0 for v in w.values()) and w['min_ns']<=w['avg_ns']<=w['max_ns'], 'C1 invalid template metrics: '+tid)
    # Full expected-model comparison above binds every local group and branch.
    result=dict(base_name=base_name,weights={projection[sid]:dict(w) for sid,w in copied.items() if projection[sid]==sid})
    for group in s['lock_groups']:
        require(all(copied[sid]['count']>0 for sid in group['member_segment_ids']), 'C2a missing lock member samples')
        result['weights'][group['group_id']]={field:sum(copied[sid][field] for sid in group['member_segment_ids']) for field in ('total_ns','count','avg_ns','min_ns','max_ns')}
    require(set(result['weights'])=={r['seg_id'] for r in s['segments']}, 'C1 final timing coverage mismatch')
    result.update(rule_name=RULE,view='sched',timing_origin='explicit synthetic templates; Loop copy then Lock sum; not measured time or WCET',
                  input_timing_sha256=hashlib.sha256(input_path.read_bytes()).hexdigest(),segments_sha256=hashlib.sha256((blocks/'segments.json').read_bytes()).hexdigest())
    write_json(root/'timing/level2'/RULE/'timing.json',result)
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base-dir',type=Path,required=True);p.add_argument('--base-name',required=True);p.add_argument('--input-timing',type=Path,required=True)
    print(json.dumps(import_branch_lock_timing(**vars(p.parse_args())),indent=2))


if __name__=='__main__': main()
