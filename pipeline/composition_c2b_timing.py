"""Explicit component templates bound to exact C2b paths; no branch maximum."""
import copy,hashlib,json
from pathlib import Path
from ..level2.segment_dag_level2 import build_level2_segments_and_dag
from .composition_c2b_model import build_lock_path_views,RULE
from .composition_model import require
from .loop_timing import _read_loop_timing_json
from .io_utils import write_json
FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')


def import_lock_path_timing(*,base_dir,base_name,input_timing):
    root=Path(base_dir)/'intermediate_results'/base_name/'pipeline';blocks=root/'blocks/level2'/RULE
    s=_read_loop_timing_json(blocks/'segments.json')
    require(not s.get('basic_extension'),'C2b main/sequential/helper extension is structural analysis only')
    source=Path(s['source_file']);require(hashlib.sha256(source.read_bytes()).hexdigest()==s['source_sha256'],'C2b source changed')
    raw,dag=build_level2_segments_and_dag(base_dir=Path(base_dir),base_name=base_name,source_file=source)
    st,_,expected,dd,_=build_lock_path_views(base_name=base_name,source_text=source.read_bytes().decode(),base_segments_json=raw,base_dag_json=dag);expected['view']='single'
    require(json.dumps(s,sort_keys=True)==json.dumps(expected,sort_keys=True),'C2b path/member/common-source binding changed')
    published=_read_loop_timing_json(blocks/'dag_seg.json')
    if 'view' in published:
        require(type(published['view']) is str and published['view'] in ('single','sched'),'C2b conflicting DAG view');published=dict(published);published.pop('view')
    require(json.dumps(published,sort_keys=True)==json.dumps(dd,sort_keys=True),'C2b DAG binding changed')
    data=_read_loop_timing_json(Path(input_timing));weights=data.get('weights')
    require(isinstance(weights,dict) and set(weights)=={t['template_id'] for t in s['templates']},'C2b requires exactly component templates, never old whole-lock weights')
    for tid,w in weights.items():
        require(isinstance(w,dict) and set(w)==set(FIELDS) and all(type(w[f]) is int and w[f]>=0 for f in FIELDS) and w['min_ns']<=w['avg_ns']<=w['max_ns'],'C2b invalid timing template: '+tid)
    result={}
    for row in s['segments']:
        if row['kind']=='lock_path_candidate':result[row['seg_id']]={f:sum(weights[t][f] for t in row['member_template_ids']) for f in FIELDS}
        else:result[row['seg_id']]=copy.deepcopy(weights[row['template_id']])
    out=dict(base_name=base_name,rule_name=RULE,view='sched',weights=result,timing_origin='explicit synthetic component templates; path-local fieldwise sum, not measured time or WCET',source_sha256=s['source_sha256'],segments_sha256=hashlib.sha256((blocks/'segments.json').read_bytes()).hexdigest())
    write_json(root/'timing/level2'/RULE/'timing.json',out);return out
