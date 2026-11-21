"""Import explicit C5f templates; C4b fieldwise group sums for the outer Lock."""
import argparse,copy,hashlib,json
from pathlib import Path
from ..level2.segment_dag_level2 import build_level2_segments_and_dag
from .composition_model import CONTROL,require
from .composition_c5f_model import RULE,build_lock_loop_condition_views
from .loop_timing import _read_loop_timing_json
from .io_utils import write_json
FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')
def _integer(v,f):require(type(v) is int,'C5f '+f+' must be exact integer')
def import_lock_loop_condition_timing(*,base_dir,base_name,input_timing):
    root=Path(base_dir)/'intermediate_results'/base_name/'pipeline';blocks=root/'blocks/level2'/RULE;s=_read_loop_timing_json(blocks/'segments.json');source=Path(s['source_file']);require(hashlib.sha256(source.read_bytes()).hexdigest()==s['source_sha256'],'C5f source changed')
    raw,dag=build_level2_segments_and_dag(base_dir=Path(base_dir),base_name=base_name,source_file=source);st,sd,expected,dd,_=build_lock_loop_condition_views(base_name=base_name,source_text=source.read_bytes().decode(),base_segments_json=raw,base_dag_json=dag);expected['view']='single';expected=json.loads(json.dumps(expected,ensure_ascii=False,indent=2));dd=json.loads(json.dumps(dd,ensure_ascii=False,indent=2));require(json.dumps(s,sort_keys=True)==json.dumps(expected,sort_keys=True),'C5f segments/mappings/templates changed')
    pub=_read_loop_timing_json(blocks/'dag_seg.json')
    if 'view' in pub:require(type(pub['view']) is str and pub['view'] in ('single','sched'),'C5f conflicting DAG view');pub=dict(pub);pub.pop('view')
    require(json.dumps(pub,sort_keys=True)==json.dumps(dd,sort_keys=True),'C5f scheduling DAG changed')
    for loop in s.get('loops',[]):_integer(loop.get('iteration_count'),'iteration_count')
    data=_read_loop_timing_json(Path(input_timing));weights=data.get('weights');templates={t['template_id']:t for t in s['templates']};require(isinstance(weights,dict) and set(weights)==set(templates),'C5f requires exactly explicit templates')
    for tid,w in weights.items():require(isinstance(w,dict) and set(w)==set(FIELDS) and all(type(v) is int and v>=0 for v in w.values()) and w['min_ns']<=w['avg_ns']<=w['max_ns'],'C5f invalid template metrics: '+tid)
    groups={g['group_id']:g for g in s['lock_groups']};result={}
    def member_template(member):
        return member.get('template_id') or (member.get('execution_segment') or {}).get('template_id')
    for row in s['segments']:
        if row['kind']=='lock_group':
            g=groups.get(row['seg_id']);require(g is not None,'C5f unknown group');tids=[member_template(m) for m in g['members']];tids=[t for t in tids if t];require(tids and all(t in weights for t in tids),'C5f group template missing');result[row['seg_id']]={f:sum(weights[t][f] for t in tids) for f in FIELDS}
        else:
            require(row.get('template_id') in weights,'C5f execution row template missing');result[row['seg_id']]=copy.deepcopy(weights[row['template_id']])
    require(set(result)=={r['seg_id'] for r in s['segments']},'C5f timing coverage');out=dict(base_name=base_name,rule_name=RULE,view='sched',weights=result,timing_origin='explicit component templates; C4b critical-region fieldwise sums; no waits or WCET',segments_sha256=hashlib.sha256((blocks/'segments.json').read_bytes()).hexdigest(),input_timing_sha256=hashlib.sha256(Path(input_timing).read_bytes()).hexdigest());write_json(root/'timing/level2'/RULE/'timing.json',out);return out
import_loop_lock_condition_timing=import_lock_loop_condition_timing
def main():
    p=argparse.ArgumentParser();p.add_argument('--base-dir',type=Path,required=True);p.add_argument('--base-name',required=True);p.add_argument('--input-timing',type=Path,required=True);print(json.dumps(import_lock_loop_condition_timing(**vars(p.parse_args())),indent=2))
if __name__=='__main__':main()
