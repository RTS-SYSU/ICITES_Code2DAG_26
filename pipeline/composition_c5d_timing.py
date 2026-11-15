"""Import explicit C5d component templates; path-local Lock candidate sums."""
import argparse,copy,hashlib,json
from pathlib import Path
from ..level2.segment_dag_level2 import build_level2_segments_and_dag
from .composition_model import CONTROL,require
from .composition_c5d_model import RULE,build_loop_condition_views as build_composition_views
from .loop_timing import _read_loop_timing_json
from .io_utils import write_json
FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')


def _integer(value,field):require(type(value) is int,'C5d '+field+' must be an exact integer')


def _validate_components(s):
    loops={l['loop_id']:l for l in s.get('loops',[])}
    for loop in loops.values():
        parent=loop.get('parent_loop_id')
        if parent is not None:require(parent in loops,'C5d Loop hierarchy names an unknown parent')
    def effective(loop):
        n=1;cur=loop;seen=set()
        while cur is not None:
            require(cur['loop_id'] not in seen,'C5d Loop hierarchy contains a cycle');seen.add(cur['loop_id'])
            _integer(cur.get('iteration_count'),'iteration_count');require(cur['iteration_count']>=0,'C5d negative Loop count')
            n*=cur['iteration_count'];cur=loops.get(cur.get('parent_loop_id'))
        return n
    comps=list(s.get('source_component_templates',[]));ids={c['template_id'] for c in comps}
    require(not (ids&{t['template_id'] for t in s.get('templates',[])}),'C5d source component/template conflict')
    mapping=s['mappings'].get('template_to_execution_segments',{})
    require(set(mapping)=={t['template_id'] for t in s.get('templates',[])}|ids,'C5d template mapping coverage')
    for c in comps:
        require(c.get('loop_id') in loops,'C5d source component names unknown Loop');targets=mapping[c['template_id']]
        require(isinstance(targets,list) and len(targets)==len(set(targets)),'C5d invalid component targets')
        if effective(loops[c['loop_id']])==0:require(targets==[],'C5d zero-trip component must keep empty targets')
        else:require(targets,'C5d inhabited component has empty targets')


def import_loop_lock_condition_timing(*,base_dir,base_name,input_timing):
    root=Path(base_dir)/'intermediate_results'/base_name/'pipeline';blocks=root/'blocks/level2'/RULE
    s=_read_loop_timing_json(blocks/'segments.json');source=Path(s['source_file'])
    require(hashlib.sha256(source.read_bytes()).hexdigest()==s['source_sha256'],'C5d source changed')
    raw,dag=build_level2_segments_and_dag(base_dir=Path(base_dir),base_name=base_name,source_file=source)
    st,sd,expected,dd,_=build_composition_views(base_name=base_name,source_text=source.read_bytes().decode(),base_segments_json=raw,base_dag_json=dag);expected['view']='single'
    expected=json.loads(json.dumps(expected,ensure_ascii=False,indent=2));dd=json.loads(json.dumps(dd,ensure_ascii=False,indent=2))
    for row in s.get('segments',[]):
        if row.get('role')=='iteration':_integer(row.get('iteration_index'),'iteration_index')
    for loop in s.get('loops',[]):_integer(loop.get('iteration_count'),'iteration_count')
    require(json.dumps(s,sort_keys=True)==json.dumps(expected,sort_keys=True),'C5d segments, mappings or templates changed')
    published=_read_loop_timing_json(blocks/'dag_seg.json')
    if 'view' in published:
        require(type(published['view']) is str and published['view'] in ('single','sched'),'C5d conflicting DAG view');published=dict(published);published.pop('view')
    require(json.dumps(published,sort_keys=True)==json.dumps(dd,sort_keys=True),'C5d scheduling DAG changed')
    _validate_components(s)
    templates={t['template_id']:t for t in s['templates']};data=_read_loop_timing_json(Path(input_timing));weights=data.get('weights')
    require(isinstance(weights,dict) and set(weights)==set(templates),'C5d requires exactly the explicit templates')
    for tid,w in weights.items():
        require(isinstance(w,dict) and set(w)==set(FIELDS),'C5d template requires five timing fields: '+tid)
        require(all(type(w[f]) is int and w[f]>=0 for f in FIELDS) and w['min_ns']<=w['avg_ns']<=w['max_ns'],'C5d invalid template metrics: '+tid)
    result={}
    for row in s['segments']:
        if row['kind']=='lock_path_candidate':
            members=row.get('member_template_ids',[]);require(members and len(members)==len(set(members)),'C5d path candidate has invalid member templates')
            require(all(t in weights for t in members),'C5d path candidate references unpublished template')
            result[row['seg_id']]={f:sum(weights[t][f] for t in members) for f in FIELDS}
        else:
            require(row.get('template_id') in weights,'C5d execution row has no published template');result[row['seg_id']]=copy.deepcopy(weights[row['template_id']])
    require(set(result)=={r['seg_id'] for r in s['segments']},'C5d timing coverage mismatch')
    blob=json.dumps(result)
    for bad in ('wait_ns','blocking_ns','blocking','wcet_ns','wcet'):
        require(bad not in blob,'C5d timing must not publish '+bad)
    out=dict(base_name=base_name,rule_name=RULE,view='sched',weights=result,timing_origin='explicit published component templates; path-local candidate sums; not measured time, waiting, blocking or WCET',input_timing_sha256=hashlib.sha256(Path(input_timing).read_bytes()).hexdigest(),segments_sha256=hashlib.sha256((blocks/'segments.json').read_bytes()).hexdigest())
    write_json(root/'timing/level2'/RULE/'timing.json',out);return out


import_loop_condition_lock_timing=import_loop_lock_condition_timing


def main():
    p=argparse.ArgumentParser();p.add_argument('--base-dir',type=Path,required=True);p.add_argument('--base-name',required=True);p.add_argument('--input-timing',type=Path,required=True)
    print(json.dumps(import_loop_lock_condition_timing(**vars(p.parse_args())),indent=2))
if __name__=='__main__':main()
