import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests.test_composition_c1 import CompositionC1Tests,ALGOS,reachable
from Code2DAG.pipeline.composition_c4a_model import RULE,build_loop_lock_views
from Code2DAG.pipeline.composition_c4a_timing import import_loop_lock_timing
from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.pipeline.schedule import run_schedule
ROOT=Path(__file__).resolve().parents[2];SOURCES=ROOT/'Code2DAG/source_files/composition_c4a'

def fixture(name):
 text=(SOURCES/(name+'.c')).read_text();lines=text.splitlines();start=next(i for i,l in enumerate(lines,1) if l.startswith('void *task'));end=next(i for i in range(start+1,len(lines)+1) if lines[i-1]=='}');main=next(i for i,l in enumerate(lines,1) if l.startswith('int main'))
 rows=[dict(seg_id='OLD',function='task',kind='compute',start_line=start+1,end_line=end-1),dict(seg_id='MAIN',function='main',kind='compute',start_line=main+1,end_line=len(lines)-1)]
 raw=dict(base_name='fixture',source_file=str(SOURCES/(name+'.c')),segments=rows,thread_entry_functions=['task'],node_locations={},node_to_segment={})
 return text,raw,dict(nodes=['OLD','MAIN'],edges=[dict(src='MAIN',dst='OLD',kind='create',custom={'keep':[1]})])

class C4aTests(unittest.TestCase):
 def test_instance_contract_zero_and_two_dimensions(self):
  expected={'zero':0,'one':1,'many':3,'same_mutex':3,'two_mutex':6,'l2_first':3,'l2_same':5,'l2_different':10,'l3':6,'outside_condition':3}
  for name,count in expected.items():
   with self.subTest(name=name):
    text,raw,dag=fixture(name);saved=copy.deepcopy((raw,dag));st,sd,s,d,_=build_loop_lock_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
    self.assertEqual(saved,(raw,dag));groups=s['lock_groups'];self.assertEqual(count,len(groups));self.assertEqual(count,len({g['group_id'] for g in groups}))
    for g in groups:
     self.assertIn('loop_id',g);self.assertIs(type(g['iteration_index']),int);self.assertIn('iteration:',g['group_id']);self.assertEqual('LOCKGROUP:'+g['mutex_id'],g['base_group_id'])
     self.assertTrue(all(m['execution_segment']['loop_iteration_id']==g['loop_iteration_id'] for m in g['members']))
     self.assertEqual(2 if name=='same_mutex' else 1,len(g['members']))
     for m in g['members']:
      for key in m['execution_segment']['source_position_ids']:self.assertIn(g['group_id'],s['mappings']['source_position_to_execution_segments'][key])
    if name=='l3':self.assertEqual({(i,j) for i in range(3) for j in range(2)},{(g['outer_iteration_index'],g['inner_iteration_index']) for g in groups})
    for graph in [sd,d]:topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']]);self.assertTrue(all(e['src']!=e['dst'] for e in graph['edges']));self.assertEqual(len(graph['nodes']),len(set(graph['nodes'])))
    byloop={}
    for r in s['segments']:
     if r.get('loop_iteration_id'):byloop.setdefault(r['loop_id'],{}).setdefault(r['iteration_index'] if name!='l3' else (r['outer_iteration_index'],r['inner_iteration_index']),[]).append(r['seg_id'])
    for iterations in byloop.values():
     keys=sorted(iterations)
     for a,b in zip(keys,keys[1:]):self.assertTrue(all(reachable(d,u,v) for u in iterations[a] for v in iterations[b]))
    for tid,targets in s['mappings']['template_to_execution_segments'].items():
     if tid in {t['template_id'] for t in s['source_component_templates']} and name=='zero':self.assertEqual([],targets,tid)
     else:self.assertTrue(targets,tid)

 def test_native_timing_algorithms_and_original_program(self):
  helper=CompositionC1Tests();totals={'zero':1100,'one':1106,'many':1118,'same_mutex':1148,'two_mutex':1433,'l2_first':1165,'l2_same':1137,'l2_different':1662,'l3':1136,'outside_condition':1131}
  for name,total in totals.items():
   with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
    source=SOURCES/(name+'.c');before=source.read_bytes();base,p=helper.native(tmp,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source);b=p/'blocks/level2'/RULE;s=json.loads((b/'segments.json').read_text());dag=json.loads((b/'dag_seg.json').read_text())
    weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])};inp=Path(tmp)/'weights.json';inp.write_text(json.dumps(dict(weights=weights)));result=import_loop_lock_timing(base_dir=base,base_name=name,input_timing=inp)
    for row in s['segments']:
     wanted={k:sum(weights[m['execution_segment']['template_id']][k] for m in row['members']) for k in ['total_ns','count','avg_ns','min_ns','max_ns']} if row['kind']=='lock_group' else weights[row['template_id']]
     self.assertEqual(wanted,result['weights'][row['seg_id']])
    for algo in ALGOS:self.assertEqual(set(dag['nodes']),set(run_schedule(base_dir=base,base_name=name,level='level2',rule_name=RULE,algo_name=algo)['priorities']))
    binary=Path(tmp)/'program';helper.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual('RESULT:'+str(total)+'\n',helper.command([binary]).stdout);self.assertEqual(before,source.read_bytes())

 def test_native_outside_condition_in_main(self):
  # Outside Condition is disjoint from the complete task-owned Loop/Lock region.
  text=(SOURCES/'many.c').read_text().replace('    pthread_join(t, NULL);','    pthread_join(t, NULL);\n    if (result % 2) {\n        result += 11;\n    } else {\n        result += 13;\n    }')
  with tempfile.TemporaryDirectory() as tmp:
   source=Path(tmp)/'outside_main.c';source.write_text(text);base,p=CompositionC1Tests().native(tmp,source)
   run_composition_blocks(base_dir=base,base_name=source.stem,source_file=source)
   s=json.loads((p/'blocks/level2'/RULE/'segments.json').read_text())
   self.assertEqual(1,len(s['condition_regions']));self.assertEqual('main',s['condition_regions'][0]['function'])
