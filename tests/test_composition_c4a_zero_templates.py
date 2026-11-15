"""Legal source templates survive zero iteration without placeholder execution."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c4a as c4a
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition_c4a_model import RULE,build_loop_lock_views
from Code2DAG.pipeline.composition_c4a_timing import import_loop_lock_timing
from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.schedule import run_schedule

CASES=[('zero','zero',0,None,1100),('two_mutex_zero','two_mutex',0,None,1100),('one','one',1,None,1106),('first_zero','l2_same',0,2,1119),('second_zero','l2_same',3,0,1125),('both_zero','l2_same',0,0,1107),('outer_zero','l3',0,2,1100),('inner_zero','l3',3,0,1100)]

def fixture(name,n,j):
 text,raw,dag=c4a.fixture(name)
 # Each input has only one header for each declared variable.
 import re
 text=re.sub(r'i < \d+','i < '+str(n),text)
 if j is not None:text=re.sub(r'j < \d+','j < '+str(j),text)
 return text,raw,dag

class C4aZeroTemplateTests(unittest.TestCase):
 def test_source_templates_empty_and_nonempty_mappings(self):
  for case,name,n,j,total in CASES:
   with self.subTest(case=case):
    text,raw,dag=fixture(name,n,j);saved=copy.deepcopy((raw,dag));views=build_loop_lock_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag);st,sd,s,d,_=views
    self.assertEqual(saved,(raw,dag));self.assertEqual(views,build_loop_lock_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag))
    headers={i:('i' if 'int i' in line else 'j') for i,line in enumerate(text.splitlines(),1) if 'for (int ' in line};expected={header:(n*j if name=='l3' else n if var=='i' else j) for header,var in headers.items()}
    loops={loop['loop_id']:loop for loop in s['loops']};mapping=s['mappings']['template_to_execution_segments'];positions=s['mappings']['source_position_to_execution_segments'];ids=set(d['nodes']);templates=s['source_component_templates']
    self.assertTrue(templates);self.assertEqual(len(templates),len({t['template_id'] for t in templates}));self.assertTrue(any(t['loop_part_role']=='ordinary' for t in templates));self.assertTrue(any(t['loop_part_role']=='critical' for t in templates))
    for template in templates:
     tid=template['template_id'];count=expected[loops[template['loop_id']]['header_line']];self.assertTrue(template['analysis_only']);self.assertIn(tid,mapping);self.assertIsInstance(mapping[tid],list);self.assertEqual(count,len(mapping[tid]))
     self.assertTrue(set(mapping[tid])<=ids)
     for key in template['source_position_ids']:self.assertIn(key,positions);self.assertEqual(mapping[tid],positions[key])
     if count==0:self.assertEqual([],mapping[tid]);self.assertFalse(any(row.get('source_template_id')==tid for row in st['segments']))
    roundtrip=json.loads(json.dumps(s));self.assertEqual(mapping,roundtrip['mappings']['template_to_execution_segments']);self.assertEqual(positions,roundtrip['mappings']['source_position_to_execution_segments'])
    if name in ['zero','two_mutex'] and n==0 or name=='l3' and n*j==0 or name=='l2_same' and n==j==0:
     self.assertFalse(s['lock_groups']);self.assertFalse(s['iteration_instances']);self.assertFalse(any(r.get('loop_part_role') for r in st['segments']))

 def test_native_zero_variants_timing_algorithms_and_business_output(self):
  helper=c1.CompositionC1Tests()
  for case,name,n,j,total in CASES:
   with self.subTest(case=case),tempfile.TemporaryDirectory() as tmp:
    text,_,_=fixture(name,n,j);source=Path(tmp)/(case+'.c');source.write_text(text);before=source.read_bytes();base,p=helper.native(tmp,source);run_composition_blocks(base_dir=base,base_name=case,source_file=source);s=json.loads((p/'blocks/level2'/RULE/'segments.json').read_text());d=json.loads((p/'blocks/level2'/RULE/'dag_seg.json').read_text())
    weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])};inp=Path(tmp)/'weights.json';inp.write_text(json.dumps(dict(weights=weights)));input_before=inp.read_bytes();timing=import_loop_lock_timing(base_dir=base,base_name=case,input_timing=inp)
    self.assertEqual(set(d['nodes']),set(timing['weights']));self.assertFalse(set(t['template_id'] for t in s['source_component_templates'])&set(timing['weights']))
    for template in s['source_component_templates']:
     targets=s['mappings']['template_to_execution_segments'][template['template_id']]
     if not targets:self.assertFalse(any(row.get('source_template_id')==template['template_id'] for row in s['segments']))
    for algo in c1.ALGOS:self.assertEqual(set(d['nodes']),set(run_schedule(base_dir=base,base_name=case,level='level2',rule_name=RULE,algo_name=algo)['priorities']))
    binary=Path(tmp)/'program';helper.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual('RESULT:'+str(total)+'\n',helper.command([binary]).stdout);self.assertEqual(before,source.read_bytes());self.assertEqual(input_before,inp.read_bytes())

 def test_unknown_missing_and_false_nonempty_bindings_reject_before_publish(self):
  helper=c1.CompositionC1Tests()
  with tempfile.TemporaryDirectory() as tmp:
   source=c4a.SOURCES/'zero.c';before=source.read_bytes();base,p=helper.native(tmp,source);run_composition_blocks(base_dir=base,base_name='zero',source_file=source);sp=p/'blocks/level2'/RULE/'segments.json';valid=sp.read_bytes();s=json.loads(valid);inp=Path(tmp)/'weights.json';weights={t['template_id']:dict(total_ns=10,count=1,avg_ns=10,min_ns=10,max_ns=10) for t in s['templates']};inp.write_text(json.dumps(dict(weights=weights)));legal=inp.read_bytes();output=p/'timing/level2'/RULE/'timing.json';import_loop_lock_timing(base_dir=base,base_name='zero',input_timing=inp);saved=output.read_bytes();tid=s['source_component_templates'][0]['template_id']
   for damage in ['unknown','missing','false_target','unknown_position','unknown_template','weight_alias']:
    bad=copy.deepcopy(s);ww=copy.deepcopy(weights)
    if damage=='unknown':bad['mappings']['template_to_execution_segments']['UNKNOWN']=[]
    elif damage=='missing':bad['mappings']['template_to_execution_segments'].pop(tid)
    elif damage=='false_target':bad['mappings']['template_to_execution_segments'][tid]=[s['segments'][0]['seg_id']]
    elif damage=='unknown_position':bad['mappings']['source_position_to_execution_segments']['LOOPSRC:task:line:9999']=[]
    elif damage=='unknown_template':bad['source_component_templates'][0]['template_id']='UNKNOWN'
    else:ww[tid]=dict(total_ns=10,count=1,avg_ns=10,min_ns=10,max_ns=10)
    sp.write_text(json.dumps(bad));inp.write_text(json.dumps(dict(weights=ww)));segments_before=sp.read_bytes();weights_before=inp.read_bytes()
    with self.assertRaises(ValueError):import_loop_lock_timing(base_dir=base,base_name='zero',input_timing=inp)
    self.assertEqual(saved,output.read_bytes());self.assertEqual(segments_before,sp.read_bytes());self.assertEqual(weights_before,inp.read_bytes());output.unlink()
    with self.assertRaises(ValueError):import_loop_lock_timing(base_dir=base,base_name='zero',input_timing=inp)
    self.assertFalse(output.exists());sp.write_bytes(valid);inp.write_bytes(legal);import_loop_lock_timing(base_dir=base,base_name='zero',input_timing=inp);self.assertEqual(saved,output.read_bytes())
   self.assertEqual(before,source.read_bytes())
