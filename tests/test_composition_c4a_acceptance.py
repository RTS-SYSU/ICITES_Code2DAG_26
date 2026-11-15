"""Remaining bounded C4a native, timing and rejection acceptance."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c4a as c4a
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition import run_composition_blocks,select_rule
from Code2DAG.pipeline.composition_c4a_model import RULE,build_loop_lock_views
from Code2DAG.pipeline.composition_c4a_timing import import_loop_lock_timing
from Code2DAG.pipeline.composition_c5d_model import RULE as C5D_RULE
from Code2DAG.pipeline.composition_c5d_timing import import_loop_lock_condition_timing
from Code2DAG.pipeline.schedule import run_schedule

class C4aAcceptanceTests(unittest.TestCase):
 def test_native_main_helper_timing_algorithms_and_outputs(self):
  original=(c4a.SOURCES/'many.c').read_text();cases={}
  a=original.index('    result += 100;');b=original.index('    return NULL;',a);body=original[a:b]
  condition='    if (result % 2) {\n        result += 11;\n    } else {\n        result += 13;\n    }\n'
  cases['main']=original[:a]+condition+original[b:];cases['main']=cases['main'].replace('    pthread_join(t, NULL);','    pthread_join(t, NULL);\n'+body)
  cases['helper']=original.replace('task','helper').replace('int main(void)', 'void *entry(void *arg) {\n    return helper(arg);\n}\nint main(void)').replace('NULL, helper, NULL','NULL, entry, NULL').replace('    pthread_join(t, NULL);','    pthread_join(t, NULL);\n'+condition)
  helper=c1.CompositionC1Tests()
  for fn,text in cases.items():
   with self.subTest(fn=fn),tempfile.TemporaryDirectory() as tmp:
    source=Path(tmp)/(fn+'.c');source.write_text(text);before=source.read_bytes();base,p=helper.native(tmp,source);run_composition_blocks(base_dir=base,base_name=fn,source_file=source);s=json.loads((p/'blocks/level2'/RULE/'segments.json').read_text());d=json.loads((p/'blocks/level2'/RULE/'dag_seg.json').read_text())
    self.assertEqual({fn},{m['function'] for g in s['lock_groups'] for m in g['members']});self.assertTrue(all(m['thread_entry_function']==(m['function'] if m['function'] in s['thread_entry_functions'] else None) for g in s['lock_groups'] for m in g['members']))
    weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])};inp=Path(tmp)/'weights.json';inp.write_text(json.dumps(dict(weights=weights)));result=import_loop_lock_timing(base_dir=base,base_name=fn,input_timing=inp)
    for row in s['segments']:
     expected={field:sum(weights[m['execution_segment']['template_id']][field] for m in row['members']) for field in ('total_ns','count','avg_ns','min_ns','max_ns')} if row['kind']=='lock_group' else weights[row['template_id']]
     self.assertEqual(expected,result['weights'][row['seg_id']])
    for algo in c1.ALGOS:self.assertEqual(set(d['nodes']),set(run_schedule(base_dir=base,base_name=fn,level='level2',rule_name=RULE,algo_name=algo)['priorities']))
    binary=Path(tmp)/'program';helper.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual('RESULT:1131\n',helper.command([binary]).stdout);self.assertEqual(before,source.read_bytes())

 def test_timing_identity_rejection_fresh_existing_and_recovery(self):
  helper=c1.CompositionC1Tests()
  for name in ['zero','many','two_mutex','l3']:
   with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
    source=c4a.SOURCES/(name+'.c');before=source.read_bytes();base,p=helper.native(tmp,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source);sp=p/'blocks/level2'/RULE/'segments.json';valid=sp.read_bytes();s=json.loads(valid);weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])};inp=Path(tmp)/'weights.json';inp.write_text(json.dumps(dict(weights=weights)));legal=inp.read_bytes();out=p/'timing/level2'/RULE/'timing.json'
    import_loop_lock_timing(base_dir=base,base_name=name,input_timing=inp);saved=out.read_bytes()
    for damage in ['boolean','iteration','mutex','member','mapping','template','partial','duplicate']:
     bad=copy.deepcopy(s);ww=copy.deepcopy(weights)
     if damage=='boolean':bad['loops'][0]['iteration_count']=True
     elif damage=='partial':ww.pop(next(iter(ww)))
     elif damage=='duplicate':pass
     elif not bad['lock_groups']:bad['lock_groups'].append({'group_id':'phantom@iteration:0'})
     elif damage=='iteration':bad['lock_groups'][0]['iteration_index']=99
     elif damage=='mutex':bad['lock_groups'][0]['mutex_id']='other'
     elif damage=='member':bad['lock_groups'][0]['member_segment_ids']=['other-iteration']
     elif damage=='mapping':bad['mappings']['source_lock_region_to_instances']={}
     else:bad['lock_groups'][0]['members'][0]['execution_segment']['template_id']='wrong'
     sp.write_text(json.dumps(bad));inp.write_text('{"weights":{},"weights":{}}' if damage=='duplicate' else json.dumps(dict(weights=ww)))
     with self.assertRaises(ValueError):import_loop_lock_timing(base_dir=base,base_name=name,input_timing=inp)
     self.assertEqual(saved,out.read_bytes());out.unlink()
     with self.assertRaises(ValueError):import_loop_lock_timing(base_dir=base,base_name=name,input_timing=inp)
     self.assertFalse(out.exists());sp.write_bytes(valid);inp.write_bytes(legal);import_loop_lock_timing(base_dir=base,base_name=name,input_timing=inp);self.assertEqual(saved,out.read_bytes())
    self.assertEqual(before,source.read_bytes())

 def test_unsupported_regions_reject_before_blocks_publication(self):
  helper=c1.CompositionC1Tests();original=(c4a.SOURCES/'many.c').read_text()
  variants={
   'nested_mutex':original.replace('    for (','    for (',1).replace('        result += 1;','        pthread_mutex_lock(&m);\n        result += 1;\n        pthread_mutex_unlock(&m);'),
   'cross_boundary':original.replace('        pthread_mutex_lock(&m);\n','').replace('    for (','    pthread_mutex_lock(&m);\n    for (',1),
  }
  for name,text in variants.items():
   with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
    source=Path(tmp)/(name+'.c');source.write_text(text);before=source.read_bytes();base,p=helper.native(tmp,source)
    with self.assertRaises(ValueError):run_composition_blocks(base_dir=base,base_name=name,source_file=source)
    self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists());self.assertFalse((p/'timing/level2'/RULE/'timing.json').exists());self.assertEqual(before,source.read_bytes())

 def test_condition_inside_upgraded_to_c5d_acceptance(self):
  helper=c1.CompositionC1Tests();original=(c4a.SOURCES/'many.c').read_text()
  text=original.replace('        result += 1;','        if (result % 2) {\n            result += 1;\n        } else {\n            result += 10;\n        }')
  self.assertEqual(C5D_RULE,select_rule(text))
  with tempfile.TemporaryDirectory() as tmp:
   source=Path(tmp)/'condition_inside.c';source.write_text(text);before=source.read_bytes();base,p=helper.native(tmp,source);run_composition_blocks(base_dir=base,base_name='condition_inside',source_file=source)
   self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists());self.assertFalse((p/'blocks/level2'/RULE/'dag_seg.json').exists())
   b=p/'blocks/level2'/C5D_RULE;s=json.loads((b/'segments.json').read_text());d=json.loads((b/'dag_seg.json').read_text())
   self.assertEqual(set(d['nodes']),{r['seg_id'] for r in s['segments']});self.assertFalse(any(e['src']==e['dst'] for e in d['edges']))
   self.assertEqual(3,len(s['iteration_instances']));self.assertEqual(3,len(s['condition_regions']));self.assertEqual(3,len(s['lock_groups']));self.assertEqual(1,len(s['lock_templates']))
   source_conditions=s['mappings']['source_condition_to_instances'];self.assertEqual(1,len(source_conditions));source_id=next(iter(source_conditions))
   self.assertEqual(3,len(source_conditions[source_id]));self.assertEqual({r['condition_id'] for r in s['condition_regions']},set(source_conditions[source_id]))
   for region in s['condition_regions']:
    self.assertEqual(source_id,region['source_condition_id']);self.assertEqual(region['loop_iteration_id']+'::',region['condition_id'][:len(region['loop_iteration_id'])+2])
    self.assertEqual({'then','else'},{branch['branch_id'] for branch in region['branches']})
   rows={r['seg_id']:r for r in s['segments']};candidates=set()
   for group in s['lock_groups']:
    self.assertEqual('m',group['mutex_id']);self.assertTrue(group['candidate_segment_ids']);self.assertEqual(group['candidate_segment_ids'],s['mappings']['grouped_node_to_member_segments'][group['group_id']])
    self.assertTrue(group['members']);self.assertEqual({'task'},{member['function'] for member in group['members']});self.assertEqual({'m'},{member['mutex_id'] for member in group['members']});self.assertTrue(all(member.get('lock_range') and member.get('unlock_range') and member.get('member_segment_id') for member in group['members']))
    for candidate in group['candidate_segment_ids']:
     candidates.add(candidate);row=rows[candidate];self.assertEqual('lock_path_candidate',row['kind']);self.assertEqual(group['group_id'],row['lock_group_id']);self.assertTrue(row['condition_memberships'])
     self.assertEqual(1,len({membership['exclusive_group'] for membership in row['condition_memberships']}));self.assertTrue({membership['branch_id'] for membership in row['condition_memberships']}<={'then','else'})
   self.assertEqual(candidates,set(s['mappings']['iteration_to_group']));self.assertEqual(len(candidates),len(s['mappings']['iteration_to_group']))
   lock=s['lock_templates'][0];self.assertEqual(3,len(s['mappings']['source_lock_region_to_group'][lock['function']+'@'+str(lock['lock_range'][0])]))
   self.assertEqual({t['template_id'] for t in s['templates']}|{t['template_id'] for t in s['source_component_templates']},set(s['mappings']['template_to_execution_segments']))
   weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])};inp=Path(tmp)/'weights.json';inp.write_text(json.dumps({'weights':weights}));timing=import_loop_lock_condition_timing(base_dir=base,base_name='condition_inside',input_timing=inp)
   self.assertEqual({r['seg_id'] for r in s['segments']},set(timing['weights']))
   for row in s['segments']:
    expected={field:sum(weights[tid][field] for tid in row['member_template_ids']) for field in ('total_ns','count','avg_ns','min_ns','max_ns')} if row['kind']=='lock_path_candidate' else weights[row['template_id']]
    self.assertEqual(expected,timing['weights'][row['seg_id']])
   for algo in c1.ALGOS:self.assertEqual(set(d['nodes']),set(run_schedule(base_dir=base,base_name='condition_inside',level='level2',rule_name=C5D_RULE,algo_name=algo)['priorities']))
   binary=Path(tmp)/'program';helper.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual('RESULT:1127\n',helper.command([binary]).stdout);self.assertEqual(before,source.read_bytes())

 def test_native_l2_second_only_and_different_mutexes(self):
  source_text=(c4a.SOURCES/'l2_same.c').read_text();cut=source_text.index('    for (int j');different=source_text[:cut]+source_text[cut:].replace('&m','&q');different=different.replace('int result;','pthread_mutex_t q = PTHREAD_MUTEX_INITIALIZER;\nint result;')
  first=(c4a.SOURCES/'l2_first.c').read_text();head=first.index('    for (int i');a=first.index('\n',head)+1;b=first.index('    }',a);body=first[a:b]
  second_only=first[:a]+'        result += 20;\n'+first[b:];j=second_only.index('    for (int j');start=second_only.index('\n',j)+1;end=second_only.index('    }',start);second_only=second_only[:start]+body+second_only[end:]
  helper=c1.CompositionC1Tests()
  for name,text,total,counts in [('different',different,1137,{'m':3,'q':2}),('second_only',second_only,1179,{'m':2})]:
   with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
    source=Path(tmp)/(name+'.c');source.write_text(text);before=source.read_bytes();base,p=helper.native(tmp,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source);s=json.loads((p/'blocks/level2'/RULE/'segments.json').read_text());d=json.loads((p/'blocks/level2'/RULE/'dag_seg.json').read_text())
    self.assertEqual(counts,{mutex:sum(g['mutex_id']==mutex for g in s['lock_groups']) for mutex in counts});self.assertEqual(sum(counts.values()),len(s['lock_groups']))
    inp=Path(tmp)/'weights.json';weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])};inp.write_text(json.dumps(dict(weights=weights)));result=import_loop_lock_timing(base_dir=base,base_name=name,input_timing=inp)
    for row in s['segments']:
     expected={f:sum(weights[m['execution_segment']['template_id']][f] for m in row['members']) for f in ('total_ns','count','avg_ns','min_ns','max_ns')} if row['kind']=='lock_group' else weights[row['template_id']]
     self.assertEqual(expected,result['weights'][row['seg_id']])
    for algo in c1.ALGOS:self.assertEqual(set(d['nodes']),set(run_schedule(base_dir=base,base_name=name,level='level2',rule_name=RULE,algo_name=algo)['priorities']))
    binary=Path(tmp)/'program';helper.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual('RESULT:'+str(total)+'\n',helper.command([binary]).stdout);self.assertEqual(before,source.read_bytes())

 def test_zero_keeps_before_and_after_source_position_empty_mappings(self):
  text,raw,dag=c4a.fixture('zero');st,sd,s,d,_=build_loop_lock_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
  self.assertFalse(s['lock_groups']);self.assertFalse(s['iteration_instances'])
  for statement in ['result += 2;','result += 3;']:
   line=next(i for i,value in enumerate(text.splitlines(),1) if statement in value)
   key='LOOPSRC:task:line:'+str(line)
   self.assertIn(key,s['mappings']['source_position_to_execution_segments'])
   self.assertEqual([],s['mappings']['source_position_to_execution_segments'][key])
