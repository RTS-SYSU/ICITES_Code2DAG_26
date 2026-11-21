"""Generic mutex pairing is independent of the C2b direct-thread policy."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.pipeline.lock_regions import match_lock_regions,LockRegionError
from Code2DAG.pipeline.composition_c2b_model import _source_matched_regions
from Code2DAG.pipeline.composition_c4a_model import _condition_function_ranges,build_loop_lock_views
from Code2DAG.tests import test_composition_c4a as c4a

class GenericLockRegionTests(unittest.TestCase):
 def test_main_task_helper_same_pairing_contract(self):
  for fn in ['main','task','helper']:
   with self.subTest(function=fn):
    source='pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;\nvoid '+fn+'(void) {\n pthread_mutex_lock(&m);\n work();\n pthread_mutex_unlock(&m);\n}\n';scope={fn:(2,6)};before=copy.deepcopy(scope)
    regions=match_lock_regions(source,scope);self.assertEqual(before,scope);self.assertEqual(regions,match_lock_regions(source,scope));self.assertEqual(1,len(regions));r=regions[0]
    self.assertEqual(fn,r['function']);self.assertEqual('m',r['mutex_id']);self.assertEqual([3,5],r['segment_range']);self.assertEqual([4,4],r['critical_range']);self.assertNotIn('thread_entry_function',r)
 def test_generated_instances_share_source_owner_but_remain_distinct(self):
  text='pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;\nvoid worker(void) {\n pthread_mutex_lock(&m);\n work();\n pthread_mutex_unlock(&m);\n}\n'
  ranges={'worker':(2,6),'worker@instance1':(2,6),'worker@instance2':(2,6)}
  regions=match_lock_regions(text,ranges)
  self.assertEqual(['worker','worker@instance1','worker@instance2'],[r['function'] for r in regions])
  self.assertEqual({'m'},{r['mutex_id'] for r in regions})
  self.assertEqual({((3,3),(5,5),(3,5))}, {((r['lock_range'][0],r['lock_range'][1]),(r['unlock_range'][0],r['unlock_range'][1]),(r['segment_range'][0],r['segment_range'][1])) for r in regions})

 def test_distinct_source_functions_remain_ambiguous(self):
  text='pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;\nvoid worker(void) {\n pthread_mutex_lock(&m);\n work();\n pthread_mutex_unlock(&m);\n}\n'
  with self.assertRaisesRegex(LockRegionError,'multiple source function owners'):
   match_lock_regions(text,{'a':(2,6),'b':(2,6)})

 def test_same_source_instances_with_disagreeing_ranges_rejected(self):
  text='pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;\nvoid worker(void) {\n pthread_mutex_lock(&m);\n work();\n pthread_mutex_unlock(&m);\n}\n'
  with self.assertRaisesRegex(LockRegionError,'disagree on range'):
   match_lock_regions(text,{'worker':(2,6),'worker@instance1':(2,5)})

 def test_same_mutex_different_functions_never_cross_pairs(self):
  text='pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;\nvoid a(void) {\n pthread_mutex_lock(&m);\n work();\n pthread_mutex_unlock(&m);\n}\nvoid b(void) {\n pthread_mutex_lock(&m);\n work();\n pthread_mutex_unlock(&m);\n}\n';ranges={'a':(2,6),'b':(7,11)}
  regions=match_lock_regions(text,ranges);self.assertEqual(['a','b'],[r['function'] for r in regions])
  cross=text.replace(' pthread_mutex_unlock(&m);\n}\nvoid b(void) {\n pthread_mutex_lock(&m);','}\nvoid b(void) {')
  with self.assertRaisesRegex(LockRegionError,'mismatched unlock'):match_lock_regions(cross,{'a':(2,5),'b':(6,9)})
 def test_c2b_wrapper_accepts_main_and_keeps_legacy_region_fields(self):
  text=(c4a.SOURCES/'many.c').read_text()
  # C2b keeps exact old fields/order, and has no new generic critical_range.
  _,rows=_source_matched_regions(text);self.assertEqual('task',rows[0]['thread_entry_function']);self.assertEqual(['mutex_id','function','thread_entry_function','lock_range','unlock_range','segment_range','start_offset','end_offset'],list(rows[0]))
  a=text.index('    result += 100;');b=text.index('    return NULL;',a);body=text[a:b];text=text[:a]+text[b:];text=text.replace('    pthread_join(t, NULL);','    pthread_join(t, NULL);\n'+body)
  _,main_rows=_source_matched_regions(text)
  self.assertTrue(main_rows);self.assertEqual({'main'},{r['function'] for r in main_rows})
  self.assertEqual({'main'},{r['thread_entry_function'] for r in main_rows})
 def test_helper_c4a_with_formal_old_segments(self):
  text,raw,dag=c4a.fixture('many');text=text.replace('task','helper');raw['segments'][0]['function']='helper'
  text=text.replace('int main(void)', 'void *entry(void *arg) {\n    return helper(arg);\n}\nint main(void)').replace('NULL, helper, NULL','NULL, entry, NULL')
  raw['segments'][1]['start_line']+=3;raw['segments'][1]['end_line']+=3;raw['thread_entry_functions']=['entry']
  entryline=next(i for i,l in enumerate(text.splitlines(),1) if 'return helper(arg)' in l)
  raw['segments'].append(dict(seg_id='ENTRY',function='entry',kind='compute',start_line=entryline,end_line=entryline));dag['nodes'].append('ENTRY');dag['edges'][0]['dst']='ENTRY';dag['edges'].append(dict(src='ENTRY',dst='OLD',kind='call'));saved=copy.deepcopy((raw,dag))
  st,sd,s,d,_=build_loop_lock_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
  self.assertEqual(saved,(raw,dag));self.assertEqual(3,len(s['lock_groups']));self.assertTrue(all(m['function']=='helper' and m['thread_entry_function'] is None for g in s['lock_groups'] for m in g['members']));self.assertEqual(['entry'],s['thread_entry_functions'])
 def test_native_helper_loop_uses_real_function_scope(self):
  from Code2DAG.tests import test_composition_c1 as c1
  from Code2DAG.pipeline.composition import run_composition_blocks
  from Code2DAG.pipeline.composition_c4a_model import RULE
  text=(c4a.SOURCES/'many.c').read_text().replace('task','helper')
  text=text.replace('int main(void)', 'void *entry(void *arg) {\n    return helper(arg);\n}\nint main(void)').replace('NULL, helper, NULL','NULL, entry, NULL')
  with tempfile.TemporaryDirectory() as tmp:
   source=Path(tmp)/'helper_loop.c';source.write_text(text);before=source.read_bytes();base,p=c1.CompositionC1Tests().native(tmp,source)
   run_composition_blocks(base_dir=base,base_name=source.stem,source_file=source)
   s=json.loads((p/'blocks/level2'/RULE/'segments.json').read_text())
   self.assertEqual({'helper'},{r['members'][0]['function'] for r in s['lock_groups']});self.assertEqual(before,source.read_bytes())
 def test_external_dependency_keeps_original_custom_source(self):
  # Function-scoped pairing must not drop dependency metadata at graph merge.
  text,raw,dag=c4a.fixture('many');original=copy.deepcopy(dag['edges'][0])
  st,sd,s,d,_=build_loop_lock_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
  self.assertEqual(dag['edges'],[r['edge'] for r in s['composition_edge_provenance']])
  actual=[e for e in d['edges'] if e['kind']==original['kind']]
  self.assertTrue(actual)
  self.assertTrue(all(e.get('custom')==original['custom'] for e in actual),actual)
