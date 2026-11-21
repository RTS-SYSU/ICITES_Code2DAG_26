"""Compare full existing Condition serialization at the C4a merge boundary."""
import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from Code2DAG.tests import test_composition_c4a as c4a
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline import composition_c4a_model as model
from Code2DAG.pipeline.condition_model import build_condition_views
from Code2DAG.pipeline.composition import run_composition_blocks

FORMS={
 'if_else':'if (result % 2) {\n result += 11;\n} else {\n result += 13;\n}',
 'no_else':'if (result % 2) {\n result += 11;\n}',
 'else_if':'if (result % 2) {\n result += 11;\n} else if (result % 3) {\n result += 13;\n} else {\n result += 17;\n}',
 'nested_then':'if (result % 2) {\n if (result % 3) {\n  result += 11;\n } else {\n  result += 13;\n }\n} else {\n result += 17;\n}',
 'nested_else':'if (result % 2) {\n result += 11;\n} else {\n if (result % 3) {\n  result += 13;\n } else {\n  result += 17;\n }\n}',
}

class C4aConditionMetadataTests(unittest.TestCase):
 def test_full_serialized_metadata_and_original_views_immutable(self):
  for name,snippet in FORMS.items():
   with self.subTest(name=name):
    text,raw,dag=c4a.fixture('many');text=text.replace('    pthread_join(t, NULL);','    pthread_join(t, NULL);\n'+snippet);raw['segments'][1]['end_line']=len(text.splitlines())-1
    for row in raw['segments']:row['local_name']='shared'
    saved=copy.deepcopy((raw,dag));captured=[]
    def spy(**kwargs):
     views=build_condition_views(**kwargs);captured.append((views,copy.deepcopy(views)));return views
    with patch.object(model,'build_condition_views',side_effect=spy):
     first=model.build_loop_lock_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
    self.assertEqual(saved,(raw,dag));original,expected=captured[0];self.assertEqual(expected,original)
    st,sd,s,d,_=first;actual={r['seg_id']:r for r in s['segments']}
    for before in expected[2]['segments']:
     if before['function']!='main':continue
     after=actual[before['seg_id']]
     for key,value in before.items():self.assertEqual(value,after[key],(name,before['seg_id'],key))
     for pos in after['source_position_ids']:self.assertIn(after['seg_id'],s['mappings']['source_position_to_execution_segments'][pos])
    self.assertEqual(expected[0]['condition_regions'],s['condition_regions'])
    self.assertEqual(expected[1]['exclusive_groups'],sd['exclusive_groups']);self.assertEqual(sd['exclusive_groups'],d['exclusive_groups'])
    structs={r['seg_id']:r for r in st['segments']}
    for before in expected[0]['segments']:
     if before['kind'] in ['condition_entry','condition_exit']:self.assertEqual(before,structs[before['seg_id']])
    memberships=[r['condition_memberships'] for r in s['segments'] if r.get('condition_memberships')]
    self.assertTrue(memberships)
    if name.startswith('nested') or name=='else_if':self.assertTrue(any(len(m)==2 for m in memberships))
    else:self.assertTrue(all(len(m)==1 for m in memberships))
    self.assertEqual(dag['edges'],[e['edge'] for e in s['composition_edge_provenance']])
    second=model.build_loop_lock_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag);self.assertEqual(first,second)

 def test_native_c4a_in_main_condition_in_worker(self):
  text=(c4a.SOURCES/'many.c').read_text();a=text.index('    result += 100;');b=text.index('    return NULL;',a);body=text[a:b]
  text=text[:a]+'''    if (result % 2) {
        result += 11;
    } else {
        result += 13;
    }
'''+text[b:]
  text=text.replace('    pthread_join(t, NULL);','    pthread_join(t, NULL);\n'+body)
  with tempfile.TemporaryDirectory() as tmp:
   source=Path(tmp)/'main_loop_worker_condition.c';source.write_text(text);before=source.read_bytes();base,p=c1.CompositionC1Tests().native(tmp,source)
   run_composition_blocks(base_dir=base,base_name=source.stem,source_file=source)
   s=json.loads((p/'blocks/level2'/model.RULE/'segments.json').read_text())
   self.assertEqual({'main'},{g['members'][0]['function'] for g in s['lock_groups']})
   self.assertEqual({'task'},{r['function'] for r in s['condition_regions']});self.assertEqual(before,source.read_bytes())
