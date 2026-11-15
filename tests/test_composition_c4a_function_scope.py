"""Other-function Conditions retain the existing Condition contract in C4a."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests.test_composition_c4a import fixture,SOURCES
from Code2DAG.tests.test_composition_c1 import CompositionC1Tests
from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.composition_c4a_model import RULE,build_loop_lock_views,_condition_function_ranges

class C4aFunctionScopeTests(unittest.TestCase):
 def test_formal_ranges_and_input_immutability(self):
  text,raw,dag=fixture('many');saved=copy.deepcopy(raw);ranges=_condition_function_ranges(text,raw['segments'])
  self.assertEqual({'task','main'},set(ranges));self.assertEqual(saved,raw)
  bad=copy.deepcopy(raw['segments']);bad[0]['function']='unknown'
  with self.assertRaisesRegex(ValueError,'function definition'):_condition_function_ranges(text,bad)

 def test_native_main_condition_retains_branch_membership(self):
  text=(SOURCES/'many.c').read_text().replace('    pthread_join(t, NULL);','    pthread_join(t, NULL);\n    if (result % 2) {\n        result += 11;\n    } else {\n        result += 13;\n    }')
  with tempfile.TemporaryDirectory() as tmp:
   source=Path(tmp)/'outside_main.c';source.write_text(text);before=source.read_bytes();base,p=CompositionC1Tests().native(tmp,source)
   run_composition_blocks(base_dir=base,base_name=source.stem,source_file=source);s=json.loads((p/'blocks/level2'/RULE/'segments.json').read_text())
   branches=[r for r in s['segments'] if r['function']=='main' and r.get('branch_id')]
   self.assertEqual(2,len(branches))
   for r in branches:
    self.assertTrue(r.get('condition_memberships'),r)
    self.assertTrue(all(m['exclusive_group'].startswith('COND:main') for m in r['condition_memberships']))
    self.assertNotIn('loop_id',r)
   self.assertEqual(before,source.read_bytes())
