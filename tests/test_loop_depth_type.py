"""Only regress exact integer depth metadata and publication boundaries."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from Code2DAG.pipeline.loop_model import LoopModelError, copy_loop_timing
from Code2DAG.pipeline.loop_nested_timing import _validate_nested_segments, import_loop_nested_timing
from Code2DAG.tests.test_loop_timing_mixed_mapping import example

MISSING = object()


def invalid_values():
    for index,expected in [(0,1),(1,2)]:
        for value in [True,False,float(expected),str(expected),None,MISSING,0,-1,3,expected+1]:
            yield index,value


def mutated(index,value):
    s,t=example()
    if value is MISSING:s['loops'][index].pop('depth')
    else:s['loops'][index]['depth']=value
    return s,t


class LoopDepthTypeTests(unittest.TestCase):
    def rejects(self,index,values):
        for value in values:
            with self.subTest(index=index,value=repr(value)):
                s,t=mutated(index,value)
                with self.assertRaisesRegex(LoopModelError,'parent or depth'):_validate_nested_segments(s)
    def test_01_outer_boolean_true_and_false_rejected(self):self.rejects(0,[True,False])
    def test_02_outer_integral_float_rejected(self):self.rejects(0,[1.0])
    def test_03_outer_string_null_and_missing_rejected(self):self.rejects(0,['1',None,MISSING])
    def test_04_inner_boolean_and_integral_float_rejected(self):self.rejects(1,[True,False,2.0])
    def test_05_inner_string_null_and_missing_rejected(self):self.rejects(1,['2',None,MISSING])
    def test_06_other_integer_depth_values_rejected(self):
        self.rejects(0,[-1,0,2,3]);self.rejects(1,[-1,0,1,3])
    def setup_case(self,temp,s=None,t=None):
        base=Path(temp)
        if s is None:s,t=example()
        blocks=base/'intermediate_results/mixed/pipeline/blocks/level2/loop_static_nested';blocks.mkdir(parents=True,exist_ok=True);sp=blocks/'segments.json';sp.write_text(json.dumps(s));tp=base/'weights.json';tp.write_text(json.dumps(t));output=base/'intermediate_results/mixed/pipeline/timing/level2/loop_static_nested/timing.json';return base,sp,tp,output
    def run_import(self,base,tp):return import_loop_nested_timing(base_dir=base,base_name='mixed',input_timing=tp)
    def test_07_exact_integer_one_two_legal_mixed_pure_and_zero_cases(self):
        for n1,n2,extras in [(2,3,('P',)),(2,3,()),(0,3,('P',)),(2,0,('P',)),(0,0,())]:
            with self.subTest(n1=n1,n2=n2,extras=extras),tempfile.TemporaryDirectory() as temp:
                s,t=example(n1,n2,extras);before=copy.deepcopy((s,t));_validate_nested_segments(s);base,sp,tp,output=self.setup_case(temp,s,t);result=self.run_import(base,tp);self.assertEqual(copy_loop_timing(s,t)['weights'],result['weights']);self.assertEqual(before,(s,t));self.assertTrue(output.exists());self.assertIs(type(s['loops'][0]['depth']),int);self.assertIs(type(s['loops'][1]['depth']),int)
    def test_08_fresh_invalid_depth_rejected_before_copy_without_mutation(self):
        for index,value in invalid_values():
            with self.subTest(index=index,value=repr(value)),tempfile.TemporaryDirectory() as temp:
                s,t=mutated(index,value);base,sp,tp,output=self.setup_case(temp,s,t);before=(sp.read_bytes(),tp.read_bytes())
                with patch('Code2DAG.pipeline.loop_nested_timing.copy_loop_timing',side_effect=AssertionError('copy called for invalid depth')) as copy_fn:
                    with self.assertRaisesRegex(LoopModelError,'parent or depth'):self.run_import(base,tp)
                    copy_fn.assert_not_called()
                self.assertFalse(output.parent.exists());self.assertEqual(before,(sp.read_bytes(),tp.read_bytes()))
    def test_09_published_output_preserved_and_legal_retry_identical(self):
        with tempfile.TemporaryDirectory() as temp:
            good,t=example();base,sp,tp,output=self.setup_case(temp,good,t);legal=sp.read_bytes();self.run_import(base,tp);published=output.read_bytes()
            for index,value in invalid_values():
                bad,_=mutated(index,value);sp.write_text(json.dumps(bad));before=(sp.read_bytes(),tp.read_bytes())
                with self.subTest(index=index,value=repr(value)):
                    with self.assertRaisesRegex(LoopModelError,'parent or depth'):self.run_import(base,tp)
                    self.assertEqual(before,(sp.read_bytes(),tp.read_bytes()));self.assertEqual(published,output.read_bytes())
            sp.write_bytes(legal);self.run_import(base,tp);self.assertEqual(published,output.read_bytes())
