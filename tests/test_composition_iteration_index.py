"""C1 exact JSON integer iteration indices; no coercion or relaxed binding."""
import json
import unittest
from unittest.mock import patch
from Code2DAG.tests import test_composition_timing_contract as boundary
from Code2DAG.pipeline.composition_model import CompositionError
from Code2DAG.pipeline.io_utils import write_json


class CompositionIterationIndexTests(unittest.TestCase):
    def helper(self):return boundary.CompositionTimingContractTests()

    def corrupt(self,c,value,original=1,missing=False):
        p=c['blocks']/'segments.json';data=json.loads(p.read_text())
        row=next(r for r in data['segments'] if r.get('role')=='iteration' and r['iteration_index']==original)
        if missing:row.pop('iteration_index')
        else:row['iteration_index']=value
        write_json(p,data)

    def reject(self,values,reason,original=1):
        for value in values:
            h=self.helper()
            with self.subTest(value=repr(value)),h.case() as c:
                self.corrupt(c,value,original);before=((c['blocks']/'segments.json').read_bytes(),c['input'].read_bytes())
                with self.assertRaisesRegex(CompositionError,reason):h.run_import(c)
                self.assertFalse(c['output'].parent.exists());self.assertEqual(before,((c['blocks']/'segments.json').read_bytes(),c['input'].read_bytes()))

    def test_integer_zero_one_and_upper_bound_are_accepted(self):
        h=self.helper()
        with h.case() as c:
            p=c['blocks']/'segments.json';s=json.loads(p.read_text());indices=[r['iteration_index'] for r in s['segments'] if r.get('role')=='iteration']
            self.assertEqual([0,1,2],indices);self.assertTrue(all(type(i) is int for i in indices));self.assertEqual(2,s['loops'][0]['iteration_count']-1)
            result=h.run_import(c);self.assertEqual(set(c['views'][3]['nodes']),set(result['weights']))

    def test_boolean_true_and_false_are_rejected(self):
        self.reject([True],'exact integer',1);self.reject([False],'exact integer',0)

    def test_float_string_null_are_rejected_without_coercion(self):
        self.reject([1.0,'1',None],'exact integer')

    def test_negative_and_out_of_range_values_keep_existing_rejection(self):
        self.reject([-1,3,1000000],'binding changed')

    def test_in_range_wrong_binding_and_missing_index_are_rejected(self):
        self.reject([2],'binding changed',1)
        h=self.helper()
        with h.case() as c:
            self.corrupt(c,None,missing=True)
            with self.assertRaisesRegex(CompositionError,'exact integer'):h.run_import(c)
            self.assertFalse(c['output'].parent.exists())

    def test_type_rejection_precedes_copy_and_output(self):
        h=self.helper()
        with h.case() as c:
            self.corrupt(c,True)
            with patch('Code2DAG.pipeline.composition_timing.aggregate_lock_timing',side_effect=AssertionError('copied invalid index')) as copied:
                with self.assertRaisesRegex(CompositionError,'exact integer'):h.run_import(c)
                copied.assert_not_called()
            self.assertFalse(c['output'].parent.exists())

    def test_existing_timing_preserved_and_legal_retry_recovers(self):
        h=self.helper()
        with h.case() as c:
            initial=h.run_import(c);published=c['output'].read_bytes();sp=c['blocks']/'segments.json';valid=sp.read_bytes()
            for value,original in [(True,1),(False,0),(1.0,1),('1',1),(None,1),(-1,1),(3,1)]:
                sp.write_bytes(valid);self.corrupt(c,value,original)
                with self.assertRaises(CompositionError):h.run_import(c)
                self.assertEqual(published,c['output'].read_bytes())
            sp.write_bytes(valid);self.assertEqual(initial,h.run_import(c));self.assertEqual(published,c['output'].read_bytes())


if __name__=='__main__':unittest.main()
