"""Regress only L3 mixed body-source target validation and publication boundaries."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from Code2DAG.tests.test_loop_l3 import SOURCE
from Code2DAG.tests.test_loop_source_mapping import payload
from Code2DAG.pipeline.loop_model import LoopModelError, copy_loop_timing
from Code2DAG.pipeline.loop_nested_model import build_loop_nested_views
from Code2DAG.pipeline.loop_nested_timing import _validate_nested_segments, import_loop_nested_timing


def example(n1=2,n2=3,extras=('P',)):
    text,s,d=payload(n1,n2);targets=list(extras)+['OLD'];s['mappings']={'source_position_to_execution_segments':{'LOOPSRC:main:line:2':['P'],'LOOPSRC:main:line:5':targets,'LOOPSRC:main:line:8':['Q']}}
    if 'R' in extras:
        s['segments'].append(dict(seg_id='R',function='helper',kind='compute',start_line=1,end_line=1));d['nodes'].append('R')
    result=build_loop_nested_views(base_name='mixed',source_text=text,base_segments_json=s,base_dag_json=d)[2]
    result['source_sha256']=hashlib.sha256(SOURCE.read_bytes()).hexdigest()
    metric=lambda n:dict(total_ns=n*7,count=7,avg_ns=n,min_ns=n-1,max_ns=n+1)
    weights={'LOOP:main@4:body_template':metric(20),'P':metric(10),'Q':metric(30)}
    if 'R' in extras:weights['R']=metric(40)
    return result,dict(weights=weights)


class LoopTimingMixedMappingTests(unittest.TestCase):
    def setup_case(self,temp,n1=2,n2=3,extras=('P',)):
        base=Path(temp);s,t=example(n1,n2,extras);blocks=base/'intermediate_results/mixed/pipeline/blocks/level2/loop_static_nested';blocks.mkdir(parents=True,exist_ok=True);sp=blocks/'segments.json';sp.write_text(json.dumps(s));tp=base/'weights.json';tp.write_text(json.dumps(t));output=base/'intermediate_results/mixed/pipeline/timing/level2/loop_static_nested/timing.json'
        return base,s,t,sp,tp,output
    def import_case(self,base,tp):return import_loop_nested_timing(base_dir=base,base_name='mixed',input_timing=tp)
    def test_01_P_and_six_leaves_import_exact_weights(self):
        with tempfile.TemporaryDirectory() as temp:
            base,s,t,sp,tp,output=self.setup_case(temp);before=(sp.read_bytes(),tp.read_bytes());result=self.import_case(base,tp)
            self.assertEqual(copy_loop_timing(s,t)['weights'],result['weights']);self.assertEqual(8,len(result['weights']));self.assertEqual(before,(sp.read_bytes(),tp.read_bytes()));self.assertTrue(output.exists())
    def test_02_multiple_extras_preserve_order_and_all_metrics(self):
        with tempfile.TemporaryDirectory() as temp:
            base,s,t,sp,tp,output=self.setup_case(temp,extras=('P','R'));before=sp.read_bytes();result=self.import_case(base,tp);self.assertEqual(copy_loop_timing(s,t)['weights'],result['weights']);self.assertEqual(before,sp.read_bytes());self.assertEqual(['P','R'],s['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:5'][:2])
    def test_03_pure_leaves_output_is_byte_identical_to_existing_copy_contract(self):
        with tempfile.TemporaryDirectory() as temp:
            base,s,t,sp,tp,output=self.setup_case(temp,extras=());expected=copy_loop_timing(s,t);expected['input_timing_sha256']=hashlib.sha256(tp.read_bytes()).hexdigest();result=self.import_case(base,tp);self.assertEqual(expected,result);self.assertEqual(json.dumps(expected,ensure_ascii=False,indent=2).encode(),output.read_bytes())
    def test_04_zero_products_with_P_are_legal(self):
        for n1,n2 in [(0,3),(2,0),(0,0)]:
            with self.subTest(n1=n1,n2=n2),tempfile.TemporaryDirectory() as temp:
                base,s,t,sp,tp,output=self.setup_case(temp,n1,n2);result=self.import_case(base,tp);self.assertEqual({'P','Q'},set(result['weights']));self.assertEqual(['P'],s['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:5'])
    def test_05_zero_products_with_empty_body_targets_are_legal(self):
        for n1,n2 in [(0,3),(2,0),(0,0)]:
            with self.subTest(n1=n1,n2=n2),tempfile.TemporaryDirectory() as temp:
                base,s,t,sp,tp,output=self.setup_case(temp,n1,n2,());result=self.import_case(base,tp);self.assertEqual({'P','Q'},set(result['weights']));self.assertEqual([],s['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:5'])
    def bad_targets(self,label):
        s,t=example(extras=());leaves=s['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:5'];row=s['segments'][0]
        values={'unknown':['unknown']+leaves,'old_owner':['OLD']+leaves,'control':['LOOP:main@3:entry']+leaves,'middle_extra':leaves[:2]+['P']+leaves[2:],'suffix_extra':leaves+['P'],'missing_leaf':leaves[:-1],'duplicate_leaf':leaves+[leaves[0]],'unordered':list(reversed(leaves)),'wrong_coordinate':leaves[:-1]+['LOOP:main@4:outer:9:iteration:0'],'duplicate_extra':['P','P']+leaves,'not_list':'P','dict_target':{'P':leaves},'null_target':None,'invalid_id':[[]]+leaves}
        if label=='old_owner':s['segments'].append(dict(seg_id='OLD',function='helper',kind='compute',start_line=1,end_line=1))
        if label=='noncompute':row['kind']='loop_entry';values[label]=['P']+leaves
        if label=='other_loop':s['segments'].append(dict(seg_id='foreign',function='helper',kind='compute',start_line=1,end_line=1,loop_id='LOOP:other@1'));values[label]=['foreign']+leaves
        s['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:5']=values[label]
        return s
    def test_06_unknown_owner_control_other_loop_and_noncompute_rejected(self):
        for label in ['unknown','old_owner','control','other_loop','noncompute']:
            with self.subTest(label=label):
                with self.assertRaises(LoopModelError):_validate_nested_segments(self.bad_targets(label))
    def test_07_extra_targets_cannot_follow_or_interrupt_leaves(self):
        for label in ['middle_extra','suffix_extra']:
            with self.subTest(label=label):
                with self.assertRaises(LoopModelError):_validate_nested_segments(self.bad_targets(label))
    def test_08_incomplete_duplicate_disordered_wrong_leaves_rejected(self):
        for label in ['missing_leaf','duplicate_leaf','unordered','wrong_coordinate']:
            with self.subTest(label=label):
                with self.assertRaises(LoopModelError):_validate_nested_segments(self.bad_targets(label))
    def test_09_duplicate_extras_and_non_list_or_non_id_targets_rejected(self):
        for label in ['duplicate_extra','not_list','dict_target','null_target','invalid_id']:
            with self.subTest(label=label):
                with self.assertRaises(LoopModelError):_validate_nested_segments(self.bad_targets(label))
    def test_10_fresh_rejection_precedes_copy_and_creates_no_timing(self):
        with tempfile.TemporaryDirectory() as temp:
            base,s,t,sp,tp,output=self.setup_case(temp,extras=());sp.write_text(json.dumps(self.bad_targets('middle_extra')));before=(sp.read_bytes(),tp.read_bytes())
            with patch('Code2DAG.pipeline.loop_nested_timing.copy_loop_timing',side_effect=AssertionError('copy before validation')) as copy_fn:
                with self.assertRaises(LoopModelError):self.import_case(base,tp)
                copy_fn.assert_not_called()
            self.assertFalse(output.parent.exists());self.assertEqual(before,(sp.read_bytes(),tp.read_bytes()))
    def test_11_rejects_preserve_published_bytes_and_legal_retry_recovers(self):
        with tempfile.TemporaryDirectory() as temp:
            base,s,t,sp,tp,output=self.setup_case(temp);legal=sp.read_bytes();self.import_case(base,tp);published=output.read_bytes()
            for label in ['unknown','old_owner','control','other_loop','noncompute','middle_extra','suffix_extra','missing_leaf','duplicate_leaf','unordered','wrong_coordinate','duplicate_extra','not_list','dict_target','null_target','invalid_id']:
                sp.write_text(json.dumps(self.bad_targets(label)));before=(sp.read_bytes(),tp.read_bytes())
                with self.subTest(label=label):
                    with self.assertRaises(LoopModelError):self.import_case(base,tp)
                    self.assertEqual(published,output.read_bytes());self.assertEqual(before,(sp.read_bytes(),tp.read_bytes()))
            sp.write_bytes(legal);self.import_case(base,tp);self.assertEqual(published,output.read_bytes())
    def test_12_in_memory_input_and_target_lists_are_unchanged(self):
        s,t=example(extras=('R','P'));before=copy.deepcopy((s,t));_validate_nested_segments(s);copy_loop_timing(s,t);self.assertEqual(before,(s,t));self.assertEqual(['R','P'],s['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:5'][:2])
