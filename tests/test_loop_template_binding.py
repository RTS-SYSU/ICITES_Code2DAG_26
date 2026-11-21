"""Direct regressions for existing Loop segment/template consistency only."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from Code2DAG.pipeline.loop_model import LoopModelError,copy_loop_timing
from Code2DAG.pipeline.loop_timing import import_loop_timing
from Code2DAG.tests.test_loop_timing_json import setup_case


class LoopTemplateBindingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.base=Path(self.tmp.name)
        self.s,self.data,self.output=setup_case(self.base)
        self.sp=self.base/'intermediate_results/test/pipeline/blocks/level2/loop_static/segments.json'
        self.tp=self.base/'weights.json';self.tp.write_text(json.dumps(self.data))
        self.lid=self.s['loops'][0]['loop_id']

    def row(self,role):return next(r for r in self.s['segments'] if r['role']==role)
    def template(self,role):return next(t for t in self.s['templates'] if t['role']==role)
    def run_import(self):return import_loop_timing(base_dir=self.base,base_name='test',input_timing=self.tp)

    def reject(self):
        self.sp.write_text(json.dumps(self.s,indent=2));before=self.sp.read_bytes()
        with patch('Code2DAG.pipeline.loop_timing.copy_loop_timing',wraps=copy_loop_timing) as spy:
            with self.assertRaises(LoopModelError) as ctx:self.run_import()
            spy.assert_not_called()
        text=str(ctx.exception)
        for field in ['node=','role=','template=']:self.assertIn(field,text)
        self.assertFalse(self.output.exists());self.assertFalse(self.output.parent.exists());self.assertEqual(before,self.sp.read_bytes())
        return text

    def test_iteration_cannot_reference_before(self):
        r=self.row('iteration');r['template_id']=self.template('before')['template_id'];self.assertIn(r['seg_id'],self.reject())

    def test_iteration_cannot_reference_after(self):
        self.row('iteration')['template_id']=self.template('after')['template_id'];self.reject()

    def test_before_cannot_reference_body(self):
        self.row('before')['template_id']=self.template('body_template')['template_id'];self.reject()

    def test_after_cannot_reference_body(self):
        self.row('after')['template_id']=self.template('body_template')['template_id'];self.reject()

    def test_cross_loop_template_reference_rejected(self):
        other=copy.deepcopy(self.s['loops'][0]);other['loop_id']='LOOP:other@5';other['original_segment']='OTHER';self.s['loops'].append(other)
        t=copy.deepcopy(self.template('body_template'));t['template_id']=other['loop_id']+':body_template';self.s['templates'].append(t)
        self.data['weights'][t['template_id']]=copy.deepcopy(self.data['weights'][self.template('body_template')['template_id']]);self.tp.write_text(json.dumps(self.data))
        self.row('iteration')['template_id']=t['template_id'];self.reject()

    def test_duplicate_template_id_rejected(self):
        self.s['templates'].append(copy.deepcopy(self.template('body_template')));self.assertIn('duplicate template_id',self.reject())

    def test_missing_template_id_rejected(self):
        self.row('iteration')['template_id']='UNKNOWN';self.reject()

    def test_iteration_range_must_match_body_template(self):
        self.row('iteration')['start_line']+=1;self.reject()

    def test_body_template_range_must_match_loop(self):
        self.template('body_template')['start_line']+=1;self.reject()

    def test_unknown_execution_role_rejected(self):
        self.row('iteration')['role']='mystery';self.reject()

    def test_empty_template_reference_rejected(self):
        self.row('iteration')['template_id']='';self.reject()

    def test_valid_n0_n1_n3_results_unchanged(self):
        for n in [0,1,3]:
            with self.subTest(n=n):
                base=self.base/('n'+str(n));s,data,target=setup_case(base,n);p=base/'weights.json';p.write_text(json.dumps(data))
                before=(base/'intermediate_results/test/pipeline/blocks/level2/loop_static/segments.json').read_bytes()
                expected=copy_loop_timing(s,data)['weights'];result=import_loop_timing(base_dir=base,base_name='test',input_timing=p)
                self.assertEqual(expected,result['weights']);self.assertEqual(before,(base/'intermediate_results/test/pipeline/blocks/level2/loop_static/segments.json').read_bytes())

    def test_conflict_does_not_call_copy_or_publish(self):
        self.row('iteration')['template_id']=self.template('before')['template_id'];self.reject()

    def test_existing_valid_timing_not_overwritten(self):
        self.run_import();before=self.output.read_bytes();self.row('iteration')['template_id']=self.template('before')['template_id'];self.sp.write_text(json.dumps(self.s));segment_bytes=self.sp.read_bytes()
        with self.assertRaises(LoopModelError):self.run_import()
        self.assertEqual(before,self.output.read_bytes());self.assertEqual(segment_bytes,self.sp.read_bytes())

    def test_legal_import_after_rejection_restores_without_poison(self):
        valid=copy.deepcopy(self.s);self.row('iteration')['template_id']=self.template('before')['template_id'];self.reject()
        self.sp.write_text(json.dumps(valid));result=self.run_import();self.assertTrue(self.output.exists());self.assertEqual(copy_loop_timing(valid,self.data)['weights'],result['weights'])

    def test_input_segments_bytes_unchanged_on_success(self):
        before=self.sp.read_bytes();self.run_import();self.assertEqual(before,self.sp.read_bytes())

    def test_duplicate_loop_id_rejected(self):
        self.s['loops'].append(copy.deepcopy(self.s['loops'][0]));self.assertIn('duplicate loop_id',self.reject())

    def test_before_and_after_ranges_match_their_templates(self):
        for role in ['before','after']:
            with self.subTest(role=role):
                original=copy.deepcopy(self.s);self.row(role)['end_line']+=1;self.reject();self.s=original

    def test_unknown_template_role_rejected(self):
        self.template('body_template')['role']='mystery';self.reject()
