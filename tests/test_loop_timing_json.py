"""Loop timing JSON duplicate-key boundary; no changes to the timing model."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from Code2DAG.pipeline.loop_model import build_loop_views,LoopModelError
from Code2DAG.pipeline.loop_timing import import_loop_timing
from Code2DAG.tests.test_loop_l1 import fixture,timing


def setup_case(folder,n=3):
    folder.mkdir(parents=True,exist_ok=True)
    text,base,dag=fixture(n);source=folder/'input.c';source.write_text(text)
    s=build_loop_views(base_name='test',source_text=text,base_segments_json=base,base_dag_json=dag)[2]
    s['source_file']=str(source);s['source_sha256']=hashlib.sha256(source.read_bytes()).hexdigest()
    pipeline=folder/'intermediate_results/test/pipeline';blocks=pipeline/'blocks/level2/loop_static';blocks.mkdir(parents=True)
    (blocks/'segments.json').write_text(json.dumps(s))
    return s,timing(s),pipeline/'timing/level2/loop_static/timing.json'


class LoopTimingJSONTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.base=Path(self.tmp.name)
        self.segments,self.input,self.output=setup_case(self.base)
        self.body=next(t['template_id'] for t in self.segments['templates'] if t['role']=='body_template')

    def run_input(self,text):
        path=self.base/'timing_input.json';path.write_text(text)
        return import_loop_timing(base_dir=self.base,base_name='test',input_timing=path)

    def duplicate(self,same=False):
        weights=self.input['weights'];second=dict(weights[self.body])
        if not same:second['avg_ns']=999
        return '{"weights":{'+','.join(json.dumps(k)+':'+json.dumps(v) for k,v in weights.items())+','+json.dumps(self.body)+':'+json.dumps(second)+'}}'

    def assert_rejected(self,text,key):
        with self.assertRaises(LoopModelError) as ctx:self.run_input(text)
        self.assertIn('duplicate key',str(ctx.exception));self.assertIn(key,str(ctx.exception))
        self.assertFalse(self.output.exists());self.assertFalse(self.output.parent.exists())

    def test_conflicting_template_key_is_rejected(self):
        self.assert_rejected(self.duplicate(),self.body)

    def test_identical_template_key_is_also_rejected(self):
        self.assert_rejected(self.duplicate(same=True),self.body)

    def test_nested_metric_duplicate_field_is_rejected(self):
        text=json.dumps(self.input).replace('"avg_ns": 11','"avg_ns": 11, "avg_ns": 11',1)
        self.assertNotEqual(text,json.dumps(self.input));self.assert_rejected(text,'avg_ns')

    def test_top_level_duplicate_key_is_rejected(self):
        text='{"weights":'+json.dumps(self.input['weights'])+',"weights":'+json.dumps(self.input['weights'])+'}'
        self.assert_rejected(text,'weights')

    def test_valid_unique_keys_import_normally(self):
        result=self.run_input(json.dumps(self.input))
        self.assertTrue(self.output.exists())
        for row in self.segments['segments']:
            self.assertEqual(self.input['weights'][row['template_id']],result['weights'][row['seg_id']])

    def test_duplicate_rejection_leaves_no_output_or_temp_file(self):
        self.assert_rejected(self.duplicate(),self.body)
        self.assertEqual([],list((self.base/'intermediate_results/test/pipeline').glob('timing/**/*')))

    def test_rejection_then_legal_import_has_no_poisoned_state(self):
        self.assert_rejected(self.duplicate(),self.body)
        result=self.run_input(json.dumps(self.input))
        self.assertTrue(self.output.exists());self.assertNotIn(999,[v['avg_ns'] for v in result['weights'].values()])

    def test_missing_and_legacy_whole_weight_still_rejected(self):
        missing=json.loads(json.dumps(self.input));del missing['weights'][self.body]
        with self.assertRaisesRegex(LoopModelError,'missing template'):self.run_input(json.dumps(missing))
        self.assertFalse(self.output.exists())
        whole={'weights':{'OLD':{'avg_ns':100}}}
        with self.assertRaisesRegex(LoopModelError,'whole-segment timing'):self.run_input(json.dumps(whole))
        self.assertFalse(self.output.exists())

    def test_n0_n1_n3_copy_results_unchanged(self):
        for n in [0,1,3]:
            with self.subTest(n=n):
                base=self.base/('n'+str(n));s,data,target=setup_case(base,n);p=base/'input.json';p.write_text(json.dumps(data))
                result=import_loop_timing(base_dir=base,base_name='test',input_timing=p)
                self.assertEqual(n+2,len(result['weights']))
                for row in s['segments']:self.assertEqual(data['weights'][row['template_id']],result['weights'][row['seg_id']])

    def test_existing_valid_output_survives_later_rejection(self):
        self.run_input(json.dumps(self.input));before=self.output.read_bytes()
        with self.assertRaisesRegex(LoopModelError,'duplicate key'):self.run_input(self.duplicate())
        self.assertEqual(before,self.output.read_bytes())
