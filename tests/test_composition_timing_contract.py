"""C1 publisher/import boundary: optional DAG view and exact JSON comparison."""
import copy
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from Code2DAG.tests.test_composition_c1 import fixture, SOURCES
from Code2DAG.pipeline.composition_model import build_composition_views, CompositionError, RULE
from Code2DAG.pipeline.composition_timing import import_composition_timing
from Code2DAG.pipeline.io_utils import write_json
from Code2DAG.pipeline.rules.base import RuleOutput
from Code2DAG.pipeline.runner import _write_rule_outputs


class CompositionTimingContractTests(unittest.TestCase):
    @contextmanager
    def case(self,nested=False):
        text,raw,dag=fixture()
        if nested:
            raw['segments'][4]['custom']={'nested':((1,[(2,('text',3))]),)}
            raw['mappings']={'custom_trace':((1,[('x',(2,3))]),)}
        frozen=copy.deepcopy((raw,dag));before=(SOURCES/'ckl.c').read_bytes()
        views=build_composition_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp)
            # Exercise the actual production JSON publisher, not a hand-written alias.
            with patch('Code2DAG.pipeline.runner._render_dot'):
                _write_rule_outputs(base_dir=base,base_name='fixture',level='level2',rule_name=RULE,
                                    output=RuleOutput(*views),dot_renderer=lambda s,d:'digraph G {}')
            root=base/'intermediate_results/fixture/pipeline';blocks=root/'blocks/level2'/RULE
            inp=base/'templates.json'
            write_json(inp,{'weights':{t['template_id']:dict(total_ns=10,count=1,avg_ns=10,min_ns=10,max_ns=10) for t in views[2]['templates']}})
            c=dict(base=base,raw=raw,dag=dag,views=views,blocks=blocks,input=inp,output=root/'timing/level2'/RULE/'timing.json')
            yield c
        self.assertEqual(frozen,(raw,dag));self.assertEqual(before,(SOURCES/'ckl.c').read_bytes())

    def run_import(self,c):
        with patch('Code2DAG.pipeline.composition_timing.build_level2_segments_and_dag',return_value=(c['raw'],c['dag'])):
            return import_composition_timing(base_dir=c['base'],base_name='fixture',input_timing=c['input'])

    def test_published_absent_view_and_tuple_to_list_are_accepted(self):
        with self.case() as c:
            self.assertIs(type(c['views'][2]['loops'][0]['statements'][0]),tuple)
            dag=json.loads((c['blocks']/'dag_seg.json').read_text());self.assertNotIn('view',dag)
            segments=json.loads((c['blocks']/'segments.json').read_text())
            self.assertIs(type(segments['loops'][0]['statements'][0]),list)
            before={p.name:p.read_bytes() for p in c['blocks'].glob('*.json')}
            out=self.run_import(c);self.assertTrue(c['output'].exists())
            self.assertEqual(set(c['views'][3]['nodes']),set(out['weights']))
            self.assertEqual(before,{p.name:p.read_bytes() for p in c['blocks'].glob('*.json')})
            self.assertIs(type(c['views'][2]['loops'][0]['statements'][0]),tuple)

    def test_nested_tuple_list_conversion_accepted_without_model_mutation(self):
        with self.case(nested=True) as c:
            before=copy.deepcopy(c['views']);out=self.run_import(c)
            self.assertEqual(before,c['views']);self.assertIs(type(c['raw']['segments'][4]['custom']['nested']),tuple)
            self.assertEqual(set(c['views'][3]['nodes']),set(out['weights']))

    def test_explicit_scheduling_alias_views_are_accepted(self):
        for view in ['sched','single']:
            with self.subTest(view=view),self.case() as c:
                p=c['blocks']/'dag_seg.json';dag=json.loads(p.read_text());dag['view']=view;write_json(p,dag)
                before=p.read_bytes();self.run_import(c);self.assertEqual(before,p.read_bytes())

    def test_conflicting_explicit_views_fail_without_publication(self):
        for view in ['struct','unknown',None,False,1,[],{}]:
            with self.subTest(view=view),self.case() as c:
                p=c['blocks']/'dag_seg.json';dag=json.loads(p.read_text());dag['view']=view;write_json(p,dag)
                before=p.read_bytes()
                with self.assertRaisesRegex(CompositionError,'conflicting scheduling DAG view'):self.run_import(c)
                self.assertFalse(c['output'].parent.exists());self.assertEqual(before,p.read_bytes())

    def test_non_view_field_values_and_unknown_fields_still_compared(self):
        for kind in ['value','extra_field']:
            with self.subTest(kind=kind),self.case() as c:
                p=c['blocks']/'dag_seg.json';dag=json.loads(p.read_text())
                if kind=='value':dag['edges'][0]['kind']='forged'
                else:dag['unrecognized']='extra'
                write_json(p,dag)
                with self.assertRaisesRegex(CompositionError,'scheduling DAG changed'):self.run_import(c)
                self.assertFalse(c['output'].parent.exists())

    def test_missing_required_dag_and_segment_fields_rejected(self):
        for name,key in [('dag_seg.json','nodes'),('dag_seg.json','edges'),('dag_seg.json','base_name'),('dag_seg.json','exclusive_groups'),('segments.json','mappings'),('segments.json','view')]:
            with self.subTest(name=name,key=key),self.case() as c:
                p=c['blocks']/name;data=json.loads(p.read_text());del data[key];write_json(p,data)
                with self.assertRaisesRegex(CompositionError,'changed'):self.run_import(c)
                self.assertFalse(c['output'].parent.exists())

    def test_extra_node_and_wrong_mapping_rejected(self):
        for kind in ['node','mapping']:
            with self.subTest(kind=kind),self.case() as c:
                p=c['blocks']/('dag_seg.json' if kind=='node' else 'segments.json');data=json.loads(p.read_text())
                if kind=='node':data['nodes'].append('unknown')
                else:
                    m=data['mappings']['generation_node_to_execution_segments'];m[next(iter(m))]=['unknown']
                write_json(p,data)
                with self.assertRaisesRegex(CompositionError,'changed'):self.run_import(c)
                self.assertFalse(c['output'].parent.exists())

    def test_rejection_preserves_valid_timing_and_legal_retry(self):
        with self.case() as c:
            legal=self.run_import(c);published=c['output'].read_bytes();p=c['blocks']/'dag_seg.json';original=p.read_bytes();inp=c['input'].read_bytes()
            dag=json.loads(original);dag['view']='struct';write_json(p,dag)
            with self.assertRaisesRegex(CompositionError,'conflicting scheduling DAG view'):self.run_import(c)
            self.assertEqual(published,c['output'].read_bytes());self.assertEqual(inp,c['input'].read_bytes())
            p.write_bytes(original);self.assertEqual(legal,self.run_import(c));self.assertEqual(published,c['output'].read_bytes())


if __name__=='__main__':unittest.main()
