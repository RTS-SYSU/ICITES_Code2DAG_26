"""Exact JSON integer semantics across the complete C1 timing boundary."""
import copy
from contextlib import contextmanager
from pathlib import Path
import tempfile
from unittest.mock import patch
import json
import unittest
from Code2DAG.tests import test_composition_timing_contract as boundary
from Code2DAG.pipeline.composition_model import CompositionError
from Code2DAG.pipeline.io_utils import write_json


def integer_paths(value, path=()):
    if type(value) is int:
        yield path,value
    elif isinstance(value,dict):
        for key,child in value.items():yield from integer_paths(child,path+(key,))
    elif isinstance(value,list):
        for key,child in enumerate(value):yield from integer_paths(child,path+(key,))


def assign(data,path,value):
    for key in path[:-1]:data=data[key]
    data[path[-1]]=value


class CompositionIntegerContractTests(unittest.TestCase):
    @contextmanager
    def variant(self,kind):
        h=boundary.CompositionTimingContractTests()
        if kind=='single':
            with h.case() as c:yield h,c
            return
        from Code2DAG.tests.test_composition_c1 import fixture
        from Code2DAG.pipeline.composition_model import build_composition_views,RULE
        from Code2DAG.pipeline.rules.base import RuleOutput
        from Code2DAG.pipeline.runner import _write_rule_outputs
        text,_,_=fixture()
        body='    for (int i = 0; i < 3; i++) {\n        value_a += i; /* BODY */\n    }'
        replacements={
            'zero':body.replace('< 3','< 0'),
            'pair':'    for (int i = 0; i < 1; i++) {\n        value_a += i;\n    }\n    value_a += 7;\n    for (int j = 0; j < 2; j++) {\n        value_a += j;\n    }',
            'nested':'    for (int i = 0; i < 2; i++) {\n        for (int j = 0; j < 3; j++) {\n            value_a += i+j;\n        }\n    }'}
        assert body in text
        text=text.replace(body,replacements[kind]);_,raw,dag=fixture(text=text)
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);source=base/'source.c';source.write_text(text);raw['source_file']=str(source)
            views=build_composition_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
            with patch('Code2DAG.pipeline.runner._render_dot'):
                _write_rule_outputs(base_dir=base,base_name='fixture',level='level2',rule_name=RULE,output=RuleOutput(*views),dot_renderer=lambda s,d:'digraph G {}')
            root=base/'intermediate_results/fixture/pipeline';inp=base/'templates.json'
            write_json(inp,{'weights':{t['template_id']:dict(total_ns=10,count=1,avg_ns=10,min_ns=10,max_ns=10) for t in views[2]['templates']}})
            yield h,dict(base=base,raw=raw,dag=dag,views=views,blocks=root/'blocks/level2'/RULE,input=inp,output=root/'timing/level2'/RULE/'timing.json')

    def test_all_persisted_integer_fields_reject_non_integer_json_types(self):
        for kind in ['single','zero','pair','nested']:
            with self.subTest(kind=kind),self.variant(kind) as (h,c):
                covered=set()
                for name in ['segments.json','dag_seg.json']:
                    p=c['blocks']/name;valid=p.read_bytes();original=json.loads(valid)
                    representatives={}
                    for path,value in integer_paths(original):
                        signature=tuple('*' if type(k) is int else k for k in path)
                        representatives.setdefault(signature,(path,value))
                    for path,value in representatives.values():
                        covered.update(k for k in path if type(k) is str)
                        for invalid in [True,False,float(value),str(value),None]:
                            with self.subTest(file=name,path=path,value=repr(invalid)):
                                changed=copy.deepcopy(original);assign(changed,path,invalid);write_json(p,changed)
                                before=p.read_bytes()
                                with self.assertRaises(CompositionError):h.run_import(c)
                                self.assertFalse(c['output'].exists());self.assertEqual(before,p.read_bytes())
                    p.write_bytes(valid)
                self.assertTrue({'iteration_count','start_line','end_line'}<=covered)
                if kind!='zero':self.assertIn('iteration_index',covered)
                if kind=='nested':self.assertTrue({'depth','outer_iteration_index','inner_iteration_index'}<=covered)
                self.assertTrue(h.run_import(c)['weights'])

    def test_count_integer_range_binding_and_missing_field(self):
        h=boundary.CompositionTimingContractTests()
        with h.case() as c:
            p=c['blocks']/'segments.json';valid=p.read_bytes();original=json.loads(valid)
            self.assertEqual(3,original['loops'][0]['iteration_count'])
            for value in [-1,0,1,4,2147483648]:
                with self.subTest(value=value):
                    data=copy.deepcopy(original);data['loops'][0]['iteration_count']=value;write_json(p,data)
                    with self.assertRaises(CompositionError):h.run_import(c)
                    self.assertFalse(c['output'].exists())
            data=copy.deepcopy(original);del data['loops'][0]['iteration_count'];write_json(p,data)
            with self.assertRaisesRegex(CompositionError,'exact integer'):h.run_import(c)
            p.write_bytes(valid);h.run_import(c)

    def test_all_metric_integer_fields_reject_boolean_and_coercion(self):
        h=boundary.CompositionTimingContractTests()
        with h.case() as c:
            valid=c['input'].read_bytes();original=json.loads(valid);tid=next(iter(original['weights']))
            for field in ['total_ns','count','avg_ns','min_ns','max_ns']:
                for invalid in [True,False,1.0,'1',None,-1]:
                    with self.subTest(field=field,value=repr(invalid)):
                        data=copy.deepcopy(original);data['weights'][tid][field]=invalid;write_json(c['input'],data)
                        with self.assertRaises(CompositionError):h.run_import(c)
                        self.assertFalse(c['output'].exists())
            c['input'].write_bytes(valid);h.run_import(c)

    def test_count_rejection_preserves_output_and_retry(self):
        h=boundary.CompositionTimingContractTests()
        with h.case() as c:
            result=h.run_import(c);published=c['output'].read_bytes();p=c['blocks']/'segments.json';valid=p.read_bytes()
            for value in [True,False,3.0,'3',None]:
                data=json.loads(valid);data['loops'][0]['iteration_count']=value;write_json(p,data);before=p.read_bytes()
                with self.assertRaisesRegex(CompositionError,'exact integer'):h.run_import(c)
                self.assertEqual(published,c['output'].read_bytes());self.assertEqual(before,p.read_bytes())
            p.write_bytes(valid);self.assertEqual(result,h.run_import(c));self.assertEqual(published,c['output'].read_bytes())
