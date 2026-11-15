"""All consumed templates map through the actual contraction, not final row names."""
import copy,json,tempfile,unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from Code2DAG.tests import test_composition_c3a as c3a
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition_c3a_model import build_branch_loop_views,_template_targets,RULE
from Code2DAG.pipeline.composition_c3a_timing import import_branch_loop_timing
from Code2DAG.pipeline.composition_model import CompositionError
from Code2DAG.pipeline.rules.base import RuleOutput
from Code2DAG.pipeline.runner import _write_rule_outputs
from Code2DAG.pipeline.io_utils import write_json


class C3aTemplateTargetTests(unittest.TestCase):
    @contextmanager
    def case(self,kind='one'):
        text=(c3a.SOURCES/'outside_lock.c').read_text()
        extra='    pthread_mutex_lock(&'+('n' if kind=='two' else 'm')+');\n    result += 9;\n    pthread_mutex_unlock(&'+('n' if kind=='two' else 'm')+');\n'
        if kind in ['two','multi']:text=text.replace('    result += 1000;',extra+'    result += 1000;')
        if kind=='two':text=text.replace('int mode;','pthread_mutex_t n = PTHREAD_MUTEX_INITIALIZER;\nint mode;')
        if kind=='names':text=text.replace('    result += 7;','    int i = 9;\n    result += i;')
        if kind=='zero':text=text.replace('i < 3','i < 0')
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);source=base/'fixture.c';source.write_text(text);before=source.read_bytes()
            _,raw,dag=c3a.fixture('outside_lock',text);raw['source_file']=str(source);frozen=copy.deepcopy((raw,dag))
            views=build_branch_loop_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
            with patch('Code2DAG.pipeline.runner._render_dot'):_write_rule_outputs(base_dir=base,base_name='fixture',level='level2',rule_name=RULE,output=RuleOutput(*views),dot_renderer=lambda s,d:'digraph G {}')
            root=base/'intermediate_results/fixture/pipeline';inp=base/'weights.json';write_json(inp,{'weights':{t['template_id']:dict(total_ns=10,count=1,avg_ns=10,min_ns=10,max_ns=10) for t in views[2]['templates']}})
            yield dict(base=base,root=root,raw=raw,dag=dag,views=views,input=inp,blocks=root/'blocks/level2'/RULE,output=root/'timing/level2'/RULE/'timing.json')
            self.assertEqual(frozen,(raw,dag));self.assertEqual(before,source.read_bytes())

    def run_import(self,c):
        with patch('Code2DAG.pipeline.composition_c3a_timing.build_level2_segments_and_dag',return_value=(c['raw'],c['dag'])):return import_branch_loop_timing(base_dir=c['base'],base_name='fixture',input_timing=c['input'])

    def assert_targets(self,s):
        mapping=s['mappings']['template_to_execution_segments']
        for row in s['segments']:
            tids=[m['execution_segment']['template_id'] for m in row['members']] if row['kind']=='lock_group' else [row['template_id']]
            for tid in tids:self.assertIn(row['seg_id'],mapping[tid]);self.assertEqual(len(mapping[tid]),len(set(mapping[tid])))
        for group in s['lock_groups']:
            for m in group['members']:
                e=m['execution_segment'];self.assertEqual([group['group_id']],mapping[e['template_id']])
                for old in e['source_segments']:self.assertIn(group['group_id'],s['mappings']['original_segment_to_execution_segments'][old])
                for pos in e['source_position_ids']:self.assertIn(group['group_id'],s['mappings']['source_position_to_execution_segments'][pos])

    def test_one_two_and_multi_member_groups_follow_actual_projection(self):
        for kind,n_groups,n_members in [('one',1,1),('two',2,2),('multi',1,2)]:
            with self.subTest(kind=kind),self.case(kind) as c:
                s=c['views'][2];self.assertEqual(n_groups,len(s['lock_groups']));self.assertEqual(n_members,sum(len(g['members']) for g in s['lock_groups']));self.assert_targets(s)
                timing=self.run_import(c)
                for g in s['lock_groups']:self.assertEqual(10*len(g['members']),timing['weights'][g['group_id']]['avg_ns'])

    def test_same_local_name_does_not_mix_loop_and_lock_templates(self):
        with self.case('names') as c:
            s=c['views'][2];self.assert_targets(s);self.run_import(c)
            for t in s['templates']:
                if t['role']=='body_template':self.assertTrue(all(not n.startswith('LOCKGROUP:') for n in s['mappings']['template_to_execution_segments'][t['template_id']]))

    def test_zero_iteration_unused_template_stays_empty(self):
        with self.case('zero') as c:
            s=c['views'][2];self.assert_targets(s);timing=self.run_import(c)
            for t in s['templates']:
                if t['role']=='body_template':self.assertEqual([],s['mappings']['template_to_execution_segments'][t['template_id']])
            self.assertFalse(any(':iteration:' in n for n in timing['weights']))

    def test_missing_ambiguous_or_unknown_projection_rejects(self):
        execution=[dict(seg_id='member',template_id='template')]
        for projection in [{},{'member':[]},{'member':['LOCKGROUP:m','LOCKGROUP:n']},{'member':'unknown'}]:
            with self.subTest(projection=projection),self.assertRaisesRegex(CompositionError,'unique legal final target'):_template_targets([dict(template_id='template')],execution,projection,{'LOCKGROUP:m'})

    def test_mapping_corruption_fails_before_publish_and_preserves_existing(self):
        for kind in ['missing','empty','conflicting','unused']:
            with self.subTest(kind=kind),self.case('zero') as c:
                sp=c['blocks']/'segments.json';valid=sp.read_bytes();s=json.loads(valid);g=s['lock_groups'][0];tid=g['members'][0]['execution_segment']['template_id'];mapping=s['mappings']['template_to_execution_segments']
                if kind=='missing':del mapping[tid]
                elif kind=='empty':mapping[tid]=[]
                elif kind=='conflicting':mapping[tid]=['wrong']
                else:mapping[next(t['template_id'] for t in s['templates'] if t['role']=='body_template')]=[g['group_id']]
                write_json(sp,s);damaged=sp.read_bytes()
                with self.assertRaises(ValueError):self.run_import(c)
                self.assertFalse(c['output'].exists());self.assertEqual(damaged,sp.read_bytes())
                sp.write_bytes(valid);result=self.run_import(c);published=c['output'].read_bytes();sp.write_bytes(damaged)
                with self.assertRaises(ValueError):self.run_import(c)
                self.assertEqual(published,c['output'].read_bytes());sp.write_bytes(valid);self.assertEqual(result,self.run_import(c))

    def test_native_outside_lock_template_consumer_matches_timing(self):
        source=c3a.SOURCES/'outside_lock.c'
        with tempfile.TemporaryDirectory() as tmp:
            base,p=c1.CompositionC1Tests().native(tmp,source)
            from Code2DAG.pipeline.composition import run_composition_blocks
            run_composition_blocks(base_dir=base,base_name=source.stem,source_file=source)
            s=json.loads((p/'blocks/level2'/RULE/'segments.json').read_text());self.assert_targets(s)
