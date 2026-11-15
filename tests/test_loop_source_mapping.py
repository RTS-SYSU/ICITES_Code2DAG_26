"""Only regress L3 source-position merging; no new syntax or timing behavior."""
import copy
import json
import unittest
from Code2DAG.pipeline.loop_nested_model import build_loop_nested_views
from Code2DAG.pipeline.loop_pair_model import build_loop_pair_views
from Code2DAG.tests.test_loop_l3 import fixture
from Code2DAG.tests.test_loop_l1 import fixture as single_fixture
from Code2DAG.tests.test_loop_l2 import fixture as pair_fixture


def payload(n1=2,n2=3,split=True):
    text,s,d=fixture(n1,n2)
    if split:
        s['segments']=[dict(seg_id='P',function='main',kind='compute',start_line=2,end_line=2,custom={'preserve':[1]}),dict(seg_id='OLD',function='main',kind='compute',start_line=3,end_line=7),dict(seg_id='Q',function='main',kind='compute',start_line=8,end_line=8,custom={'preserve':[2]})]
        s['node_to_segment']={'main':'P'};d=dict(nodes=['P','OLD','Q'],edges=[dict(src='P',dst='OLD',kind='intra',source={'tag':[1]}),dict(src='OLD',dst='Q',kind='intra',source={'tag':[2]})])
    else:
        s['segments'] += [dict(seg_id='P',function='helper',kind='compute',start_line=1,end_line=1,custom={'preserve':[1]}),dict(seg_id='Q',function='helper',kind='compute',start_line=2,end_line=2,custom={'preserve':[2]})]
        d['nodes'] += ['P','Q']
    return text,s,d


class LoopSourceMappingTests(unittest.TestCase):
    def run_model(self,case):
        text,s,d=case;before=json.dumps(case);v=build_loop_nested_views(base_name='mapping',source_text=text,base_segments_json=s,base_dag_json=d);self.assertEqual(before,json.dumps(case));return v
    def leaves(self,n1=2,n2=3):return [f'LOOP:main@4:outer:{i}:iteration:{j}' for i in range(n1) for j in range(n2)]
    def test_01_P_OLD_Q_existing_positions_preserved(self):
        text,s,d=payload();s['mappings']={'source_position_to_execution_segments':{'LOOPSRC:main:line:2':['P'],'LOOPSRC:main:line:8':['Q']}}
        st,sd,ss,dd,_=self.run_model((text,s,d));m=ss['mappings']['source_position_to_execution_segments']
        self.assertEqual({'LOOPSRC:main:line:2':['P'],'LOOPSRC:main:line:8':['Q'],'LOOPSRC:main:line:5':self.leaves()},m)
        for row in [s['segments'][0],s['segments'][2]]:self.assertEqual(row,next(r for r in ss['segments'] if r['seg_id']==row['seg_id']))
        self.assertTrue(all(any(e.get('source')==raw['source'] for e in dd['edges']) for raw in d['edges']))
    def test_02_multiple_keys_targets_keep_first_occurrence_order(self):
        text,s,d=payload();s['mappings']={'source_position_to_execution_segments':{'LOOPSRC:legacy:line:1':['Q','OLD','P','Q','P'],'LOOPSRC:legacy:line:2':['P','Q','P'],'LOOPSRC:legacy:line:3':[]}}
        m=self.run_model((text,s,d))[2]['mappings']['source_position_to_execution_segments']
        self.assertEqual(['Q','P'],m['LOOPSRC:legacy:line:1']);self.assertEqual(['P','Q'],m['LOOPSRC:legacy:line:2']);self.assertEqual([],m['LOOPSRC:legacy:line:3'])
    def test_03_owner_only_key_survives_as_empty_without_stale_owner(self):
        text,s,d=payload();s['mappings']={'source_position_to_execution_segments':{'LOOPSRC:main:line:3':['OLD','OLD']}}
        m=self.run_model((text,s,d))[2]['mappings']['source_position_to_execution_segments'];self.assertEqual([],m['LOOPSRC:main:line:3']);self.assertFalse(any('OLD' in values for values in m.values()))
    def test_04_body_collision_stably_merges_existing_and_all_leaves(self):
        text,s,d=payload();s['mappings']={'source_position_to_execution_segments':{'LOOPSRC:main:line:5':['Q','OLD','P','Q']}}
        m=self.run_model((text,s,d))[2]['mappings']['source_position_to_execution_segments'];self.assertEqual(['Q','P']+self.leaves(),m['LOOPSRC:main:line:5'])
    def test_05_prefix_body_suffix_collisions(self):
        text,s,d=payload(split=False);s['mappings']={'source_position_to_execution_segments':{'LOOPSRC:main:line:2':['Q','OLD','Q'],'LOOPSRC:main:line:5':['P','OLD','P'],'LOOPSRC:main:line:8':['P','OLD','Q','P']}}
        m=self.run_model((text,s,d))[2]['mappings']['source_position_to_execution_segments'];self.assertEqual(['Q','LOOP:main@3:before'],m['LOOPSRC:main:line:2']);self.assertEqual(['P']+self.leaves(),m['LOOPSRC:main:line:5']);self.assertEqual(['P','Q','LOOP:main@3:after'],m['LOOPSRC:main:line:8'])
    def test_06_no_prior_mapping_has_original_contract(self):
        case=fixture();v=self.run_model(case);m=v[2]['mappings']['source_position_to_execution_segments']
        self.assertEqual({'LOOPSRC:main:line:5':self.leaves(),'LOOPSRC:main:line:2':['LOOP:main@3:before'],'LOOPSRC:main:line:8':['LOOP:main@3:after']},m)
        self.assertEqual(14,len(v[1]['nodes']));self.assertEqual(8,len(v[3]['nodes']))
    def test_07_zero_products_keep_untouched_positions_and_empty_body(self):
        for n1,n2 in [(0,3),(2,0),(0,0)]:
            text,s,d=payload(n1,n2);s['mappings']={'source_position_to_execution_segments':{'LOOPSRC:main:line:2':['P','OLD'],'LOOPSRC:main:line:8':['Q'],'LOOPSRC:main:line:5':['OLD']}}
            m=self.run_model((text,s,d))[2]['mappings']['source_position_to_execution_segments'];self.assertEqual(['P'],m['LOOPSRC:main:line:2']);self.assertEqual(['Q'],m['LOOPSRC:main:line:8']);self.assertEqual([],m['LOOPSRC:main:line:5'])
    def test_08_zero_collision_does_not_erase_unaffected_target(self):
        text,s,d=payload(0,3);s['mappings']={'source_position_to_execution_segments':{'LOOPSRC:main:line:5':['P','OLD','P']}}
        m=self.run_model((text,s,d))[2]['mappings']['source_position_to_execution_segments'];self.assertEqual(['P'],m['LOOPSRC:main:line:5'])
    def test_09_other_two_maps_unchanged_input_and_output_not_aliased(self):
        case=payload();reference=self.run_model(copy.deepcopy(case));text,s,d=case;s['mappings']={'source_position_to_execution_segments':{'LOOPSRC:legacy:line:1':['P','OLD','P']},'other_mapping':{'retain':['x']}};before=json.dumps(case)
        actual=self.run_model(case)
        for name in ['generation_node_to_execution_segments','original_segment_to_execution_segments']:self.assertEqual(reference[2]['mappings'][name],actual[2]['mappings'][name])
        actual[2]['mappings']['source_position_to_execution_segments']['LOOPSRC:legacy:line:1'].append('Q');self.assertEqual(before,json.dumps(case));self.assertEqual({'retain':['x']},actual[2]['mappings']['other_mapping'])
    def test_10_no_single_pair_delegation_bytes_unchanged(self):
        cases=[single_fixture(),pair_fixture()[:3],('int main(void) {\n    int x=1;\n}\n',dict(segments=[dict(seg_id='A',function='main',kind='compute',start_line=2,end_line=2)],mappings={'source_position_to_execution_segments':{'LOOPSRC:main:line:2':['A']}}),dict(nodes=['A'],edges=[]))]
        for text,s,d in cases:
            kwargs=dict(base_name='delegate',source_text=text,base_segments_json=s,base_dag_json=d);self.assertEqual(json.dumps(build_loop_pair_views(**kwargs)),json.dumps(build_loop_nested_views(**kwargs)))
