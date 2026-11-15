"""Loop L1 model tests: synthetic template timing, never execute/instrument C."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from Code2DAG.pipeline.loop_model import analyze_static_loop, build_loop_views, copy_loop_timing, LoopModelError
from Code2DAG.pipeline.rules.loop_rule import LoopStaticRule
from Code2DAG.pipeline.rules.level2_effective_line_rule import Level2EffectiveLineRule
from Code2DAG.pipeline.runner import default_rule_for_level
from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.pipeline.algo.common import topo_sort

ROOT=Path(__file__).resolve().parents[2]
SOURCE=ROOT/'Code2DAG/source_files/loop_l1/loop_l1.c'
ALGOS=['FIFO','LPF','heft','t_level','zhao2020']


def fixture(n=3):
    text=SOURCE.read_text().replace('i < 3','i < '+str(n))
    s=dict(base_name='loop_l1',source_file=str(SOURCE),segments=[dict(seg_id='OLD',function='main',kind='compute',start_line=4,end_line=10)],node_locations={'main/printf1':dict(function='main',line=10)},node_to_segment={'main':'OLD','main/printf1':'OLD'})
    d=dict(nodes=['OLD'],edges=[])
    return text,s,d


def build(n=3):
    text,s,d=fixture(n)
    return build_loop_views(base_name='loop_l1',source_text=text,base_segments_json=s,base_dag_json=d)


def timing(s):
    return {'weights':{t['template_id']:dict(total_ns=90+i,count=2,avg_ns=10+i,min_ns=3+i,max_ns=20+i) for i,t in enumerate(s['templates'])}}


def pairs(d):return {(e['src'],e['dst']) for e in d['edges']}


class LoopL1Tests(unittest.TestCase):
    def test_01_three_iterations_split_boundaries_unique_ids(self):
        st,sd,s,d,m=build()
        rows=s['segments'];self.assertEqual(5,len(rows));self.assertEqual(7,len(st['segments']))
        body=[r for r in rows if r['role']=='iteration']
        self.assertEqual([0,1,2],[r['iteration_index'] for r in body]);self.assertEqual(3,len({r['seg_id'] for r in body}))
        self.assertEqual([(4,4),(6,7),(6,7),(6,7),(9,10)],[(r['start_line'],r['end_line']) for r in rows])
        self.assertEqual(['before','iteration','iteration','iteration','after'],[r['role'] for r in rows])
        self.assertEqual('loop_entry',st['segments'][1]['kind']);self.assertEqual('loop_exit',st['segments'][-2]['kind'])
        self.assertFalse(s['capabilities']['runtime_instrumentation_supported'])

    def test_02_one_iteration(self):
        st,sd,s,d,m=build(1)
        self.assertEqual(1,len([r for r in s['segments'] if r['role']=='iteration']))
        self.assertEqual(3,len(d['nodes']));self.assertEqual(2,len(d['edges']))

    def test_03_zero_iteration_empty_path_no_phantom_mapping(self):
        st,sd,s,d,m=build(0);lid=s['loops'][0]['loop_id']
        self.assertEqual(['before','after'],[r['role'] for r in s['segments']])
        self.assertIn((lid+':entry',lid+':exit'),pairs(sd));self.assertEqual({(lid+':before',lid+':after')},pairs(d))
        self.assertEqual([],s['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:6'])
        self.assertNotIn(lid+':body_template',d['nodes'])

    def test_04_three_independent_mapping_namespaces_no_fabricated_generation(self):
        st,sd,s,d,m=build();maps=s['mappings'];ids=[r['seg_id'] for r in s['segments'] if r['role']=='iteration']
        self.assertEqual(ids,maps['source_position_to_execution_segments']['LOOPSRC:main:line:6'])
        self.assertEqual(ids,maps['source_position_to_execution_segments']['LOOPSRC:main:line:7'])
        self.assertEqual(d['nodes'],maps['original_segment_to_execution_segments']['OLD'])
        self.assertEqual([s['loops'][0]['loop_id']+':after'],maps['generation_node_to_execution_segments']['main/printf1'])
        self.assertFalse(any(k.startswith('LOOPSRC:') for k in maps['generation_node_to_execution_segments']))
        self.assertTrue(all(r['generation_origin'] is None for r in s['segments'] if r['role']=='iteration'))
        self.assertNotIn('node_to_segment',s)

    def test_05_actual_generation_body_call_maps_to_every_iteration(self):
        text,base,dag=fixture();base['node_locations']['main/body1']=dict(function='main',line=6);base['node_to_segment']['main/body1']='OLD'
        st,sd,s,d,m=build_loop_views(base_name='x',source_text=text,base_segments_json=base,base_dag_json=dag)
        ids=[r['seg_id'] for r in s['segments'] if r['role']=='iteration']
        self.assertEqual(ids,s['mappings']['generation_node_to_execution_segments']['main/body1'])
        self.assertTrue(all(r['generation_origin']==['main/body1'] for r in s['segments'] if r['role']=='iteration'))

    def test_06_exact_iteration_order_entry_exit_and_post_successor(self):
        st,sd,s,d,m=build();lid=s['loops'][0]['loop_id']
        ordered=[lid+':before',lid+':entry']+[lid+f':iteration:{i}' for i in range(3)]+[lid+':exit',lid+':after']
        self.assertEqual(set(zip(ordered,ordered[1:])),pairs(sd))
        execution=[n for n in ordered if n not in (lid+':entry',lid+':exit')]
        self.assertEqual(set(zip(execution,execution[1:])),pairs(d))
        self.assertEqual(execution,topo_sort(d['nodes'],list(pairs(d))))
        self.assertEqual(2,sum(e['kind']=='loop_iteration_order' for e in d['edges']))

    def test_07_external_edges_types_custom_attributes_and_unaffected_nodes(self):
        text,base,dag=fixture()
        base['segments']=[dict(seg_id='P',function='worker',kind='compute',start_line=1,end_line=1),*base['segments'],dict(seg_id='Q',function='worker',kind='compute',start_line=12,end_line=12)]
        dag={'nodes':['P','OLD','Q'],'edges':[dict(src='P',dst='OLD',kind=k,tag=k) for k in ['create','sem_dep']]+[dict(src='OLD',dst='Q',kind=k,tag=k) for k in ['join','intra']]}
        original=copy.deepcopy((base,dag));st,sd,s,d,m=build_loop_views(base_name='x',source_text=text,base_segments_json=base,base_dag_json=dag)
        self.assertEqual(original,(base,dag));lid=s['loops'][0]['loop_id']
        self.assertTrue(all(dict(src='P',dst=lid+':before',kind=k,tag=k) in d['edges'] for k in ['create','sem_dep']))
        self.assertTrue(all(dict(src=lid+':after',dst='Q',kind=k,tag=k) in d['edges'] for k in ['join','intra']))
        self.assertEqual(base['segments'][0],s['segments'][0]);self.assertEqual(['P'],s['mappings']['original_segment_to_execution_segments']['P'])

    def test_08_reject_cross_loop_whole_segment_timing(self):
        s=build()[2]
        with self.assertRaisesRegex(LoopModelError,'whole-segment timing spanning loop boundaries'):
            copy_loop_timing(s,{'weights':{'OLD':{'avg_ns':100}}})
        t=timing(s);t['weights']['OLD']={'avg_ns':100}
        with self.assertRaisesRegex(LoopModelError,'whole-segment timing'):copy_loop_timing(s,t)

    def test_09_template_metrics_copied_exactly_without_aliasing(self):
        s=build()[2];t=timing(s);before=copy.deepcopy(t);expanded=copy_loop_timing(s,t)
        self.assertEqual(before,t)
        for r in s['segments']:self.assertEqual(t['weights'][r['template_id']],expanded['weights'][r['seg_id']])
        body=[r for r in s['segments'] if r['role']=='iteration'];expanded['weights'][body[0]['seg_id']]['avg_ns']=999
        self.assertNotEqual(999,expanded['weights'][body[1]['seg_id']]['avg_ns']);self.assertEqual(before,t)
        for n in [0,1]:
            z=build(n)[2];self.assertEqual(len(z['segments']),len(copy_loop_timing(z,timing(z))['weights']))

    def test_10_missing_unknown_and_invalid_timing_rejected(self):
        s=build()[2];t=timing(s);key=s['templates'][1]['template_id']
        for bad in [None,{'count':3},{'avg_ns':-1},{'avg_ns':float('nan')},{'avg_ns':float('inf')}]:
            data=copy.deepcopy(t);data['weights'][key]=bad
            with self.assertRaises(LoopModelError):copy_loop_timing(s,data)
        data=copy.deepcopy(t);del data['weights'][key]
        with self.assertRaisesRegex(LoopModelError,'missing template'):copy_loop_timing(s,data)
        data=copy.deepcopy(t);data['weights']['UNKNOWN']={'avg_ns':1}
        with self.assertRaisesRegex(LoopModelError,'unknown template'):copy_loop_timing(s,data)

    def test_11_five_unchanged_algorithms_accept_all_counts(self):
        for n in [0,1,3]:
            st,sd,s,d,m=build(n);t=copy_loop_timing(s,timing(s))
            for name in ALGOS:
                with self.subTest(n=n,algo=name):
                    algo=get_algo(name);result=algo.compute(segments_json=s,dag_json=d,timing_json=t);algo.validate(result)
                    self.assertEqual(set(d['nodes']),set(result['priorities']))

    def test_12_unsupported_syntax_rejected_with_stable_reasons(self):
        src=fixture()[0];header='for (int i = 0; i < 3; i++) {'
        variants={
            'initial':src.replace('i = 0','i = 1'), 'dynamic':src.replace('i < 3','i < limit'),
            'leq':src.replace('i < 3','i <= 3'),'prefix':src.replace('i++','++i'),
            'plus_equal':src.replace('i++','i += 1'),'reverse':src.replace('i++','i--'),
            'no_braces':src.replace(header,'for (int i = 0; i < 3; i++)'),
            'header_side_effect':src.replace('i < 3','i < (f(), 3)'),
            'body_break':src.replace('result += 2;','break;'), 'body_continue':src.replace('result += 2;','continue;'),
            'body_return':src.replace('result += 2;','return 0;'),'body_goto':src.replace('result += 2;','goto done;'),
            'create':src.replace('result += 2;','pthread_create(&t, NULL, work, NULL);'),
            'sem':src.replace('result += 2;','sem_wait(&s);'),
            'mutation':src.replace('result += 2;','i += 1;'),'escape':src.replace('result += 2;','work(&i);'),
            'condition':src.replace('result += 2;','if (i) { result++; }'),
            'mutex':src.replace('result += 2;','pthread_mutex_lock(&m);'),
            'nested':src.replace('result += 2;','for (int j=0; j<2; j++) { result++; }'),
            'while':src.replace(header,'while (result < 3) {'),
            'do':src.replace(header,'do {').replace('    }','    } while (result < 3);'),
        }
        for label,text in variants.items():
            with self.subTest(label=label):
                with self.assertRaises(LoopModelError) as c:analyze_static_loop(text)
                self.assertTrue(str(c.exception).startswith('Loop L1 '))
        for label in ['dynamic','body_break','body_continue']:
            with self.assertRaises(LoopModelError) as a:analyze_static_loop(variants[label])
            with self.assertRaises(LoopModelError) as b:analyze_static_loop(variants[label])
            self.assertEqual(str(a.exception),str(b.exception))

    def test_13_missing_boundary_or_cross_segment_loop_rejected(self):
        text,s,d=fixture();s['segments'][0]['end_line']=7
        with self.assertRaisesRegex(LoopModelError,'complete loop in one existing'):build_loop_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
        with self.assertRaisesRegex(LoopModelError,'standalone closing brace'):analyze_static_loop(text.replace('    }','    } result += 1;'))

    def test_14_no_loop_compatibility_and_default_unchanged(self):
        text,s,d=fixture();text=text[:text.index('    for')]+text[text.index('    result += 10;'):]
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'plain.c';p.write_text(text)
            kwargs=dict(base_dir=Path(t),base_name='plain',source_file=p,block_info={})
            with patch('Code2DAG.pipeline.rules.level2_effective_line_rule.segment_dag_level2.build_level2_segments_and_dag',return_value=(s,d)):
                expected=Level2EffectiveLineRule().build(**kwargs);actual=LoopStaticRule().build(**kwargs)
            self.assertEqual(json.dumps(expected.__dict__,sort_keys=True).encode(),json.dumps(actual.__dict__,sort_keys=True).encode())
        self.assertEqual('effective_line_merge',default_rule_for_level('level2'))
        views=build_loop_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual((s,d,s,d),views[:4])

    def test_15_source_bytes_unchanged(self):
        original=SOURCE.read_bytes();digest=hashlib.sha256(original).hexdigest()
        for n in [0,1,3]:build(n)
        self.assertEqual(digest,hashlib.sha256(SOURCE.read_bytes()).hexdigest());self.assertEqual(original,SOURCE.read_bytes())

    def test_16_comments_crlf_and_literals_preserve_lines(self):
        src=fixture()[0]
        for variant in [src.replace('\n','\r\n'),src.replace('result += 2;','result += 2; /* ordinary */'),src.replace('i < 3','i < 0x3'),src.replace('i < 3','i < 03')]:
            info=analyze_static_loop(variant);self.assertEqual(3,info['iteration_count']);self.assertEqual(8,info['exit_line'])

    def test_17_parenthesized_induction_mutation_and_escape_rejected(self):
        src=fixture()[0]
        for statement in ['(i)++;','(i) = 1;','work(&(i));','++(i);','++( ( i ) );','( ( i ) ) += 1;','work(&( ( i ) ));']:
            with self.subTest(statement=statement):
                with self.assertRaisesRegex(LoopModelError,'induction variable'):
                    analyze_static_loop(src.replace('result += 2;',statement))

    def test_18_zero_generation_body_mapping_and_legacy_metadata(self):
        text,s,d=fixture(0);s['node_locations']['main/body1']=dict(function='main',line=6);s['node_to_segment']['main/body1']='OLD';s['mappings']={'unrelated':{'keep':'value'}}
        st,sd,out,dd,m=build_loop_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual([],out['mappings']['generation_node_to_execution_segments']['main/body1'])
        self.assertEqual({'keep':'value'},out['mappings']['unrelated'])

    def test_19_multiline_ordinary_statement_uses_existing_statement_parser(self):
        source=fixture()[0].replace('result += 2;','result +=\n            2;')
        loop=analyze_static_loop(source)
        self.assertEqual([(6,6),(7,8)],loop['statements'])
        self.assertEqual(9,loop['exit_line'])

    def test_20_zero_only_loop_segment_projects_external_dependencies(self):
        text,s,d=fixture(0);s['segments'][0]['start_line']=5;s['segments'][0]['end_line']=8
        s['segments'].insert(0,dict(seg_id='P',function='main',kind='compute',start_line=4,end_line=4))
        s['segments'].append(dict(seg_id='Q',function='main',kind='compute',start_line=9,end_line=10))
        s['node_to_segment']['main/printf1']='Q'
        d={'nodes':['P','OLD','Q'],'edges':[dict(src='P',dst='OLD',kind='intra'),dict(src='OLD',dst='Q',kind='intra')]}
        st,sd,out,dd,m=build_loop_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual(['P','Q'],dd['nodes']);self.assertEqual({('P','Q')},pairs(dd))
        self.assertEqual([],out['mappings']['original_segment_to_execution_segments']['OLD'])
        self.assertEqual(['Q'],out['mappings']['generation_node_to_execution_segments']['main/printf1'])
