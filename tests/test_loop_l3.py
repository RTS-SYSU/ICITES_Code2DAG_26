"""Limited perfect-nest regression; synthetic timing, no execution of Loop C."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from Code2DAG.pipeline.errors import ValidationError
from Code2DAG.pipeline.loop_model import LoopModelError
from Code2DAG.pipeline.loop_nested_model import analyze_nested_loops, build_loop_nested_views, MAX_STRUCTURAL_NODES
from Code2DAG.pipeline.loop_pair_model import build_loop_pair_views
from Code2DAG.pipeline.rules.loop_nested_rule import LoopStaticNestedRule
from Code2DAG.pipeline.rules.loop_pair_rule import LoopStaticPairRule
from Code2DAG.pipeline.runner import run_blocks, default_rule_for_level
from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.tests.test_loop_l1 import fixture as single_fixture
from Code2DAG.tests.test_loop_l2 import fixture as pair_fixture

ALGOS = ['FIFO','LPF','heft','t_level','zhao2020']
SOURCE = Path(__file__).resolve().parents[1] / 'source_files/loop_l3/loop_l3.c'


def fixture(n1=2, n2=3, prefix=True, suffix=True, multi=False, text=None):
    if text is None:
        text = SOURCE.read_text().replace('i < 2', 'i < '+str(n1)).replace('j < 3', 'j < '+str(n2))
        if not prefix: text = 'int result;\n' + text.replace('    int result = 1;\n', '')
        if not suffix: text = text.replace('    result += 10;\n', '')
        if multi: text = text.replace('            result += i + j;', '            result += i + j;\n            result += 1;')
    lines = text.splitlines();first = next(i for i,l in enumerate(lines,1) if 'int main' in l) + 1
    end = next(i for i,l in enumerate(lines,1) if 'return 0;' in l) - 1
    s = dict(base_name='loop_l3', source_file=str(SOURCE), segments=[dict(seg_id='OLD', function='main',kind='compute',start_line=first,end_line=end)],node_locations={},node_to_segment={'main':'OLD'})
    return text, s, dict(nodes=['OLD'],edges=[])


def build(*args,**kwargs):
    text,s,d = fixture(*args,**kwargs)
    return build_loop_nested_views(base_name='loop_l3',source_text=text,base_segments_json=s,base_dag_json=d)


def metrics(payload):
    values = {'before':10,'body_template':20,'after':30}
    return {'weights':{t['template_id']:dict(total_ns=values[t['role']]*7,count=7,avg_ns=values[t['role']],min_ns=values[t['role']]-1,max_ns=values[t['role']]+1) for t in payload['templates']}}


class LoopL3ModelTests(unittest.TestCase):
    def check(self,n1,n2,prefix=True,suffix=True,multi=False):
        st,sd,ss,dd,meta=build(n1,n2,prefix,suffix,multi);outer,inner=ss['loops'];a,b=outer['loop_id'],inner['loop_id']
        leaves=[f'{b}:outer:{i}:iteration:{j}' for i in range(n1) for j in range(n2)]
        expected=([a+':before'] if prefix else [])+leaves+([a+':after'] if suffix else [])
        chain=([a+':before'] if prefix else [])+[a+':entry']
        for i in range(n1): chain += [f'{b}:outer:{i}:entry']+[f'{b}:outer:{i}:iteration:{j}' for j in range(n2)]+[f'{b}:outer:{i}:exit']
        chain += [a+':exit']+([a+':after'] if suffix else [])
        self.assertEqual(expected,dd['nodes']);self.assertEqual(chain,sd['nodes'])
        self.assertEqual(set(zip(expected,expected[1:])),{(e['src'],e['dst']) for e in dd['edges']});self.assertEqual(max(0,len(expected)-1),len(dd['edges']))
        self.assertEqual(set(zip(chain,chain[1:])),{(e['src'],e['dst']) for e in sd['edges']})
        rows=[r for r in ss['segments'] if r['role']=='iteration']
        self.assertEqual(n1*n2,len(rows));self.assertEqual([(i,j) for i in range(n1) for j in range(n2)],[(r['outer_iteration_index'],r['inner_iteration_index']) for r in rows])
        self.assertTrue(all(r['iteration_index']==r['inner_iteration_index'] and r['loop_id']==b and r['template_id']==b+':body_template' for r in rows))
        self.assertEqual(1,outer['depth']);self.assertEqual(2,inner['depth']);self.assertEqual(a,inner['parent_loop_id']);self.assertEqual(len(sd['nodes']),len(set(sd['nodes'])))
        self.assertEqual(expected,ss['mappings']['original_segment_to_execution_segments']['OLD']);self.assertEqual(expected,ss['mappings']['generation_node_to_execution_segments']['main'])
        for first,last in inner['statements']:
            for line in range(first,last+1):self.assertEqual(leaves,ss['mappings']['source_position_to_execution_segments'][f'LOOPSRC:main:line:{line}'])
        self.assertTrue(all(r['generation_origin'] is None for r in ss['segments']))
        self.assertNotIn('node_to_segment',ss);self.assertFalse(ss['capabilities']['runtime_instrumentation_supported'])
        return st,sd,ss,dd
    def test_01_two_by_three(self):self.check(2,3)
    def test_02_one_by_one(self):self.check(1,1)
    def test_03_zero_by_three(self):self.check(0,3)
    def test_04_two_by_zero(self):self.check(2,0)
    def test_05_both_zero(self):self.check(0,0)
    def test_06_optional_prefix_suffix_and_empty_graph(self):
        for n1,n2 in [(2,3),(0,3),(2,0),(0,0)]:
            for prefix,suffix in [(False,False),(True,False),(False,True)]:
                with self.subTest(n1=n1,n2=n2,prefix=prefix,suffix=suffix):self.check(n1,n2,prefix,suffix)
    def test_07_multiple_body_statements(self):self.check(2,3,multi=True)
    def test_08_multiline_headers_braces_and_literals(self):
        pre='static char *documentation="for if {} '+chr(92)+'\n#define hidden";\n/* for example */\n'
        text=pre+SOURCE.read_text().replace('for (int i = 0; i < 2; i++) {','for (int i = 0;\n         i < 2;\n         i++)\n    {').replace('for (int j = 0; j < 3; j++) {','for (int j = 0;\n             j < 3;\n             j++)\n        {')
        st,sd,s,d,_=build(text=text);self.assertEqual(8,len(d['nodes']))
        physical=[i for i,l in enumerate(text.splitlines(),1) if 'for (int' in l];self.assertEqual(physical,[r['header_line'] for r in s['loops']])
        body=next(i for i,l in enumerate(text.splitlines(),1) if 'result += i + j;' in l);self.assertEqual(body,s['loops'][1]['body_start']);self.assertEqual(6,len(s['mappings']['source_position_to_execution_segments'][f'LOOPSRC:main:line:{body}']))
    def test_09_true_generation_origin_and_zero_empty_mapping(self):
        for n in [0,2]:
            text,s,d=fixture(n,3);s['node_locations']={'main/work1':dict(function='main',line=5)};s['node_to_segment']['main/work1']='OLD'
            result=build_loop_nested_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)[2];rows=[r for r in result['segments'] if r['role']=='iteration']
            self.assertEqual([r['seg_id'] for r in rows],result['mappings']['generation_node_to_execution_segments']['main/work1']);self.assertTrue(all(r['generation_origin']==['main/work1'] for r in rows));self.assertTrue(all(not key.startswith('LOOPSRC:') for key in result['mappings']['generation_node_to_execution_segments']))
    def test_10_external_dependencies_and_unaffected_attributes(self):
        for n1,n2,prefix,suffix in [(2,3,True,True),(2,3,False,False),(0,0,False,False)]:
            text,s,d=fixture(n1,n2,prefix,suffix)
            p=dict(seg_id='P',function='worker',kind='compute',start_line=1,end_line=1,custom={'keep':1});q=dict(p,seg_id='Q')
            s['segments']=[p,*s['segments'],q];s['node_to_segment']['worker/p']='P'
            incoming=[dict(src='P',dst='OLD',kind=k,custom={'source':[k]}) for k in ['create','sem_dep']];outgoing=[dict(src='OLD',dst='Q',kind=k,custom={'source':[k]}) for k in ['join','intra']];d=dict(nodes=['P','OLD','Q'],edges=incoming+outgoing);before=copy.deepcopy((s,d))
            st,sd,ss,dd,_=build_loop_nested_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
            self.assertEqual(before,(s,d));self.assertEqual(p,ss['segments'][0]);self.assertEqual(q,ss['segments'][-1]);self.assertEqual(['P'],ss['mappings']['generation_node_to_execution_segments']['worker/p'])
            for original in incoming+outgoing:
                matches=[e for e in dd['edges'] if e['kind']==original['kind'] and e.get('custom')==original['custom']];self.assertTrue(matches,original)
                self.assertTrue(any(e==original or original['kind'] in [x['kind'] for x in e.get('control_projection',[])] or e.get('custom')==original['custom'] for e in matches))
    def test_11_cycle_rejected_not_deleted(self):
        text,s,d=fixture();d['edges']=[dict(src='OLD',dst='OLD',kind='intra',custom='cycle')]
        with self.assertRaisesRegex(ValidationError,'cycle'):build_loop_nested_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
    def test_12_size_guard_before_any_expansion_range(self):
        text,s,d=fixture(2147483647,0)
        with patch('Code2DAG.pipeline.loop_nested_model.range',side_effect=AssertionError('allocated before guard'),create=True):
            with self.assertRaisesRegex(LoopModelError,'structural node limit exceeded'):build_loop_nested_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual(10000,MAX_STRUCTURAL_NODES)
    def test_13_size_limit_boundary(self):
        st,sd,s,d,_=build(1,9994);self.assertEqual(MAX_STRUCTURAL_NODES,len(sd['nodes']))
        with self.assertRaisesRegex(LoopModelError,'structural node limit exceeded'):build(1,9995)
    def test_14_outer_extra_work_and_same_variable_rejected(self):
        source=SOURCE.read_text()
        bad=[source.replace('        for (int j','        result++;\n        for (int j'),source.replace('    }\n    result += 10;','        result++;\n    }\n    result += 10;'),source.replace('int j = 0; j < 3; j++','int i = 0; i < 3; i++')]
        for text in bad:
            with self.subTest(text=text):
                with self.assertRaisesRegex(LoopModelError,'outer body|distinct induction'):build(text=text)
    def test_15_both_induction_variables_cannot_mutate_shadow_or_escape(self):
        for variable in ['i','j']:
            for statement in [variable+'++;','++'+variable+';','('+variable+') += 1;','int '+variable+';','int *'+variable+';','int unused, '+variable+';','counter_type '+variable+';','const counter_type '+variable+';','int const '+variable+';','struct item *'+variable+';','escape(&('+variable+'));']:
                with self.subTest(statement=statement):
                    with self.assertRaisesRegex(LoopModelError,'induction variable'):build(text=SOURCE.read_text().replace('result += i + j;',statement))
    def test_16_unsupported_syntax_rejected(self):
        source=SOURCE.read_text();bad=[source.replace('j < 3',s) for s in ['j <= 3','j < limit','j < 1+2','j < -1']]+[source.replace('j++',s) for s in ['++j','j += 1']]+[source.replace('j = 0','j = 1'),source.replace('j++) {','j++)'),source.replace('            result += i + j;','            for (int k=0;k<1;k++) {\n                result += k;\n            }')]
        bad += [source.replace('result += i + j;',s) for s in ['break;','continue;','return 0;','goto done;','if (i) { result++; }','pthread_mutex_lock(&m);','pthread_create(&t,0,f,0);','pthread_join(t,0);','sem_wait(&s);',';']]
        bad += [source.replace('    result += 10;','    for (int k=0;k<1;k++) {\n        result += k;\n    }\n    result += 10;'),source.replace('for (int j = 0; j < 3; j++)','while (1)'),source.replace('for (int j = 0; j < 3; j++)','do'), '#define ACTIVE 1\n'+source]
        for text in bad:
            with self.subTest(text=text):
                with self.assertRaises(LoopModelError):build(text=text)
    def test_17_loop_crossing_existing_segments_rejected(self):
        text,s,d=fixture();s['segments'][0]['end_line']=5
        with self.assertRaisesRegex(LoopModelError,'one existing compute Level-2 segment'):build_loop_nested_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
    def test_18_raw_delegation_for_no_single_and_sequential_loop(self):
        cases=[single_fixture(),pair_fixture()[:3],fixture(text='int main(void) {\n    int result=1;\n    return 0;\n}\n')]
        for text,s,d in cases:
            kw=dict(base_name='compat',source_text=text,base_segments_json=s,base_dag_json=d)
            self.assertEqual(json.dumps(build_loop_pair_views(**kw)),json.dumps(build_loop_nested_views(**kw)))
        self.assertEqual('effective_line_merge',default_rule_for_level('level2'))
    def test_19_rule_delegates_RuleOutput_without_changes(self):
        for text in [single_fixture()[0],pair_fixture()[0],'int main(void) {\n    int x=1;\n}\n']:
            sentinel=object()
            with tempfile.TemporaryDirectory() as temp:
                p=Path(temp)/'source.c';p.write_text(text);kw=dict(base_dir=Path(temp),base_name='x',source_file=p,block_info={})
                with patch.object(LoopStaticPairRule,'build',return_value=sentinel) as delegate:self.assertIs(sentinel,LoopStaticNestedRule().build(**kw));delegate.assert_called_once_with(**kw)
    def test_20_reject_blocks_without_partial_outputs_then_legal_recovery(self):
        with tempfile.TemporaryDirectory() as temp:
            base=Path(temp);pipeline=base/'intermediate_results/x/pipeline';pipeline.mkdir(parents=True);(pipeline/'block_info.json').write_text('{}');p=base/'source.c';text,s,d=fixture();s['source_file']=str(p);kw=dict(base_dir=base,base_name='x',level='level2',rule_name='loop_static_nested',source_file=p)
            p.write_text(text.replace('j < 3','j <= 3'))
            with self.assertRaises(LoopModelError):run_blocks(**kw)
            self.assertFalse((pipeline/'blocks').exists());p.write_text(text)
            with patch('Code2DAG.pipeline.rules.loop_nested_rule.build_level2_segments_and_dag',return_value=(s,d)):run_blocks(**kw)
            previous={str(f.relative_to(pipeline)):f.read_bytes() for f in (pipeline/'blocks').rglob('*') if f.is_file()}
            p.write_text(text.replace('j < 3','j < limit'))
            with self.assertRaises(LoopModelError):run_blocks(**kw)
            self.assertEqual(previous,{str(f.relative_to(pipeline)):f.read_bytes() for f in (pipeline/'blocks').rglob('*') if f.is_file()})
    def test_21_input_objects_and_C_bytes_unchanged(self):
        text,s,d=fixture();frozen=copy.deepcopy((text,s,d));digest=hashlib.sha256(SOURCE.read_bytes()).hexdigest();build_loop_nested_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d);self.assertEqual(frozen,(text,s,d));self.assertEqual(digest,hashlib.sha256(SOURCE.read_bytes()).hexdigest())

from Code2DAG.pipeline.loop_model import copy_loop_timing
from Code2DAG.pipeline.loop_nested_timing import _validate_nested_segments, import_loop_nested_timing


def corrupted(case):
    payload=copy.deepcopy(build()[2]);outer,inner=payload['loops'];leaves=[r for r in payload['segments'] if r['role']=='iteration'];row=leaves[0]
    if case=='duplicate_coordinate':leaves[1]['outer_iteration_index']=0;leaves[1]['inner_iteration_index']=0
    elif case=='missing_coordinate':payload['segments'].remove(row)
    elif case=='duplicate_node':leaves[1]['seg_id']=row['seg_id']
    elif case=='missing_outer_index':row.pop('outer_iteration_index')
    elif case=='negative_index':row['inner_iteration_index']=-1
    elif case=='large_index':row['outer_iteration_index']=2
    elif case=='bool_index':row['inner_iteration_index']=True
    elif case=='wrong_iteration_index':row['iteration_index']=1
    elif case=='wrong_node_coordinate':row['seg_id']+='wrong'
    elif case=='wrong_parent':row['parent_loop_id']=inner['loop_id']
    elif case=='cross_template':row['template_id']=outer['loop_id']+':before'
    elif case=='cross_loop':row['loop_id']=outer['loop_id']
    elif case=='unknown_role':row['role']='outer_work'
    elif case=='empty_template':row['template_id']=''
    elif case=='missing_template':row['template_id']='unknown'
    elif case=='row_range':row['start_line']-=1
    elif case=='template_range':next(t for t in payload['templates'] if t['role']=='body_template')['start_line']-=1
    elif case=='duplicate_template':payload['templates'].append(copy.deepcopy(payload['templates'][0]))
    elif case=='duplicate_loop':inner['loop_id']=outer['loop_id']
    elif case=='missing_before':payload['segments'].pop(0)
    elif case=='body_mapping_last_outer':payload['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:5']=[r['seg_id'] for r in leaves[3:]]
    elif case=='whole_owner_mapping_last_outer':payload['mappings']['original_segment_to_execution_segments']['OLD']=[r['seg_id'] for r in leaves[3:]]
    elif case=='outer_body_template':payload['templates'].append(dict(template_id=outer['loop_id']+':body_template',loop_id=outer['loop_id'],role='body_template',start_line=4,end_line=6))
    else:raise AssertionError(case)
    return payload


class LoopL3TimingTests(unittest.TestCase):
    def test_22_exact_copy_all_counts_all_fields_and_independent_dicts(self):
        for n1,n2 in [(2,3),(1,1),(0,3),(2,0),(0,0)]:
            payload=build(n1,n2)[2];data=metrics(payload);before=copy.deepcopy((payload,data));_validate_nested_segments(payload);result=copy_loop_timing(payload,data)
            for row in payload['segments']:self.assertEqual(data['weights'][row['template_id']],result['weights'][row['seg_id']])
            self.assertEqual(10+n1*n2*20+30,sum(m['avg_ns'] for m in result['weights'].values()));self.assertEqual(before,(payload,data))
            leaves=[r['seg_id'] for r in payload['segments'] if r['role']=='iteration']
            if len(leaves)>1:
                result['weights'][leaves[0]]['avg_ns']=999;self.assertEqual(20,result['weights'][leaves[-1]]['avg_ns']);self.assertEqual(before,(payload,data))
    def test_23_five_algorithms_real_compute_including_empty_graphs(self):
        for n1,n2,prefix,suffix in [(2,3,True,True),(1,1,False,False),(0,3,False,False),(2,0,False,False),(0,0,False,False),(0,0,True,True)]:
            st,sd,s,d,_=build(n1,n2,prefix,suffix);_validate_nested_segments(s);timing=copy_loop_timing(s,metrics(s))
            for name in ALGOS:
                with self.subTest(n1=n1,n2=n2,prefix=prefix,suffix=suffix,algo=name):
                    algorithm=get_algo(name);result=algorithm.compute(dag_json=d,segments_json=s,timing_json=timing);algorithm.validate(result);self.assertEqual(set(d['nodes']),set(result['priorities']))
    def rejects(self,cases,reason):
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaisesRegex(LoopModelError,reason):_validate_nested_segments(corrupted(case))
    def test_24_duplicate_coordinate(self):self.rejects(['duplicate_coordinate'],'duplicate two-dimensional coordinate')
    def test_25_missing_coordinate(self):self.rejects(['missing_coordinate'],'incomplete Cartesian product')
    def test_26_duplicate_node(self):self.rejects(['duplicate_node'],'duplicate node ID')
    def test_27_missing_negative_large_bool_indices(self):self.rejects(['missing_outer_index','negative_index','large_index','bool_index'],'two-dimensional index')
    def test_28_iteration_index_and_node_identity(self):self.rejects(['wrong_iteration_index','wrong_node_coordinate'],'iteration_index|node identity')
    def test_29_cross_layer_role_template_parent_and_loop(self):self.rejects(['cross_template','cross_loop','wrong_parent','unknown_role','empty_template','missing_template'],'cross-layer|unknown role')
    def test_30_source_range_conflicts(self):self.rejects(['row_range','template_range'],'source range')
    def test_31_duplicate_templates_and_loop_metadata(self):self.rejects(['duplicate_template','duplicate_loop'],'duplicate template_id|duplicate loop_id')
    def test_32_source_and_owner_mappings_cannot_lose_outer_iterations(self):self.rejects(['body_mapping_last_outer','whole_owner_mapping_last_outer'],'mapping conflicts')
    def test_33_prefix_and_outer_template_rejected(self):self.rejects(['missing_before','outer_body_template'],'missing or repeated before|cross-layer')
    def setup_import(self,temp,payload=None):
        base=Path(temp);payload=copy.deepcopy(payload if payload is not None else build()[2]);payload['source_sha256']=hashlib.sha256(SOURCE.read_bytes()).hexdigest()
        blocks=base/'intermediate_results/loop_l3/pipeline/blocks/level2/loop_static_nested';blocks.mkdir(parents=True,exist_ok=True)
        sp=blocks/'segments.json';sp.write_text(json.dumps(payload));tp=base/'templates.json';tp.write_text(json.dumps(metrics(build()[2])))
        output=base/'intermediate_results/loop_l3/pipeline/timing/level2/loop_static_nested/timing.json'
        return base,payload,sp,tp,output
    def test_34_import_legal_with_no_input_mutation(self):
        with tempfile.TemporaryDirectory() as temp:
            base,payload,sp,tp,output=self.setup_import(temp);before=(sp.read_bytes(),tp.read_bytes(),SOURCE.read_bytes())
            result=import_loop_nested_timing(base_dir=base,base_name='loop_l3',input_timing=tp);self.assertTrue(output.exists());self.assertEqual(copy_loop_timing(payload,metrics(payload))['weights'],result['weights']);self.assertEqual(before,(sp.read_bytes(),tp.read_bytes(),SOURCE.read_bytes()))
    def test_35_all_binding_faults_preserve_valid_output_and_retry(self):
        cases=['duplicate_coordinate','missing_coordinate','duplicate_node','missing_outer_index','negative_index','large_index','bool_index','wrong_iteration_index','wrong_node_coordinate','wrong_parent','cross_template','cross_loop','unknown_role','empty_template','missing_template','row_range','template_range','duplicate_template','duplicate_loop','missing_before','body_mapping_last_outer','whole_owner_mapping_last_outer','outer_body_template']
        with tempfile.TemporaryDirectory() as temp:
            base,payload,sp,tp,output=self.setup_import(temp);legal=sp.read_bytes();import_loop_nested_timing(base_dir=base,base_name='loop_l3',input_timing=tp);previous=output.read_bytes()
            for case in cases:
                broken=corrupted(case);broken['source_sha256']=payload['source_sha256'];sp.write_text(json.dumps(broken));input_before=sp.read_bytes()
                with self.subTest(case=case):
                    with self.assertRaises(LoopModelError):import_loop_nested_timing(base_dir=base,base_name='loop_l3',input_timing=tp)
                    self.assertEqual(previous,output.read_bytes());self.assertEqual(input_before,sp.read_bytes())
            sp.write_bytes(legal);import_loop_nested_timing(base_dir=base,base_name='loop_l3',input_timing=tp);self.assertEqual(previous,output.read_bytes())
    def test_36_reject_before_copy_no_half_output_then_recover(self):
        with tempfile.TemporaryDirectory() as temp:
            base,payload,sp,tp,output=self.setup_import(temp,corrupted('cross_template'))
            with patch('Code2DAG.pipeline.loop_nested_timing.copy_loop_timing',side_effect=AssertionError('copied invalid binding')) as copy_fn:
                with self.assertRaisesRegex(LoopModelError,'cross-layer'):import_loop_nested_timing(base_dir=base,base_name='loop_l3',input_timing=tp)
                copy_fn.assert_not_called()
            self.assertFalse(output.parent.exists());payload=build()[2];payload['source_sha256']=hashlib.sha256(SOURCE.read_bytes()).hexdigest();sp.write_text(json.dumps(payload));import_loop_nested_timing(base_dir=base,base_name='loop_l3',input_timing=tp);self.assertTrue(output.exists())
    def test_37_duplicate_JSON_same_conflicting_nested_and_segments(self):
        with tempfile.TemporaryDirectory() as temp:
            base,payload,sp,tp,output=self.setup_import(temp);legal=json.dumps(metrics(payload));key=payload['templates'][0]['template_id']
            bad=['{"weights":{'+json.dumps(key)+':{"avg_ns":10},'+json.dumps(key)+':{"avg_ns":20}}}', '{"weights":{'+json.dumps(key)+':{"avg_ns":10},'+json.dumps(key)+':{"avg_ns":10}}}', '{"weights":{'+json.dumps(key)+':{"avg_ns":10,"avg_ns":10}}}']
            for raw in bad:
                tp.write_text(raw)
                with self.assertRaisesRegex(LoopModelError,'duplicate key'):import_loop_nested_timing(base_dir=base,base_name='loop_l3',input_timing=tp)
                self.assertFalse(output.exists())
            tp.write_text(legal);sp.write_text('{"rule_name":"loop_static_nested","rule_name":"loop_static_nested"}')
            with self.assertRaisesRegex(LoopModelError,'duplicate key: rule_name'):import_loop_nested_timing(base_dir=base,base_name='loop_l3',input_timing=tp)
            self.assertFalse(output.exists())
    def test_38_missing_whole_and_invalid_metric_reject_without_publication(self):
        with tempfile.TemporaryDirectory() as temp:
            base,payload,sp,tp,output=self.setup_import(temp);good=metrics(payload);missing=copy.deepcopy(good);missing['weights'].pop(payload['templates'][1]['template_id']);whole=copy.deepcopy(good);whole['weights']['OLD']={'avg_ns':999};invalid=copy.deepcopy(good);invalid['weights'][payload['templates'][1]['template_id']]['avg_ns']=-1
            for data,reason in [(missing,'missing template'),(whole,'whole-segment timing'),(invalid,'invalid template metric')]:
                tp.write_text(json.dumps(data))
                with self.assertRaisesRegex(LoopModelError,reason):import_loop_nested_timing(base_dir=base,base_name='loop_l3',input_timing=tp)
                self.assertFalse(output.exists())
    def test_39_zero_product_still_requires_body_input_without_leaf_weights(self):
        for n1,n2 in [(0,3),(2,0),(0,0)]:
            payload=build(n1,n2,False,False)[2];_validate_nested_segments(payload)
            with self.assertRaisesRegex(LoopModelError,'missing template'):copy_loop_timing(payload,{'weights':{}})
            self.assertEqual({},copy_loop_timing(payload,metrics(payload))['weights']);self.assertEqual(1,len(payload['templates']))
    def test_40_fake_zero_iteration_node_rejected(self):
        payload=copy.deepcopy(build(0,3)[2]);payload['segments'].insert(1,copy.deepcopy(build()[2]['segments'][1]))
        with self.assertRaisesRegex(LoopModelError,'out-of-range'): _validate_nested_segments(payload)
    def test_41_source_hash_change_refuses_to_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            base,payload,sp,tp,output=self.setup_import(temp);import_loop_nested_timing(base_dir=base,base_name='loop_l3',input_timing=tp);previous=output.read_bytes();payload['source_sha256']='0'*64;sp.write_text(json.dumps(payload))
            with self.assertRaisesRegex(LoopModelError,'source changed'):import_loop_nested_timing(base_dir=base,base_name='loop_l3',input_timing=tp)
            self.assertEqual(previous,output.read_bytes())
