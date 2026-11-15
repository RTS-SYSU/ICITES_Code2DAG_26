"""Two sequential static loops only; synthetic timing, no runtime rewriting."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from Code2DAG.pipeline.loop_pair_model import analyze_loop_pair,build_loop_pair_views
from Code2DAG.pipeline.loop_model import build_loop_views,copy_loop_timing,LoopModelError
from Code2DAG.pipeline.loop_timing import _validate_loop_segments
from Code2DAG.pipeline.loop_pair_timing import import_loop_pair_timing
from Code2DAG.pipeline.rules.loop_pair_rule import LoopStaticPairRule
from Code2DAG.pipeline.rules.loop_rule import LoopStaticRule
from Code2DAG.pipeline.runner import run_blocks,default_rule_for_level
from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.tests.test_loop_l1 import fixture as single_fixture

ROOT=Path(__file__).resolve().parents[2];SOURCE=ROOT/'Code2DAG/source_files/loop_l2/loop_l2.c';ALGOS=['FIFO','LPF','heft','t_level','zhao2020']

def line(text,token):return next(i for i,l in enumerate(text.splitlines(),1) if token in l)

def fixture(n1=2,n2=3,text=None):
    text=text if text is not None else SOURCE.read_text().replace('i < 2','i < '+str(n1)).replace('j < 3','j < '+str(n2))
    markers=dict(pre=line(text,'result = 1;'),h1=line(text,'for (int i'),b1=line(text,'step1(i);' if 'step1(i);' in text else 'result += i + 1;'),middle=line(text,'result += 10;'),h2=line(text,'for (int j'),b2=line(text,'step2(j);' if 'step2(j);' in text else 'result += j + 2;'),post=line(text,'result += 100;'),last=line(text,'printf('))
    s=dict(base_name='loop_l2',source_file=str(SOURCE),segments=[dict(seg_id='OLD',function='main',kind='compute',start_line=markers['pre'],end_line=markers['last'])],node_locations={'main/printf1':dict(function='main',line=markers['last'])},node_to_segment={'main':'OLD','main/printf1':'OLD'})
    return text,s,dict(nodes=['OLD'],edges=[]),markers


def build(n1=2,n2=3):
    text,s,d,m=fixture(n1,n2)
    return build_loop_pair_views(base_name='loop_l2',source_text=text,base_segments_json=s,base_dag_json=d)


def weights(s):
    roles={}
    for i,loop in enumerate(s['loops']):
        for role,value in [('before',3),('body_template',11 if i==0 else 23),('after',7 if i==0 else 31)]:roles[loop['loop_id']+':'+role]=value
    return {'weights':{t['template_id']:dict(total_ns=roles[t['template_id']],count=1,avg_ns=roles[t['template_id']],min_ns=roles[t['template_id']],max_ns=roles[t['template_id']]) for t in s['templates']}}


class LoopL2Tests(unittest.TestCase):
    def check_counts(self,n1,n2):
        st,sd,s,d,meta=build(n1,n2);a,b=[l['loop_id'] for l in s['loops']]
        expected=[a+':before',*[a+':iteration:'+str(i) for i in range(n1)],a+':after',*[b+':iteration:'+str(i) for i in range(n2)],b+':after']
        controls=[a+':before',a+':entry',*[a+':iteration:'+str(i) for i in range(n1)],a+':exit',a+':after',b+':entry',*[b+':iteration:'+str(i) for i in range(n2)],b+':exit',b+':after']
        self.assertEqual(expected,d['nodes']);self.assertEqual(controls,sd['nodes'])
        self.assertEqual(set(zip(expected,expected[1:])),{(e['src'],e['dst']) for e in d['edges']});self.assertEqual(len(expected)-1,len(d['edges']))
        self.assertEqual(set(zip(controls,controls[1:])),{(e['src'],e['dst']) for e in sd['edges']})
        if n1==0:self.assertIn((a+':entry',a+':exit'),{(e['src'],e['dst']) for e in sd['edges']})
        if n2==0:self.assertIn((b+':entry',b+':exit'),{(e['src'],e['dst']) for e in sd['edges']})
        self.assertEqual(3+n1+n2,len(s['segments']));_validate_loop_segments(s)
        return st,sd,s,d

    def test_01_counts_two_and_three(self):self.check_counts(2,3)
    def test_02_counts_zero_and_three(self):self.check_counts(0,3)
    def test_03_counts_two_and_zero(self):self.check_counts(2,0)
    def test_04_counts_both_zero(self):self.check_counts(0,0)

    def test_05_unique_independent_ids_variables_and_indices(self):
        st,sd,s,d,_=build();self.assertEqual(['i','j'],[l['variable'] for l in s['loops']])
        self.assertEqual(2,len({l['loop_id'] for l in s['loops']}));self.assertEqual(5,len({t['template_id'] for t in s['templates']}));self.assertEqual(len(sd['nodes']),len(set(sd['nodes'])))
        for loop,n in zip(s['loops'],[2,3]):self.assertEqual(list(range(n)),[r['iteration_index'] for r in s['segments'] if r.get('loop_id')==loop['loop_id'] and r['role']=='iteration'])

    def test_06_middle_before_after_ranges_once_same_old_segment(self):
        st,sd,s,d,_=build();self.assertEqual([(4,4),(6,6),(6,6),(8,8),(13,13),(13,13),(13,13),(15,16)],[(r['start_line'],r['end_line']) for r in s['segments']])
        mid=[r for r in s['segments'] if r['start_line']<=8<=r['end_line']];self.assertEqual(1,len(mid));self.assertEqual('after',mid[0]['role']);self.assertEqual(s['loops'][0]['loop_id'],mid[0]['loop_id'])
        self.assertFalse(any(r.get('role')=='before' and r.get('loop_id')==s['loops'][1]['loop_id'] for r in s['segments']))
        self.assertEqual(d['nodes'],s['mappings']['original_segment_to_execution_segments']['OLD'])

    def test_07_three_mapping_namespaces_no_cross_loop_or_fake_origin(self):
        st,sd,s,d,_=build();a,b=[l['loop_id'] for l in s['loops']];m=s['mappings']
        self.assertEqual([a+':iteration:0',a+':iteration:1'],m['source_position_to_execution_segments']['LOOPSRC:main:line:6'])
        self.assertEqual([b+':iteration:0',b+':iteration:1',b+':iteration:2'],m['source_position_to_execution_segments']['LOOPSRC:main:line:13'])
        self.assertEqual([a+':after'],m['source_position_to_execution_segments']['LOOPSRC:main:line:8'])
        self.assertEqual([b+':after'],m['generation_node_to_execution_segments']['main/printf1']);self.assertEqual(d['nodes'],m['generation_node_to_execution_segments']['main'])
        self.assertTrue(all(r['generation_origin'] is None for r in s['segments'] if r['role']=='iteration'))
        self.assertTrue(all(not n.startswith('LOOPSRC:') for n in m['generation_node_to_execution_segments']))
        self.assertEqual(len(d['nodes']),len(set(m['original_segment_to_execution_segments']['OLD'])))

    def test_08_generation_calls_in_each_body_are_not_mixed(self):
        text=SOURCE.read_text().replace('result += i + 1;','step1(i);').replace('result += j + 2;','step2(j);');text,s,d,loc=fixture(text=text)
        for k in [1,2]:s['node_locations']['main/step'+str(k)]=dict(function='main',line=loc['b'+str(k)]);s['node_to_segment']['main/step'+str(k)]='OLD'
        result=build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)[2]
        for k,n in [(1,2),(2,3)]:
            lid=result['loops'][k-1]['loop_id'];self.assertEqual([lid+':iteration:'+str(i) for i in range(n)],result['mappings']['generation_node_to_execution_segments']['main/step'+str(k)])

    def test_09_multiline_both_headers_and_following_statements(self):
        text=SOURCE.read_text().replace('for (int i = 0; i < 2; i++) {','for (int i = 0;\n         i < 2;\n         i++)\n    {');text,s,d,m=fixture(text=text)
        st,sd,ss,dd,_=build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual([m['h1'],m['h2']],[l['header_line'] for l in ss['loops']]);self.assertEqual([m['b1'],m['b2']],[l['body_start'] for l in ss['loops']])
        self.assertEqual(1,len([r for r in ss['segments'] if r['start_line']<=m['middle']<=r['end_line']]))
        self.assertEqual((m['post'],m['last']),(ss['segments'][-1]['start_line'],ss['segments'][-1]['end_line']))

    def test_10_timing_exact_per_loop_copy_and_no_aliasing_all_counts(self):
        for n1,n2 in [(2,3),(0,3),(2,0),(0,0)]:
            s=build(n1,n2)[2];data=weights(s);before=copy.deepcopy(data);_validate_loop_segments(s);r=copy_loop_timing(s,data)
            for row in s['segments']:self.assertEqual(data['weights'][row['template_id']],r['weights'][row['seg_id']])
            self.assertEqual(3+n1*11+7+n2*23+31,sum(v['avg_ns'] for v in r['weights'].values()));self.assertEqual(before,data)
            if n1 and n2:
                a,b=[l['loop_id'] for l in s['loops']];r['weights'][a+':iteration:0']['avg_ns']=999
                self.assertEqual(23,r['weights'][b+':iteration:0']['avg_ns']);self.assertEqual(before,data)

    def test_11_cross_loop_binding_whole_weight_and_duplicate_JSON_rejected(self):
        s=build()[2];a,b=[l['loop_id'] for l in s['loops']];row=next(r for r in s['segments'] if r['role']=='iteration');row['template_id']=b+':body_template'
        with self.assertRaisesRegex(LoopModelError,'cross-loop'):_validate_loop_segments(s)
        with self.assertRaisesRegex(LoopModelError,'whole-segment timing'):copy_loop_timing(build()[2],{'weights':{'OLD':{'avg_ns':1}}})

    def test_12_L1_and_no_loop_raw_outputs_byte_compatible(self):
        text,s,d=single_fixture();args=dict(base_name='single',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual(json.dumps(build_loop_views(**args),sort_keys=True).encode(),json.dumps(build_loop_pair_views(**args),sort_keys=True).encode())
        plain='int main(void) {\n    int before = 1;\n}\n';args['source_text']=plain
        self.assertEqual(json.dumps(build_loop_views(**args),sort_keys=True).encode(),json.dumps(build_loop_pair_views(**args),sort_keys=True).encode())
        self.assertEqual('effective_line_merge',default_rule_for_level('level2'))

    def test_13_single_rule_delegates_exact_output(self):
        text,s,d=single_fixture()
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'single.c';p.write_text(text);s['source_file']=str(p);kwargs=dict(base_dir=Path(tmp),base_name='single',source_file=p,block_info={})
            with patch('Code2DAG.pipeline.rules.loop_rule.build_level2_segments_and_dag',return_value=(s,d)):
                old=LoopStaticRule().build(**kwargs);new=LoopStaticPairRule().build(**kwargs)
            self.assertEqual(json.dumps(old.__dict__,sort_keys=True).encode(),json.dumps(new.__dict__,sort_keys=True).encode())

    def test_14_reject_three_nested_and_other_unsupported_before_DAG_write(self):
        text=SOURCE.read_text();bad=[text.replace('    result += 100;','    for (int k = 0; k < 1; k++) {\n        result += k;\n    }\n    result += 100;'),text.replace('    }\n    result += 10;','    result += 10;',1).replace('    result += 100;','    }\n    result += 100;',1)]
        bad += [text.replace('j < 3',x) for x in ['j <= 3','j < limit','j < (1 + 2)']]
        bad += [text.replace('j++',x) for x in ['++j','j += 1']]
        bad += [text.replace('result += j + 2;',x) for x in ['break;','continue;','return 0;','goto done;','if (j) { result++; }','pthread_mutex_lock(&m);','pthread_create(&t,NULL,f,NULL);','pthread_join(t,NULL);']]
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);p=base/'bad.c';pipeline=base/'intermediate_results/bad/pipeline';pipeline.mkdir(parents=True);(pipeline/'block_info.json').write_text('{}')
            for source in bad:
                p.write_text(source)
                with self.assertRaises(LoopModelError):run_blocks(base_dir=base,base_name='bad',level='level2',rule_name='loop_static_pair',source_file=p)
                self.assertFalse((pipeline/'blocks').exists())

    def test_15_five_algorithms_all_count_pairs(self):
        for n1,n2 in [(2,3),(0,3),(2,0),(0,0)]:
            st,sd,s,d,_=build(n1,n2);data=copy_loop_timing(s,weights(s))
            for name in ALGOS:
                a=get_algo(name);result=a.compute(segments_json=s,dag_json=d,timing_json=data);a.validate(result);self.assertEqual(set(d['nodes']),set(result['priorities']))

    def test_16_distinct_existing_segments_preserve_original_order_and_mappings(self):
        text,s,d,m=fixture();s['segments']=[dict(seg_id='A',function='main',kind='compute',start_line=m['pre'],end_line=m['middle']),dict(seg_id='B',function='main',kind='compute',start_line=m['h2'],end_line=m['last'])];s['node_to_segment']={'main':'A','main/printf1':'B'};d={'nodes':['A','B'],'edges':[dict(src='A',dst='B',kind='intra',origin='original')]}
        st,sd,out,dd,_=build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d);self.assertEqual(8,len(dd['nodes']));self.assertEqual(4,len(out['mappings']['original_segment_to_execution_segments']['A']));self.assertEqual(4,len(out['mappings']['original_segment_to_execution_segments']['B']))
        self.assertEqual(1,len([r for r in out['segments'] if r['start_line']<=8<=r['end_line']]))
        self.assertTrue(any(e.get('origin')=='original' for e in sd['edges']))

    def test_17_external_dependency_types_and_source_bytes_unchanged(self):
        text,s,d,m=fixture();raw=SOURCE.read_bytes();s['segments'].insert(0,dict(seg_id='P',function='worker',kind='compute',start_line=1,end_line=1,custom='keep'));s['segments'].append(dict(seg_id='Q',function='worker',kind='compute',start_line=18,end_line=18))
        d={'nodes':['P','OLD','Q'],'edges':[dict(src='P',dst='OLD',kind=k,origin=k) for k in ['create','sem_dep']]+[dict(src='OLD',dst='Q',kind=k,origin=k) for k in ['join','intra']]};before=copy.deepcopy((s,d))
        st,sd,out,dd,_=build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d);self.assertEqual(before,(s,d));self.assertEqual(s['segments'][0],out['segments'][0]);self.assertEqual({'create','sem_dep','join','intra'},{e['kind'] for e in dd['edges'] if e.get('origin')})
        self.assertEqual(hashlib.sha256(raw).hexdigest(),hashlib.sha256(SOURCE.read_bytes()).hexdigest())

    def test_18_timing_directory_adapter_without_C_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);s=build()[2];s['source_sha256']=hashlib.sha256(SOURCE.read_bytes()).hexdigest();p=base/'intermediate_results/loop_l2/pipeline/blocks/level2/loop_static_pair';p.mkdir(parents=True);sp=p/'segments.json';sp.write_text(json.dumps(s));before=sp.read_bytes();data=weights(s);tp=base/'weights.json';tp.write_text(json.dumps(data));r=import_loop_pair_timing(base_dir=base,base_name='loop_l2',input_timing=tp)
            self.assertEqual(copy_loop_timing(s,data)['weights'],r['weights']);self.assertEqual(before,sp.read_bytes())
            old=(base/'intermediate_results/loop_l2/pipeline/timing/level2/loop_static_pair/timing.json').read_bytes();key=s['templates'][0]['template_id'];raw='{"weights":'+json.dumps(data['weights'])+',"weights":'+json.dumps(data['weights'])+'}';tp.write_text(raw)
            with self.assertRaisesRegex(LoopModelError,'duplicate key'):import_loop_pair_timing(base_dir=base,base_name='loop_l2',input_timing=tp)
            self.assertEqual(old,(base/'intermediate_results/loop_l2/pipeline/timing/level2/loop_static_pair/timing.json').read_bytes())

    def test_19_loops_in_different_functions_rejected(self):
        text='''int total;
void first(void) {
    total = 1;
    for (int i=0; i<2; i++) {
        total += i;
    }
}
void second(void) {
    for (int j=0; j<3; j++) {
        total += j;
    }
    total += 10;
}
'''
        s={'segments':[dict(seg_id='A',function='first',kind='compute',start_line=3,end_line=6),dict(seg_id='B',function='second',kind='compute',start_line=9,end_line=12)],'node_locations':{},'node_to_segment':{}}
        with self.assertRaisesRegex(LoopModelError,'same function'):build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json={'nodes':['A','B'],'edges':[]})

    def test_20_no_loop_rule_delegates_exact_output(self):
        text='int main(void) {\n    int before = 1;\n}\n';s={'segments':[dict(seg_id='P',function='main',kind='compute',start_line=2,end_line=2)]};d={'nodes':['P'],'edges':[]}
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'plain.c';p.write_text(text);kwargs=dict(base_dir=Path(tmp),base_name='plain',source_file=p,block_info={})
            with patch('Code2DAG.pipeline.rules.level2_effective_line_rule.segment_dag_level2.build_level2_segments_and_dag',return_value=(s,d)):
                old=LoopStaticRule().build(**kwargs);new=LoopStaticPairRule().build(**kwargs)
            self.assertEqual(json.dumps(old.__dict__,sort_keys=True).encode(),json.dumps(new.__dict__,sort_keys=True).encode())

    def test_21_zero_iteration_mappings_remain_empty_without_cross_group_fallback(self):
        for n1,n2 in [(0,3),(2,0),(0,0)]:
            text=SOURCE.read_text().replace('i < 2','i < '+str(n1)).replace('j < 3','j < '+str(n2)).replace('result += i + 1;','step1(i);').replace('result += j + 2;','step2(j);');text,s,d,m=fixture(text=text)
            for k in [1,2]:s['node_locations']['main/step'+str(k)]=dict(function='main',line=m['b'+str(k)]);s['node_to_segment']['main/step'+str(k)]='OLD'
            result=build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)[2]
            for k,n in [(1,n1),(2,n2)]:
                lid=result['loops'][k-1]['loop_id'];expected=[lid+':iteration:'+str(i) for i in range(n)]
                self.assertEqual(expected,result['mappings']['generation_node_to_execution_segments']['main/step'+str(k)])
                self.assertEqual(expected,result['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:'+str(m['b'+str(k)])])

    def test_22_existing_middle_segment_is_retained_once_and_traceable(self):
        text,s,d,m=fixture();s['segments']=[dict(seg_id='A',function='main',kind='compute',start_line=4,end_line=7),dict(seg_id='MID',function='main',kind='compute',start_line=8,end_line=8),dict(seg_id='B',function='main',kind='compute',start_line=9,end_line=16)];s['node_to_segment']={'main':'A','main/printf1':'B'};d={'nodes':['A','MID','B'],'edges':[dict(src='A',dst='MID',kind='intra'),dict(src='MID',dst='B',kind='intra')]}
        st,sd,out,dd,_=build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual(8,len(dd['nodes']));self.assertEqual(1,dd['nodes'].count('MID'));self.assertEqual(['MID'],out['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:8']);self.assertEqual(['MID'],out['mappings']['original_segment_to_execution_segments']['MID'])
        self.assertEqual(s['segments'][1],next(r for r in out['segments'] if r['seg_id']=='MID'))
        data=weights(out);data['weights']['MID']=dict(total_ns=7,count=1,avg_ns=7,min_ns=7,max_ns=7);_validate_loop_segments(out);result=copy_loop_timing(out,data)
        self.assertEqual(3+2*11+7+3*23+31,sum(v['avg_ns'] for v in result['weights'].values()))
