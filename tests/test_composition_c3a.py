"""Leaf-local Loop expansion and exact branch-specific timing binding."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.tests import test_composition_c2a as c2a
from Code2DAG.pipeline.composition_c3a_model import build_branch_loop_views,RULE
from Code2DAG.pipeline.composition_c3a_timing import import_branch_loop_timing
from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort
ROOT=Path(__file__).resolve().parents[2];SOURCES=ROOT/'Code2DAG/source_files/composition_c3a'
EXPECTED={'then':(3,[3,20]),'else':(3,[10,3]),'both':(6,[2,8]),'no_else_zero':(0,[0,0]),'no_else_one':(1,[1,0]),'no_else_many':(3,[3,0]),'else_if':(5,[3,2,30]),'nested_then':(3,[3,7,20]),'nested_else':(3,[10,3,7]),'l2':(5,[13,20]),'l3':(6,[6,20]),'outside_lock':(3,[10,27])}


def fixture(name,text=None):
    source=SOURCES/(name+'.c');text=source.read_text() if text is None else text
    # Shared fixture is only an input segment builder, never the expected oracle.
    if 'pthread_mutex_lock(' in text:raw,dag=c2a.fixture(text,source)
    else:
        lines=text.splitlines();a=next(i for i,l in enumerate(lines,1) if l.startswith('void *worker'))+1;b=next(i for i in range(a,len(lines)) if lines[i-1]=='}')-1
        create=next(i for i,l in enumerate(lines,1) if 'pthread_create(' in l);join=next(i for i,l in enumerate(lines,1) if 'pthread_join(' in l)
        rows=[dict(seg_id='OLD',function='worker',kind='compute',start_line=a,end_line=b),dict(seg_id='CREATE',function='main',kind='compute',start_line=create-2,end_line=create),dict(seg_id='JOIN',function='main',kind='compute',start_line=join,end_line=len(lines)-1)]
        raw=dict(base_name='fixture',source_file=str(source),segments=rows,thread_entry_functions=['worker'],node_locations={'main/pthread_create1':dict(function='main',line=create),'main/pthread_join1':dict(function='main',line=join)},node_to_segment={'main/pthread_create1':'CREATE','main/pthread_join1':'JOIN'})
        dag=dict(nodes=[r['seg_id'] for r in rows],edges=[dict(src='CREATE',dst='OLD',kind='create',origin={'id':1}),dict(src='OLD',dst='JOIN',kind='join',origin={'id':2}),dict(src='CREATE',dst='JOIN',kind='intra')])
    return text,raw,dag


class CompositionC3aTests(unittest.TestCase):
    def test_counts_memberships_namespace_zero_and_mapping(self):
        for name,(count,values) in EXPECTED.items():
            with self.subTest(name=name):
                text,raw,dag=fixture(name);frozen=copy.deepcopy((raw,dag))
                st,sd,s,d,_=build_branch_loop_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
                self.assertEqual(frozen,(raw,dag));iterations=[r for r in s['segments'] if r.get('role')=='iteration']
                self.assertEqual(count,len(iterations));self.assertTrue(all(r['condition_memberships'] for r in iterations))
                if name.startswith('nested'):self.assertTrue(all(len(r['condition_memberships'])==2 for r in iterations))
                for a in iterations:
                    for b in iterations:
                        ma={m['exclusive_group']:m['branch_id'] for m in a['condition_memberships']};mb={m['exclusive_group']:m['branch_id'] for m in b['condition_memberships']}
                        if any(ma[k]!=mb[k] for k in ma.keys()&mb.keys()):self.assertFalse(c1.reachable(d,a['seg_id'],b['seg_id']))
                self.assertEqual(len(s['templates']),len({r['template_id'] for r in s['templates']}))
                for graph in [sd,d]:self.assertEqual(len(graph['nodes']),len(set(graph['nodes'])));topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
                for i,line in enumerate(text.splitlines(),1):
                    if '/* BODY_' in line:
                        mapped=s['mappings']['source_position_to_execution_segments']['LOOPSRC:worker:line:'+str(i)]
                        expected=[r['seg_id'] for r in iterations if r['start_line']<=i<=r['end_line']]
                        self.assertEqual(expected,mapped)

    def test_native_all_paths_timing_and_five_algorithms(self):
        import re
        from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
        from Code2DAG.pipeline.composition_model import CONTROL
        for name,(count,business) in EXPECTED.items():
            source=SOURCES/(name+'.c');before=source.read_bytes();text=source.read_text();lines=text.splitlines();h=c1.CompositionC1Tests()
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                base,p=h.native(tmp,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source)
                b=p/'blocks/level2'/RULE;s=json.loads((b/'segments.json').read_text());st=json.loads((b/'segments_struct.json').read_text());d=json.loads((b/'dag_seg.json').read_text());sd=json.loads((b/'dag_struct.json').read_text())
                raw,dag=build_level2_segments_and_dag(base_dir=base,base_name=name,source_file=source)
                self.assertEqual([e['edge'] for e in s['composition_edge_provenance']],dag['edges'])
                iterations=[r for r in s['segments'] if r.get('role')=='iteration'];self.assertEqual(count,len(iterations));self.assertFalse(any(r['kind'] in CONTROL for r in s['segments']))
                for graph in [sd,d]:
                    self.assertTrue(all(e['src']!=e['dst'] for e in graph['edges']));self.assertEqual(len(graph['nodes']),len(set(graph['nodes'])));topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
                for lid in {r['loop_id'] for r in iterations}:
                    rows=[r for r in iterations if r['loop_id']==lid]
                    if name!='l3':self.assertEqual(list(range(len(rows))),[r['iteration_index'] for r in rows])
                    for a,c in zip(rows,rows[1:]):self.assertTrue(c1.reachable(d,a['seg_id'],c['seg_id']))
                for i,line in enumerate(lines,1):
                    if '/* BODY_' in line:
                        targets=s['mappings']['source_position_to_execution_segments']['LOOPSRC:worker:line:'+str(i)]
                        self.assertEqual([r['seg_id'] for r in iterations if r['start_line']<=i<=r['end_line']],targets)
                weights={}
                for template in s['templates']:
                    value=sum(int(m.group(1)) for line in lines[template['start_line']-1:template['end_line']] if (m:=re.search(r'result\s*\+=\s*(\d+)\s*;',line)))
                    weights[template['template_id']]=dict(total_ns=value,count=1,avg_ns=value,min_ns=value,max_ns=value)
                inp=Path(tmp)/'weights.json';inp.write_text(json.dumps({'weights':weights}));input_before=inp.read_bytes()
                timing=import_branch_loop_timing(base_dir=base,base_name=name,input_timing=inp)
                for r in s['segments']:
                    expected=({field:sum(weights[m['execution_segment']['template_id']][field] for m in r['members']) for field in ['total_ns','count','avg_ns','min_ns','max_ns']} if r['kind']=='lock_group' else weights[r['template_id']])
                    self.assertEqual(expected,timing['weights'][r['seg_id']])
                for algo in c1.ALGOS:
                    scheduled=run_schedule(base_dir=base,base_name=name,level='level2',rule_name=RULE,algo_name=algo);self.assertEqual(set(d['nodes']),set(scheduled['priorities']))
                binary=Path(tmp)/'business';h.command(['gcc-13','-pthread','-std=c11',source,'-o',binary])
                for mode,value in enumerate(business):self.assertEqual('RESULT:'+str(1100+value)+'\n',h.command([binary,*(['x']*mode)]).stdout)
                output=p/'timing/level2'/RULE/'timing.json';saved=output.read_bytes();sp=b/'segments.json';valid=sp.read_bytes()
                for damage in ['branch','template','iteration','mapping']:
                    bad=json.loads(valid)
                    if iterations:
                        row=next(r for r in bad['segments'] if r.get('role')=='iteration')
                        if damage=='branch':row['condition_memberships'][0]['branch_id']='wrong'
                        elif damage=='template':row['template_id']='other-branch'
                        elif damage=='iteration':row['iteration_index']=True
                        else:bad['mappings']['generation_node_to_execution_segments']['forged']=['unknown']
                    else:bad['loops'][0]['iteration_count']=True
                    sp.write_text(json.dumps(bad))
                    with self.assertRaises(ValueError):import_branch_loop_timing(base_dir=base,base_name=name,input_timing=inp)
                    self.assertEqual(saved,output.read_bytes())
                sp.write_bytes(valid);self.assertEqual(timing,import_branch_loop_timing(base_dir=base,base_name=name,input_timing=inp));self.assertEqual(input_before,inp.read_bytes())
            self.assertEqual(before,source.read_bytes())

    def test_invalid_source_and_failed_blocks_have_no_partial_view(self):
        from Code2DAG.pipeline.runner import run_blocks
        from Code2DAG.pipeline.composition_model import CompositionError
        original=(SOURCES/'then.c').read_text()
        # Reverse nesting: an existing Condition lies inside a for body.
        before='    if (mode == 0) {'
        reverse=original.replace(before,'    for (int k = 0; k < 2; k++) {\n'+before).replace('    result += 1000;','    }\n    result += 1000;')
        dynamic=original.replace('i < 3','i < mode')
        crossing=original.replace('        }\n    } else {','    } else {',1)
        for text in [reverse,dynamic,crossing]:
            with self.subTest(text=text):
                _,raw,dag=fixture('then',text)
                with self.assertRaises(ValueError):build_branch_loop_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
        h=c1.CompositionC1Tests()
        for text in [reverse,dynamic]:
            with self.subTest(native=text),tempfile.TemporaryDirectory() as tmp:
                src=Path(tmp)/'rejected.c';src.write_text(text);before=src.read_bytes();base,p=h.native(tmp,src)
                with self.assertRaises(ValueError):run_blocks(base_dir=base,base_name='rejected',level='level2',rule_name=RULE,source_file=src)
                self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists());self.assertEqual(before,src.read_bytes())

    def test_fresh_timing_rejection_and_legal_recovery(self):
        source=SOURCES/'both.c';h=c1.CompositionC1Tests()
        with tempfile.TemporaryDirectory() as tmp:
            base,p=h.native(tmp,source);run_composition_blocks(base_dir=base,base_name='both',source_file=source)
            b=p/'blocks/level2'/RULE;sp=b/'segments.json';valid=sp.read_bytes();s=json.loads(valid)
            weights={t['template_id']:dict(total_ns=10,count=1,avg_ns=10,min_ns=10,max_ns=10) for t in s['templates']};inp=Path(tmp)/'weights.json';inp.write_text(json.dumps({'weights':weights}));good_input=inp.read_bytes();out=p/'timing/level2'/RULE/'timing.json'
            bad=copy.deepcopy(s);rows=[r for r in bad['segments'] if r.get('role')=='iteration'];rows[0]['template_id']=rows[-1]['template_id'];sp.write_text(json.dumps(bad));damaged=sp.read_bytes()
            with self.assertRaises(ValueError):import_branch_loop_timing(base_dir=base,base_name='both',input_timing=inp)
            self.assertFalse(out.exists());self.assertEqual(damaged,sp.read_bytes());self.assertEqual(good_input,inp.read_bytes())
            sp.write_bytes(valid);tim=import_branch_loop_timing(base_dir=base,base_name='both',input_timing=inp);saved=out.read_bytes()
            for data in [{'weights':{}},{'weights':{**weights,'unselected-node':next(iter(weights.values()))}}]:
                inp.write_text(json.dumps(data))
                with self.assertRaises(ValueError):import_branch_loop_timing(base_dir=base,base_name='both',input_timing=inp)
                self.assertEqual(saved,out.read_bytes())
            inp.write_bytes(good_input);self.assertEqual(tim,import_branch_loop_timing(base_dir=base,base_name='both',input_timing=inp))
