"""Per-iteration Condition identities, never one shared conditional body node."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.tests import test_composition_c2a as c2a
from Code2DAG.pipeline.composition_c3b_model import build_loop_condition_views,RULE
from Code2DAG.pipeline.composition_c3b_timing import import_loop_condition_timing
from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort
ROOT=Path(__file__).resolve().parents[2];SOURCES=ROOT/'Code2DAG/source_files/composition_c3b'
COUNTS={'zero':0,'one':1,'many':3,'common':3,'no_else':3,'else_if':6,'nested_then':6,'nested_else':6,'l2_first':3,'l2_both':5,'l3':6,'outside_lock':3}


def fixture(name,text=None):
    source=SOURCES/(name+'.c');text=source.read_text() if text is None else text
    if 'pthread_mutex_lock(' in text:raw,dag=c2a.fixture(text,source)
    else:
        lines=text.splitlines();a=next(i for i,l in enumerate(lines,1) if l.startswith('void *worker'))+1;b=next(i for i in range(a,len(lines)) if lines[i-1]=='}')-1
        create=next(i for i,l in enumerate(lines,1) if 'pthread_create(' in l);join=next(i for i,l in enumerate(lines,1) if 'pthread_join(' in l)
        rows=[dict(seg_id='OLD',function='worker',kind='compute',start_line=a,end_line=b),dict(seg_id='CREATE',function='main',kind='compute',start_line=create-2,end_line=create),dict(seg_id='JOIN',function='main',kind='compute',start_line=join,end_line=len(lines)-1)]
        raw=dict(base_name='fixture',source_file=str(source),segments=rows,thread_entry_functions=['worker'],node_locations={'main/pthread_create1':dict(function='main',line=create),'main/pthread_join1':dict(function='main',line=join)},node_to_segment={'main/pthread_create1':'CREATE','main/pthread_join1':'JOIN'})
        dag=dict(nodes=[r['seg_id'] for r in rows],edges=[dict(src='CREATE',dst='OLD',kind='create'),dict(src='OLD',dst='JOIN',kind='join'),dict(src='CREATE',dst='JOIN',kind='intra')])
    return text,raw,dag


class CompositionC3bTests(unittest.TestCase):
    def test_iteration_condition_instances_memberships_and_zero(self):
        for name,count in COUNTS.items():
            with self.subTest(name=name):
                text,raw,dag=fixture(name);before=copy.deepcopy((raw,dag));st,sd,s,d,_=build_loop_condition_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
                self.assertEqual(before,(raw,dag));regions=s['condition_regions'];self.assertEqual(count,len(regions));self.assertEqual(count,len({r['condition_id'] for r in regions}))
                for r in st['segments']:
                    if r['kind'] in ['condition_entry','condition_exit'] or r.get('condition_memberships'):
                        self.assertIn('loop_id',r);self.assertIs(type(r['iteration_index']),int);self.assertIn('iteration:',r['iteration_identity'])
                for graph in [sd,d]:self.assertEqual(len(graph['nodes']),len(set(graph['nodes'])));topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
                if name=='zero':self.assertFalse(regions);self.assertFalse(any(r.get('loop_iteration_id') for r in s['segments']))
                if name.startswith('nested'):self.assertTrue(any(len(r.get('condition_memberships',[]))==2 for r in s['segments']))
                if name=='l3':self.assertEqual({(i,j) for i in range(2) for j in range(3)},{(r['outer_iteration_index'],r['inner_iteration_index']) for r in regions})

    def test_native_zero_function_namespace_minimal_reproduction(self):
        # A valid function name must remain the source identity even at N=0.
        text=(SOURCES/'zero.c').read_text().replace('worker','task')
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'zero_task.c';source.write_text(text);before=source.read_bytes();base,p=c1.CompositionC1Tests().native(tmp,source)
            run_composition_blocks(base_dir=base,base_name=source.stem,source_file=source)
            s=json.loads((p/'blocks/level2'/RULE/'segments.json').read_text())
            body=next(i for i,line in enumerate(text.splitlines(),1) if '/* THEN */' in line)
            actual=s['mappings']['source_position_to_execution_segments']
            self.assertEqual([],s['condition_regions'])
            self.assertIn('LOOPSRC:task:line:'+str(body),actual)
            self.assertEqual([],actual['LOOPSRC:task:line:'+str(body)])
            self.assertFalse(any(k.startswith('LOOPSRC:worker:') for k in actual))
            self.assertEqual(before,source.read_bytes())
