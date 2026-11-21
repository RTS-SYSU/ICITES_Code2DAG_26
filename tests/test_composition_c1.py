"""C1 composition semantics; all native scratch data is temporary, never executed."""
import copy
import hashlib
import itertools
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.composition_model import build_composition_views, CompositionError, RULE, CONTROL
from Code2DAG.pipeline.composition_timing import import_composition_timing
from Code2DAG.pipeline.rules_registry import get_rule
from Code2DAG.pipeline.runner import run_blocks, run_collector
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.errors import ValidationError
from Code2DAG.pipeline.loop_model import LoopModelError
from Code2DAG.pipeline.lock_model import LockModelError

ROOT=Path(__file__).resolve().parents[2]
SOURCES=ROOT/'Code2DAG/source_files/composition_c1'
ORDERS=[''.join(p).lower() for p in itertools.permutations('CKL')]
ALGOS=['FIFO','LPF','heft','t_level','zhao2020']
FILES=['segments_struct.json','dag_struct.json','segments_sched.json','dag_sched.json','segments.json','dag_seg.json','rule_meta.json']


def line(text, token): return next(i for i,l in enumerate(text.splitlines(),1) if token in l)


def fixture(order='ckl',text=None):
    text=(SOURCES/(order+'.c')).read_text() if text is None else text
    locka=line(text,'    pthread_mutex_lock');unlocka=line(text,'    pthread_mutex_unlock')
    csb=line(text,'/* CS_B */');last=line(text,'/* SUFFIX */')
    creates=[i for i,l in enumerate(text.splitlines(),1) if 'pthread_create(' in l]
    joins=[i for i,l in enumerate(text.splitlines(),1) if 'pthread_join(' in l]
    defs=[('A0','worker_a','compute',line(text,'/* PREFIX */'),locka-1),('A_CS','worker_a','mutex_cs',locka,unlocka),('A2','worker_a','compute',unlocka+1,last),('B_CS','worker_b','mutex_cs',csb-1,csb+1),
          ('M0','main','compute',line(text,'pthread_t a, b;'),creates[0]),('M1','main','compute',creates[1],creates[1]),('M2','main','compute',joins[0],joins[0]),('M3','main','compute',joins[1],joins[1])]
    rows=[dict(seg_id=sid,function=fn,kind=kind,start_line=a,end_line=b) for sid,fn,kind,a,b in defs]
    locations={};mapping={}
    for fn,sid,a,b in [('worker_a','A_CS',locka,unlocka),('worker_b','B_CS',csb-1,csb+1)]:
        for op,n in [('lock',a),('unlock',b)]:
            key=fn+'/pthread_mutex_'+op+'1';locations[key]=dict(function=fn,line=n);mapping[key]=sid
    for api,ls in [('create',creates),('join',joins)]:
        for i,n in enumerate(ls):
            key='main/pthread_'+api+str(i+1);locations[key]=dict(function='main',line=n);mapping[key]='M'+str(i+(0 if api=='create' else 2))
    mapping.update(worker_a='A0',worker_b='B_CS',main='M0')
    edges=[dict(src=a,dst=b,kind=k) for a,b,k in [('A0','A_CS','intra'),('A_CS','A2','intra'),('M0','M1','intra'),('M1','M2','intra'),('M2','M3','intra'),('M0','A0','create'),('M1','B_CS','create'),('A2','M2','join'),('B_CS','M3','join')]]
    return text,dict(base_name='fixture',source_file=str(SOURCES/(order+'.c')),segments=rows,node_locations=locations,node_to_segment=mapping,thread_entry_functions=['main','worker_a','worker_b']),dict(nodes=[r['seg_id'] for r in rows],edges=edges)


def build(order='ckl',text=None):
    t,s,d=fixture(order,text)
    return build_composition_views(base_name='fixture',source_text=t,base_segments_json=s,base_dag_json=d)


def reachable(d,a,b):
    todo=[a];seen=set()
    while todo:
        n=todo.pop()
        if n==b:return True
        if n not in seen: seen.add(n);todo.extend(e['dst'] for e in d['edges'] if e['src']==n)
    return False


class CompositionC1Tests(unittest.TestCase):
    def graph_contract(self,order,text,views):
        st,sd,ss,dd,_=views
        self.assertEqual(17,len(sd['nodes']));self.assertEqual(12,len(dd['nodes']))
        self.assertEqual(1,len(ss['condition_regions']));self.assertEqual(1,len(ss['loops']));self.assertEqual(1,len(ss['lock_groups']))
        self.assertFalse(any(r['kind'] in CONTROL for r in ss['segments']))
        for graph in (sd,dd):topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
        def containing(marker):
            n=line(text,marker)
            return [r['seg_id'] for r in st['segments'] if r['kind'] not in CONTROL and r['function']=='worker_a' and r['start_line']<=n<=r['end_line']]
        t,e=containing('/* THEN */'),containing('/* ELSE */')
        self.assertEqual(1,len(t));self.assertEqual(1,len(e));self.assertFalse(reachable(dd,t[0],e[0]));self.assertFalse(reachable(dd,e[0],t[0]))
        leaves=[r for r in ss['segments'] if r.get('role')=='iteration']
        self.assertEqual([0,1,2],[r['iteration_index'] for r in leaves]);self.assertEqual(3,len({r['seg_id'] for r in leaves}))
        ids=[r['seg_id'] for r in leaves]
        for a,b in zip(ids,ids[1:]):self.assertTrue(reachable(dd,a,b));self.assertFalse(reachable(dd,b,a))
        group=ss['lock_groups'][0];gid=group['group_id'];self.assertEqual('LOCKGROUP:shared_mutex',gid);self.assertEqual(2,len(group['member_segment_ids']))
        blocks={'c':t+e,'k':[gid],'l':ids}
        for a,b in zip(order,order[1:]):
            for u in blocks[a]:
                for v in blocks[b]:self.assertTrue(reachable(dd,u,v),(order,u,v))
        ce=next(r['seg_id'] for r in st['segments'] if r['kind']=='condition_entry');cx=next(r['seg_id'] for r in st['segments'] if r['kind']=='condition_exit')
        self.assertTrue(all(reachable(sd,ce,u) and reachable(sd,u,cx) for u in t+e))
        for choice in ['then','else']:
            active={r['seg_id'] for r in st['segments'] if all(m['branch_id']==choice for m in r.get('condition_memberships',[]))}
            edges=[x for x in sd['edges'] if x['src'] in active and x['dst'] in active and (x.get('exclusive_group') is None or x['branch_id']==choice)]
            self.assertTrue(reachable({'edges':edges},ce,cx));self.assertNotIn((e if choice=='then' else t)[0],active)
        mapping=ss['mappings'];key=f"LOOPSRC:worker_a:line:{line(text,'/* BODY */')}"
        self.assertEqual(ids,mapping['source_position_to_execution_segments'][key]);self.assertTrue(all(r['generation_origin'] is None for r in leaves))
        self.assertFalse(any(k.startswith('LOOPSRC:') for k in mapping['generation_node_to_execution_segments']))
        for member in group['members']:
            self.assertEqual([gid],mapping['source_position_to_execution_segments'][f"LOOPSRC:{member['function']}:line:{member['critical_range'][0]}"])
        self.assertEqual(2,sum(x['kind']=='create' for x in dd['edges']));self.assertEqual(2,sum(x['kind']=='join' for x in dd['edges']))
        self.assertTrue(all(v in dd['nodes'] for m in [mapping['generation_node_to_execution_segments'],mapping['original_segment_to_execution_segments'],mapping['source_position_to_execution_segments']] for targets in m.values() for v in targets))

    def test_six_permutations_exact_graph_and_mapping(self):
        for order in ORDERS:
            with self.subTest(order=order):self.graph_contract(order,(SOURCES/(order+'.c')).read_text(),build(order))

    def test_source_inputs_and_uninvolved_custom_attributes_unchanged(self):
        text,s,d=fixture();s['segments'][4]['custom']={'keep':[1,2]};s['mappings']={'custom':{'unchanged':True}}
        before=copy.deepcopy((text,s,d));v=build_composition_views(base_name='fixture',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual(before,(text,s,d));self.assertEqual({'keep':[1,2]},next(r for r in v[2]['segments'] if r['seg_id']=='M0')['custom']);self.assertEqual({'unchanged':True},v[2]['mappings']['custom'])

    def test_full_edge_origins_and_distinct_custom_dependency_kinds(self):
        text,s,d=fixture()
        d['edges'] += [dict(src='M0',dst='A_CS',kind=k,custom={'source':[i]}) for i,k in enumerate(['sem_dep','intra','custom'])]
        v=build_composition_views(base_name='fixture',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual(d['edges'],[r['edge'] for r in v[2]['composition_edge_provenance']]);self.assertEqual(list(range(len(d['edges']))),[r['base_edge_index'] for r in v[2]['composition_edge_provenance']])
        all_origins=json.dumps(v[3]['edges'])
        for k in ['sem_dep','custom']:self.assertIn(k,all_origins)
        self.assertIn('"source": [2]',all_origins)

    def test_zero_one_and_three_iterations(self):
        for n in [0,1,3]:
            text=(SOURCES/'ckl.c').read_text().replace('i < 3','i < '+str(n));st,sd,s,d,_=build(text=text)
            leaves=[r for r in s['segments'] if r.get('role')=='iteration'];self.assertEqual(n,len(leaves))
            key=f"LOOPSRC:worker_a:line:{line(text,'/* BODY */')}";self.assertEqual([r['seg_id'] for r in leaves],s['mappings']['source_position_to_execution_segments'][key])
            if n==0:self.assertTrue(any(x['src'].endswith(':entry') and x['dst'].endswith(':exit') for x in sd['edges']))

    def test_condition_c2_c3_c4_forms(self):
        source=(SOURCES/'ckl.c').read_text()
        variants=[source.replace('        value_a += 11; /* THEN */','        if (flag == 2) {\n            value_a += 31;\n        } else {\n            value_a += 37;\n        }'),
                  source.replace('    } else {\n        value_a += 13;', '    } else if (flag == 2) {\n        value_a += 31;\n    } else {\n        value_a += 13;'),
                  source.replace('    } else {\n        value_a += 13; /* ELSE */\n','')]
        for i,text in enumerate(variants):
            with self.subTest(i=i):
                st,sd,s,d,_=build(text=text);self.assertEqual(2 if i<2 else 1,len(s['condition_regions']));self.assertEqual(3,len([r for r in s['segments'] if r.get('role')=='iteration']));self.assertEqual(1,len(s['lock_groups']))

    def test_loop_l2_and_l3_forms(self):
        source=(SOURCES/'ckl.c').read_text();body='    for (int i = 0; i < 3; i++) {\n        value_a += i; /* BODY */\n    }'
        pair='    for (int i = 0; i < 1; i++) {\n        value_a += i;\n    }\n    value_a += 7;\n    for (int j = 0; j < 2; j++) {\n        value_a += j;\n    }'
        nested='    for (int i = 0; i < 2; i++) {\n        for (int j = 0; j < 3; j++) {\n            value_a += i+j;\n        }\n    }'
        for replacement,n in [(pair,3),(nested,6)]:
            st,sd,s,d,_=build(text=source.replace(body,replacement));self.assertEqual(2,len(s['loops']));self.assertEqual(n,len([r for r in s['segments'] if r.get('role')=='iteration']))

    def test_condition_loop_lock_nesting_rejected(self):
        for name in ['condition_contains_lock','condition_contains_loop','loop_contains_lock','all_three_nested']:
            # Keep the rejection self-contained; C0 files are not a test dependency.
            source=(SOURCES/'ckl.c').read_text()
            if name=='condition_contains_lock':source=source.replace('        value_a += 11; /* THEN */','        pthread_mutex_lock(&shared_mutex);\n        value_a += 11;\n        pthread_mutex_unlock(&shared_mutex);')
            elif name=='condition_contains_loop':source=source.replace('        value_a += 11; /* THEN */','        for (int j=0; j<1; j++) {\n            value_a += j;\n        }')
            else:source=source.replace('        value_a += i; /* BODY */','        pthread_mutex_lock(&shared_mutex);\n        value_a += i;\n        pthread_mutex_unlock(&shared_mutex);')
            with self.subTest(name=name),self.assertRaisesRegex(CompositionError,'nesting|overlap'):build(text=source)

    def test_unsupported_loop_macro_and_missing_weight_scopes_rejected(self):
        source=(SOURCES/'ckl.c').read_text()
        for text in [source.replace('i < 3','i <= 3'),source.replace('i < 3','i < flag'),source.replace('value_a += i; /* BODY */','break;'),'#define ACTIVE 1\n'+source]:
            with self.subTest(text=text),self.assertRaises((CompositionError,LoopModelError,LockModelError)):build(text=text)

    def test_cycle_retained_not_bypassed(self):
        text,s,d=fixture();d['edges'].append(dict(src='A2',dst='B_CS',kind='custom',tag='cycle witness'))
        topo_sort(d['nodes'],[(e['src'],e['dst']) for e in d['edges']])
        with self.assertRaisesRegex(ValidationError,'cycle'):build_composition_views(base_name='fixture',source_text=text,base_segments_json=s,base_dag_json=d)

    def test_size_guard_and_unmatched_lock(self):
        with self.assertRaisesRegex(CompositionError,'node limit'):build(text=(SOURCES/'ckl.c').read_text().replace('i < 3','i < 2147483647'))
        text,s,d=fixture();text=text.replace('    pthread_mutex_unlock(&shared_mutex);','    /* missing unlock */',1)
        with self.assertRaisesRegex(CompositionError,'unpaired'):build_composition_views(base_name='fixture',source_text=text,base_segments_json=s,base_dag_json=d)

    def command(self,args,cwd=ROOT):
        p=subprocess.run([str(a) for a in args],cwd=cwd,capture_output=True,text=True,timeout=120)
        self.assertEqual(0,p.returncode,(args,p.stderr[-3000:]));return p

    def native(self,temp,source):
        base=Path(temp)/'native';expand=Path(temp)/'expand';expand.mkdir();name=source.stem
        self.command(['gcc-13','-std=c11','-pthread','-fsyntax-only',source]);self.command(['gcc-13','-O0','-g','-fdump-rtl-expand','-c',source,'-o',expand/(name+'.o')]);dump=next(expand.glob('*.expand'))
        for mode in ['threads','full']:
            self.command([sys.executable,'-B','-m','Code2DAG.generation.legacy',dump,'--source-file',source,'--output-base',base,'--force',*(['--threads-only'] if mode=='threads' else [])])
        result=base/'intermediate_results'/name;shutil.copyfile(result/'config_files'/(name+'.dot'),result/'dag_generation/dag.dot')
        run_collector(base_dir=base,base_name=name,source_file=source)
        return base,result/'pipeline'

    def native_check(self,order):
        self.assertIsNotNone(shutil.which('gcc-13'),'native GCC13 required; no skip')
        source=SOURCES/(order+'.c');before=source.read_bytes()
        with tempfile.TemporaryDirectory() as tmp:
            base,p=self.native(tmp,source);name=source.stem
            run_composition_blocks(base_dir=base,base_name=name,source_file=source)
            b=p/'blocks/level2'/RULE
            views=[json.loads((b/n).read_text()) for n in ['segments_struct.json','dag_struct.json','segments_sched.json','dag_sched.json']]+[{}]
            self.graph_contract(order,source.read_text(),views)
            s=views[2];weights={t['template_id']:dict(total_ns=10*(i+1),count=1,avg_ns=10*(i+1),min_ns=10*(i+1),max_ns=10*(i+1)) for i,t in enumerate(s['templates'])}
            inp=Path(tmp)/'templates.json';inp.write_text(json.dumps({'weights':weights}));timing=import_composition_timing(base_dir=base,base_name=name,input_timing=inp)
            for r in s['segments']:
                if r['kind']=='lock_group':expected={k:sum(weights[m['execution_segment']['template_id']][k] for m in r['members']) for k in ['total_ns','count','avg_ns','min_ns','max_ns']}
                else:expected=weights[r['template_id']]
                self.assertEqual(expected,timing['weights'][r['seg_id']])
            leaf=[r['seg_id'] for r in s['segments'] if r.get('role')=='iteration'];self.assertIsNot(timing['weights'][leaf[0]],timing['weights'][leaf[1]])
            for algo in ALGOS:
                schedule=run_schedule(base_dir=base,base_name=name,level='level2',rule_name=RULE,algo_name=algo);self.assertEqual(set(views[3]['nodes']),set(schedule['priorities']))
            # Binding and input failures cannot overwrite published timing or blocks.
            output=p/'timing/level2'/RULE/'timing.json';published=output.read_bytes();legal=inp.read_bytes();sp=b/'segments.json';segments_before=sp.read_bytes();original_c=source.read_bytes()
            bad=copy.deepcopy(s);next(r for r in bad['segments'] if r.get('role')=='iteration')['template_id']='wrong';bad['view']='single';sp.write_text(json.dumps(bad))
            with self.assertRaisesRegex(CompositionError,'binding changed'):import_composition_timing(base_dir=base,base_name=name,input_timing=inp)
            self.assertEqual(published,output.read_bytes());sp.write_bytes(segments_before)
            for invalid in [json.dumps({'weights':{}}),'{"weights":{},"weights":{}}',json.dumps({'weights':{**weights,'old-segment':next(iter(weights.values()))}})]:
                inp.write_text(invalid)
                with self.assertRaises(ValueError):import_composition_timing(base_dir=base,base_name=name,input_timing=inp)
                self.assertEqual(published,output.read_bytes())
            inp.write_bytes(legal);self.assertEqual(timing,import_composition_timing(base_dir=base,base_name=name,input_timing=inp));self.assertEqual(published,output.read_bytes());self.assertEqual(segments_before,sp.read_bytes());self.assertEqual(original_c,source.read_bytes())
            self.assertFalse(list(p.rglob('*.c')));self.assertFalse((p/'instrument').exists())
        self.assertEqual(before,source.read_bytes())

    def test_native_ckl(self):self.native_check('ckl')
    def test_native_clk(self):self.native_check('clk')
    def test_native_kcl(self):self.native_check('kcl')
    def test_native_klc(self):self.native_check('klc')
    def test_native_lck(self):self.native_check('lck')
    def test_native_lkc(self):self.native_check('lkc')

    def test_standalone_native_publisher_byte_compatibility(self):
        for path,rule in [('condition_c3/condition_c3.c','conditional_branches'),('lock_l1/lock_l1.c','lock_grouped'),('loop_l3/loop_l3.c','loop_static_nested')]:
            source=ROOT/'Code2DAG/source_files'/path
            with self.subTest(path=path),tempfile.TemporaryDirectory() as tmp:
                base,p=self.native(tmp,source);name=source.stem
                expected=run_blocks(base_dir=base,base_name=name,level='level2',rule_name=rule,source_file=source)
                b=p/'blocks/level2'/rule;files={n:(b/n).read_bytes() for n in FILES}
                self.assertEqual(expected,run_composition_blocks(base_dir=base,base_name=name,source_file=source));self.assertEqual(files,{n:(b/n).read_bytes() for n in FILES});self.assertFalse((p/'blocks/level2'/RULE).exists())
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'plain.c';source.write_text('int main(void) {\n    int value=1;\n    value+=1;\n    return 0;\n}\n');base,p=self.native(tmp,source)
            expected=run_blocks(base_dir=base,base_name='plain',level='level2',rule_name='effective_line_merge',source_file=source);b=p/'blocks/level2/effective_line_merge';files={n:(b/n).read_bytes() for n in FILES}
            self.assertEqual(expected,run_composition_blocks(base_dir=base,base_name='plain',source_file=source));self.assertEqual(files,{n:(b/n).read_bytes() for n in FILES})


if __name__=='__main__':unittest.main()
