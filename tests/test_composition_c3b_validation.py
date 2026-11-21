"""Bounded C3b native paths and exact iteration/branch timing contracts."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c3b as c3b
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition_c3b_model import RULE,build_loop_condition_views
from Code2DAG.pipeline.composition_c3b_timing import import_loop_condition_timing
from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort


def expected(name,mode):
    def branch(k,kind):
        x=k+mode
        if kind=='no_else':return 1 if x%2==0 else 0
        if kind=='else_if':return [1,10,100][x%3]
        if kind=='nested_then':return (1 if x%2==0 else 10) if x%3<2 else 100
        if kind=='nested_else':return 1 if x%3==0 else 10 if x%2==0 else 100
        return 1 if x%2==0 else 10
    if name=='l3':body=sum(branch(i+j,'basic') for i in range(2) for j in range(3))
    else:
        n=0 if name=='zero' else 1 if name=='one' else 3
        body=sum(branch(i,name) for i in range(n))
        if name=='common':body+=n*5
        if name=='l2_first':body+=5+2*3
        if name=='l2_both':body+=5+sum(branch(j,'basic') for j in range(2))
        if name=='outside_lock':body+=7
    return 1100+body


class C3bValidationTests(unittest.TestCase):
    def test_native_all_paths_timing_five_algorithms_and_identity_rejection(self):
        h=c1.CompositionC1Tests()
        for name,count in c3b.COUNTS.items():
            source=c3b.SOURCES/(name+'.c');before=source.read_bytes()
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                base,p=h.native(tmp,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source)
                b=p/'blocks/level2'/RULE;sp=b/'segments.json';valid=sp.read_bytes();s=json.loads(valid);st=json.loads((b/'segments_struct.json').read_text());d=json.loads((b/'dag_seg.json').read_text());sd=json.loads((b/'dag_struct.json').read_text())
                self.assertEqual(count,len(s['condition_regions']));self.assertEqual(count,len(sd['exclusive_groups']))
                for graph in [sd,d]:
                    topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']]);self.assertEqual(len(graph['nodes']),len(set(graph['nodes'])));self.assertTrue(all(e['src']!=e['dst'] for e in graph['edges']))
                for lid in {r['loop_id'] for r in s['iteration_instances']}:
                    instances=[r for r in s['iteration_instances'] if r['loop_id']==lid]
                    for a,c in zip(instances,instances[1:]):self.assertTrue(c1.reachable(sd,a['exit'],c['entry']))
                rows=s['segments']
                for a in rows:
                    ma={m['exclusive_group']:m['branch_id'] for m in a.get('condition_memberships',[])}
                    for c in rows:
                        mc={m['exclusive_group']:m['branch_id'] for m in c.get('condition_memberships',[])}
                        if any(ma[k]!=mc[k] for k in ma.keys()&mc.keys()):self.assertFalse(c1.reachable(d,a['seg_id'],c['seg_id']))
                weights={}
                for index,t in enumerate(s['templates']):
                    value=10+index+100*(t.get('iteration_index') or 0)+1000*(t.get('outer_iteration_index') or 0)
                    weights[t['template_id']]=dict(total_ns=value,count=1,avg_ns=value,min_ns=value,max_ns=value)
                inp=Path(tmp)/'weights.json';inp.write_text(json.dumps({'weights':weights}));input_before=inp.read_bytes();result=import_loop_condition_timing(base_dir=base,base_name=name,input_timing=inp)
                for row in rows:
                    wanted={f:sum(weights[m['execution_segment']['template_id']][f] for m in row['members']) for f in ['total_ns','count','avg_ns','min_ns','max_ns']} if row['kind']=='lock_group' else weights[row['template_id']]
                    self.assertEqual(wanted,result['weights'][row['seg_id']])
                if name=='zero':self.assertFalse(any('::COND:' in n or 'C3BTPL:LOOP:' in tid for n in result['weights'] for tid in weights));self.assertEqual([],s['condition_regions'])
                for algo in c1.ALGOS:
                    schedule=run_schedule(base_dir=base,base_name=name,level='level2',rule_name=RULE,algo_name=algo);self.assertEqual(set(d['nodes']),set(schedule['priorities']))
                binary=Path(tmp)/'business';h.command(['gcc-13','-pthread','-std=c11',source,'-o',binary])
                for mode in [0,1,2]:self.assertEqual('RESULT:'+str(expected(name,mode))+'\n',h.command([binary,*(['arg']*mode)]).stdout)
                saved=(p/'timing/level2'/RULE/'timing.json').read_bytes()
                for damage in ['index','branch','parent','exclusive','mapping','template','extra_node']:
                    bad=json.loads(valid);components=[r for r in bad['segments'] if r.get('loop_iteration_id')]
                    if not components:bad['condition_regions'].append({'condition_id':'fake'})
                    else:
                        row=next((r for r in components if r.get('condition_memberships')),components[0])
                        if damage=='index':row['iteration_index']=True
                        elif damage=='branch':row['condition_memberships'][0]['branch_id']='wrong'
                        elif damage=='exclusive':row['condition_memberships'][0]['exclusive_group']='wrong'
                        elif damage=='parent':bad['condition_regions'][0]['parent_id']='wrong'
                        elif damage=='mapping':bad['mappings']['source_condition_to_instances'][next(iter(bad['mappings']['source_condition_to_instances']))]=[]
                        elif damage=='template':row['template_id']='wrong-iteration-template'
                        else:bad['segments'].append(dict(row,seg_id='phantom'))
                    sp.write_text(json.dumps(bad))
                    with self.assertRaises(ValueError):import_loop_condition_timing(base_dir=base,base_name=name,input_timing=inp)
                    self.assertEqual(saved,(p/'timing/level2'/RULE/'timing.json').read_bytes())
                sp.write_bytes(valid);self.assertEqual(result,import_loop_condition_timing(base_dir=base,base_name=name,input_timing=inp));self.assertEqual(input_before,inp.read_bytes())
            self.assertEqual(before,source.read_bytes())

    def test_runtime_alternates_iteration_branches_and_copies_common_once(self):
        text='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode;
int result;
void *task(void *unused) {
    for (int i = 0; i < 3; i++) {
        result += 2;
        if ((i + mode) % 2 == 0) {
            printf("T:%d\\n", i);
            result += 1;
        } else {
            printf("E:%d\\n", i);
            result += 10;
        }
        result += 3;
    }
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t;
    mode = argc - 1;
    pthread_create(&t, NULL, task, NULL);
    pthread_join(t, NULL);
    printf("RESULT:%d\\n", result);
    return 0;
}
'''
        h=c1.CompositionC1Tests()
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'alternating.c';src.write_text(text);before=src.read_bytes();base,p=h.native(tmp,src);run_composition_blocks(base_dir=base,base_name=src.stem,source_file=src)
            s=json.loads((p/'blocks/level2'/RULE/'segments.json').read_text());self.assertEqual(3,len(s['condition_regions']))
            binary=Path(tmp)/'business';h.command(['gcc-13','-pthread',src,'-o',binary])
            self.assertEqual(['T:0','E:1','T:2','RESULT:27'],h.command([binary]).stdout.splitlines())
            self.assertEqual(['E:0','T:1','E:2','RESULT:36'],h.command([binary,'arg']).stdout.splitlines());self.assertEqual(before,src.read_bytes())

    def test_unsupported_source_rejection_never_publishes_partial_dag(self):
        from Code2DAG.pipeline.runner import run_blocks
        source=(c3b.SOURCES/'many.c').read_text();h=c1.CompositionC1Tests()
        two=source.replace('    }\n    result += 1000;','        if (mode == 9) {\n            result += 7;\n        }\n    }\n    result += 1000;',1)
        crossed=source.replace('        } else {','    }\n        } else {',1)
        # Source remains valid C, but the mutex is deliberately inside the Loop.
        locked=source.replace('int mode;','pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;\nint mode;').replace('        if ((i + mode)', '        pthread_mutex_lock(&m);\n        if ((i + mode)',1).replace('    }\n    result += 1000;','        pthread_mutex_unlock(&m);\n    }\n    result += 1000;',1)
        for text in [two,locked]:
            with self.subTest(text=text),tempfile.TemporaryDirectory() as tmp:
                src=Path(tmp)/'reject.c';src.write_text(text);before=src.read_bytes();base,p=h.native(tmp,src)
                with self.assertRaises(ValueError):run_blocks(base_dir=base,base_name=src.stem,source_file=src,level='level2',rule_name=RULE)
                self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists());self.assertEqual(before,src.read_bytes())
        _,raw,dag=c3b.fixture('many',crossed)
        with self.assertRaises(ValueError):build_loop_condition_views(base_name='fixture',source_text=crossed,base_segments_json=raw,base_dag_json=dag)

    def test_partial_conflicting_timing_and_zero_phantom_fail_before_publish(self):
        h=c1.CompositionC1Tests()
        for name in ['zero','many']:
            source=c3b.SOURCES/(name+'.c')
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                base,p=h.native(tmp,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source)
                b=p/'blocks/level2'/RULE;sp=b/'segments.json';valid=sp.read_bytes();s=json.loads(valid)
                weights={t['template_id']:dict(total_ns=10,count=1,avg_ns=10,min_ns=10,max_ns=10) for t in s['templates']};inp=Path(tmp)/'weights.json';inp.write_text(json.dumps({'weights':weights}));input_before=inp.read_bytes();output=p/'timing/level2'/RULE/'timing.json'
                invalid=copy.deepcopy(s)
                if name=='zero':invalid['condition_regions'].append(dict(condition_id='phantom',iteration_index=0))
                else:next(r for r in invalid['segments'] if r.get('condition_memberships'))['iteration_index']=True
                sp.write_text(json.dumps(invalid));damaged=sp.read_bytes()
                with self.assertRaises(ValueError):import_loop_condition_timing(base_dir=base,base_name=name,input_timing=inp)
                self.assertFalse(output.exists());self.assertEqual(damaged,sp.read_bytes());sp.write_bytes(valid)
                result=import_loop_condition_timing(base_dir=base,base_name=name,input_timing=inp);saved=output.read_bytes()
                for text in [json.dumps({'weights':{}}),'{"weights":{},"weights":{}}',json.dumps({'weights':{**weights,'wrong-iteration':next(iter(weights.values()))}})]:
                    inp.write_text(text)
                    with self.assertRaises(ValueError):import_loop_condition_timing(base_dir=base,base_name=name,input_timing=inp)
                    self.assertEqual(saved,output.read_bytes())
                inp.write_bytes(input_before);self.assertEqual(result,import_loop_condition_timing(base_dir=base,base_name=name,input_timing=inp))

    def test_existing_cross_iteration_branch_and_two_dimensional_binding_rejects(self):
        h=c1.CompositionC1Tests()
        for name in ['many','l3']:
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                source=c3b.SOURCES/(name+'.c');base,p=h.native(tmp,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source)
                b=p/'blocks/level2'/RULE;sp=b/'segments.json';valid=sp.read_bytes();s=json.loads(valid)
                weights={t['template_id']:dict(total_ns=10,count=1,avg_ns=10,min_ns=10,max_ns=10) for t in s['templates']};inp=Path(tmp)/'weights.json';inp.write_text(json.dumps({'weights':weights}));output=p/'timing/level2'/RULE/'timing.json'
                nodes=[r for r in s['segments'] if r.get('condition_memberships')];first=nodes[0]
                another_iteration=next(r for r in nodes if r['source_condition_node_id']==first['source_condition_node_id'] and r['loop_iteration_id']!=first['loop_iteration_id'])
                another_branch=next(r for r in nodes if r['loop_iteration_id']==first['loop_iteration_id'] and r['condition_memberships']!=first['condition_memberships'])
                damages=[('template_id',another_iteration['template_id']),('template_id',another_branch['template_id'])]
                if name=='l3':damages += [('outer_iteration_index',1),('inner_iteration_index',99)]
                for field,value in damages:
                    bad=copy.deepcopy(s);next(r for r in bad['segments'] if r['seg_id']==first['seg_id'])[field]=value;sp.write_text(json.dumps(bad))
                    with self.assertRaises(ValueError):import_loop_condition_timing(base_dir=base,base_name=name,input_timing=inp)
                    self.assertFalse(output.exists())
                sp.write_bytes(valid);import_loop_condition_timing(base_dir=base,base_name=name,input_timing=inp);self.assertTrue(output.exists())
