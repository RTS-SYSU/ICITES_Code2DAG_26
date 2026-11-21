"""C5a: a Condition leaf contains a static Loop whose body owns matched Locks."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition import select_rule,run_composition_blocks
from Code2DAG.pipeline.composition_c5a_model import RULE,build_branch_loop_lock_views
from Code2DAG.pipeline.composition_c5a_timing import import_branch_loop_lock_timing
from Code2DAG.pipeline.composition_model import CONTROL
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort

ROOT=Path(__file__).resolve().parents[2];SOURCES=ROOT/'Code2DAG/source_files/composition_c5a'
ALGOS=c1.ALGOS
# groups, iteration instances, loops, inside-loop Lock regions, condition regions
FORMS={
 'then':(3,3,1,1,1),'else':(2,2,1,1,1),'no_else':(3,3,1,1,1),
 'else_if':(5,5,2,2,2),'nested_then':(3,3,1,1,2),'nested_else':(2,2,1,1,2),
 'nested_inner_else':(2,2,1,1,2),
 'both_branches':(5,5,2,2,1),'same_mutex_branches':(5,5,2,2,1),
 'same_mutex_adjacent':(3,6,1,2,1),'two_locks':(6,15,1,2,1),'two_mutex':(6,15,1,2,1),
 'l2':(5,5,2,2,1),'l2_zero':(2,2,2,2,1),'l3':(6,6,2,1,1),
 'l3_zero':(0,0,2,1,1),'l3_inner_zero':(0,0,2,1,1),'one':(1,1,1,1,1),
 'outside_lock':(4,3,1,1,1),'independent_loop':(3,5,2,1,1),
 'main_condition':(2,2,1,1,1),'helper_condition':(3,3,1,1,2),
 'sem':(3,3,1,1,1),'outside_condition':(3,3,1,1,2),'zero':(0,0,1,1,1),
}
EXPECTED={
 'both_branches':(1103,1108,1108),'else':(1107,1102,1102),'else_if':(1103,1102,1130),
 'helper_condition':(1114,1118,1118),'independent_loop':(1203,1207,1207),
 'l2':(1107,1107,1107),'l2_zero':(1104,1107,1107),'l3':(1106,1107,1107),
 'l3_zero':(1100,1107,1107),'l3_inner_zero':(1100,1107,1107),'one':(1101,1107,1107),
 'main_condition':(1102,1107,1107),'nested_else':(1110,1102,1107),'nested_then':(1103,1107,1120),
 'nested_inner_else':(1110,1107,1102),
 'no_else':(1103,1100,1100),'outside_condition':(1114,1118,1118),'outside_lock':(1112,1116,1116),
 'same_mutex_adjacent':(1115,1107,1107),'same_mutex_branches':(1103,1108,1108),
 'sem':(1103,1107,1107),'then':(1103,1107,1107),'two_locks':(1145,1107,1107),
 'two_mutex':(1145,1107,1107),'zero':(1100,1107,1107),
}
NESTED={'nested_then':2,'nested_else':2,'nested_inner_else':2}


class CompositionC5aTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp=tempfile.TemporaryDirectory();cls._root=Path(cls._tmp.name);cls.h=c1.CompositionC1Tests();cls.cache={}
        for source in sorted(SOURCES.glob('*.c')):
            name=source.stem;text=source.read_text();assert select_rule(text)==RULE,(name,select_rule(text))
            case_root=cls._root/name;case_root.mkdir()
            base,p=cls.h.native(case_root,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source)
            block=p/'blocks/level2'/RULE
            cls.cache[name]=dict(source=source,text=text,base=base,pipeline=p,block=block,
                                 segments=json.loads((block/'segments.json').read_text()),
                                 dag=json.loads((block/'dag_seg.json').read_text()),
                                 segments_struct=json.loads((block/'segments_struct.json').read_text()))

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def built(self,name):return self.cache[name]

    def test_condition_forms_groups_loop_identity_and_trace_maps(self):
        for name,(ngroups,niter,nloops,nlocks,nconditions) in FORMS.items():
            with self.subTest(name=name):
                item=self.built(name);s=item['segments'];groups=s['lock_groups'];loops=s['loops']
                self.assertEqual(ngroups,len(groups));self.assertEqual(ngroups,len({g['group_id'] for g in groups}))
                self.assertEqual(niter,len(s['iteration_instances']));self.assertEqual(nloops,len(loops))
                self.assertEqual(nlocks,len(s['lock_templates']));self.assertEqual(nconditions,len(s['condition_regions']))
                self.assertEqual(len(s['lock_templates']),len({(t['function'],tuple(t['lock_range'])) for t in s['lock_templates']}))
                self.assertFalse(any(r['kind'] in CONTROL for r in s['segments']))
                self.assertFalse(any(e['src']==e['dst'] for e in item['dag']['edges']))
                topo_sort(item['dag']['nodes'],[(e['src'],e['dst']) for e in item['dag']['edges']])
                self.assertEqual(len(item['dag']['nodes']),len(set(item['dag']['nodes'])))
                # Every Lock member keeps its complete Condition membership and
                # every reverse mapping resolves to its final Lock group.
                for group in groups:
                    self.assertTrue(group['members']);self.assertEqual(group['group_id'] in s['mappings']['grouped_node_to_member_segments'],True)
                    self.assertEqual(group['member_segment_ids'],s['mappings']['grouped_node_to_member_segments'][group['group_id']])
                    self.assertEqual(group['member_template_ids'],s['mappings']['grouped_node_to_member_templates'][group['group_id']])
                    self.assertEqual(len(group['member_segment_ids']),len(s['mappings']['grouped_node_to_member_occurrences'][group['group_id']]))
                    for member in group['members']:
                        row=member['execution_segment']
                        self.assertEqual(group['group_id'],s['mappings']['iteration_to_group'].get(row['seg_id'],group['group_id']))
                        self.assertEqual(row['condition_memberships'],group['condition_memberships'])
                        for sid in row['source_segments']:
                            self.assertIn(group['group_id'],s['mappings']['original_segment_to_execution_segments'][sid])
                        for node in row.get('generation_origin') or []:
                            self.assertIn(group['group_id'],s['mappings']['generation_node_to_execution_segments'][node])
                        for key in row['source_position_ids']:
                            self.assertIn(group['group_id'],s['mappings']['source_position_to_execution_segments'][key])
                    if group.get('loop_id'):
                        for member in group['members']:
                            self.assertEqual(group['group_id'],s['mappings']['iteration_to_group'][member['execution_segment']['seg_id']])
                # Nested Conditions are retained as a complete membership list.
                if name in NESTED:
                    self.assertTrue(groups)
                    self.assertTrue(all(len(g['condition_memberships'])==NESTED[name] for g in groups))
                for instance in s['iteration_instances']:
                    if name=='independent_loop' and instance['loop_id']=='LOOP:worker@10':
                        self.assertEqual([],instance['condition_memberships'])
                    elif name not in ('zero','l3_zero'):
                        self.assertTrue(instance['condition_memberships'],(name,instance))
                # Source Component and Loop templates inherit the leaf scope.
                for component in s['source_component_templates']:
                    if any(loop['lock_inside'] and loop['loop_id']==component['loop_id'] for loop in loops):
                        self.assertTrue(component['condition_memberships'],(name,component['template_id']))
                for template in s['templates']:
                    if template.get('loop_id'):
                        loop=next(loop for loop in loops if loop['loop_id']==template['loop_id'])
                        self.assertEqual(loop['condition_memberships'],template['condition_memberships'])
                # Source Lock regions trace to every concrete iteration group.
                for key,targets in s['mappings']['source_lock_region_to_group'].items():
                    self.assertEqual(targets,s['mappings']['source_lock_region_to_instances'][key])
                    self.assertTrue(all(target in {g['group_id'] for g in groups} for target in targets))
                by_loop={loop['loop_id']:loop for loop in loops}
                def effective(loop):
                    value=1;cursor=loop
                    while cursor is not None:
                        value*=cursor['iteration_count'];parent=cursor.get('parent_loop_id');cursor=by_loop.get(parent)
                    return value
                if name in ('zero','l3_zero','l3_inner_zero'):
                    self.assertEqual([],groups);self.assertEqual([],s['iteration_instances'])
                    for component in s['source_component_templates']:
                        self.assertEqual([],s['mappings']['template_to_execution_segments'][component['template_id']])
                    self.assertTrue(any(loop['iteration_count']==0 for loop in loops))
                else:
                    for component in s['source_component_templates']:
                        targets=s['mappings']['template_to_execution_segments'][component['template_id']]
                        if effective(by_loop[component['loop_id']])==0:self.assertEqual([],targets)
                        else:self.assertTrue(targets,(name,component['template_id']))
                    for value in s['mappings']['template_to_execution_segments'].values():
                        if value==[]:self.assertTrue(any(component['template_id'] for component in s['source_component_templates']))
                # A partially zero L2 keeps the zero-trip source template as the
                # only legal empty mapping while the second Loop is expanded.
                if name=='l2_zero':
                    empties=[c for c in s['source_component_templates'] if s['mappings']['template_to_execution_segments'][c['template_id']]==[]]
                    self.assertEqual(1,len(empties));self.assertEqual(0,effective(by_loop[empties[0]['loop_id']]))
                    self.assertEqual(2,len(groups));self.assertEqual(2,len(s['iteration_instances']))
                if name=='else_if':
                    by={tuple((m['exclusive_group'],m['branch_id']) for m in g['condition_memberships']) for g in groups}
                    self.assertIn((('COND:worker#001','then'),),by)
                    self.assertIn((('COND:worker#001','else'),('COND:worker#002','then')),by)
                if name=='else':
                    self.assertTrue(all(g['condition_memberships'][0]['branch_id']=='else' for g in groups))
                if name=='nested_inner_else':
                    self.assertTrue(all([(m['branch_id']) for m in g['condition_memberships']]==['else','else'] for g in groups))
                if name=='outside_lock':
                    self.assertEqual(1,len(s['independent_lock_regions']))
                    self.assertEqual(1,len([g for g in groups if not g['condition_memberships']]))
                    self.assertEqual(3,len([g for g in groups if g['condition_memberships']]))
                # Do not allow then/else or sibling-group reachability.
                for left in groups:
                    for right in groups:
                        if left is right:continue
                        lm={m['exclusive_group']:m['branch_id'] for m in left['condition_memberships']}
                        rm={m['exclusive_group']:m['branch_id'] for m in right['condition_memberships']}
                        if any(lm[k]!=rm[k] for k in lm.keys()&rm.keys()):
                            self.assertFalse(c1.reachable(item['dag'],left['group_id'],right['group_id']))
                            self.assertFalse(c1.reachable(item['dag'],right['group_id'],left['group_id']))

    def test_native_runtime_timing_and_five_algorithms(self):
        for name,(modes) in EXPECTED.items():
            with self.subTest(name=name):
                item=self.built(name);source=item['source'];before=source.read_bytes();s=item['segments'];base=item['base'];h=self.h
                weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])}
                inp=self._root/(name+'-weights.json');inp.write_text(json.dumps({'weights':weights}))
                timing=import_branch_loop_lock_timing(base_dir=base,base_name=name,input_timing=inp)
                for row in s['segments']:
                    if row['kind']=='lock_group':
                        expected={field:sum(weights[m['execution_segment']['template_id']][field] for m in row['members']) for field in ('total_ns','count','avg_ns','min_ns','max_ns')}
                        self.assertEqual(expected,timing['weights'][row['seg_id']])
                        self.assertEqual(row['member_segment_ids'],[c['segment_id'] for c in timing['lock_aggregation']['contributions'][row['seg_id']]])
                    else:self.assertEqual(weights[row['template_id']],timing['weights'][row['seg_id']])
                self.assertEqual({r['seg_id'] for r in s['segments']},set(timing['weights']))
                for algo in ALGOS:
                    schedule=run_schedule(base_dir=base,base_name=name,level='level2',rule_name=RULE,algo_name=algo)
                    self.assertEqual(set(item['dag']['nodes']),set(schedule['priorities']))
                binary=self._root/(name+'-program')
                h.command(['gcc-13','-pthread','-std=c11',source,'-o',binary])
                for argc,value in enumerate(modes):
                    expected='RESULT:'+str(value)+'\n'
                    self.assertEqual(expected,h.command([binary,*(['x']*argc)]).stdout)
                self.assertEqual(before,source.read_bytes())

    def test_zero_trip_and_empty_template_mappings(self):
        for name in ('zero','l3_zero','l3_inner_zero'):
            with self.subTest(name=name):
                item=self.built(name);s=item['segments'];self.assertEqual([],s['lock_groups']);self.assertEqual([],s['iteration_instances'])
                self.assertTrue(any(loop['iteration_count']==0 for loop in s['loops']))
                self.assertFalse(any(row['kind']=='lock_group' for row in s['segments']))
                for component in s['source_component_templates']:
                    self.assertEqual([],s['mappings']['template_to_execution_segments'][component['template_id']])
                weights={t['template_id']:dict(total_ns=1,count=1,avg_ns=1,min_ns=1,max_ns=1) for t in s['templates']}
                inp=self._root/(name+'-zero-weights.json');inp.write_text(json.dumps({'weights':weights}))
                timing=import_branch_loop_lock_timing(base_dir=item['base'],base_name=name,input_timing=inp)
                self.assertFalse(any(sid.startswith('LOCKGROUP:') for sid in timing['weights']))
                self.assertEqual({row['seg_id'] for row in s['segments']},set(timing['weights']))
                self.assertEqual({},timing['lock_aggregation']['contributions'])

    def test_timing_binding_rejects_damage_without_overwrite(self):
        item=self.built('then');base=item['base'];block=item['block'];sp=block/'segments.json';original=sp.read_bytes();s=json.loads(original)
        weights={t['template_id']:dict(total_ns=i+1,count=1,avg_ns=i+1,min_ns=i+1,max_ns=i+1) for i,t in enumerate(s['templates'])}
        inp=self._root/'binding-weights.json';inp.write_text(json.dumps({'weights':weights}));legal_input=inp.read_bytes()
        output=base/'intermediate_results'/'then'/'pipeline/timing/level2'/RULE/'timing.json'
        timing=import_branch_loop_lock_timing(base_dir=base,base_name='then',input_timing=inp);published=output.read_bytes()
        damages=[]
        bad=copy.deepcopy(s);bad['lock_groups'][0]['condition_memberships'][0]['branch_id']='forged';damages.append(bad)
        bad=copy.deepcopy(s);bad['segments'][0]['template_id']='C5ATPL:forged';damages.append(bad)
        bad=copy.deepcopy(s);bad['mappings']['generation_node_to_execution_segments']['forged']=['unknown'];damages.append(bad)
        bad=copy.deepcopy(s);bad['iteration_instances'][0]['iteration_index']=True;damages.append(bad)
        bad=copy.deepcopy(s);tid=s['source_component_templates'][0]['template_id'];bad['mappings']['template_to_execution_segments'][tid]=['unknown'];damages.append(bad)
        bad=copy.deepcopy(s);bad['lock_groups'][0]['member_segment_ids']=[];damages.append(bad)
        for bad in damages:
            sp.write_text(json.dumps(bad))
            with self.assertRaises(ValueError):import_branch_loop_lock_timing(base_dir=base,base_name='then',input_timing=inp)
            self.assertEqual(published,output.read_bytes())
        sp.write_bytes(original)
        for data in [{'weights':{}},{'weights':{**weights,'forged':next(iter(weights.values()))}},{'weights':{**weights,next(iter(weights)):{**next(iter(weights.values())),'count':1.0}}}]:
            inp.write_text(json.dumps(data))
            with self.assertRaises(ValueError):import_branch_loop_lock_timing(base_dir=base,base_name='then',input_timing=inp)
            self.assertEqual(published,output.read_bytes())
        inp.write_bytes(legal_input)
        self.assertEqual(timing,import_branch_loop_lock_timing(base_dir=base,base_name='then',input_timing=inp))
        self.assertEqual(published,output.read_bytes());self.assertEqual(original,sp.read_bytes())

    def test_invalid_nesting_and_lock_shapes_are_rejected(self):
        base_text=(SOURCES/'then.c').read_text()
        cases={}
        cases['lock_cross_loop']=base_text.replace('''            result += 1;
            pthread_mutex_unlock(&m1);
        }''','''            result += 1;
        }
        pthread_mutex_unlock(&m1);''')
        cases['lock_no_loop']=base_text.replace('''        for (int i = 0; i < 3; i++) {
            pthread_mutex_lock(&m1);
            result += 1;
            pthread_mutex_unlock(&m1);
        }''','''        pthread_mutex_lock(&m1);
        result += 1;
        pthread_mutex_unlock(&m1);''')
        cases['condition_inside_loop']=base_text.replace('''        for (int i = 0; i < 3; i++) {
            pthread_mutex_lock(&m1);''','''        for (int i = 0; i < 3; i++) {
            if (mode > 1) {
            pthread_mutex_lock(&m1);''').replace('''            pthread_mutex_unlock(&m1);
        }''','''            pthread_mutex_unlock(&m1);
            }
        }''')
        cases['condition_inside_lock']=base_text.replace('''            pthread_mutex_lock(&m1);
            result += 1;''','''            pthread_mutex_lock(&m1);
            if (mode > 1) {
            result += 1;
            }''')
        cases['missing_unlock']=base_text.replace('            pthread_mutex_unlock(&m1);\n','',1)
        cases['nested_lock']=base_text.replace('''            pthread_mutex_lock(&m1);
            result += 1;''','''            pthread_mutex_lock(&m1);
            pthread_mutex_lock(&m2);
            result += 1;
            pthread_mutex_unlock(&m2);''')
        cases['dynamic_bound']=base_text.replace('i < 3','i < mode')
        cases['unbraced']=base_text.replace('''        for (int i = 0; i < 3; i++) {
            pthread_mutex_lock(&m1);
            result += 1;
            pthread_mutex_unlock(&m1);
        }''','''        for (int i = 0; i < 3; i++)
            pthread_mutex_lock(&m1);
            result += 1;
            pthread_mutex_unlock(&m1);''')
        cases['while']=base_text.replace('for (int i = 0; i < 3; i++) {','while (mode > 2) {')
        cases['three_conditions']=base_text.replace('''        for (int i = 0; i < 3; i++) {''','''        if (mode > 1) {
        if (mode > 2) {
        for (int i = 0; i < 3; i++) {''').replace('''            pthread_mutex_unlock(&m1);
        }''','''            pthread_mutex_unlock(&m1);
        }
        }
        }''')
        cases['loop_not_leaf']=base_text.replace('''    if (mode == 0) {
        for (int i = 0; i < 3; i++) {''','''    if (mode == 0) {
        if (mode > 1) { result += 3; }
        for (int i = 0; i < 3; i++) {''')
        cases['third_level']='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
int mode,result;
void *worker(void *arg) {
    result += 100;
    if (mode == 0) {
        for (int a = 0; a < 2; a++) {
            for (int b = 0; b < 2; b++) {
                for (int c = 0; c < 2; c++) {
                    pthread_mutex_lock(&m1);
                    result += 1;
                    pthread_mutex_unlock(&m1);
                }
            }
        }
    } else {
        result += 7;
    }
    result += 1000;
    return NULL;
}
int main(int argc, char **argv) {pthread_t t;mode=argc-1;pthread_create(&t,NULL,worker,NULL);pthread_join(t,NULL);printf("%d\\n",result);return 0;}
'''
        cases['independent_lock']='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
int result;
void *worker(void *arg) {
    for (int i = 0; i < 2; i++) {
        pthread_mutex_lock(&m1);
        result += 1;
        pthread_mutex_unlock(&m1);
    }
    return NULL;
}
int main(void) {pthread_t t;pthread_create(&t,NULL,worker,NULL);pthread_join(t,NULL);printf("%d\\n",result);return 0;}
'''
        cases['cross_function']='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
int mode,result;
void *worker(void *arg) {
    result += 100;
    if (mode == 0) {
        for (int i = 0; i < 2; i++) {
            pthread_mutex_lock(&m1);
            result += 1;
        }
    } else {
        result += 7;
    }
    result += 1000;
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t;
    mode = argc - 1;
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    pthread_mutex_unlock(&m1);
    printf("%d\\n", result);
    return 0;
}
'''
        cases['reverse_condition_loop']='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
int mode,result;
void *worker(void *arg) {
    result += 100;
    for (int k = 0; k < 2; k++) {
        if (mode == 0) {
            for (int i = 0; i < 3; i++) {
                pthread_mutex_lock(&m1);
                result += 1;
                pthread_mutex_unlock(&m1);
            }
        } else {
            result += 7;
        }
    }
    result += 1000;
    return NULL;
}
int main(int argc, char **argv) {pthread_t t;mode=argc-1;pthread_create(&t,NULL,worker,NULL);pthread_join(t,NULL);printf("%d\\n",result);return 0;}
'''
        for name,text in cases.items():
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                source=Path(tmp)/(name+'.c');source.write_text(text);before=source.read_bytes()
                base,p=self.h.native(tmp,source);raw,dag=__import__('Code2DAG.level2.segment_dag_level2',fromlist=['build_level2_segments_and_dag']).build_level2_segments_and_dag(base_dir=base,base_name=name,source_file=source)
                with self.assertRaises(ValueError):build_branch_loop_lock_views(base_name=name,source_text=text,base_segments_json=raw,base_dag_json=dag)
                self.assertEqual(before,source.read_bytes())
                try:selected=select_rule(text)
                except ValueError:selected=None
                if selected==RULE:
                    with self.assertRaises(ValueError):run_composition_blocks(base_dir=base,base_name=name,source_file=source)
                    self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists())


if __name__=='__main__':unittest.main()
