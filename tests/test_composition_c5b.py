"""C5b: a Condition leaf Lock contains static Loops and contracts to one LOCKGROUP."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition import select_rule,run_composition_blocks
from Code2DAG.pipeline.composition_c5b_model import RULE,build_condition_lock_loop_views
from Code2DAG.pipeline.composition_c5b_timing import import_condition_lock_loop_timing
from Code2DAG.pipeline.composition_model import CONTROL
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort

ROOT=Path(__file__).resolve().parents[2];SOURCES=ROOT/'Code2DAG/source_files/composition_c5b'
ALGOS=c1.ALGOS
# groups, iteration instances, loops, condition regions, groups' member counts,
# source-only helper segments, independent Lock regions
FORMS={
 'then':(1,3,1,1,[5],0,0),'else':(1,2,1,1,[4],0,0),'no_else':(1,3,1,1,[5],0,0),
 'else_if':(2,5,2,2,[5,4],0,0),'nested_then':(1,3,1,2,[5],0,0),
 'nested_else':(1,2,1,2,[4],0,0),'nested_inner_else':(1,2,1,2,[4],0,0),
 'both_branches':(2,5,2,1,[5,4],0,0),'same_mutex_branches':(2,5,2,1,[5,4],0,0),
 'l2':(1,5,2,1,[7],0,0),'l3':(1,6,2,1,[8],0,0),
 'zero':(1,0,1,1,[2],0,0),'only_loop':(1,3,1,1,[3],0,0),
 'only_loop_zero':(1,0,1,1,[0],0,0),
 'l3_outer_zero':(1,0,2,1,[2],0,0),'l3_inner_zero':(1,0,2,1,[2],0,0),
 'l3_both_zero':(1,0,2,1,[2],0,0),'two_mutex':(2,5,2,1,[4,5],0,0),
 'two_regions':(2,5,2,1,[4,5],0,0),'main_condition':(1,3,1,1,[5],0,0),
 'helper_condition':(1,3,1,2,[5],1,0),'helper_uncalled':(1,3,1,3,[5],2,0),
 'outside_lock':(2,2,1,1,[1,4],0,1),'outside_loop':(1,4,2,1,[4],0,0),
 'outside_loop_l2':(1,7,3,1,[5],0,0),'outside_loop_l3':(1,7,3,1,[5],0,0),
 'outside_loop_zero':(1,3,2,1,[5],0,0),'outside_condition':(1,3,1,2,[5],0,0),
 'sem':(1,3,1,1,[5],0,0),'one':(1,1,1,1,[3],0,0),
 'l2_zero_first':(1,3,2,1,[5],0,0),'l2_zero_second':(1,2,2,1,[4],0,0),
}
EXPECTED={
 'both_branches':(1129,1126,1126),'else':(1107,1126,1126),'else_if':(1129,1126,1107),
 'helper_condition':(1140,1118,1118),'helper_uncalled':(1140,1118,1118),
 'l2':(1138,1107,1107),'l2_zero_first':(1132,1107,1107),'l2_zero_second':(1126,1107,1107),
 'l3':(1138,1107,1107),'l3_both_zero':(1120,1107,1107),'l3_inner_zero':(1120,1107,1107),
 'l3_outer_zero':(1120,1107,1107),'main_condition':(1139,1117,1117),
 'nested_else':(1107,1126,1109),'nested_inner_else':(1107,1109,1126),
 'nested_then':(1129,1109,1107),'no_else':(1129,1100,1100),'one':(1123,1107,1107),
 'only_loop':(1109,1107,1107),'only_loop_zero':(1100,1107,1107),
 'outside_condition':(1150,1128,1128),'outside_lock':(1147,1128,1128),
 'outside_loop':(1226,1207,1207),'outside_loop_l2':(1331,1309,1309),
 'outside_loop_l3':(1329,1307,1307),'outside_loop_zero':(1129,1107,1107),
 'same_mutex_branches':(1129,1126,1126),'sem':(1129,1107,1107),
 'then':(1129,1107,1107),'two_mutex':(1155,1107,1107),
 'two_regions':(1155,1107,1107),'zero':(1120,1107,1107),
}
NESTED={'nested_then':2,'nested_else':2,'nested_inner_else':2,'else_if':2}


class CompositionC5bTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp=tempfile.TemporaryDirectory();cls._root=Path(cls._tmp.name);cls.h=c1.CompositionC1Tests();cls.cache={}
        for source in sorted(SOURCES.glob('*.c')):
            name=source.stem;text=source.read_text();assert select_rule(text)==RULE,(name,select_rule(text))
            root=cls._root/name;root.mkdir();base,p=cls.h.native(root,source)
            run_composition_blocks(base_dir=base,base_name=name,source_file=source)
            block=p/'blocks/level2'/RULE
            cls.cache[name]=dict(source=source,text=text,base=base,pipeline=p,block=block,
                                 segments=json.loads((block/'segments.json').read_text()),
                                 dag=json.loads((block/'dag_seg.json').read_text()),
                                 dag_struct=json.loads((block/'dag_struct.json').read_text()))

    @classmethod
    def tearDownClass(cls):cls._tmp.cleanup()

    def built(self,name):return self.cache[name]

    def test_condition_lock_group_memberships_loop_identity_and_maps(self):
        for name,(ngroups,niters,nloops,nconditions,member_counts,nsource_only,nindependent) in FORMS.items():
            with self.subTest(name=name):
                item=self.built(name);s=item['segments'];groups=s['lock_groups'];loops=s['loops']
                self.assertEqual(ngroups,len(groups));self.assertEqual(ngroups,len({g['group_id'] for g in groups}))
                self.assertEqual(niters,len(s['iteration_instances']));self.assertEqual(nloops,len(loops))
                self.assertEqual(nconditions,len(s['condition_regions']))
                self.assertEqual(sorted(member_counts),sorted(len(g['members']) for g in groups))
                self.assertEqual(nsource_only,len(s.get('source_only_segments',[])))
                self.assertEqual(nindependent,len(s.get('independent_lock_regions',[])))
                self.assertFalse(any(r['kind'] in CONTROL for r in s['segments']))
                self.assertEqual(len(s['lock_templates']),len(groups))
                for graph in (item['dag_struct'],item['dag']):
                    self.assertEqual(len(graph['nodes']),len(set(graph['nodes'])))
                    self.assertFalse(any(e['src']==e['dst'] for e in graph['edges']))
                    topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
                # Every group keeps complete Condition memberships, complete
                # member reverse maps, original nodes and source positions.
                for group in groups:
                    self.assertEqual(group['group_id'] in s['mappings']['grouped_node_to_member_segments'],True)
                    self.assertEqual(group['member_segment_ids'],s['mappings']['grouped_node_to_member_segments'][group['group_id']])
                    self.assertEqual(group['member_template_ids'],s['mappings']['grouped_node_to_member_templates'][group['group_id']])
                    self.assertEqual(len(group['member_segment_ids']),len(s['mappings']['grouped_node_to_member_occurrences'][group['group_id']]))
                    for member in group['members']:
                        self.assertEqual(group['condition_memberships'],member['condition_memberships'])
                        self.assertEqual(group['group_id'],s['mappings']['iteration_to_group'].get(member['seg_id'],group['group_id']))
                        for oid in member['source_segments']:
                            if oid in s['mappings']['original_segment_to_execution_segments']:
                                self.assertIn(group['group_id'],s['mappings']['original_segment_to_execution_segments'][oid])
                        for node in member.get('generation_origin') or []:
                            self.assertIn(group['group_id'],s['mappings']['generation_node_to_execution_segments'][node])
                        for key in member['source_position_ids']:
                            self.assertIn(group['group_id'],s['mappings']['source_position_to_execution_segments'][key])
                    if group['member_segment_ids']:
                        self.assertIn(group['group_id'],s['mappings']['structural_node_to_execution_segments'].get(group['member_segment_ids'][0],[]) or [group['group_id']])
                # Groups inherit the full path through nested Conditions.
                if name in NESTED:
                    if name=='else_if':
                        lengths=sorted(len(g['condition_memberships']) for g in groups);self.assertEqual([1,2],lengths)
                        paths={tuple(m['branch_id'] for m in g['condition_memberships']) for g in groups}
                        self.assertIn(('then',),paths);self.assertIn(('else','then'),paths)
                    else:
                        self.assertTrue(all(len(g['condition_memberships'])==NESTED[name] for g in groups))
                if name=='nested_inner_else':
                    self.assertTrue(all(tuple(m['branch_id'] for m in g['condition_memberships'])==('else','else') for g in groups))
                if name in ('then','else','nested_then','both_branches','two_mutex','outside_loop','else_if'):
                    self.assertTrue([r for r in s['segments'] if r['kind']!='lock_group' and r['condition_memberships']])
                if name=='outside_lock':
                    self.assertEqual(1,len([g for g in groups if not g['condition_memberships']]))
                    self.assertEqual(1,len([g for g in groups if g['condition_memberships']]))
                # Lock group source mapping and reverse template maps.
                for key,target in s['mappings']['source_lock_region_to_group'].items():
                    self.assertIn(target,{g['group_id'] for g in groups})
                    self.assertEqual([target],s['mappings']['source_lock_region_to_instances'][key])
                # Lock Loops keep their group; external Loops do not.
                for loop in loops:
                    if loop.get('lock_region'):
                        self.assertIn(loop.get('lock_group_id'),{g['group_id'] for g in groups})
                    else:
                        self.assertIsNone(loop.get('lock_group_id'))
                # Empty mappings are legal only for zero-trip Lock Loops.
                for tid,targets in s['mappings']['template_to_execution_segments'].items():
                    if targets==[]:
                        template=next(t for t in s['templates'] if t['template_id']==tid)
                        self.assertEqual('body_template',template.get('role'))
                        self.assertTrue(template.get('start_line') is not None)
                # N=0 retains the Lock critical region/node but no iteration.
                if name in ('zero','only_loop_zero','l3_outer_zero','l3_inner_zero','l3_both_zero','l2_zero_first','l2_zero_second'):
                    zero_loops=[loop for loop in loops if loop.get('lock_region') and loop['iteration_count']==0]
                    if zero_loops:self.assertTrue(any(len(g['members'])==0 for g in groups) or any(not g['member_segment_ids'] for g in groups) or name!='only_loop_zero')
                if name=='only_loop_zero':
                    self.assertEqual([],groups[0]['members']);self.assertEqual([],groups[0]['member_segment_ids'])
                # L3 identity is fully preserved in the structural view.
                if name=='l3':
                    ids={(i.get('outer_iteration_index'),i.get('inner_iteration_index')) for i in s['iteration_instances']}
                    self.assertEqual({(a,b) for a in range(2) for b in range(3)},ids)
                # No branch-exclusive group is mutually reachable.
                for left in groups:
                    for right in groups:
                        if left is right:continue
                        lm=[m['branch_id'] for m in left['condition_memberships']];rm=[m['branch_id'] for m in right['condition_memberships']]
                        if lm and rm and lm!=rm:
                            self.assertFalse(c1.reachable(item['dag'],left['group_id'],right['group_id']))
                            self.assertFalse(c1.reachable(item['dag'],right['group_id'],left['group_id']))
                # Structural loop iterations are forward-only.
                for loop in loops:
                    rows=[i for i in s['iteration_instances'] if i['loop_id']==loop['loop_id']]
                    def ident(i):
                        if i.get('outer_iteration_index') is not None or i.get('inner_iteration_index') is not None:
                            return (i.get('outer_iteration_index'),i.get('inner_iteration_index'))
                        return i.get('iteration_index')
                    rows.sort(key=ident)
                    for a,b in zip(rows,rows[1:]):
                        self.assertTrue(c1.reachable(item['dag_struct'],a['segment_id'],b['segment_id']))
                        self.assertFalse(c1.reachable(item['dag_struct'],b['segment_id'],a['segment_id']))

    def test_external_loop_identity_and_prefix_only_critical_region(self):
        for name in ('outside_loop','outside_loop_l2','outside_loop_l3','outside_loop_zero'):
            with self.subTest(name=name):
                item=self.built(name);s=item['segments'];external=[i for i in s['iteration_instances'] if not i.get('lock_group_id')]
                expected={'outside_loop':2,'outside_loop_l2':4,'outside_loop_l3':4,'outside_loop_zero':0}[name]
                self.assertEqual(expected,len(external));self.assertTrue(all(not i['condition_memberships'] for i in external))
                for row in s['segments']:
                    if row['seg_id'].startswith('LOOP:worker@10'):
                        self.assertNotEqual('lock_group',row['kind'])
        for name in ('only_loop','only_loop_zero'):
            with self.subTest(name=name):
                item=self.built(name);s=item['segments'];g=s['lock_groups'][0]
                self.assertEqual(3 if name=='only_loop' else 0,len(g['members']))
                self.assertEqual([],g['condition_memberships'][1:])

    def test_native_runtime_timing_five_algorithms_and_source_hash(self):
        for name,(modes) in EXPECTED.items():
            with self.subTest(name=name):
                item=self.built(name);src=item['source'];before=src.read_bytes();s=item['segments'];base=item['base'];h=self.h
                weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])}
                inp=self._root/(name+'-weights.json');inp.write_text(json.dumps({'weights':weights}))
                timing=import_condition_lock_loop_timing(base_dir=base,base_name=name,input_timing=inp)
                groups={g['group_id']:g for g in s['lock_groups']}
                for row in s['segments']:
                    if row['kind']=='lock_group':
                        expected={f:sum(weights[m['template_id']][f] for m in groups[row['seg_id']]['members']) for f in ('total_ns','count','avg_ns','min_ns','max_ns')}
                        self.assertEqual(expected,timing['weights'][row['seg_id']])
                    else:self.assertEqual(weights[row['template_id']],timing['weights'][row['seg_id']])
                self.assertEqual({row['seg_id'] for row in s['segments']},set(timing['weights']))
                self.assertNotIn('wait_ns',json.dumps(timing['lock_aggregation']));self.assertNotIn('wcet',json.dumps(timing['lock_aggregation']))
                for algo in ALGOS:
                    schedule=run_schedule(base_dir=base,base_name=name,level='level2',rule_name=RULE,algo_name=algo)
                    self.assertEqual(set(item['dag']['nodes']),set(schedule['priorities']))
                binary=self._root/(name+'-program');h.command(['gcc-13','-pthread','-std=c11',src,'-o',binary])
                for argc,value in enumerate(modes):
                    self.assertEqual('RESULT:'+str(value)+'\n',h.command([binary,*(['x']*argc)]).stdout)
                self.assertEqual(before,src.read_bytes())

    def test_direct_model_custom_edges_and_input_immutability(self):
        item=self.built('then');src=item['source'];text=item['text']
        raw,dag=__import__('Code2DAG.level2.segment_dag_level2',fromlist=['build_level2_segments_and_dag']).build_level2_segments_and_dag(base_dir=item['base'],base_name='then',source_file=src)
        for i,edge in enumerate(dag['edges']):
            edge['edge_id']='C5BQA:'+str(i);edge['custom']={'keep':[i,'x']}
        frozen=copy.deepcopy((raw,dag))
        st,sd,s,dd,meta=build_condition_lock_loop_views(base_name='then',source_text=text,base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(frozen,(raw,dag))
        self.assertEqual([{'base_edge_index':i,'edge':copy.deepcopy(e)} for i,e in enumerate(dag['edges'])],s['composition_edge_provenance'])
        for edge in dd['edges']:
            blob=json.dumps(edge)
            self.assertNotIn('C5BQA:',json.dumps(s['mappings']))  # ids live in provenance, not ids
            for item2 in edge.get('provenance',[]):
                inner=item2.get('edge',{})
                if 'edge_id' in inner:self.assertTrue(inner['edge_id'].startswith('C5BQA:'))
        self.assertTrue(any(g['condition_memberships'] for g in s['lock_groups']))

    def test_timing_binding_rejects_damage_without_overwrite(self):
        item=self.built('then');base=item['base'];block=item['block'];sp=block/'segments.json';original=sp.read_bytes();s=json.loads(original)
        weights={t['template_id']:dict(total_ns=i+1,count=1,avg_ns=i+1,min_ns=i+1,max_ns=i+1) for i,t in enumerate(s['templates'])}
        inp=self._root/'binding.json';inp.write_text(json.dumps({'weights':weights}));legal=inp.read_bytes()
        output=base/'intermediate_results'/'then'/'pipeline/timing/level2'/RULE/'timing.json'
        timing=import_condition_lock_loop_timing(base_dir=base,base_name='then',input_timing=inp);published=output.read_bytes()
        damages=[]
        bad=copy.deepcopy(s);bad['lock_groups'][0]['condition_memberships'][0]['branch_id']='bad';damages.append(bad)
        bad=copy.deepcopy(s);bad['segments'][0]['template_id']='C5BTPL:bad';damages.append(bad)
        bad=copy.deepcopy(s);bad['iteration_instances'][0]['iteration_index']=True;damages.append(bad)
        bad=copy.deepcopy(s);bad['loops'][0]['iteration_count']=True;damages.append(bad)
        bad=copy.deepcopy(s);bad['mappings']['generation_node_to_execution_segments']['bad']=['bad'];damages.append(bad)
        bad=copy.deepcopy(s);bad['mappings']['grouped_node_to_member_segments'][s['lock_groups'][0]['group_id']]=[];damages.append(bad)
        bad=copy.deepcopy(s);bad['mappings']['source_lock_region_to_group'][next(iter(bad['mappings']['source_lock_region_to_group']))]='bad';damages.append(bad)
        for bad in damages:
            sp.write_text(json.dumps(bad))
            with self.assertRaises(ValueError):import_condition_lock_loop_timing(base_dir=base,base_name='then',input_timing=inp)
            self.assertEqual(published,output.read_bytes())
        sp.write_bytes(original)
        for data in [{'weights':{}},{'weights':{**weights,'bad':next(iter(weights.values()))}},{'weights':{**weights,next(iter(weights)):{**next(iter(weights.values())),'count':1.0}}}]:
            inp.write_text(json.dumps(data))
            with self.assertRaises(ValueError):import_condition_lock_loop_timing(base_dir=base,base_name='then',input_timing=inp)
            self.assertEqual(published,output.read_bytes())
        inp.write_bytes(legal);self.assertEqual(timing,import_condition_lock_loop_timing(base_dir=base,base_name='then',input_timing=inp))
        self.assertEqual(published,output.read_bytes());self.assertEqual(original,sp.read_bytes())

    def test_invalid_boundaries_and_types_are_rejected(self):
        base_text=(SOURCES/'then.c').read_text()
        cases={}
        cases['lock_cross_loop']=base_text.replace('''        for (int i = 0; i < 3; i++) {
            result += 3;
        }
        result += 6;
        pthread_mutex_unlock(&m1);''','''        for (int i = 0; i < 3; i++) {
            result += 3;
        result += 6;
        pthread_mutex_unlock(&m1);
        }''')
        cases['missing_unlock']=base_text.replace('        pthread_mutex_unlock(&m1);\n','',1)
        cases['nested_lock']=base_text.replace('        pthread_mutex_lock(&m1);','        pthread_mutex_lock(&m1);\n        pthread_mutex_lock(&m2);').replace('        pthread_mutex_unlock(&m1);','        pthread_mutex_unlock(&m2);\n        pthread_mutex_unlock(&m1);')
        cases['cross_function']=base_text.replace('        pthread_mutex_unlock(&m1);\n','',1).replace('    pthread_join(t, NULL);','    pthread_join(t, NULL);\n    pthread_mutex_unlock(&m1);')
        cases['condition_inside_lock']=base_text.replace('        result += 4;','        if (mode > 1) { result += 4; }')
        cases['condition_inside_loop']=base_text.replace('            result += 3;','            if (mode > 1) { result += 3; }')
        cases['three_conditions']=base_text.replace('    if (mode == 0) {','    if (mode < 2) {\n    if (mode < 3) {\n    if (mode == 0) {').replace('    result += 1000;','    }\n    }\n    result += 1000;',1)
        cases['third_level']=base_text.replace('        for (int i = 0; i < 3; i++) {','        for (int q = 0; q < 2; q++) {\n        for (int r = 0; r < 2; r++) {\n        for (int i = 0; i < 3; i++) {').replace('            result += 3;\n        }','            result += 3;\n        }\n        }\n        }')
        cases['lock_no_loop']=base_text.replace('''        for (int i = 0; i < 3; i++) {
            result += 3;
        }''','''        result += 3;''')
        cases['dynamic_bound']=base_text.replace('i < 3','i < mode')
        cases['while']=base_text.replace('for (int i = 0; i < 3; i++) {','while (mode > 2) {')
        cases['nonleaf_lock']=base_text.replace('    if (mode == 0) {','    if (mode == 0) {\n        if (mode > 1) { result += 9; }')
        cases['external_lock_loop']='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
int result;
void *worker(void *arg) {
    pthread_mutex_lock(&m1);
    for (int i = 0; i < 2; i++) {
        result += 1;
    }
    pthread_mutex_unlock(&m1);
    return NULL;
}
int main(void) {pthread_t t;pthread_create(&t,NULL,worker,NULL);pthread_join(t,NULL);return 0;}
'''
        cases['lock_cross_condition']='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
int mode,result;
void *worker(void *arg) {
    if (mode == 0) {
        pthread_mutex_lock(&m1);
        for (int i = 0; i < 2; i++) {
            result += 1;
        }
    } else {
        result += 2;
        pthread_mutex_unlock(&m1);
    }
    return NULL;
}
int main(int argc,char **argv){pthread_t t;mode=argc-1;pthread_create(&t,NULL,worker,NULL);pthread_join(t,NULL);return 0;}
'''
        cases['lock_inside_loop']='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
int mode,result;
void *worker(void *arg) {
    if (mode == 0) {
        for (int i = 0; i < 2; i++) {
            pthread_mutex_lock(&m1);
            result += 1;
            pthread_mutex_unlock(&m1);
        }
    } else {
        result += 2;
    }
    return NULL;
}
int main(int argc,char **argv){pthread_t t;mode=argc-1;pthread_create(&t,NULL,worker,NULL);pthread_join(t,NULL);return 0;}
'''
        for name,text in cases.items():
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                source=Path(tmp)/(name+'.c');source.write_text(text);before=source.read_bytes();root=Path(tmp)/'gen';root.mkdir()
                try:
                    base,p=self.h.native(root,source)
                    raw,dag=__import__('Code2DAG.level2.segment_dag_level2',fromlist=['build_level2_segments_and_dag']).build_level2_segments_and_dag(base_dir=base,base_name=name,source_file=source)
                    with self.assertRaises(ValueError):build_condition_lock_loop_views(base_name=name,source_text=text,base_segments_json=raw,base_dag_json=dag)
                    self.assertEqual(before,source.read_bytes())
                except AssertionError:
                    raise
                except Exception as exc:
                    # Generation-level rejection is acceptable only when the
                    # source itself is syntactically invalid; none of these are.
                    raise self.failureException('unexpected generation failure '+name+': '+repr(exc))


if __name__=='__main__':unittest.main()
