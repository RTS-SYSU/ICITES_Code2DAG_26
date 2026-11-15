"""C5d: Loop iterations own a complete Lock whose critical region contains a Condition."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition import select_rule,run_composition_blocks
from Code2DAG.pipeline.composition_c5d_model import RULE,build_loop_condition_views
from Code2DAG.pipeline.composition_c5d_timing import import_loop_lock_condition_timing
from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
from Code2DAG.pipeline.composition_model import CONTROL
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort

ROOT=Path(__file__).resolve().parents[2];SOURCES=ROOT/'Code2DAG/source_files/composition_c5d';ALGOS=c1.ALGOS
# groups, condition instances, source Lock templates
FORMS={'else_if':(3,6,1),'external':(3,3,1),'extra_lock':(3,4,1),'helper':(3,3,1),'helper_condition':(3,4,1),'l1_many':(3,3,1),'l1_one':(1,1,1),'l1_zero':(0,0,1),
 'l2':(5,5,2),'l3':(6,6,1),'l3_inner_zero':(0,0,1),'l3_outer_zero':(0,0,1),'main':(3,3,1),'nested':(3,6,1),'no_else':(3,3,1),'outside_condition':(3,4,1),'outside_loop':(3,3,1),'sem':(3,3,1)}
EXPECTED={'else_if':(1154,1154,1154),'external':(1152,1153,1152),'extra_lock':(1195,1194,1195),'helper':(1159,1160,1159),'helper_condition':(1165,1164,1165),
 'l1_many':(1152,1153,1152),'l1_one':(1117,1118,1117),'l1_zero':(1100,1100,1100),
 'l2':(1187,1188,1187),'l3':(1205,1205,1205),'l3_inner_zero':(1100,1100,1100),'l3_outer_zero':(1100,1100,1100),'main':(1159,1160,1159),'nested':(1163,1162,1164),'no_else':(1148,1145,1148),'outside_condition':(1195,1194,1195),'outside_loop':(1170,1171,1170),'sem':(1152,1153,1152)}
FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')

class CompositionC5dTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp=tempfile.TemporaryDirectory();cls._root=Path(cls._tmp.name);cls.h=c1.CompositionC1Tests();cls.cache={}
        for source in sorted(SOURCES.glob('*.c')):
            name=source.stem;text=source.read_text();assert select_rule(text)==RULE,(name,select_rule(text));root=cls._root/name;root.mkdir();base,p=cls.h.native(root,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source);b=p/'blocks/level2'/RULE
            cls.cache[name]=dict(source=source,text=text,base=base,p=p,block=b,segments=json.loads((b/'segments.json').read_text()),struct=json.loads((b/'segments_struct.json').read_text()),dag=json.loads((b/'dag_seg.json').read_text()),dag_struct=json.loads((b/'dag_struct.json').read_text()))
    @classmethod
    def tearDownClass(cls):cls._tmp.cleanup()
    def built(self,name):return self.cache[name]
    def test_instances_branch_candidates_and_maps(self):
        for name,(ngroups,nconds,nlocks) in FORMS.items():
            with self.subTest(name=name):
                item=self.built(name);s=item['segments'];groups=s['lock_groups'];regions=s['condition_regions']
                self.assertEqual(ngroups,len(groups));self.assertEqual(ngroups,len({g['group_id'] for g in groups}));self.assertEqual(nconds,len(regions));self.assertEqual(nlocks,len(s['lock_templates']))
                self.assertEqual(len(item['dag']['nodes']),len(set(item['dag']['nodes'])));self.assertFalse(any(e['src']==e['dst'] for e in item['dag']['edges']));topo_sort(item['dag']['nodes'],[(e['src'],e['dst']) for e in item['dag']['edges']])
                for g in groups:
                    self.assertTrue(g['group_id'] in s['mappings']['grouped_node_to_member_segments'])
                    self.assertEqual(g['candidate_segment_ids'],s['mappings']['grouped_node_to_member_segments'][g['group_id']])
                    iter_ids={m['execution_segment'].get('loop_iteration_id') for m in g['members'] if m.get('execution_segment') and m['execution_segment'].get('loop_iteration_id')}
                    if g.get('loop_iteration_id') and iter_ids:self.assertEqual({g['loop_iteration_id']},iter_ids)
                for r in regions:
                    if r.get('loop_iteration_id'):self.assertIn(r['loop_iteration_id']+'::',r['condition_id'])
                paths={tuple(m['branch_id'] for m in r.get('condition_memberships',[])) for r in s['segments'] if r['kind']=='lock_path_candidate'}
                if name in ('l1_many','no_else','else_if','nested'):
                    self.assertTrue(any('then' in p for p in paths))
                if name in ('l1_many','else_if'):self.assertTrue(any(p[-1]=='else' if p else False for p in paths) or any(len(p)>1 for p in paths))
                # path candidates keep their complete condition memberships and source Lock identity
                for row in [r for r in s['segments'] if r['kind']=='lock_path_candidate']:
                    self.assertTrue(row['condition_memberships']);self.assertTrue(row['member_template_ids']);self.assertEqual(row['mutex_id'],row['lock_group']['mutex_id']);self.assertIn(row['lock_group_id'],{g['group_id'] for g in groups})
                if name in ('l1_zero','l3_inner_zero','l3_outer_zero'):
                    self.assertEqual([],groups);self.assertEqual([],regions);self.assertEqual([],s['iteration_instances'])
                    for c in s['source_component_templates']:self.assertEqual([],s['mappings']['template_to_execution_segments'][c['template_id']])
                if name=='l2':self.assertEqual(5,len(s['iteration_instances']));self.assertEqual(2,len(s['loops']))
                if name=='l3':self.assertEqual(6,len(s['iteration_instances']));self.assertEqual(2,len(s['loops']))
    def test_timing_path_sums_five_algorithms_and_runtime(self):
        for name,modes in EXPECTED.items():
            with self.subTest(name=name):
                item=self.built(name);src=item['source'];before=src.read_bytes();s=item['segments']
                weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])};inp=self._root/(name+'-w.json');inp.write_text(json.dumps({'weights':weights}));timing=import_loop_lock_condition_timing(base_dir=item['base'],base_name=name,input_timing=inp)
                self.assertEqual({r['seg_id'] for r in s['segments']},set(timing['weights']))
                for row in s['segments']:
                    if row['kind']=='lock_path_candidate':expected={f:sum(weights[t][f] for t in row['member_template_ids']) for f in FIELDS}
                    else:expected=weights[row['template_id']]
                    self.assertEqual(expected,timing['weights'][row['seg_id']])
                for algo in ALGOS:
                    schedule=run_schedule(base_dir=item['base'],base_name=name,level='level2',rule_name=RULE,algo_name=algo);self.assertEqual(set(item['dag']['nodes']),set(schedule['priorities']))
                binary=self._root/(name+'-prog');self.h.command(['gcc-13','-pthread','-std=c11',src,'-o',binary])
                for argc,value in enumerate(modes):self.assertEqual('RESULT:%d\n'%value,self.h.command([binary,*(['x']*argc)]).stdout)
                self.assertEqual(before,src.read_bytes())
    def test_model_input_and_custom_edges(self):
        item=self.built('l1_many');from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
        raw,dag=build_level2_segments_and_dag(base_dir=item['base'],base_name='l1_many',source_file=item['source'])
        for i,e in enumerate(dag['edges']):e['edge_id']='C5DQA:'+str(i);e['custom']={'keep':[i,{'nested':True}]}
        frozen=copy.deepcopy((raw,dag));st,sd,s,dd,meta=build_loop_condition_views(base_name='l1_many',source_text=item['text'],base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(frozen,(raw,dag));self.assertEqual([{'base_edge_index':i,'edge':copy.deepcopy(e)} for i,e in enumerate(dag['edges'])],s['composition_edge_provenance'])
        self.assertTrue(any('C5DQA:' in json.dumps(e.get('provenance',[])) for e in dd['edges']))


class C5dInternalSemProjectionTests(unittest.TestCase):
    def setUp(self):self.h=c1.CompositionC1Tests();self._temps=[]
    def tearDown(self):
        for tmp in self._temps:tmp.cleanup()
    def sem_source(self,fixture='l1_many.c',post='critical',wait='both'):
        text=(SOURCES/fixture).read_text()
        text=text.replace('#include <stdio.h>','#include <stdio.h>\n#include <semaphore.h>')
        text=text.replace('int mode,result;','int mode,result;\nstatic sem_t sem_x;')
        if post=='critical':
            if fixture=='l3.c':
                text=text.replace('            result += 2;\n            if ','            sem_post(&sem_x);\n            result += 2;\n            if ',1)
            else:
                text=text.replace('        result += 2;\n        if ','        sem_post(&sem_x);\n        result += 2;\n        if ',1)
        elif post=='before_lock':
            text=text.replace('        result += 1;\n        pthread_mutex_lock(&m1);','        result += 1;\n        sem_post(&sem_x);\n        pthread_mutex_lock(&m1);',1)
        elif post=='suffix':
            if fixture=='l3.c':
                text=text.replace('            result += 5;\n            pthread_mutex_unlock(&m1);','            result += 5;\n            sem_post(&sem_x);\n            pthread_mutex_unlock(&m1);',1)
            else:
                text=text.replace('        result += 5;\n        pthread_mutex_unlock(&m1);','        result += 5;\n        sem_post(&sem_x);\n        pthread_mutex_unlock(&m1);',1)
        else:raise AssertionError(post)
        if fixture=='l3.c':
            old='            if ((((i + j) + mode) % 2) == 0) {\n                result += 3;\n            } else {\n                result += 4;\n            }'
            then_body='                sem_wait(&sem_x);\n                result += 3;'
            else_body='                sem_wait(&sem_x);\n                result += 4;'
        elif fixture=='no_else.c':
            old='        if (((i + mode) % 2) == 0) {\n            result += 3;\n        }'
            then_body='            sem_wait(&sem_x);\n            result += 3;'
            else_body=''
        else:
            old='        if (((i + mode) % 2) == 0) {\n            result += 3;\n        } else {\n            result += 4;\n        }'
            then_body='            sem_wait(&sem_x);\n            result += 3;'
            else_body='            sem_wait(&sem_x);\n            result += 4;'
        if wait in ('then_sem','else_sem','both_sem'):
            indent='                ' if fixture=='l3.c' else '            '
            then_sem=indent+'sem_wait(&sem_x);';else_sem=indent+'sem_wait(&sem_x);'
            if wait in ('then_sem','both_sem'):then_body=then_sem
            elif fixture=='no_else.c':raise AssertionError('no_else only has then')
            if wait in ('else_sem','both_sem'):else_body=else_sem
        if fixture=='l3.c':
            if wait=='both':block='            if ((((i + j) + mode) % 2) == 0) {\n'+then_body+'\n            } else {\n'+else_body+'\n            }'
            elif wait=='then':block='            if ((((i + j) + mode) % 2) == 0) {\n'+then_body+'\n            } else {\n                result += 4;\n            }'
            elif wait=='else':block='            if ((((i + j) + mode) % 2) == 0) {\n                result += 3;\n            } else {\n'+else_body+'\n            }'
            elif wait=='then_sem':block='            if ((((i + j) + mode) % 2) == 0) {\n'+then_body+'\n            } else {\n                result += 4;\n            }'
            elif wait=='else_sem':block='            if ((((i + j) + mode) % 2) == 0) {\n                result += 3;\n            } else {\n'+else_body+'\n            }'
            elif wait=='both_sem':block='            if ((((i + j) + mode) % 2) == 0) {\n'+then_body+'\n            } else {\n'+else_body+'\n            }'
            else:raise AssertionError(wait)
        elif fixture=='no_else.c':
            if wait not in ('then','then_sem'):raise AssertionError('no_else only has then')
            block='        if (((i + mode) % 2) == 0) {\n'+then_body+'\n        }'
        else:
            if wait=='both':block='        if (((i + mode) % 2) == 0) {\n'+then_body+'\n        } else {\n'+else_body+'\n        }'
            elif wait=='then':block='        if (((i + mode) % 2) == 0) {\n'+then_body+'\n        } else {\n            result += 4;\n        }'
            elif wait=='else':block='        if (((i + mode) % 2) == 0) {\n            result += 3;\n        } else {\n'+else_body+'\n        }'
            elif wait=='then_sem':block='        if (((i + mode) % 2) == 0) {\n'+then_body+'\n        } else {\n            result += 4;\n        }'
            elif wait=='else_sem':block='        if (((i + mode) % 2) == 0) {\n            result += 3;\n        } else {\n'+else_body+'\n        }'
            elif wait=='both_sem':block='        if (((i + mode) % 2) == 0) {\n'+then_body+'\n        } else {\n'+else_body+'\n        }'
            else:raise AssertionError(wait)
        text=text.replace(old,block,1)
        text=text.replace('    pthread_create(&t, NULL, worker, NULL);','    sem_init(&sem_x, 0, 0);\n    pthread_create(&t, NULL, worker, NULL);')
        text=text.replace('    pthread_join(t, NULL);\n    printf','    pthread_join(t, NULL);\n    sem_destroy(&sem_x);\n    printf')
        return text
    def raw_dag(self,text,name):
        tmp=tempfile.TemporaryDirectory();self._temps.append(tmp);root=Path(tmp.name)
        src=root/(name+'.c');src.write_text(text);base,p=self.h.native(root,src)
        return build_level2_segments_and_dag(base_dir=base,base_name=name,source_file=src)
    def direct(self,text,name):
        raw,dag=self.raw_dag(text,name);frozen=copy.deepcopy((raw,dag))
        st,sd,s,dd,meta=build_loop_condition_views(base_name=name,source_text=text,base_segments_json=raw,base_dag_json=dag)
        return raw,dag,frozen,st,sd,s,dd,meta
    def decorate(self,raw,dag):
        sem=[e for e in dag['edges'] if e.get('kind')=='sem_dep'];self.assertTrue(sem)
        for i,e in enumerate(sem):
            e['edge_id']='SEMC5D:'+str(i);e['custom']={'keep':[i,{'nested':True}]};e['provenance']=[{'source':'circle.txt','index':i}]
        return sem
    def absorb_rows(self,s):
        return s['mappings'].get('absorbed_internal_sem_dependencies',[])
    def branch_ids(self,s,seg_id):
        row=next(r for r in s['segments'] if r['seg_id']==seg_id)
        return {m['branch_id'] for m in row.get('condition_memberships',[])}
    def check_topology(self,s,dd):
        self.assertFalse(any(e['src']==e['dst'] for e in dd['edges']))
        topo_sort([r['seg_id'] for r in s['segments']],[(e['src'],e['dst']) for e in dd['edges']])
    def test_collapsed_sem_witnesses_l1_l3_no_else_and_only_branch(self):
        cases=[('l1_many.c','both',6,3,{'then','else'}),('l3.c','both',12,6,{'then','else'}),('no_else.c','then',3,3,{'then'}),('l1_many.c','then',3,3,{'then'}),('l1_many.c','else',3,3,{'else'})]
        for fixture,wait,count,iterations,branches in cases:
            with self.subTest(fixture=fixture,wait=wait):
                text=self.sem_source(fixture,'critical',wait);raw,dag=self.raw_dag(text,fixture.replace('.c','_'+wait));sem=self.decorate(raw,dag);frozen=copy.deepcopy((raw,dag))
                st,sd,s,dd,meta=build_loop_condition_views(base_name='sem',source_text=text,base_segments_json=raw,base_dag_json=dag)
                self.assertEqual(frozen,(raw,dag));self.assertEqual([], [e for e in dd['edges'] if e.get('kind')=='sem_dep']);self.check_topology(s,dd)
                self.assertEqual(count,len(self.absorb_rows(s)));self.assertEqual(iterations,len(s['iteration_instances']))
                seen_branches=set()
                for item in self.absorb_rows(s):
                    self.assertEqual(item['projected_src'],item['projected_dst']);item_branches=self.branch_ids(s,item['projected_src']);self.assertEqual(1,len(item_branches));self.assertIn(next(iter(item_branches)),branches);seen_branches.update(item_branches)
                    expected={'src':sem[0]['src'],'dst':sem[0]['dst'],'kind':'sem_dep','edge_id':'SEMC5D:0','custom':{'keep':[0,{'nested':True}]},'provenance':[{'source':'circle.txt','index':0}]}
                    self.assertEqual(expected,item['edge'])
                self.assertEqual(branches,seen_branches)
                if fixture=='no_else.c':
                    false=[r for r in s['segments'] if r['kind']=='lock_path_candidate' and any(m['branch_id']=='else' for m in r.get('condition_memberships',[]))]
                    self.assertTrue(false);self.assertFalse(any(any(m['branch_id']=='else' for m in next(r for r in s['segments'] if r['seg_id']==item['projected_src']).get('condition_memberships',[])) for item in self.absorb_rows(s)))
    def test_distinct_sem_edges_branch_paths_and_metadata(self):
        text=self.sem_source('l1_many.c','before_lock','both');raw,dag=self.raw_dag(text,'sem_distinct');sem=self.decorate(raw,dag);frozen=copy.deepcopy((raw,dag))
        st,sd,s,dd,meta=build_loop_condition_views(base_name='sem_distinct',source_text=text,base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(frozen,(raw,dag));self.assertEqual([],self.absorb_rows(s));self.check_topology(s,dd)
        edges=[e for e in dd['edges'] if e.get('kind')=='sem_dep'];self.assertEqual(6,len(edges));self.assertEqual(6,len({(e['src'],e['dst']) for e in edges}))
        self.assertEqual(3,len({e['src'] for e in edges}));self.assertEqual(6,len({e['dst'] for e in edges}))
        for edge in edges:
            self.assertEqual('SEMC5D:0',edge['edge_id']);self.assertEqual(sem[0]['custom'],edge['custom']);self.assertEqual(sem[0]['provenance'],edge['provenance'])
            src_iter=next(r for r in s['segments'] if r['seg_id']==edge['src'])['loop_iteration_id'];dst_row=next(r for r in s['segments'] if r['seg_id']==edge['dst']);self.assertEqual(src_iter,dst_row['loop_iteration_id'])
            self.assertIn(self.branch_ids(s,edge['dst']).pop(),{'then','else'})
        self.assertEqual({('then',0),('else',0),('then',1),('else',1),('then',2),('else',2)},{(self.branch_ids(s,e['dst']).pop(),next(r for r in s['segments'] if r['seg_id']==e['dst'])['iteration_index']) for e in edges})
    def test_sem_rejections_true_cycle_cross_mutex_and_input_immutability(self):
        text=(SOURCES/'l2.c').read_text()
        text=text.replace('#include <stdio.h>','#include <stdio.h>\n#include <semaphore.h>').replace('int mode,result;','int mode,result;\nstatic sem_t sem_x;')
        text=text.replace('        result += 2;\n        if (((i + mode) % 2) == 0) {','        sem_post(&sem_x);\n        result += 2;\n        if (((i + mode) % 2) == 0) {',1)
        text=text.replace('        result += 2;\n        if (((j + mode) % 2) == 0) {\n            result += 3;','        result += 2;\n        if (((j + mode) % 2) == 0) {\n            sem_wait(&sem_x);\n            result += 3;',1)
        text=text.replace('    pthread_create(&t, NULL, worker, NULL);','    sem_init(&sem_x, 0, 0);\n    pthread_create(&t, NULL, worker, NULL);').replace('    pthread_join(t, NULL);\n    printf','    pthread_join(t, NULL);\n    sem_destroy(&sem_x);\n    printf')
        raw,dag=self.raw_dag(text,'sem_cross');frozen=copy.deepcopy((raw,dag))
        with self.assertRaises(Exception):build_loop_condition_views(base_name='sem_cross',source_text=text,base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(frozen,(raw,dag))
        text=self.sem_source('l1_many.c','critical','both');raw,dag=self.raw_dag(text,'sem_cycle');self.decorate(raw,dag)
        source=next(r for r in raw['segments'] if r['function']=='worker' and r['start_line']<=10<=r['end_line']);target=next(r for r in raw['segments'] if r['function']=='worker' and r['start_line']<=27<=r['end_line'])
        dag['edges'].append({'src':target['seg_id'],'dst':source['seg_id'],'kind':'sem_dep','edge_id':'TRUE_CYCLE'});frozen=copy.deepcopy((raw,dag))
        with self.assertRaises(Exception):build_loop_condition_views(base_name='sem_cycle',source_text=text,base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(frozen,(raw,dag))
    def test_sem_real_c_and_five_algorithms(self):
        text=self.sem_source('l1_many.c','critical','both')
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'sem_runtime.c';src.write_text(text);before=src.read_bytes();base,p=self.h.native(tmp,src);run_composition_blocks(base_dir=base,base_name='sem_runtime',source_file=src)
            s=json.loads((p/'blocks/level2'/RULE/'segments.json').read_text());d=json.loads((p/'blocks/level2'/RULE/'dag_seg.json').read_text());self.check_topology(s,d)
            weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])};inp=Path(tmp)/'weights.json';inp.write_text(json.dumps({'weights':weights}));import_loop_lock_condition_timing(base_dir=base,base_name='sem_runtime',input_timing=inp)
            binary=Path(tmp)/'program';self.h.command(['gcc-13','-pthread','-std=c11',src,'-o',binary]);self.assertEqual('RESULT:1152\n',self.h.command([binary]).stdout)
            for algo in ALGOS:self.assertEqual(set(d['nodes']),set(run_schedule(base_dir=base,base_name='sem_runtime',level='level2',rule_name=RULE,algo_name=algo)['priorities']))
            self.assertEqual(before,src.read_bytes())
    def test_sem_only_suffix_no_else_and_multiline_formal_ranges(self):
        cases=[('l1_many.c','critical','then_sem',3,{'then'}),('l1_many.c','critical','else_sem',3,{'else'}),('l1_many.c','critical','both_sem',6,{'then','else'}),('l1_many.c','suffix','both',6,{'then','else'}),('no_else.c','critical','then_sem',3,{'then'}),('l3.c','critical','both_sem',12,{'then','else'})]
        for fixture,post,wait,count,expected in cases:
            with self.subTest(fixture=fixture,post=post,wait=wait):
                text=self.sem_source(fixture,post,wait);raw,dag,frozen,st,sd,s,dd,meta=self.direct(text,fixture.replace('.c','_'+post+'_'+wait))
                self.assertEqual(frozen,(raw,dag));self.assertEqual([], [e for e in dd['edges'] if e.get('kind')=='sem_dep']);self.check_topology(s,dd)
                rows=self.absorb_rows(s);self.assertEqual(count,len(rows));seen=set()
                for item in rows:
                    self.assertEqual(item['projected_src'],item['projected_dst']);branches=self.branch_ids(s,item['projected_src']);self.assertEqual(1,len(branches));self.assertIn(next(iter(branches)),expected);seen.update(branches)
                self.assertEqual(expected,seen)
                if fixture=='no_else.c':self.assertNotIn('else',seen)
        text=self.sem_source('l1_many.c','critical','then_sem').replace('            sem_wait(&sem_x);','            sem_wait(\n                &sem_x\n            );',1)
        raw,dag,frozen,st,sd,s,dd,meta=self.direct(text,'multiline_then');self.check_topology(s,dd);self.assertEqual(3,len(self.absorb_rows(s)));self.assertEqual({'then'},{next(iter(self.branch_ids(s,a['projected_src']))) for a in self.absorb_rows(s)})
    def test_else_if_and_nested_full_membership_paths(self):
        text=(SOURCES/'else_if.c').read_text();text=text.replace('#include <stdio.h>','#include <stdio.h>\n#include <semaphore.h>').replace('int mode,result;','int mode,result;\nstatic sem_t sem_x;')
        old='''        result += 2;
        if (((i + mode) % 3) == 0) {
            result += 3;
        } else if (((i + mode) % 3) == 1) {
            result += 4;
        } else {
            result += 5;
        }'''
        new='''        sem_post(&sem_x);
        result += 2;
        if (((i + mode) % 3) == 0) {
            sem_wait(&sem_x);
            result += 3;
        } else if (((i + mode) % 3) == 1) {
            sem_wait(&sem_x);
            result += 4;
        } else {
            sem_wait(&sem_x);
            result += 5;
        }'''
        self.assertIn(old,text);text=text.replace(old,new,1).replace('    pthread_create(&t, NULL, worker, NULL);','    sem_init(&sem_x, 0, 0);\n    pthread_create(&t, NULL, worker, NULL);').replace('    pthread_join(t, NULL);\n    printf','    pthread_join(t, NULL);\n    sem_destroy(&sem_x);\n    printf')
        raw,dag,frozen,st,sd,s,dd,meta=self.direct(text,'else_if_sem');self.check_topology(s,dd);rows=self.absorb_rows(s);self.assertEqual(9,len(rows))
        paths={tuple((m['exclusive_group'].split('::')[-1],m['branch_id']) for m in next(r for r in s['segments'] if r['seg_id']==a['projected_src'])['condition_memberships']) for a in rows}
        self.assertEqual(3,len(paths));self.assertEqual({'then','else'},{p[-1][1] for p in paths});self.assertTrue(any(len(p)==2 and p[0][1]=='else' and p[1][1]=='then' for p in paths));self.assertTrue(any(len(p)==2 and p[0][1]=='else' and p[1][1]=='else' for p in paths))
        text=(SOURCES/'nested.c').read_text();text=text.replace('#include <stdio.h>','#include <stdio.h>\n#include <semaphore.h>').replace('int mode,result;','int mode,result;\nstatic sem_t sem_x;')
        old='''        result += 2;
        if (((i + mode) % 2) == 0) {
            result += 3;
            if (((i + mode) % 3) == 0) {
                result += 4;
            } else {
                result += 5;
            }
        } else {
            result += 6;
        }'''
        new='''        sem_post(&sem_x);
        result += 2;
        if (((i + mode) % 2) == 0) {
            result += 3;
            if (((i + mode) % 3) == 0) {
                sem_wait(&sem_x);
                result += 4;
            } else {
                result += 5;
            }
        } else {
            sem_wait(&sem_x);
            result += 6;
        }'''
        self.assertIn(old,text);text=text.replace(old,new,1).replace('    pthread_create(&t, NULL, worker, NULL);','    sem_init(&sem_x, 0, 0);\n    pthread_create(&t, NULL, worker, NULL);').replace('    pthread_join(t, NULL);\n    printf','    pthread_join(t, NULL);\n    sem_destroy(&sem_x);\n    printf')
        raw,dag,frozen,st,sd,s,dd,meta=self.direct(text,'nested_sem');self.check_topology(s,dd);rows=self.absorb_rows(s);self.assertEqual(6,len(rows))
        paths={tuple((m['exclusive_group'].split('::')[-1],m['branch_id']) for m in next(r for r in s['segments'] if r['seg_id']==a['projected_src'])['condition_memberships']) for a in rows}
        self.assertIn((('COND:worker#001','then'),('COND:worker#002','then')),paths);self.assertIn((('COND:worker#001','else'),),paths);self.assertNotIn((('COND:worker#001','then'),('COND:worker#002','else')),paths)
    def test_ambiguous_sem_branch_ranges_reject(self):
        from types import SimpleNamespace
        from Code2DAG.pipeline.composition_c5d_model import _project_internal_sem_edges
        clean='sem_post(&s);\nsem_wait(&s);\n';raw={'segments':[{'seg_id':'R','function':'f','start_line':1,'end_line':2}]}
        execution=[dict(seg_id='S',kind='compute',function='f',start_line=1,end_line=2,loop_iteration_id='I',source_segments=['R'],condition_memberships=[{'exclusive_group':'C','branch_id':'then'}])]
        regions=[{'condition_id':'C','source_condition_id':'C','branches':[{'branch_id':'then','start_line':1,'end_line':2},{'branch_id':'else','start_line':1,'end_line':2}]}]
        branches=[SimpleNamespace(branch_id='then',start_offset=0,end_offset=100),SimpleNamespace(branch_id='else',start_offset=0,end_offset=100)]
        with self.assertRaises(Exception):_project_internal_sem_edges([{'src':'R','dst':'R','kind':'sem_dep'}],execution,raw,clean,regions,[SimpleNamespace(condition_id='C',branches=branches)])
    def zero_owner_source(self,loop,n,m,first_lock=True,second_lock=True,same_mutex=False):
        if loop=='L3':
            text=(SOURCES/'l3.c').read_text().replace('i < 2','i < '+str(n)).replace('j < 3','j < '+str(m));return text
        text=(SOURCES/'l2.c').read_text().replace('i < 2','i < '+str(n)).replace('j < 3','j < '+str(m))
        first='''        result += 1;
        pthread_mutex_lock(&m1);
        result += 2;
        if (((i + mode) % 2) == 0) {
            result += 3;
        } else {
            result += 4;
        }
        result += 5;
        pthread_mutex_unlock(&m1);
        result += 6;'''
        second='''        result += 1;
        pthread_mutex_lock(&m2);
        result += 2;
        if (((j + mode) % 2) == 0) {
            result += 3;
        } else {
            result += 4;
        }
        result += 5;
        pthread_mutex_unlock(&m2);
        result += 6;'''
        if not first_lock:text=text.replace(first,'        result += 1;\n        result += 6;',1)
        if not second_lock:text=text.replace(second,'        result += 1;\n        result += 6;',1)
        if same_mutex:text=text.replace('&m2','&m1')
        return text
    def test_l2_l3_zero_owner_instances_and_nonzero_control(self):
        cases=[('l2_second_zero','L2',1,0,False,True,0),('l2_first_zero','L2',0,1,True,False,0),('l2_both_zero','L2',0,0,False,True,0),('l2_control','L2',1,1,False,True,1),('l2_independent','L2',1,1,True,True,2),('l2_same_mutex','L2',1,1,True,True,2),('l3_outer_zero','L3',0,1,True,True,0),('l3_inner_zero','L3',1,0,True,True,0),('l3_both_zero','L3',0,0,True,True,0),('l3_control','L3',1,1,True,True,1),('l3_many','L3',2,3,True,True,6)]
        for name,loop,n,m,first,second,expected in cases:
            with self.subTest(name=name):
                text=self.zero_owner_source(loop,n,m,first,second,same_mutex=(name=='l2_same_mutex'));self.assertEqual(RULE,select_rule(text))
                raw,dag,frozen,st,sd,s,dd,meta=self.direct(text,name);self.assertEqual(frozen,(raw,dag));self.check_topology(s,dd)
                self.assertEqual(expected,len(s['lock_groups']));self.assertEqual(expected,sum(1 for g in s['lock_groups'] if g['candidate_segment_ids']))
                iterations={i['loop_iteration_id'] for i in s['iteration_instances']}
                templates={t['mutex_id']+'@'+str(t['lock_range'][0]):t for t in s['lock_templates']}
                for group in s['lock_groups']:
                    self.assertTrue(group['candidate_segment_ids']);self.assertIn(group['loop_iteration_id'],iterations)
                    for member in group['members']:
                        key=member['mutex_id']+'@'+str(member['lock_range'][0]);self.assertIn(key,templates);self.assertEqual(templates[key]['loop_id'],group['loop_id'])
                        self.assertIn(group['group_id'],s['mappings']['source_lock_region_to_group'][member['function']+'@'+str(member['lock_range'][0])])
                for loop_rec in s['loops']:
                    if loop_rec['iteration_count']==0:
                        for comp in s['source_component_templates']:
                            if comp['loop_id']==loop_rec['loop_id']:self.assertEqual([],s['mappings']['template_to_execution_segments'][comp['template_id']])
