"""C5e: one outer Lock critical region contains a Condition whose leaf owns Loops."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.tests import test_composition_c4a as c4a
from Code2DAG.pipeline.composition import select_rule,run_composition_blocks
from Code2DAG.pipeline.composition_c5e_model import RULE,build_lock_condition_loop_views
from Code2DAG.pipeline.composition_c5e_timing import import_lock_condition_loop_timing
from Code2DAG.pipeline.composition_c5d_model import RULE as C5D_RULE
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort

ROOT=Path(__file__).resolve().parents[2];SOURCES=ROOT/'Code2DAG/source_files/composition_c5e';ALGOS=c1.ALGOS
FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')

class CompositionC5eTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp=tempfile.TemporaryDirectory();cls._root=Path(cls._tmp.name);cls.h=c1.CompositionC1Tests();cls.cache={}
        for source in sorted(SOURCES.glob('*.c')):
            name=source.stem;text=source.read_text();assert select_rule(text)==RULE,(name,select_rule(text));root=cls._root/name;root.mkdir();base,p=cls.h.native(root,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source);b=p/'blocks/level2'/RULE
            cls.cache[name]=dict(source=source,text=text,base=base,p=p,block=b,segments=json.loads((b/'segments.json').read_text()),struct=json.loads((b/'segments_struct.json').read_text()),dag=json.loads((b/'dag_seg.json').read_text()),dag_struct=json.loads((b/'dag_struct.json').read_text()))
    @classmethod
    def tearDownClass(cls):cls._tmp.cleanup()
    def test_candidates_lock_groups_condition_memberships_and_zero(self):
        for name,item in self.cache.items():
            with self.subTest(name=name):
                s=item['segments'];d=item['dag'];groups=s['lock_groups'];candidates=[r for r in s['segments'] if r['kind']=='lock_path_candidate'];target_groups=[g for g in groups if g.get('candidate_segment_ids')]
                self.assertGreaterEqual(len(target_groups),1);self.assertEqual({r['seg_id'] for r in candidates},{cid for g in target_groups for cid in g['candidate_segment_ids']})
                for g in target_groups:self.assertTrue(g['candidate_segment_ids'])
                self.assertTrue(all(g.get('candidate_segment_ids') or g.get('member_segment_ids') for g in groups))
                self.assertTrue(all(r['member_template_ids'] for r in candidates));self.assertTrue(all(r['condition_memberships'] for r in candidates))
                self.assertEqual(len(d['nodes']),len(set(d['nodes'])));self.assertEqual(set(d['nodes']),{r['seg_id'] for r in s['segments']});self.assertFalse(any(e['src']==e['dst'] for e in d['edges']));topo_sort(d['nodes'],[(e['src'],e['dst']) for e in d['edges']])
                target_group_ids={g['group_id'] for g in target_groups};self.assertTrue(all(r.get('lock_group_id') in target_group_ids for r in s['iteration_instances'] if r.get('lock_group_id') is not None))
                # Loop templates occur only in candidates on the loop's branch path.
                loop_by_id={loop['loop_id']:loop for loop in s['loops']}
                for loop in s['loops']:
                    path={(m['exclusive_group'],m['branch_id']) for m in loop.get('condition_memberships',[])}
                    if not path:continue
                    self.assertTrue(path);tid=loop['loop_id']+':body_template';effective=1;cursor=loop
                    while cursor is not None:effective*=cursor['iteration_count'];cursor=loop_by_id.get(cursor.get('parent_loop_id'))
                    if tid not in {t['template_id'] for t in s['templates']} or effective==0:continue
                    for row in candidates:
                        cand_path={(m['exclusive_group'],m['branch_id']) for m in row['condition_memberships']}
                        if path<=cand_path:self.assertIn(tid,row['member_template_ids'])
                        else:self.assertNotIn(tid,row['member_template_ids'])
                if name in ('then_zero','l3_inner_zero','l3_outer_zero'):
                    self.assertTrue(any(loop['iteration_count']==0 for loop in s['loops']))
                self.assertEqual(len(candidates),sum(len(g['candidate_segment_ids']) for g in target_groups))
                mapping=s['mappings']['template_to_execution_segments'];rows={r['seg_id']:r for r in s['segments']}
                self.assertEqual({t['template_id'] for t in s['templates']},{k for k in mapping})
                for tid,targets in mapping.items():
                    self.assertTrue(all(t in rows for t in targets))
                    for target in targets:
                        row=rows[target]
                        if row['kind']=='lock_path_candidate':self.assertIn(tid,row['member_template_ids'])
                        elif row['kind']=='lock_group':pass
                        else:self.assertEqual(tid,row.get('template_id'))
                for loop in s['loops']:
                    if loop.get('condition_memberships'):continue
                    tid=loop['loop_id']+':body_template'
                    if tid not in mapping:continue
                    if loop['iteration_count']==0:self.assertEqual([],mapping[tid])
                    else:self.assertTrue(mapping[tid]);self.assertTrue(all(rows[t]['loop_id']==loop['loop_id'] for t in mapping[tid]))
    def test_target_and_external_same_mutex_ids_and_mappings(self):
        item=self.cache['external_same_mutex'];source=item['source'];before=source.read_bytes()
        s=item['segments'];st=item['struct'];d=item['dag'];sd=item['dag_struct']
        groups=s['lock_groups'];ids=[g['group_id'] for g in groups]
        self.assertEqual(len(ids),len(set(ids)))
        target=[g for g in groups if g.get('candidate_segment_ids')]
        external=[g for g in groups if g.get('member_segment_ids') and not g.get('candidate_segment_ids')]
        self.assertEqual(1,len(target));self.assertEqual(1,len(external))
        self.assertEqual('m1',target[0]['mutex_id']);self.assertEqual('m1',external[0]['mutex_id'])
        self.assertNotEqual(target[0]['group_id'],external[0]['group_id'])
        target_key=target[0]['members'][0]['function']+'@'+str(target[0]['members'][0]['lock_range'][0])
        external_key=external[0]['members'][0]['function']+'@'+str(external[0]['members'][0]['lock_range'][0])
        smap=s['mappings']['source_lock_region_to_group']
        self.assertEqual([target[0]['group_id']],smap[target_key])
        self.assertEqual([external[0]['group_id']],smap[external_key])
        gmap=s['mappings']['grouped_node_to_member_segments']
        self.assertEqual(target[0]['candidate_segment_ids'],gmap[target[0]['group_id']])
        self.assertEqual(external[0]['member_segment_ids'],gmap[external[0]['group_id']])
        dnodes=set(d['nodes']);gids={g['group_id'] for g in groups}
        self.assertTrue(set(gmap[target[0]['group_id']])<=dnodes)
        self.assertTrue(set(gmap[external[0]['group_id']])<={m['member_segment_id'] for m in external[0]['members']})
        for values in smap.values():self.assertTrue(set(values)<=gids)
        for values in s['mappings']['source_position_to_execution_segments'].values():self.assertTrue(set(values)<=dnodes)
        for graph in (d,sd):
            self.assertEqual(len(graph['nodes']),len(set(graph['nodes'])))
            topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
        target_ids=set(target[0]['candidate_segment_ids']);external_ids=set(external[0]['member_segment_ids'])
        self.assertFalse(any((e['src'] in target_ids and e['dst'] in external_ids) or (e['src'] in external_ids and e['dst'] in target_ids) for e in d['edges']))
        self.assertEqual(before,source.read_bytes())
        binary=self._root/'external_same_mutex-prog'
        self.h.command(['gcc-13','-pthread','-std=c11',source,'-o',binary])
        self.assertEqual('RESULT:1118\n',self.h.command([binary]).stdout)
        self.assertEqual('RESULT:1135\n',self.h.command([binary,'x']).stdout)

    def test_timing_path_sums_five_algorithms_and_native_c(self):
        for name,item in self.cache.items():
            with self.subTest(name=name):
                s=item['segments'];weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])};inp=self._root/(name+'-w.json');inp.write_text(json.dumps({'weights':weights}));timing=import_lock_condition_loop_timing(base_dir=item['base'],base_name=name,input_timing=inp)
                self.assertEqual({r['seg_id'] for r in s['segments']},set(timing['weights']))
                for row in s['segments']:
                    if row['kind']=='lock_path_candidate':expected={f:sum(weights[t][f] for t in row['member_template_ids']) for f in FIELDS}
                    elif row['kind']=='lock_group':
                        group=next(g for g in s['lock_groups'] if g['group_id']==row['seg_id']);tids=[m.get('template_id') or (m.get('execution_segment') or {}).get('template_id') for m in group['members']];expected={f:sum(weights[t][f] for t in tids) for f in FIELDS}
                    else:expected=weights[row['template_id']]
                    self.assertEqual(expected,timing['weights'][row['seg_id']])
                for algo in ALGOS:self.assertEqual(set(item['dag']['nodes']),set(run_schedule(base_dir=item['base'],base_name=name,level='level2',rule_name=RULE,algo_name=algo)['priorities']))
                binary=self._root/(name+'-prog');self.h.command(['gcc-13','-pthread','-std=c11',item['source'],'-o',binary])
                before=item['source'].read_bytes()
                for argc in range(4):self.assertTrue(self.h.command([binary,*(['x']*argc)]).stdout.startswith('RESULT:'))
                self.assertEqual(before,item['source'].read_bytes())
    def test_model_input_custom_edges_and_candidate_ids(self):
        item=self.cache['then'];from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
        raw,dag=build_level2_segments_and_dag(base_dir=item['base'],base_name='then',source_file=item['source'])
        for i,e in enumerate(dag['edges']):e['edge_id']='C5EQA:'+str(i);e['custom']={'keep':[i,{'nested':True}]}
        frozen=copy.deepcopy((raw,dag));st,sd,s,dd,meta=build_lock_condition_loop_views(base_name='then',source_text=item['text'],base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(frozen,(raw,dag));self.assertFalse(any(e['src']==e['dst'] for e in dd['edges']));self.assertTrue(any('C5EQA:' in json.dumps(e.get('provenance',[])) for e in dd['edges']))
        self.assertTrue(all(r['seg_id'].startswith('LOCKPATH:') for r in s['segments'] if r['kind']=='lock_path_candidate'))
    def test_runtime_rejections_and_rule_isolation(self):
        # Direct C2b/C3a must reject this three-layer input; unified entry accepts C5e.
        text=(SOURCES/'then.c').read_text()
        for rule in ('composition_c2b','composition_c3a'):
            with self.subTest(rule=rule),tempfile.TemporaryDirectory() as tmp:
                src=Path(tmp)/'x.c';src.write_text(text);base,p=self.h.native(tmp,src)
                from Code2DAG.pipeline.runner import run_blocks
                with self.assertRaises(ValueError):run_blocks(base_dir=base,base_name='x',source_file=src,level='level2',rule_name=rule)
                self.assertFalse((p/'blocks/level2'/rule/'segments.json').exists())
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'x.c';src.write_text(text);base,p=self.h.native(tmp,src);run_composition_blocks(base_dir=base,base_name='x',source_file=src);self.assertTrue((p/'blocks/level2'/RULE/'segments.json').exists())
    def test_external_metadata_double_and_rejections(self):
        for name in ('external_condition','external_two'):
            s=self.cache[name]['segments'];target={g['group_id'] for g in s['lock_groups'] if g.get('candidate_segment_ids')}
            external=[r for r in s['segments'] if r.get('condition_memberships') and any(m['exclusive_group'].startswith('COND:main') for m in r['condition_memberships'])]
            self.assertTrue(external);self.assertTrue(all(r.get('lock_group_id') not in target for r in external))
            self.assertEqual({r['condition_id'] for r in s['condition_regions']},{'COND:worker#001','COND:main#001'} if name=='external_condition' else {'COND:worker#001','COND:main#001','COND:main#002'})
        for name in ('double','double_same','double_zero'):
            s=self.cache[name]['segments'];targets=[g for g in s['lock_groups'] if g.get('candidate_segment_ids')]
            self.assertEqual(2,len(targets));self.assertEqual(2,len({g['group_id'] for g in targets}));self.assertEqual(2,len(s['condition_regions']))
            for loop in s['loops']:self.assertIn(loop.get('lock_group_id'),{g['group_id'] for g in targets})
        combo=self.cache['external_combo']['segments'];self.assertGreaterEqual(len(combo['lock_groups']),2);self.assertTrue(any(g['mutex_id']=='m2' for g in combo['lock_groups']));self.assertTrue(any(not loop.get('condition_memberships') for loop in combo['loops']))
        text=(SOURCES/'then.c').read_text().replace('    result += 6;\n    pthread_mutex_unlock(&m1);','    result += 6;')
        text=text.replace('        }\n    } else {','            pthread_mutex_unlock(&m1);\n        }\n    } else {',1)
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'partial.c';src.write_text(text);base,p=self.h.native(tmp,src);raw,dag=__import__('Code2DAG.level2.segment_dag_level2',fromlist=['build_level2_segments_and_dag']).build_level2_segments_and_dag(base_dir=base,base_name='partial',source_file=src)
            with self.assertRaises(ValueError):build_lock_condition_loop_views(base_name='partial',source_text=text,base_segments_json=raw,base_dag_json=dag)
        text=(SOURCES/'then.c').read_text().replace('pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;','pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;\npthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;').replace('    result += 2;\n    if (mode == 0) {','    result += 2;\n    pthread_mutex_lock(&m2);\n    if (mode == 0) {').replace('    result += 6;\n    pthread_mutex_unlock(&m1);','    pthread_mutex_unlock(&m2);\n    result += 6;\n    pthread_mutex_unlock(&m1);')
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'nested.c';src.write_text(text);base,p=self.h.native(tmp,src)
            with self.assertRaises(ValueError):run_composition_blocks(base_dir=base,base_name='nested',source_file=src)
            self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists())

    def test_external_loop_template_mapping_and_timing_rejections(self):
        item=self.cache['external_loop'];s=item['segments'];mapping=s['mappings']['template_to_execution_segments'];rows={r['seg_id']:r for r in s['segments']}
        for loop in s['loops']:
            if loop.get('condition_memberships'):continue
            tid=loop['loop_id']+':body_template';self.assertIn(tid,mapping);self.assertTrue(mapping[tid]);self.assertTrue(all(rows[t].get('loop_id')==loop['loop_id'] for t in mapping[tid]))
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'external_loop.c';src.write_text(item['text']);base,p=self.h.native(tmp,src);run_composition_blocks(base_dir=base,base_name='external_loop',source_file=src);b=p/'blocks/level2'/RULE;sp=b/'segments.json';original=sp.read_bytes();s0=json.loads(original);weights={t['template_id']:dict(total_ns=1,count=1,avg_ns=1,min_ns=1,max_ns=1) for t in s0['templates']};inp=Path(tmp)/'w.json';inp.write_text(json.dumps({'weights':weights}));loop=next(l for l in s0['loops'] if not l.get('condition_memberships'));tid=loop['loop_id']+':body_template';out=p/'timing/level2'/RULE/'timing.json'
            for damage in ('missing','empty','wrong','extra'):
                bad=copy.deepcopy(s0);m=bad['mappings']['template_to_execution_segments']
                if damage=='missing':m.pop(tid)
                elif damage=='empty':m[tid]=[]
                elif damage=='wrong':m[tid]=[next(r['seg_id'] for r in bad['segments'] if r['kind']=='lock_path_candidate')]
                else:m['C5E:UNKNOWN']=[]
                sp.write_text(json.dumps(bad));before=sp.read_bytes()
                with self.assertRaises(ValueError):import_lock_condition_loop_timing(base_dir=base,base_name='external_loop',input_timing=inp)
                self.assertFalse(out.exists());self.assertEqual(before,sp.read_bytes());sp.write_bytes(original)
    def test_damaged_provenance_rejection_all_regions(self):
        from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
        for fixture in ('then','external_combo','double'):
            item=self.cache[fixture];raw,dag=build_level2_segments_and_dag(base_dir=item['base'],base_name=fixture,source_file=item['source']);frozen=copy.deepcopy((raw,dag))
            for label,entry in [('index',{'base_edge_index':900,'edge':copy.deepcopy(dag['edges'][-1])}),('negative',{'base_edge_index':-1,'edge':copy.deepcopy(dag['edges'][0])}),('bool',{'base_edge_index':True,'edge':copy.deepcopy(dag['edges'][0])}),('missing_edge',{'base_edge_index':0}),('edge_type',{'base_edge_index':0,'edge':'bad'}),('entry_type','bad')]:
                bad=copy.deepcopy(dag);bad['edges'][-1].setdefault('provenance',[]).append(entry)
                with self.subTest(fixture=fixture,label=label):
                    with self.assertRaises(ValueError):build_lock_condition_loop_views(base_name=fixture,source_text=item['text'],base_segments_json=copy.deepcopy(raw),base_dag_json=bad)
                    self.assertEqual(frozen,(raw,dag))
            mixed=copy.deepcopy(dag);mixed['edges'][-1].setdefault('provenance',[]).extend([{'opaque':[1,{'x':True}]},{'base_edge_index':0,'edge':copy.deepcopy(dag['edges'][0])},{'keep':{'n':2}}]);frozen_mixed=copy.deepcopy((raw,mixed))
            st,sd,s,dd,meta=build_lock_condition_loop_views(base_name=fixture,source_text=item['text'],base_segments_json=raw,base_dag_json=mixed);self.assertEqual(frozen_mixed,(raw,mixed));self.assertFalse(any(e['src']==e['dst'] for e in dd['edges']))
