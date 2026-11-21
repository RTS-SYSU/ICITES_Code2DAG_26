"""C5c: an outer static Loop instantiates an independent Condition per iteration."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition import select_rule,run_composition_blocks
from Code2DAG.pipeline.composition_c5c_model import RULE,build_loop_condition_views
from Code2DAG.pipeline.composition_c5c_timing import import_loop_condition_lock_timing
from Code2DAG.pipeline.composition_model import CONTROL
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort

ROOT=Path(__file__).resolve().parents[2];SOURCES=ROOT/'Code2DAG/source_files/composition_c5c'
ALGOS=c1.ALGOS
# groups, iteration instances, loops, condition regions, source Lock regions,
# source component templates, source-only helper templates, independent Lock regions
FORMS={
 'after_condition':(3,3,1,4,1,2,0,0),'both_branches':(6,3,1,3,2,3,0,0),
 'else':(2,2,1,2,1,2,0,0),'else_if':(6,3,1,6,2,3,0,0),
 'helper':(3,3,1,3,1,2,0,0),'helper_condition':(3,3,1,4,1,2,1,0),
 'l2':(5,5,2,5,2,8,0,0),'l2_zero_first':(3,3,2,3,2,8,0,0),
 'l2_zero_second':(2,2,2,2,2,8,0,0),'l3':(6,6,2,6,1,2,0,0),
 'l3_both_zero':(0,0,2,0,1,2,0,0),'l3_inner_zero':(0,0,2,0,1,2,0,0),
 'l3_outer_zero':(0,0,2,0,1,2,0,0),'main_condition':(3,3,1,3,1,2,0,0),
 'nested_else':(3,3,1,6,1,3,0,0),'nested_inner_else':(3,3,1,6,1,3,0,0),
 'nested_then':(3,3,1,6,1,3,0,0),'no_else':(3,3,1,3,1,3,0,0),
 'one':(1,1,1,1,1,4,0,0),'outside_condition':(3,3,1,4,1,2,0,0),
 'outside_lock':(3,2,1,2,2,4,0,1),'outside_loop':(2,2,2,2,1,4,0,0),
 'same_mutex_branches':(6,3,1,3,2,3,0,0),'sem':(3,3,1,3,1,2,0,0),
 'then':(3,3,1,3,1,4,0,0),'two_locks_same':(2,2,1,2,2,3,0,0),
 'two_mutex':(4,2,1,2,2,3,0,0),'zero':(0,0,1,0,1,4,0,0),
}
EXPECTED={
 'after_condition':(1119,1132,1132),'both_branches':(1115,1112,1112),
 'else':(1114,1104,1104),'else_if':(1106,1112,1121),
 'helper':(1111,1126,1126),'helper_condition':(1119,1132,1132),
 'l2':(1136,1135,1135),'l2_zero_first':(1124,1121,1121),
 'l2_zero_second':(1112,1114,1114),'l3':(1112,1142,1142),
 'l3_both_zero':(1100,1100,1100),'l3_inner_zero':(1100,1100,1100),
 'l3_outer_zero':(1100,1100,1100),'main_condition':(1116,1131,1131),
 'nested_else':(1121,1106,1127),'nested_inner_else':(1121,1127,1106),
 'nested_then':(1106,1121,1127),'no_else':(1118,1100,1100),
 'one':(1106,1107,1107),'outside_condition':(1119,1132,1132),
 'outside_lock':(1126,1128,1128),'outside_loop':(1122,1124,1124),
 'same_mutex_branches':(1115,1112,1112),'sem':(1106,1121,1121),
 'then':(1118,1121,1121),'two_locks_same':(1112,1114,1114),
 'two_mutex':(1112,1114,1114),'zero':(1100,1100,1100),
}
NESTED={'nested_then':2,'nested_else':2,'nested_inner_else':2,'else_if':2}
FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')


class CompositionC5cTests(unittest.TestCase):
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
                                 struct=json.loads((block/'segments_struct.json').read_text()),
                                 dag=json.loads((block/'dag_seg.json').read_text()),
                                 dag_struct=json.loads((block/'dag_struct.json').read_text()))

    @classmethod
    def tearDownClass(cls):cls._tmp.cleanup()

    def built(self,name):return self.cache[name]

    def effective(self,segments,loop_id):
        loops={loop['loop_id']:loop for loop in segments['loops']};count=1;cursor=loops[loop_id]
        while cursor is not None:
            count*=cursor['iteration_count'];parent=cursor.get('parent_loop_id');cursor=loops.get(parent) if parent else None
        return count

    def group_templates(self,s):
        out={}
        for group in s['lock_groups']:
            out[group['group_id']]=list(dict.fromkeys(member['execution_segment']['template_id'] for member in group['members']))
        return out

    def test_condition_instances_namespaces_and_lock_groups(self):
        for name,metrics in FORMS.items():
            with self.subTest(name=name):
                item=self.built(name);s=item['segments'];groups=s['lock_groups'];loops=s['loops'];regions=s['condition_regions']
                ngroups,niters,nloops,nconditions,nlocks,ncomponents,nsource_only,nindependent=metrics
                self.assertEqual(ngroups,len(groups));self.assertEqual(ngroups,len({g['group_id'] for g in groups}))
                self.assertEqual(niters,len(s['iteration_instances']));self.assertEqual(nloops,len(loops))
                self.assertEqual(nconditions,len(regions));self.assertEqual(nlocks,len(s['lock_templates']))
                self.assertEqual(ncomponents,len(s['source_component_templates']))
                self.assertEqual(nsource_only,len(s.get('source_only_segments',[])))
                self.assertEqual(nindependent,len(s.get('independent_lock_regions',[])))
                self.assertFalse(any(r['kind'] in CONTROL for r in s['segments']))
                self.assertEqual(len(item['dag']['nodes']),len(set(item['dag']['nodes'])))
                self.assertFalse(any(e['src']==e['dst'] for e in item['dag']['edges']))
                topo_sort(item['dag']['nodes'],[(e['src'],e['dst']) for e in item['dag']['edges']])
                topo_sort(item['dag_struct']['nodes'],[(e['src'],e['dst']) for e in item['dag_struct']['edges']])
                # Every Condition instance is independently namespaced.
                condition_ids={r['condition_id'] for r in regions}
                for region in regions:
                    if region.get('loop_iteration_id'):
                        self.assertIn(region['loop_iteration_id']+'::',region['condition_id'])
                ids_by_iteration={inst['loop_iteration_id']:{r['condition_id'] for r in regions if r.get('loop_iteration_id')==inst['loop_iteration_id']} for inst in s['iteration_instances']}
                for left in ids_by_iteration:
                    for right in ids_by_iteration:
                        if left!=right:self.assertFalse(ids_by_iteration[left]&ids_by_iteration[right])
                # Every source Condition owns exactly one instance per effective iteration.
                for template in s['condition_templates']:
                    loop_id=template['loop_id'];effective=self.effective(s,loop_id)
                    for source_region in template['condition_regions']:
                        source_id=source_region['source_condition_id']
                        instance_regions=[r for r in regions if r.get('source_condition_id')==source_id and r.get('loop_id')==loop_id]
                        self.assertEqual(effective,len(instance_regions),(name,source_id))
                        self.assertEqual(effective,len(s['mappings']['source_condition_to_instances'][source_id]))
                        self.assertEqual(len(instance_regions),len({r['condition_id'] for r in instance_regions}))
                if name=='zero' or name.startswith('l3_'):
                    self.assertTrue(all(not values for values in s['mappings']['condition_template_node_to_instances'].values()))
                # A source component keeps one legal target per real iteration,
                # and a zero-trip component keeps an explicitly empty list.
                component_by_source={component['source_condition_node_id']:component for component in s['source_component_templates']}
                for component in s['source_component_templates']:
                    effective=self.effective(s,component['loop_id']);targets=s['mappings']['template_to_execution_segments'][component['template_id']]
                    self.assertEqual(effective,len(targets));self.assertEqual(effective,len(s['mappings']['source_component_to_templates'][component['source_condition_node_id']]))
                    if effective==0:self.assertEqual([],targets)
                    else:self.assertTrue(targets)
                # Lock groups never cross an iteration, branch or mutex.
                for group in groups:
                    self.assertTrue(group['member_segment_ids']);self.assertEqual(group['member_segment_ids'],group['member_node_ids'])
                    self.assertEqual(group['member_segment_ids'],s['mappings']['grouped_node_to_member_segments'][group['group_id']])
                    self.assertEqual(group['member_template_ids'],s['mappings']['grouped_node_to_member_templates'][group['group_id']])
                    self.assertEqual(group['member_template_ids'],self.group_templates(s)[group['group_id']])
                    self.assertEqual(len(group['member_segment_ids']),len(s['mappings']['grouped_node_to_member_occurrences'][group['group_id']]))
                    member_rows=[]
                    for member in group['members']:
                        row=member['execution_segment'];member_rows.append(row)
                        self.assertEqual(group['mutex_id'],row.get('mutex_id'))
                        self.assertEqual(group['condition_memberships'],member['condition_memberships'])
                        self.assertEqual(group['condition_memberships'],row.get('condition_memberships'))
                        if row.get('role')=='iteration':self.assertEqual(group['group_id'],s['mappings']['iteration_to_group'][row['seg_id']])
                        self.assertEqual([group['group_id']],s['mappings']['structural_node_to_execution_segments'][row['seg_id']])
                        for sid in row.get('source_segments',[]):
                            self.assertIn(group['group_id'],s['mappings']['original_segment_to_execution_segments'][sid])
                        for node in row.get('generation_origin') or []:
                            self.assertIn(group['group_id'],s['mappings']['generation_node_to_execution_segments'][node])
                        for key in row.get('source_position_ids',[]):
                            self.assertIn(group['group_id'],s['mappings']['source_position_to_execution_segments'][key])
                    if group.get('loop_iteration_id'):
                        self.assertEqual({row['loop_iteration_id'] for row in member_rows},{group['loop_iteration_id']})
                        for row in member_rows:
                            for membership in row.get('condition_memberships',[]):
                                self.assertIn(membership['exclusive_group'],ids_by_iteration[group['loop_iteration_id']])
                    if group['condition_memberships']:
                        for membership in group['condition_memberships']:
                            self.assertIn(membership['exclusive_group'],condition_ids)
                            self.assertIn(membership['exclusive_group']+':'+membership['branch_id'],item['dag_struct']['exclusive_groups'][membership['exclusive_group']])
                    else:
                        self.assertFalse(group.get('loop_iteration_id'))
                # Every source Lock region points at its concrete group(s), and
                # N=0 keeps the legal empty list.
                source_lock=dict(s['mappings']['source_lock_region_to_group'])
                self.assertEqual(source_lock,s['mappings']['source_lock_region_to_instances'])
                self.assertEqual(set(source_lock),{lock.get('region_id',lock['function']+'@'+str(lock['lock_range'][0])) for lock in s['lock_templates']})
                for key,targets in source_lock.items():
                    self.assertTrue(all(target in {g['group_id'] for g in groups} for target in targets))
                self.assertEqual(set(item['dag_struct']['exclusive_groups']),{r['condition_id'] for r in regions})
                if name in NESTED:
                    if name=='else_if':
                        self.assertEqual(sorted({len(g['condition_memberships']) for g in groups}),[1,2])
                        self.assertTrue({tuple(m['branch_id'] for m in g['condition_memberships']) for g in groups}>={('then',),('else','then')})
                    else:
                        self.assertTrue(all(len(g['condition_memberships'])==NESTED[name] for g in groups))
                if name=='nested_inner_else':
                    self.assertTrue(all(tuple(m['branch_id'] for m in g['condition_memberships'])==('else','else') for g in groups))
                if name=='same_mutex_branches':
                    paths={tuple((m['exclusive_group'],m['branch_id']) for m in g['condition_memberships']) for g in groups}
                    self.assertTrue(any(path[-1][1]=='then' for path in paths));self.assertTrue(any(path[-1][1]=='else' for path in paths))
                if name=='both_branches':
                    self.assertEqual({'m1','m2'},{g['mutex_id'] for g in groups})
                if name=='two_mutex':
                    self.assertEqual({'m1','m2'},{g['mutex_id'] for g in groups})
                    self.assertEqual({1}, {len(g['members']) for g in groups})
                if name=='two_locks_same':
                    self.assertEqual([2,2],sorted(len(g['members']) for g in groups))
                if name in ('then','both_branches','same_mutex_branches'):
                    for instance in s['iteration_instances']:
                        paths={tuple(m['branch_id'] for m in row['condition_memberships']) for row in s['segments']
                               if row.get('loop_iteration_id')==instance['loop_iteration_id'] and row.get('condition_memberships')}
                        self.assertIn(('then',),paths);self.assertIn(('else',),paths)
                if name=='helper':
                    self.assertEqual(['main','task'],s['thread_entry_functions']);self.assertEqual('worker',loops[0]['function'])
                if name=='main_condition':self.assertEqual('main',loops[0]['function'])
                if name=='helper_condition':self.assertEqual('worker',loops[0]['function'])

    def test_external_components_helpers_and_no_false_dependencies(self):
        for name in ('outside_condition','after_condition','helper','helper_condition','outside_lock','outside_loop','sem'):
            with self.subTest(name=name):
                item=self.built(name);s=item['segments'];groups=s['lock_groups'];loops=s['loops'];regions=s['condition_regions']
                if name in ('outside_condition','after_condition'):
                    external=[r for r in regions if not r.get('loop_iteration_id') and r['condition_id'].startswith('COND:')]
                    self.assertEqual(1,len(external))
                    self.assertTrue(any(r.get('function')=='main' for r in external) if name=='outside_condition' else any(r.get('function')=='worker' for r in external))
                    self.assertTrue(any(r.get('condition_memberships') for r in s['segments'] if r['function']==external[0]['function']))
                if name=='helper_condition':
                    self.assertEqual({'C5CHELPER:observer'},set(s['mappings']['source_only_segment_to_structural_nodes']))
                    self.assertFalse(any(row['function']=='observer' for row in s['segments']))
                    self.assertTrue(any(row.get('source_only') for row in item['struct']['segments'] if row['function']=='observer'))
                    observer=[r for r in regions if r['function']=='observer'];self.assertEqual(1,len(observer));self.assertFalse(observer[0].get('loop_iteration_id'))
                if name=='outside_lock':
                    self.assertEqual(1,len(s['independent_lock_regions']));self.assertEqual(1,len([g for g in groups if not g['condition_memberships']]))
                    self.assertTrue(all(g['condition_memberships'] for g in groups if g['group_id']!='LOCKGROUP:m2'))
                if name=='outside_loop':
                    self.assertEqual(2,len(loops))
                    plain=min(loops,key=lambda loop:loop['header_line'])['loop_id']
                    self.assertEqual(1,len([loop for loop in loops if loop['loop_id'] in {g.get('loop_id') for g in groups}]))
                    self.assertTrue(all(inst['loop_id']!=plain for inst in s['iteration_instances']))
                    self.assertFalse([g for g in groups if g.get('loop_id')==plain])
                if name=='sem':self.assertTrue({'create','join','sem_dep'} & {edge['kind'] for edge in item['dag']['edges']})
                # Mutually exclusive branch groups are never connected in either direction.
                for left in groups:
                    for right in groups:
                        if left is right or not left['condition_memberships'] or not right['condition_memberships']:continue
                        lm={m['exclusive_group']:m['branch_id'] for m in left['condition_memberships']}
                        rm={m['exclusive_group']:m['branch_id'] for m in right['condition_memberships']}
                        if any(lm[key]!=rm[key] for key in lm.keys()&rm.keys()):
                            self.assertFalse(c1.reachable(item['dag'],left['group_id'],right['group_id']))
                            self.assertFalse(c1.reachable(item['dag'],right['group_id'],left['group_id']))
                # A group never spans two iteration identities or two mutexes.
                for group in groups:
                    self.assertEqual(1,len({member['execution_segment'].get('loop_iteration_id') for member in group['members']}))
                    self.assertEqual(1,len({member['execution_segment'].get('mutex_id') for member in group['members']}))
                self.assertTrue(all(edge.get('provenance') for edge in item['dag']['edges']))
                self.assertEqual(len(s['composition_edge_provenance']),len(s['composition_edge_provenance']))

    def test_runtime_timing_and_five_algorithms(self):
        for name,modes in EXPECTED.items():
            with self.subTest(name=name):
                item=self.built(name);source=item['source'];before=source.read_bytes();s=item['segments'];groups={g['group_id']:g for g in s['lock_groups']}
                weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(s['templates'])}
                inp=self._root/(name+'-weights.json');inp.write_text(json.dumps({'weights':weights}))
                timing=import_loop_condition_lock_timing(base_dir=item['base'],base_name=name,input_timing=inp)
                self.assertEqual(set(weights),{t['template_id'] for t in s['templates']})
                self.assertEqual({row['seg_id'] for row in s['segments']},set(timing['weights']))
                for row in s['segments']:
                    if row['kind']=='lock_group':
                        expected={field:sum(weights[member['execution_segment']['template_id']][field] for member in groups[row['seg_id']]['members']) for field in FIELDS}
                        self.assertEqual(expected,timing['weights'][row['seg_id']])
                    else:self.assertEqual(weights[row['template_id']],timing['weights'][row['seg_id']])
                for algo in ALGOS:
                    schedule=run_schedule(base_dir=item['base'],base_name=name,level='level2',rule_name=RULE,algo_name=algo)
                    self.assertEqual(set(item['dag']['nodes']),set(schedule['priorities']))
                binary=self._root/(name+'-program');self.h.command(['gcc-13','-pthread','-std=c11',source,'-o',binary])
                for argc,value in enumerate(modes):
                    self.assertEqual('RESULT:'+str(value)+'\n',self.h.command([binary,*(['x']*argc)]).stdout)
                self.assertEqual(before,source.read_bytes())

    def test_zero_trip_template_mappings_and_no_instances(self):
        for name in ('zero','l3_outer_zero','l3_inner_zero','l3_both_zero','l2_zero_first','l2_zero_second'):
            with self.subTest(name=name):
                item=self.built(name);s=item['segments'];groups=s['lock_groups'];zero_loops=[]
                for loop in s['loops']:
                    effective=self.effective(s,loop['loop_id'])
                    if effective==0:
                        zero_loops.append(loop['loop_id'])
                        self.assertFalse([g for g in groups if g.get('loop_id')==loop['loop_id']])
                        self.assertFalse([i for i in s['iteration_instances'] if i.get('loop_id')==loop['loop_id']])
                        self.assertFalse([r for r in s['condition_regions'] if r.get('loop_id')==loop['loop_id']])
                    for component in [c for c in s['source_component_templates'] if c['loop_id']==loop['loop_id']]:
                        targets=s['mappings']['template_to_execution_segments'][component['template_id']]
                        if effective==0:self.assertEqual([],targets)
                        else:self.assertEqual(effective,len(targets))
                self.assertTrue(zero_loops)
                weights={t['template_id']:dict(total_ns=1,count=1,avg_ns=1,min_ns=1,max_ns=1) for t in s['templates']}
                inp=self._root/(name+'-zero-weights.json');inp.write_text(json.dumps({'weights':weights}))
                timing=import_loop_condition_lock_timing(base_dir=item['base'],base_name=name,input_timing=inp)
                self.assertEqual({row['seg_id'] for row in s['segments']},set(timing['weights']))
                zero_groups={group['group_id'] for group in groups if group.get('loop_id') in zero_loops}
                self.assertFalse(zero_groups & set(timing['weights']))
                if name in ('zero','l3_outer_zero','l3_inner_zero','l3_both_zero'):
                    self.assertFalse(any(sid.startswith('LOCKGROUP:') for sid in timing['weights']))

    def test_timing_rejection_and_no_partial_publication(self):
        item=self.built('then');base=item['base'];block=item['block'];sp=block/'segments.json';original=sp.read_bytes();s=json.loads(original)
        output=base/'intermediate_results'/'then'/'pipeline/timing/level2'/RULE/'timing.json'
        weights={t['template_id']:dict(total_ns=i+1,count=1,avg_ns=i+1,min_ns=i+1,max_ns=i+1) for i,t in enumerate(s['templates'])}
        inp=self._root/'binding-weights.json';inp.write_text(json.dumps({'weights':weights}));legal_input=inp.read_bytes()
        timing=import_loop_condition_lock_timing(base_dir=base,base_name='then',input_timing=inp);published=output.read_bytes()
        damages=[]
        bad=copy.deepcopy(s);bad['lock_groups'][0]['condition_memberships'][0]['branch_id']='forged';damages.append(bad)
        bad=copy.deepcopy(s);bad['segments'][0]['template_id']='C5CTPL:forged';damages.append(bad)
        bad=copy.deepcopy(s);component=bad['source_component_templates'][0];bad['mappings']['template_to_execution_segments'][component['template_id']]=['unknown'];damages.append(bad)
        bad=copy.deepcopy(s);bad['iteration_instances'][0]['iteration_index']=True;damages.append(bad)
        bad=copy.deepcopy(s);bad['lock_groups'][0]['member_segment_ids']=[];damages.append(bad)
        for bad in damages:
            sp.write_text(json.dumps(bad))
            with self.assertRaises(ValueError):import_loop_condition_lock_timing(base_dir=base,base_name='then',input_timing=inp)
            self.assertEqual(published,output.read_bytes())
        sp.write_bytes(original)
        for data in [{'weights':{}},{'weights':{**weights,'forged':next(iter(weights.values()))}},{'weights':{**weights,next(iter(weights)):{**next(iter(weights.values())),'count':1.0}}}]:
            inp.write_text(json.dumps(data))
            with self.assertRaises(ValueError):import_loop_condition_lock_timing(base_dir=base,base_name='then',input_timing=inp)
            self.assertEqual(published,output.read_bytes())
        inp.write_bytes(legal_input)
        self.assertEqual(timing,import_loop_condition_lock_timing(base_dir=base,base_name='then',input_timing=inp))
        self.assertEqual(published,output.read_bytes());self.assertEqual(original,sp.read_bytes())

    def test_nested_external_condition_namespaces_and_exclusive_groups(self):
        text=(SOURCES/'after_condition.c').read_text().replace('''    if (result % 2) {
        result += 11;
    } else {
        result += 13;
    }''','''    if (result % 2) {
        if (mode > 1) {
            result += 11;
        } else {
            result += 12;
        }
    } else {
        result += 13;
    }''')
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'nested_external.c';source.write_text(text)
            self.assertEqual(RULE,select_rule(text));base,p=self.h.native(tmp,source);run_composition_blocks(base_dir=base,base_name='nested_external',source_file=source)
            block=p/'blocks/level2'/RULE;s=json.loads((block/'segments.json').read_text());st=json.loads((block/'segments_struct.json').read_text());d=json.loads((block/'dag_struct.json').read_text())
            external=sorted([r for r in s['condition_regions'] if r['function']=='worker' and not r.get('loop_iteration_id')],key=lambda r:r['entry_line'])
            self.assertEqual(2,len(external));root,child=external
            self.assertEqual(root['condition_id'],child['parent_id']);self.assertIsNone(root['parent_id'])
            self.assertEqual({root['condition_id'],child['condition_id']},set(k for k in d['exclusive_groups'] if k in {root['condition_id'],child['condition_id']}))
            for region in external:
                self.assertEqual(2,len(d['exclusive_groups'][region['condition_id']]))
                self.assertTrue(all(value.startswith(region['condition_id']+':') for value in d['exclusive_groups'][region['condition_id']]))
            for row in st['segments']:
                if row['function']=='worker' and row.get('branch_id') and 'iteration:' not in row['seg_id']:
                    condition=row['branch_id'].split(':')[0]+':'+row['branch_id'].split(':')[1]
                    self.assertIn(condition,{root['condition_id'],child['condition_id']})

    def test_custom_node_and_edge_fields_are_preserved_without_mutation(self):
        item=self.built('then');text=item['text']
        from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
        raw,dag=build_level2_segments_and_dag(base_dir=item['base'],base_name='then',source_file=item['source'])
        base_row=next(row for row in raw['segments'] if row['function']=='worker' and row['kind']!='mutex_cs')
        base_row['custom']={'keep':[1,{'nested':True}]}
        for index,edge in enumerate(dag['edges']):edge['edge_id']='C5CQA:'+str(index);edge['custom']={'keep':[index,{'nested':['edge']}]}
        frozen=copy.deepcopy((raw,dag));st,sd,s,dd,meta=build_loop_condition_views(base_name='then',source_text=text,base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(frozen,(raw,dag))
        self.assertEqual([{'base_edge_index':index,'edge':copy.deepcopy(edge)} for index,edge in enumerate(dag['edges'])],s['composition_edge_provenance'])
        for index,edge in enumerate(dag['edges']):
            matches=[out for out in dd['edges'] if any(w.get('base_edge_index')==index or w.get('edge',{}).get('edge_id')==edge.get('edge_id') for w in out.get('provenance',[]))]
            self.assertTrue(matches,('base edge lost',index))
            for out in matches:
                if out.get('edge_id')==edge['edge_id']:self.assertEqual(edge['custom'],out['custom'])
        self.assertTrue(any(row.get('custom')==base_row['custom'] for row in s['segments']))
        self.assertFalse(any(row.get('custom')==base_row['custom'] for row in st['segments'] if row['kind'] in CONTROL))

    def test_external_condition_with_multiple_source_only_helpers(self):
        text=(SOURCES/'then.c').read_text()
        helpers='''void observer_alpha(int value) {
    if (value % 2) {
        result += 11;
    } else {
        result += 13;
    }
}
void audit_beta(int value) {
    if (value % 3) {
        result += 17;
    } else {
        result += 19;
    }
}
'''
        text=text.replace('void *worker(void *arg) {',helpers+'void *worker(void *arg) {',1)
        text=text.replace('    printf("RESULT:%d\\n", result);','''    if (result % 2) {
        result += 41;
    } else {
        result += 43;
    }
    printf("RESULT:%d\\n", result);''',1)
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'external_helpers.c';source.write_text(text);before=source.read_bytes()
            self.assertEqual(RULE,select_rule(text));base,p=self.h.native(tmp,source);run_composition_blocks(base_dir=base,base_name='external_helpers',source_file=source)
            block=p/'blocks/level2'/RULE;s=json.loads((block/'segments.json').read_text());st=json.loads((block/'segments_struct.json').read_text())
            helper_regions=[r for r in s['condition_regions'] if r['function'] in ('observer_alpha','audit_beta')]
            self.assertEqual(2,len(helper_regions));self.assertTrue(all(not r.get('loop_iteration_id') for r in helper_regions))
            self.assertTrue(any(r['function']=='main' and not r.get('loop_iteration_id') for r in s['condition_regions']))
            source_only=[r for r in st['segments'] if r.get('source_only')]
            self.assertEqual({'observer_alpha','audit_beta'},{r['function'] for r in source_only})
            self.assertFalse(any(r['function'] in ('observer_alpha','audit_beta') for r in s['segments']))
            self.assertEqual({'C5CHELPER:observer_alpha','C5CHELPER:audit_beta'},set(s['mappings']['source_only_segment_to_structural_nodes']))
            self.assertFalse(any(r['function'] in ('observer_alpha','audit_beta') for r in s['templates'] if r.get('source_only')))
            weights={t['template_id']:dict(total_ns=1,count=1,avg_ns=1,min_ns=1,max_ns=1) for t in s['templates']}
            inp=self._root/'external-helper-weights.json';inp.write_text(json.dumps({'weights':weights}))
            output=base/'intermediate_results'/'external_helpers'/'pipeline/timing/level2'/RULE/'timing.json'
            timing=import_loop_condition_lock_timing(base_dir=base,base_name='external_helpers',input_timing=inp);published=output.read_bytes()
            self.assertFalse(any(key in timing['weights'] for key in s['mappings']['source_only_segment_to_structural_nodes']))
            inp.write_text(json.dumps({'weights':{**weights,'C5CHELPER:observer_alpha':dict(total_ns=1,count=1,avg_ns=1,min_ns=1,max_ns=1)}}))
            with self.assertRaises(ValueError):import_loop_condition_lock_timing(base_dir=base,base_name='external_helpers',input_timing=inp)
            self.assertEqual(published,output.read_bytes())
            self.assertEqual(before,source.read_bytes())

    def test_malformed_base_witness_and_opaque_provenance_contract(self):
        item=self.built('then')
        from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
        raw,dag=build_level2_segments_and_dag(base_dir=item['base'],base_name='then',source_file=item['source'])
        base_edge=copy.deepcopy(dag['edges'][0]);opaque1={'qa':{'nested':[1,True,None]},'order':['first']};opaque2={'qa':{'nested':['second']}}
        formal={'base_edge_index':0,'edge':copy.deepcopy(base_edge)};formal['edge']['provenance']=[copy.deepcopy(opaque1)]
        dag['edges'][0]['provenance']=[copy.deepcopy(opaque1),copy.deepcopy(formal),copy.deepcopy(opaque2)]
        frozen=copy.deepcopy((raw,dag));st,sd,s,dd,meta=build_loop_condition_views(base_name='then',source_text=item['text'],base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(frozen,(raw,dag))
        self.assertEqual([opaque1,formal,opaque2],s['composition_edge_provenance'][0]['edge']['provenance'])
        bads=[]
        for name,value in [
            ('bool_index',[{'base_edge_index':True,'edge':copy.deepcopy(base_edge)}]),
            ('negative_index',[{'base_edge_index':-1,'edge':copy.deepcopy(base_edge)}]),
            ('out_of_range',[{'base_edge_index':len(dag['edges']),'edge':copy.deepcopy(base_edge)}]),
            ('missing_edge',[{'base_edge_index':0}]),
            ('non_dict_edge',[{'base_edge_index':0,'edge':[]}]),
            ('missing_edge_field',[{'base_edge_index':0,'edge':{'src':base_edge['src'],'dst':base_edge['dst']}}]),
            ('bad_endpoint',[{'base_edge_index':0,'edge':{'src':'UNKNOWN','dst':base_edge['dst'],'kind':'intra'}}]),
            ('string_item',['opaque']),('number_item',[1]),('bool_item',[True]),('list_item',[[]]),
            ('non_list_container','bad'),
        ]:
            bad=copy.deepcopy((raw,dag));bad[1]['edges'][0]['provenance']=value;bads.append((name,bad))
        for name,bad in bads:
            snapshot=copy.deepcopy(bad)
            with self.assertRaises(ValueError,msg=name):build_loop_condition_views(base_name='then',source_text=item['text'],base_segments_json=bad[0],base_dag_json=bad[1])
            self.assertEqual(snapshot,bad,msg=name)

    def test_internal_sem_projection_l1_l3_and_zero(self):
        from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
        h=self.h
        def leaf_edges(edge):
            for item in edge.get('provenance',[]):
                inner=item.get('edge') if isinstance(item,dict) else None
                if isinstance(inner,dict) and inner.get('provenance'):yield from leaf_edges(inner)
                else:yield item
        def sem_source(name):
            text=(SOURCES/name).read_text()
            text=text.replace('#include <stdio.h>','#include <stdio.h>\n#include <semaphore.h>')
            text=text.replace('pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;','pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;\nstatic sem_t sem_x;')
            text=text.replace('        pthread_mutex_unlock(&m1);\n        result += 3;','        pthread_mutex_unlock(&m1);\n        sem_post(&sem_x);\n        sem_wait(&sem_x);\n        result += 3;')
            text=text.replace('                pthread_mutex_unlock(&m1);\n            } else {','                pthread_mutex_unlock(&m1);\n                sem_post(&sem_x);\n                sem_wait(&sem_x);\n            } else {')
            text=text.replace('    mode = argc - 1;','    mode = argc - 1;\n    sem_init(&sem_x, 0, 0);')
            text=text.replace('    printf("RESULT:%d\\n", result);','    sem_destroy(&sem_x);\n    printf("RESULT:%d\\n", result);')
            return text
        for fixture,expected_sem,expected_groups in [('then.c',3,3),('l3.c',6,6)]:
            with self.subTest(fixture=fixture),tempfile.TemporaryDirectory() as tmp:
                text=sem_source(fixture);source=Path(tmp)/('sem_'+fixture);source.write_text(text);before=source.read_bytes()
                base,p=h.native(tmp,source);raw,dag=build_level2_segments_and_dag(base_dir=base,base_name=source.stem,source_file=source)
                sem_edges=[e for e in dag['edges'] if e['kind']=='sem_dep'];self.assertTrue(sem_edges)
                for i,e in enumerate(sem_edges):e['edge_id']='SEMQA:'+str(i);e['custom']={'keep':[i,{'nested':True}]}
                frozen=copy.deepcopy((raw,dag));st,sd,s,dd,meta=build_loop_condition_views(base_name=source.stem,source_text=text,base_segments_json=raw,base_dag_json=dag)
                self.assertEqual(frozen,(raw,dag));self.assertEqual(before,source.read_bytes())
                self.assertEqual(expected_groups,len(s['lock_groups']))
                fixed=[e for e in dd['edges'] if e['kind']=='sem_dep'];self.assertEqual(expected_sem,len(fixed))
                rows={r['seg_id']:r for r in s['segments']};groups={g['group_id']:g for g in s['lock_groups']}
                for edge in fixed:
                    src_id=groups[edge['src']].get('loop_iteration_id') if edge['src'] in groups else rows[edge['src']].get('loop_iteration_id')
                    dst_id=groups[edge['dst']].get('loop_iteration_id') if edge['dst'] in groups else rows[edge['dst']].get('loop_iteration_id')
                    self.assertEqual(src_id,dst_id);self.assertTrue(src_id)
                    leaves=[(leaf.get('edge') if isinstance(leaf,dict) else leaf) for leaf in leaf_edges(edge)]
                    self.assertTrue(any(isinstance(item,dict) and item.get('edge_id','').startswith('SEMQA:') for item in leaves))
                    for item in leaves:
                        if isinstance(item,dict) and item.get('edge_id','').startswith('SEMQA:'):
                            self.assertEqual({'keep':[int(item['edge_id'].split(':')[1]),{'nested':True}]},item['custom'])
                self.assertEqual([{'base_edge_index':i,'edge':copy.deepcopy(e)} for i,e in enumerate(dag['edges'])],s['composition_edge_provenance'])
        with tempfile.TemporaryDirectory() as tmp:
            text=sem_source('l3.c').replace('i < 2','i < 0').replace('j < 3','j < 0');source=Path(tmp)/'sem_zero.c';source.write_text(text)
            base,p=h.native(tmp,source);raw,dag=build_level2_segments_and_dag(base_dir=base,base_name='sem_zero',source_file=source)
            st,sd,s,dd,meta=build_loop_condition_views(base_name='sem_zero',source_text=text,base_segments_json=raw,base_dag_json=dag)
            self.assertEqual([],s['lock_groups']);self.assertEqual([],s['iteration_instances']);self.assertEqual([],s['condition_regions'])
            self.assertFalse(any(e['kind']=='sem_dep' for e in dd['edges']))

    def test_internal_sem_same_group_and_unsupported_nonintra(self):
        from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
        text=(SOURCES/'two_locks_same.c').read_text()
        text=text.replace('#include <stdio.h>','#include <stdio.h>\n#include <semaphore.h>')
        text=text.replace('pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;','pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;\nstatic sem_t sem_x;')
        text=text.replace('        result += 2;\n        pthread_mutex_unlock(&m1);','        result += 2;\n        sem_post(&sem_x);\n        pthread_mutex_unlock(&m1);')
        text=text.replace('        pthread_mutex_lock(&m1);\n        result += 4;','        pthread_mutex_lock(&m1);\n        sem_wait(&sem_x);\n        result += 4;')
        text=text.replace('    mode = argc - 1;','    mode = argc - 1;\n    sem_init(&sem_x, 0, 0);')
        text=text.replace('    printf("RESULT:%d\\n", result);','    sem_destroy(&sem_x);\n    printf("RESULT:%d\\n", result);')
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'same_group_sem.c';source.write_text(text);before=source.read_bytes();base,p=self.h.native(tmp,source)
            raw,dag=build_level2_segments_and_dag(base_dir=base,base_name='same_group_sem',source_file=source)
            for i,e in enumerate(dag['edges']):
                if e['kind']=='sem_dep':e['edge_id']='SEMGROUP:'+str(i);e['custom']={'keep':i}
            frozen=copy.deepcopy((raw,dag));st,sd,s,dd,meta=build_loop_condition_views(base_name='same_group_sem',source_text=text,base_segments_json=raw,base_dag_json=dag)
            self.assertEqual(frozen,(raw,dag));self.assertEqual(before,source.read_bytes())
            self.assertEqual(2,len(s['lock_groups']));self.assertEqual([2,2],sorted(len(g['members']) for g in s['lock_groups']))
            self.assertFalse(any(e['kind']=='sem_dep' for e in dd['edges']))
            removed=[item['edge'] for item in dd.get('removed_internal_edges',[]) if item['edge'].get('kind')=='sem_dep']
            self.assertEqual(2,len(removed));self.assertTrue(all(edge['edge_id'].startswith('SEMGROUP:') and edge.get('custom')=={'keep':int(edge['edge_id'].split(':')[1])} for edge in removed))
            self.assertFalse(any(edge['src']==edge['dst'] for edge in dd['edges']))
            text2=text.replace('#include <semaphore.h>','').replace('static sem_t sem_x;','').replace('    sem_init(&sem_x, 0, 0);\n','').replace('        sem_post(&sem_x);\n','').replace('        sem_wait(&sem_x);\n','').replace('    sem_destroy(&sem_x);\n','')
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'unsupported_nonintra.c';source.write_text(text2);base,p=self.h.native(tmp,source);raw,dag=build_level2_segments_and_dag(base_dir=base,base_name='unsupported_nonintra',source_file=source)
            target=next(e for e in dag['edges'] if e['src'].startswith('MU:worker') and e['dst'].startswith('MU:worker'))
            target['kind']='create';snapshot=copy.deepcopy((raw,dag))
            with self.assertRaises(ValueError):build_loop_condition_views(base_name='unsupported_nonintra',source_text=text2,base_segments_json=raw,base_dag_json=dag)
            self.assertEqual(snapshot,(raw,dag))

    def test_invalid_nesting_partial_locks_and_boundaries_are_rejected(self):
        base_text=(SOURCES/'then.c').read_text();nested_text=(SOURCES/'nested_then.c').read_text();cases={}
        cases['lock_crosses_condition_branch']=base_text.replace('''        result += 2;
        pthread_mutex_unlock(&m1);
        result += 3;
    } else {
        result += 7;
    }''','''        result += 2;
        result += 3;
    } else {
        result += 7;
    }
    pthread_mutex_unlock(&m1);''')
        cases['partial_lock']=base_text.replace('        pthread_mutex_unlock(&m1);\n','',1)
        cases['nested_lock']=base_text.replace('''        pthread_mutex_lock(&m1);
        result += 2;''','''        pthread_mutex_lock(&m1);
        pthread_mutex_lock(&m2);
        result += 2;
        pthread_mutex_unlock(&m2);''')
        cases['cross_function_lock']=base_text.replace('        pthread_mutex_unlock(&m1);\n','',1).replace('    pthread_join(t, NULL);','    pthread_join(t, NULL);\n    pthread_mutex_unlock(&m1);')
        cases['cross_thread_lock']='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode;
int result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
void *worker(void *arg) {
    result += 100;
    for (int i = 0; i < 3; i++) {
        if (mode == 0) {
            pthread_mutex_lock(&m1);
            result += 2;
        } else {
            result += 7;
        }
    }
    result += 1000;
    return NULL;
}
void *task(void *arg) {
    pthread_mutex_unlock(&m1);
    result += 5;
    return NULL;
}
int main(int argc, char **argv) {pthread_t t,u;mode=argc-1;pthread_create(&t,NULL,worker,NULL);pthread_create(&u,NULL,task,NULL);pthread_join(t,NULL);pthread_join(u,NULL);printf("RESULT:%d\\n",result);return 0;}
'''
        cases['loop_inside_condition']=base_text.replace('''        result += 1;
        pthread_mutex_lock(&m1);''','''        result += 1;
        for (int k = 0; k < 2; k++) {
            result += 4;
        }
        pthread_mutex_lock(&m1);''')
        cases['third_level_loop']=base_text.replace('''    for (int i = 0; i < 3; i++) {
    if (mode == 0) {''','''    for (int j = 0; j < 2; j++) {
    for (int k = 0; k < 2; k++) {
    for (int i = 0; i < 3; i++) {
    if (mode == 0) {''').replace('''    }
    result += 1000;''','''    }
    }
    }
    result += 1000;''')
        cases['condition_contains_loop_not_loop_body']='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode;
int result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
void *worker(void *arg) {
    result += 100;
    if (mode == 0) {
        for (int i = 0; i < 3; i++) {
            pthread_mutex_lock(&m1);
            result += 2;
            pthread_mutex_unlock(&m1);
        }
    } else {
        result += 7;
    }
    result += 1000;
    return NULL;
}
int main(int argc, char **argv) {pthread_t t;mode=argc-1;pthread_create(&t,NULL,worker,NULL);pthread_join(t,NULL);printf("RESULT:%d\\n",result);return 0;}
'''
        cases['loop_inside_lock']=base_text.replace('''    for (int i = 0; i < 3; i++) {
    if (mode == 0) {''','''    pthread_mutex_lock(&m2);
    for (int i = 0; i < 3; i++) {
    if (mode == 0) {''').replace('''    }
    result += 1000;''','''    }
    pthread_mutex_unlock(&m2);
    result += 1000;''')
        cases['three_nested_conditions']=nested_text.replace('''    if (mode < 2) {
        if (mode == 0) {''','''    if (mode < 2) {
        if (mode < 3) {
        if (mode == 0) {''').replace('''        } else {
            result += 7;
        }
    } else {''','''        } else {
            result += 7;
        }
        }
    } else {''')
        cases['lock_without_condition']='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
void *worker(void *arg) {
    result += 100;
    for (int i = 0; i < 3; i++) {
        pthread_mutex_lock(&m1);
        result += 2;
        pthread_mutex_unlock(&m1);
    }
    result += 1000;
    return NULL;
}
int main(void) {pthread_t t;pthread_create(&t,NULL,worker,NULL);pthread_join(t,NULL);printf("RESULT:%d\\n",result);return 0;}
'''
        for name,text in cases.items():
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                source=Path(tmp)/(name+'.c');source.write_text(text);before=source.read_bytes()
                base,p=self.h.native(tmp,source)
                from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
                raw,dag=build_level2_segments_and_dag(base_dir=base,base_name=name,source_file=source);snapshot=copy.deepcopy((raw,dag))
                with self.assertRaises(ValueError):build_loop_condition_views(base_name=name,source_text=text,base_segments_json=raw,base_dag_json=dag)
                self.assertEqual(snapshot,(raw,dag));self.assertEqual(before,source.read_bytes())
                try:selected=select_rule(text)
                except ValueError:selected=None
                if selected==RULE:
                    with self.assertRaises(ValueError):run_composition_blocks(base_dir=base,base_name=name,source_file=source)
                    self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists())

    def test_selector_priority_does_not_misdispatch_old_compositions(self):
        expected={'composition_c1':'composition_c1','composition_c2a':'composition_c2a','composition_c2b':'composition_c2b',
                  'composition_c3a':'composition_c3a','composition_c3b':'composition_c3b','composition_c4a':'composition_c4a',
                  'composition_c4b':'composition_c4b','composition_c5a':'composition_c5a','composition_c5b':'composition_c5b'}
        for group,rule in expected.items():
            for source in sorted((ROOT/'Code2DAG/source_files'/group).glob('*.c')):
                self.assertEqual(rule,select_rule(source.read_text()),(group,source.name))
        for source in sorted(SOURCES.glob('*.c')):self.assertEqual(RULE,select_rule(source.read_text()),source.name)


if __name__=='__main__':unittest.main()
