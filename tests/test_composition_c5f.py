import json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition import select_rule,run_composition_blocks
from Code2DAG.pipeline.composition_c5f_model import RULE,build_lock_loop_condition_views,_c5f_external_view_alias_map,_c5f_rebind_external_view
from Code2DAG.pipeline.composition_c5f_timing import import_lock_loop_condition_timing
from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort
ROOT=Path(__file__).resolve().parents[2];SOURCES=ROOT/'Code2DAG/source_files/composition_c5f';INVALID=ROOT/'Code2DAG/source_files/composition_c5f_invalid';ALGOS=c1.ALGOS;FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')
def member_template(member):
 return member.get('template_id') or (member.get('execution_segment') or {}).get('template_id')
class C5fTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.tmp=tempfile.TemporaryDirectory();cls.root=Path(cls.tmp.name);cls.h=c1.CompositionC1Tests();cls.cache={}
  for source in sorted(SOURCES.glob('*.c')):
   name=source.stem;text=source.read_text();assert select_rule(text)==RULE,(name,select_rule(text));d=cls.root/name;d.mkdir();base,p=cls.h.native(d,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source);b=p/'blocks/level2'/RULE;cls.cache[name]=dict(source=source,text=text,base=base,p=p,block=b,segments=json.loads((b/'segments.json').read_text()),struct=json.loads((b/'segments_struct.json').read_text()),dag=json.loads((b/'dag_seg.json').read_text()))
 @classmethod
 def tearDownClass(cls):cls.tmp.cleanup()
 def test_groups_conditions_instances_and_external(self):
  for name,item in self.cache.items():
   with self.subTest(name=name):
    s=item['segments'];d=item['dag'];groups=s['lock_groups'];cands=[r for r in s['segments'] if r['kind']=='lock_path_candidate'];self.assertFalse(cands)
    self.assertTrue(groups);self.assertEqual(len(d['nodes']),len(set(d['nodes'])));self.assertFalse(any(e['src']==e['dst'] for e in d['edges']));topo_sort(d['nodes'],[(e['src'],e['dst']) for e in d['edges']])
    group_ids={g['group_id'] for g in groups};final_ids={r['seg_id'] for r in s['segments']}
    for g in groups:
     self.assertTrue(g['members']);self.assertTrue(g['member_segment_ids']);self.assertEqual([],g.get('candidate_segment_ids',[]));self.assertTrue(set(g['member_segment_ids'])<=final_ids or g['group_id'] in final_ids)
    for loop in s['loops']:
     if loop.get('lock_group_id'):self.assertIn(loop['lock_group_id'],group_ids)
    for inst in s['iteration_instances']:
     if inst.get('loop_id') in {l['loop_id'] for l in s['loops'] if l.get('lock_group_id')}:self.assertIn(inst.get('lock_group_id'),group_ids)
    for region in s['condition_regions']:
     self.assertTrue(region.get('condition_id'))
     if region.get('loop_iteration_id'):
      self.assertTrue(region['condition_id'].startswith(region['loop_iteration_id']+'::'))
    external_regions=[r for r in s['condition_regions'] if not r.get('loop_iteration_id')]
    external_ids={r['condition_id'] for r in external_regions}
    for row in s['segments']:
     if row.get('condition_memberships'):
      exclusive={m['exclusive_group'] for m in row['condition_memberships']}
      if exclusive & external_ids:
       self.assertNotIn(row.get('lock_group_id'),group_ids)
       self.assertIsNone(row.get('loop_iteration_id'))
       self.assertNotIn(row.get('lock_group_id'),group_ids)
    # External rows are ordinary scheduling rows and never inherit a target
    # Lock/Loop identity.
    for row in s['segments']:
     if row.get('function') in {r['function'] for r in external_regions} and row.get('condition_memberships') and row['seg_id'] not in {m['seg_id'] for g in groups for m in g['members']}:
      self.assertIsNone(row.get('lock_group_id'))
      self.assertIsNone(row.get('loop_iteration_id'))
    if s.get('source_only_segments'):
     helper_functions={r['function'] for r in s['source_only_segments']}
     self.assertTrue(any(r.get('function') in helper_functions for r in external_regions))
     for row in s['source_only_segments']:self.assertTrue(row.get('source_only'));self.assertTrue(row.get('analysis_only'))
     self.assertFalse(any(r.get('function') in helper_functions for r in s['segments']))
    # Formal mapping coverage is restricted to published nodes.
    for key,values in s['mappings']['grouped_node_to_member_segments'].items():self.assertIn(key,group_ids)
    for key,values in s['mappings']['template_to_execution_segments'].items():self.assertTrue(set(values)<=final_ids,(name,key,values))
    zero_template_ids=set();loop_by_id={loop['loop_id']:loop for loop in s['loops']}
    for loop in s['loops']:
     effective=1;cursor=loop;seen=set()
     while cursor is not None:
      self.assertNotIn(cursor['loop_id'],seen);seen.add(cursor['loop_id']);effective*=cursor.get('iteration_count',0);cursor=loop_by_id.get(cursor.get('parent_loop_id'))
     if effective==0:zero_template_ids.add(loop['loop_id']+':body_template')
    for tid,values in s['mappings']['template_to_execution_segments'].items():
     if not values:self.assertIn(tid,zero_template_ids,(name,tid))
    region_ids={r['condition_id'] for r in s['condition_regions']}
    for key,values in s['mappings']['source_condition_to_instances'].items():self.assertTrue(set(values)<=region_ids,(name,key,values))
    # Structural graph keeps external control rows and no dangling endpoints.
    st_nodes={r['seg_id'] for r in item['struct']['segments']}
    self.assertFalse(any(e['src'] not in st_nodes or e['dst'] not in st_nodes or e['src']==e['dst'] for e in item['struct'].get('edges',[])))
    topo_sort(list(st_nodes),[(e['src'],e['dst']) for e in item['struct'].get('edges',[])])
 def test_timing_five_algorithms_and_native(self):
  for name,item in self.cache.items():
   with self.subTest(name=name):
    s=item['segments'];weights={t['template_id']:dict(total_ns=i+1,count=1,avg_ns=i+1,min_ns=i+1,max_ns=i+1) for i,t in enumerate(s['templates'])};inp=self.root/(name+'-w.json');inp.write_text(json.dumps({'weights':weights}));timing=import_lock_loop_condition_timing(base_dir=item['base'],base_name=name,input_timing=inp);self.assertEqual({r['seg_id'] for r in s['segments']},set(timing['weights']))
    groups={g['group_id']:g for g in s['lock_groups']}
    for row in s['segments']:
     if row['kind']=='lock_group':g=groups[row['seg_id']];tids=[member_template(m) for m in g['members']];tids=[t for t in tids if t];expected={f:sum(weights[t][f] for t in tids) for f in FIELDS}
     else:expected=weights[row['template_id']]
     self.assertEqual(expected,timing['weights'][row['seg_id']])
    for algo in ALGOS:self.assertEqual(set(item['dag']['nodes']),set(run_schedule(base_dir=item['base'],base_name=name,level='level2',rule_name=RULE,algo_name=algo)['priorities']))
    bin=self.root/(name+'-prog');self.h.command(['gcc-13','-pthread','-std=c11',item['source'],'-o',bin]);before=item['source'].read_bytes()
    for argc in range(4):self.assertTrue(self.h.command([bin,*(['x']*argc)]).stdout.startswith('RESULT:'))
    self.assertEqual(before,item['source'].read_bytes())
 def test_target_condition_branches_remain_separate(self):
  item=self.cache['then'];s=item['segments'];d=item['dag']
  rows={r['seg_id']:r for r in s['segments']}
  target=s['lock_groups'][0]
  self.assertEqual('resource_membership_only',target['aggregation_method'])
  branches={}
  for member in target['members']:
   memberships=member.get('condition_memberships') or []
   if memberships:
    branch=memberships[-1]['branch_id']
    branches.setdefault(branch,[]).append(member)
  self.assertGreaterEqual(len(branches),2)
  for members in branches.values():
   self.assertTrue(all(m['seg_id'] in rows for m in members))
  self.assertEqual(set(target['member_segment_ids']),set(s['mappings']['grouped_node_to_member_segments'][target['group_id']]))
  self.assertTrue(all(rows[mid].get('lock_group_id')==target['group_id'] for mid in target['member_segment_ids']))
  def reachable(start,end):
   pending=[start];seen=set()
   while pending:
    node=pending.pop()
    if node==end:return True
    if node in seen:continue
    seen.add(node);pending.extend(edge['dst'] for edge in d['edges'] if edge['src']==node)
   return False
  by_instance={}
  for members in branches.values():
   for member in members:
    by_instance.setdefault(member.get('loop_iteration_id'),[]).append(member)
  for members in by_instance.values():
   for a in members:
    for b in members:
     if a is b:continue
     if a['condition_memberships'][-1]['exclusive_group']==b['condition_memberships'][-1]['exclusive_group'] and a['condition_memberships'][-1]['branch_id']!=b['condition_memberships'][-1]['branch_id']:
      self.assertFalse(reachable(a['seg_id'],b['seg_id']))
  weights={t['template_id']:dict(total_ns=i+1,count=1,avg_ns=i+1,min_ns=i+1,max_ns=i+1) for i,t in enumerate(s['templates'])}
  inp=self.root/'then-branch-w.json';inp.write_text(json.dumps({'weights':weights}))
  timing=import_lock_loop_condition_timing(base_dir=item['base'],base_name='then',input_timing=inp)
  for members in branches.values():
   for member in members:
    self.assertEqual(weights[member_template(member)],timing['weights'][member['seg_id']])
  self.assertNotIn(target['group_id'],d['nodes'])
  self.assertNotIn(target['group_id'],timing['weights'])
 def test_external_formal_dependencies_and_lock_loop(self):
  for name in ('external_condition','external_before','external_after','external_other_thread','external_dependency','external_lock','external_loop','external_helper','external_no_old_helper','external_lock_loop','external_loop_lock','external_before_both','external_both_sides','external_l2','external_l3','external_double_lock','external_same_mutex','external_same_var','external_combo','external_zero','external_main','external_zero_body','external_zero_mixed','external_l2_zero_first','external_l2_zero_both','external_l3_outer_zero','external_l3_both_zero','external_two_loops_zero_one','external_same_var_zero','external_before_nested'):
   with self.subTest(name=name):
    item=self.cache[name];s=item['segments'];d=item['dag'];nodes=set(d['nodes']);kinds=[e for e in d['edges'] if e.get('kind') in ('create','join','sem_dep','intra')]
    self.assertTrue(kinds)
    for e in kinds:self.assertIn(e['src'],nodes);self.assertIn(e['dst'],nodes);self.assertNotEqual(e['src'],e['dst'])
    if name=='external_lock':
     self.assertIn('LOCKGROUP:m2',{g['group_id'] for g in s['lock_groups']})
     ext=next(g for g in s['lock_groups'] if g['group_id']=='LOCKGROUP:m2')
     self.assertTrue(ext['members']);self.assertTrue(any(member_template(m) for m in ext['members']))
    if name=='external_loop':
     ext_loops=[l for l in s['loops'] if not l.get('lock_group_id')]
     self.assertTrue(ext_loops)
     for loop in ext_loops:self.assertNotIn(loop.get('lock_group_id'),{g['group_id'] for g in s['lock_groups']})
    if name in ('external_helper','external_no_old_helper'):
     self.assertTrue(s.get('source_only_segments'))
    if name in ('external_condition','external_dependency'):
     self.assertTrue(any(e.get('kind')=='join' for e in d['edges']))
     self.assertTrue(any(e.get('kind')=='create' for e in d['edges']))
 def test_sem_projection_and_witness_contract(self):
  for name in ('sem_branches','sem_inside_after','sem_then','sem_else','sem_no_else','sem_nested','sem_same_iteration','sem_wait_after','sem_outside_to_group','sem_l2','sem_l3','sem_l2_zero','sem_l3_zero','sem_zero','sem_double'):
   with self.subTest(name=name):
    item=self.cache[name];s=item['segments'];d=item['dag'];nodes=set(d['nodes']);absorbed=s['mappings'].get('absorbed_internal_sem_dependencies',[])
    self.assertFalse(any(e['src']==e['dst'] for e in d['edges']))
    for edge in d['edges']:
     self.assertIn(edge['src'],nodes);self.assertIn(edge['dst'],nodes)
     if edge.get('kind')=='sem_dep':self.assertNotEqual(edge['src'],edge['dst'])
    for record in absorbed:
     self.assertEqual(record['projected_src'],record['projected_dst']);self.assertEqual(record['edge'].get('kind'),'sem_dep')
    if name=='sem_same_iteration':self.assertEqual(3,len(absorbed))
    if name in ('sem_zero','sem_l2_zero','sem_l3_zero'):
     zero_loops=[loop for loop in s['loops'] if loop.get('iteration_count')==0]
     self.assertTrue(zero_loops)
     for loop in zero_loops:self.assertFalse([i for i in s['iteration_instances'] if i.get('loop_id')==loop['loop_id']])
     zero_ids={loop['loop_id'] for loop in zero_loops}
     for record in absorbed:self.assertFalse(str(record['source_instance']).startswith(tuple(zero_ids)) or str(record['destination_instance']).startswith(tuple(zero_ids)))
    if name=='sem_outside_to_group':
     sem_edges=[e for e in d['edges'] if e.get('kind')=='sem_dep'];self.assertTrue(sem_edges)
     for edge in sem_edges:self.assertNotEqual(edge['src'],edge['dst'])
    destinations=[a['destination_instance'] for a in absorbed]+[e['dst'] for e in d['edges'] if e.get('kind')=='sem_dep']
    if name=='sem_branches':
     self.assertTrue(any(':BR:worker#003@' in value for value in destinations));self.assertTrue(any(':BR:worker#004@' in value for value in destinations))
    if name=='sem_then':
     self.assertTrue(destinations);self.assertTrue(all(':BR:worker#003@' in value for value in destinations))
    if name=='sem_else':
     self.assertTrue(destinations);self.assertTrue(all(':BR:worker#004@' in value for value in destinations))
    if name=='sem_no_else':
     self.assertTrue(destinations);self.assertTrue(all(':BR:worker#003@' in value for value in destinations))
    if name=='sem_nested':
     self.assertTrue(any(':BR:worker#005@' in value for value in destinations));self.assertTrue(any(':BR:worker#006@' in value for value in destinations))
    if name=='sem_double':
     self.assertEqual(2,{g['group_id'].split('@')[0] for g in s['lock_groups']}.__len__())
     self.assertIn('LOCKGROUP:m1@worker:12',{g['group_id'] for g in s['lock_groups']})
     self.assertIn('LOCKGROUP:m2@worker:28',{g['group_id'] for g in s['lock_groups']})
 def test_sem_guard_fields_witnesses_and_genuine_cycles(self):
  import copy
  def native(name):
   item=self.cache[name];return build_level2_segments_and_dag(base_dir=item['base'],base_name=name,source_file=item['source'])
  def run(name,raw,dag):
   return build_lock_loop_condition_views(base_name=name,source_text=self.cache[name]['text'],base_segments_json=raw,base_dag_json=dag)
  # Projected sem edge preserves explicit fields and the inputs stay untouched.
  name='sem_outside_to_group';raw,dag=native(name);sem=next(e for e in dag['edges'] if e.get('kind')=='sem_dep');sem.update(edge_id='C5F:SEM:TEST',sync_object='sx',custom={'tag':'kept'})
  raw_before=copy.deepcopy(raw);dag_before=copy.deepcopy(dag)
  _,_,_,final,_=run(name,raw,dag)
  projected=[e for e in final['edges'] if e.get('kind')=='sem_dep']
  self.assertTrue(projected)
  for edge in projected:
   self.assertEqual('C5F:SEM:TEST',edge.get('edge_id'));self.assertEqual('sx',edge.get('sync_object'));self.assertEqual({'tag':'kept'},edge.get('custom'))
  self.assertEqual(raw_before,raw);self.assertEqual(dag_before,dag)
  # Absorbed witness preserves the complete raw edge record.
  name='sem_same_iteration';raw,dag=native(name);sem=next(e for e in dag['edges'] if e.get('kind')=='sem_dep' and e['src']==e['dst']);sem.update(edge_id='C5F:ABS:TEST',sync_object='sx',custom={'tag':'absorbed'})
  _,_,s,_,_=run(name,raw,dag);self.assertTrue(s['mappings']['absorbed_internal_sem_dependencies'])
  for record in s['mappings']['absorbed_internal_sem_dependencies']:
   self.assertEqual('C5F:ABS:TEST',record['edge'].get('edge_id'));self.assertEqual('sx',record['edge'].get('sync_object'));self.assertEqual({'tag':'absorbed'},record['edge'].get('custom'))
  # A genuine multi-node sem cycle remains a topo error.
  name='sem_same_iteration';raw,dag=native(name);base_edge=next(e for e in dag['edges'] if e.get('src')!=e.get('dst'))
  dag['edges'].append({'src':base_edge['dst'],'dst':base_edge['src'],'kind':'sem_dep','edge_id':'C5F:TRUE:CYCLE'})
  with self.assertRaises(Exception):run(name,raw,dag)
  # A self sem_dep without formal post/wait is not a collapsed witness.
  name='sem_same_iteration';raw,dag=native(name);target=next(row['seg_id'] for row in raw['segments'] if 'main' in row['seg_id'])
  dag['edges'].append({'src':target,'dst':target,'kind':'sem_dep'})
  with self.assertRaises(Exception):run(name,raw,dag)
  # An intra self loop is still rejected.
  name='sem_same_iteration';raw,dag=native(name);target=next(row['seg_id'] for row in raw['segments'] if 'main' in row['seg_id'])
  dag['edges'].append({'src':target,'dst':target,'kind':'intra'})
  with self.assertRaises(Exception):run(name,raw,dag)
  # A self sem witness with non-sem provenance is rejected.
  name='sem_same_iteration';raw,dag=native(name);sem=next(e for e in dag['edges'] if e.get('kind')=='sem_dep' and e['src']==e['dst']);witness_edge=copy.deepcopy(dag['edges'][0]);sem['provenance']=[{'base_edge_index':0,'edge':witness_edge}]
  witness_edge['kind']='intra'
  with self.assertRaises(Exception):run(name,raw,dag)
 def test_external_lock_loop_layouts_metadata_and_sources(self):
  expectations={
   'external_lock_loop':(2,1),'external_loop_lock':(2,1),'external_before_both':(2,1),
   'external_both_sides':(3,2),'external_l2':(1,2),'external_l3':(1,2),
   'external_l2_zero':(1,2),'external_l3_zero':(1,2),'external_double_lock':(3,0),
   'external_same_mutex':(2,0),'external_same_var':(2,1),'external_combo':(2,1),'external_zero':(1,1),'external_main':(2,1),'external_l2_before':(1,2),'external_l3_before':(1,2),'external_zero_body':(1,1),'external_zero_mixed':(1,1),'external_l2_zero_first':(1,2),'external_l2_zero_both':(1,2),'external_l3_outer_zero':(1,2),'external_l3_both_zero':(1,2),'external_two_loops_zero_one':(1,2),'external_same_var_zero':(2,1),'external_before_nested':(1,0)}
  for name,(group_count,external_loop_count) in expectations.items():
   with self.subTest(name=name):
    item=self.cache[name];s=item['segments'];d=item['dag'];final_ids={r['seg_id'] for r in s['segments']}
    self.assertEqual(group_count,len(s['lock_groups']))
    groups={g['group_id']:g for g in s['lock_groups']};target_groups={g['group_id'] for g in s['lock_groups'] if g.get('mutex_id')=='m1' and '@' not in g['group_id']}
    external_loops=[l for l in s['loops'] if l.get('lock_group_id') is None]
    self.assertEqual(external_loop_count,len(external_loops))
    external_rows=[r for r in s['segments'] if r.get('loop_id') in {l['loop_id'] for l in external_loops}] if external_loops else []
    for loop in external_loops:
     self.assertIsNone(loop.get('lock_group_id'));self.assertIn(loop.get('function'),{'worker','main',None})
    for row in external_rows:
     self.assertIsNone(row.get('lock_group_id'));self.assertIsNone(row.get('loop_iteration_id'))
     self.assertFalse(row.get('condition_memberships'))
    for inst in s['iteration_instances']:
     if inst.get('loop_id') in {l['loop_id'] for l in external_loops}:
      self.assertIsNone(inst.get('lock_group_id'));self.assertFalse(inst.get('condition_memberships'))
    for g in s['lock_groups']:
     self.assertTrue(g['members']);self.assertFalse(g.get('candidate_segment_ids'))
     if g['group_id'] not in target_groups:
      for member in g['members']:self.assertIsNone(member.get('loop_iteration_id'))
    for key,values in s['mappings']['original_segment_to_execution_segments'].items():
     self.assertTrue(set(values)<=final_ids,(name,key,values))
    for key,values in s['mappings']['source_lock_region_to_group'].items():self.assertTrue(set(values)<={g['group_id'] for g in s['lock_groups']},(name,key,values))
    self.assertFalse(any(e['src'] not in final_ids or e['dst'] not in final_ids or e['src']==e['dst'] for e in d['edges']))
 def test_external_zero_l2_l3_and_input_immutability(self):
  for name in ('external_l2_zero','external_l3_zero','external_zero','external_zero_body','external_zero_mixed','external_l2_zero_first','external_l2_zero_both','external_l3_outer_zero','external_l3_both_zero','external_two_loops_zero_one','external_same_var_zero'):
   with self.subTest(name=name):
    s=self.cache[name]['segments'];zero=[loop for loop in s['loops'] if loop.get('iteration_count')==0]
    self.assertTrue(zero)
    for loop in zero:self.assertFalse([i for i in s['iteration_instances'] if i.get('loop_id')==loop['loop_id']])
  l3=self.cache['external_l3']['segments'];self.assertTrue(any(loop.get('parent_loop_id') for loop in l3['loops']))
  same=self.cache['external_same_mutex']['segments'];gids={g['group_id'] for g in same['lock_groups']};self.assertEqual(2,len(gids));self.assertTrue(any('@external:' in gid for gid in gids))
  import copy
  def native(name):
   item=self.cache[name];return build_level2_segments_and_dag(base_dir=item['base'],base_name=name,source_file=item['source'])
  for name in ('external_lock_loop','external_loop_lock','external_both_sides','external_l2','external_l3','external_l2_zero','external_l3_zero','external_double_lock','external_same_mutex'):
   with self.subTest(immutability=name):
    raw,dag=native(name);before_raw=copy.deepcopy(raw);before_dag=copy.deepcopy(dag)
    build_lock_loop_condition_views(base_name=name,source_text=self.cache[name]['text'],base_segments_json=raw,base_dag_json=dag)
    self.assertEqual(before_raw,raw);self.assertEqual(before_dag,dag)
 def test_external_zero_loop_source_mapping_contract(self):
  final_ids={r['seg_id'] for r in self.cache['external_zero_body']['segments']['segments']}
  for name in ('external_zero','external_zero_body','external_zero_mixed','external_l2_zero','external_l2_zero_first','external_l2_zero_both','external_l3_zero','external_l3_outer_zero','external_l3_both_zero','external_two_loops_zero_one','external_same_var_zero'):
   with self.subTest(name=name):
    s=self.cache[name]['segments'];ids={r['seg_id'] for r in s['segments']}
    for key,values in s['mappings']['original_segment_to_execution_segments'].items():
     self.assertTrue(set(values)<=ids,(name,key,values))
    loop_by_id={l['loop_id']:l for l in s['loops']};zero_ids=set()
    for loop in s['loops']:
     eff=1;cursor=loop;seen=set()
     while cursor is not None:
      self.assertNotIn(cursor['loop_id'],seen);seen.add(cursor['loop_id']);eff*=cursor.get('iteration_count',0);cursor=loop_by_id.get(cursor.get('parent_loop_id'))
     if eff==0:zero_ids.add(loop['loop_id'])
    self.assertTrue(zero_ids)
    for lid in zero_ids:
     tid=lid+':body_template'
     if tid in {t['template_id'] for t in s['templates']}:
      self.assertEqual([],s['mappings']['template_to_execution_segments'][tid])
     self.assertFalse([i for i in s['iteration_instances'] if i.get('loop_id')==lid])
    if name=='external_same_var_zero':self.assertIn([],s['mappings']['original_segment_to_execution_segments'].values())
    if name=='external_zero_mixed':
     self.assertFalse(any(not values and key.startswith('SEG:worker') for key,values in s['mappings']['original_segment_to_execution_segments'].items()))
 def test_external_condition_alias_rebinding_and_integrity(self):
  names=('external_after','external_before','external_before_nested','external_two',
         'external_else_if','external_no_else','external_helper','external_no_old_helper','external_combo')
  for name in names:
   with self.subTest(name=name):
    s=self.cache[name]['segments'];regions={r['condition_id']:r for r in s['condition_regions']}
    external_ids={r['condition_id'] for r in s['condition_regions'] if not r.get('loop_iteration_id')}
    source=s['mappings']['source_condition_to_instances'];target_instances={r['condition_id'] for r in s['condition_regions'] if r.get('loop_iteration_id')}
    self.assertTrue(external_ids)
    for row in s['segments']+s.get('source_only_segments',[]):
     for membership in row.get('condition_memberships',[]):
      group=membership['exclusive_group'];self.assertIn(group,regions)
      self.assertIn(membership['branch_id'],{b['branch_id'] for b in regions[group]['branches']})
      if group in external_ids:self.assertIsNone(row.get('loop_iteration_id'))
    for key,values in source.items():
     if key in external_ids:self.assertTrue(all(value in external_ids for value in values))
     else:self.assertTrue(all(value in target_instances for value in values))
    self.assertFalse(external_ids & {k for k,v in source.items() if not all(x in external_ids for x in v)})
  after=self.cache['external_after']['segments'];regions={r['condition_id']:r for r in after['condition_regions']}
  external=next(r for r in after['condition_regions'] if not r.get('loop_iteration_id'))
  self.assertEqual(('worker',21,25),(external['function'],external['entry_line'],external['exit_line']))
  rows={r['seg_id']:r for r in after['segments']}
  self.assertEqual(['COND:worker#002'],[m['exclusive_group'] for m in rows['BR:worker#006@22-22']['condition_memberships']])
  self.assertEqual(['COND:worker#002'],[m['exclusive_group'] for m in rows['BR:worker#007@24-24']['condition_memberships']])
  self.assertEqual({'COND:worker#002':['COND:worker#002']},{k:v for k,v in after['mappings']['source_condition_to_instances'].items() if k=='COND:worker#002'})
  # Timing is rebuilt from the same rebounded rows; no local alias is used.
  item=self.cache['external_after'];s=item['segments'];weights={t['template_id']:dict(total_ns=i+1,count=1,avg_ns=i+1,min_ns=i+1,max_ns=i+1) for i,t in enumerate(s['templates'])};inp=self.root/'external_after-rebind-w.json';inp.write_text(json.dumps({'weights':weights}));timing=import_lock_loop_condition_timing(base_dir=item['base'],base_name='external_after',input_timing=inp)
  self.assertEqual({r['seg_id'] for r in s['segments']},set(timing['weights']))
  for row in s['segments']:
   if row.get('template_id'):self.assertIn(row['template_id'],{t['template_id'] for t in s['templates']})

 def test_external_alias_map_conflict_and_idempotence(self):
  def branch(name,start):return dict(branch_id=name,start_line=start,end_line=start)
  def region(cid,entry,exit,parent=None):return dict(condition_id=cid,function='worker',entry_line=entry,exit_line=exit,parent_id=parent,branches=[branch('then',entry+1),branch('else',exit-1)])
  local=region('COND:worker#001',10,14);formal=region('COND:worker#002',10,14)
  view=(dict(condition_regions=[local],segments=[dict(seg_id='X',condition_memberships=[dict(exclusive_group='COND:worker#001',branch_id='then')],custom={'keep':[1,{'x':True}]})]),
        dict(edges=[]),dict(condition_regions=[dict(local)],segments=[]),dict(edges=[]),[dict(formal)])
  alias=_c5f_external_view_alias_map(view,[formal]);self.assertEqual({'COND:worker#001':'COND:worker#002'},alias)
  first,_=_c5f_rebind_external_view(view,[formal]);second,_=_c5f_rebind_external_view(first,[formal])
  self.assertEqual(first,second)
  self.assertEqual('COND:worker#002',first[0]['condition_regions'][0]['condition_id'])
  self.assertEqual('COND:worker#002',first[0]['segments'][0]['condition_memberships'][0]['exclusive_group'])
  self.assertEqual({'keep':[1,{'x':True}]},first[0]['segments'][0]['custom'])
  # One local alias resolving to two different formal identities is refused.
  other=region('COND:worker#003',20,24)
  same_alias_other=region('COND:worker#001',20,24)
  bad=(dict(condition_regions=[local],segments=[]),dict(edges=[]),dict(condition_regions=[same_alias_other],segments=[]),dict(edges=[]),[dict(formal),dict(other)])
  with self.assertRaises(ValueError):_c5f_external_view_alias_map(bad,[formal,other])
  # Repeated complete construction is stable after canonicalization.
  item=self.cache['external_before'];raw,dag=build_level2_segments_and_dag(base_dir=item['base'],base_name='external_before',source_file=item['source']);frozen=json.loads(json.dumps((raw,dag),sort_keys=True));_,_,s1,_,_=build_lock_loop_condition_views(base_name='external_before',source_text=item['text'],base_segments_json=raw,base_dag_json=dag)
  _,_,s2,_,_=build_lock_loop_condition_views(base_name='external_before',source_text=item['text'],base_segments_json=raw,base_dag_json=dag)
  self.assertEqual(frozen,json.loads(json.dumps((raw,dag),sort_keys=True)))
  self.assertEqual(s1['condition_regions'],s2['condition_regions'])
  self.assertEqual(s1['mappings']['source_condition_to_instances'],s2['mappings']['source_condition_to_instances'])
  self.assertEqual([r.get('condition_memberships') for r in s1['segments']],[r.get('condition_memberships') for r in s2['segments']])

 def test_structural_mapping_reference_integrity(self):
  for name,item in self.cache.items():
   with self.subTest(name=name):
    s=item['segments'];struct=item['struct'];struct_ids={r['seg_id'] for r in struct['segments']};exec_ids=set(item['dag']['nodes']);m=s['mappings']
    for field in ('source_position_to_structural_nodes','original_segment_to_structural_nodes',
                  'generation_node_to_structural_nodes','original_node_to_structural_nodes'):
     for key,values in m.get(field,{}).items():
      self.assertIsInstance(values,list,field)
      self.assertTrue(set(values)<=struct_ids,(field,key,values))
    for key,values in m.get('structural_node_to_execution_segments',{}).items():
     self.assertIn(key,struct_ids);self.assertTrue(set(values)<=exec_ids)
    for field,values in m.items():
     if field.endswith('_to_execution_segments'):
      for key,items in values.items():self.assertTrue(set(items)<=exec_ids,(field,key,items))
     elif field.endswith('_to_execution_segment'):
      for key,value in values.items():self.assertIn(value,exec_ids,(field,key,value))
  after=self.cache['external_after']['segments'];struct_ids={r['seg_id'] for r in self.cache['external_after']['struct']['segments']}
  self.assertEqual(['BR:worker#003@20-20'],after['mappings']['source_position_to_structural_nodes']['LOOPSRC:worker:line:20'])
  self.assertTrue(set(after['mappings']['original_segment_to_structural_nodes']['SEG:worker#003@20-25'])<=struct_ids)
  for name,key in (('external_before','LOOPSRC:worker:line:12'),('external_before_nested','LOOPSRC:worker:line:12'),('external_main','LOOPSRC:main:line:38'),('external_two','LOOPSRC:main:line:26'),('external_else_if','LOOPSRC:main:line:26'),('external_no_else','LOOPSRC:main:line:26')):
   s=self.cache[name]['segments'];ids={r['seg_id'] for r in self.cache[name]['struct']['segments']}
   for node in s['mappings']['source_position_to_structural_nodes'].get(key,[]):self.assertIn(node,ids,(name,key,node))
  # Zero/L3 zero mappings may be empty, but never retain a removed node.
  for name in ('zero','external_zero_mixed','external_l3_both_zero','external_l2_zero_both'):
   s=self.cache[name]['segments'];ids={r['seg_id'] for r in self.cache[name]['struct']['segments']}
   for key,values in s['mappings']['source_position_to_structural_nodes'].items():self.assertTrue(set(values)<=ids,(name,key,values))
  # Repeated construction preserves deterministic mapping order and the input model.
  import copy
  item=self.cache['external_after'];raw,dag=build_level2_segments_and_dag(base_dir=item['base'],base_name='external_after',source_file=item['source']);frozen=copy.deepcopy((raw,dag));_,_,s1,_,_=build_lock_loop_condition_views(base_name='external_after',source_text=item['text'],base_segments_json=raw,base_dag_json=dag);_,_,s2,_,_=build_lock_loop_condition_views(base_name='external_after',source_text=item['text'],base_segments_json=raw,base_dag_json=dag)
  self.assertEqual(frozen,(raw,dag));self.assertEqual(s1['mappings']['source_position_to_structural_nodes'],s2['mappings']['source_position_to_structural_nodes']);self.assertEqual(s1['mappings']['original_segment_to_structural_nodes'],s2['mappings']['original_segment_to_structural_nodes'])

 def test_condition_source_identity_collision_and_stability(self):
  import copy
  # case003-style collision: target and external share local #001.
  before=self.cache['external_before']['segments'];source=before['mappings']['source_condition_to_instances']
  target_keys={k for k,v in source.items() if any(x.startswith('LOOP:') for x in v)}
  external_keys=set(source)-target_keys
  self.assertEqual(2,len(source));self.assertEqual(1,len(target_keys));self.assertEqual(1,len(external_keys))
  for key in target_keys:self.assertTrue(all(x.startswith('LOOP:') for x in source[key]))
  for key in external_keys:self.assertEqual([key],source[key]);self.assertFalse(any(x.startswith('LOOP:') for x in source[key]))
  self.assertTrue(all(k.startswith('C5FSRC:') for k in source))
  loop_source={x for loop in before['loops'] for x in (loop.get('source_condition_ids') or [])}
  self.assertEqual(target_keys,loop_source)
  for row in before['segments']:
   for membership in row.get('condition_memberships',[]):
    self.assertIn(membership['exclusive_group'],{r['condition_id'] for r in before['condition_regions']})
  # Nested target parent/child plus external collision.
  nested=self.cache['external_before_nested']['segments'];mapping=nested['mappings']['source_condition_to_instances']
  target_k={k for k,v in mapping.items() if any(x.startswith('LOOP:') for x in v)};external_k=set(mapping)-target_k
  self.assertEqual(3,len(mapping));self.assertEqual(2,len(target_k));self.assertEqual(1,len(external_k))
  # Different-function aliases remain independent.
  after=self.cache['external_after']['segments'];m=after['mappings']['source_condition_to_instances']
  self.assertEqual(2,len(m));self.assertTrue(any(v and all(x.startswith('LOOP:') for x in v) for v in m.values()));self.assertTrue(any(v==[k] for k,v in m.items()))
  # Repeated construction is stable and does not mutate raw/dag.
  item=self.cache['external_before'];raw,dag=build_level2_segments_and_dag(base_dir=item['base'],base_name='external_before',source_file=item['source'])
  frozen=copy.deepcopy((raw,dag));_,_,s2,_,_=build_lock_loop_condition_views(base_name='external_before',source_text=item['text'],base_segments_json=raw,base_dag_json=dag)
  self.assertEqual((raw,dag),frozen);self.assertEqual({k:sorted(v) for k,v in source.items()},{k:sorted(v) for k,v in s2['mappings']['source_condition_to_instances'].items()})
 def test_condition_source_registry_duplicate_and_conflict(self):
  import copy,types
  from Code2DAG.pipeline.composition_c5f_model import _canonicalize_condition_sources
  def branch(name,start,end):return types.SimpleNamespace(branch_id=name,start_line=start,end_line=end)
  def formal(cid,fn,entry,exit):return types.SimpleNamespace(condition_id=cid,function=fn,entry_line=entry,exit_line=exit,parent_id=None,branches=[branch('then',entry+1,entry+1),branch('else',exit-1,exit-1)])
  def clone(cid,fn,entry,exit,local):return dict(condition_id=cid,function=fn,entry_line=entry,exit_line=exit,parent_id=None,source_condition_id=local,branches=[dict(branch_id='then',start_line=entry+1,end_line=entry+1),dict(branch_id='else',start_line=exit-1,end_line=exit-1)])
  def payload(cid,fn,entry,exit,local):return dict(condition_id=local,function=fn,entry_line=entry,exit_line=exit,parent_id=None,branches=[dict(branch_id='then',start_line=entry+1,end_line=entry+1),dict(branch_id='else',start_line=exit-1,end_line=exit-1)])
  def call(target_forms,external_forms,tclones,epayls,helpers=None):
   helpers=helpers or []
   return _canonicalize_condition_sources(source_text='qa',base_name='qa',target_regions=tclones,external_payloads=epayls,helper_payloads=helpers,target_conditions=target_forms,external_conditions=external_forms,loops=[],struct_loops=[],struct_rows=[],sched_rows=[],struct_edges=[],sched_edges=[])
  # Different formal sources sharing one local alias become distinct canonical IDs.
  t1=formal('A','worker',10,14);e1=formal('B','worker',20,24);tclone=clone('I1','worker',10,14,'COND:worker#001');ep=payload('B','worker',20,24,'COND:worker#001')
  source=call([t1],[e1],[tclone],[ep])
  self.assertIsNotNone(source);self.assertEqual(2,len(source));self.assertNotEqual(tclone['source_condition_id'],ep['condition_id']);self.assertNotIn(tclone['source_condition_id'],source[ep['condition_id']])
  # Same formal source discovered twice under different local aliases canonicalizes once.
  t2=formal('A','worker',10,14);tclone2=clone('I2','worker',10,14,'COND:worker#001');ep2=payload('A','worker',10,14,'COND:worker#002')
  source2=_canonicalize_condition_sources(source_text='qa',base_name='qa',target_regions=[tclone2],external_payloads=[ep2],helper_payloads=[],target_conditions=[t2],external_conditions=[t2],loops=[],struct_loops=[],struct_rows=[],sched_rows=[],struct_edges=[],sched_edges=[])
  self.assertIsNotNone(source2);self.assertEqual(tclone2['source_condition_id'],ep2['condition_id']);self.assertEqual(1,len(source2))
  # Metadata conflict is rejected, never auto-assigned to the first ID.
  t3=formal('A','worker',10,14);bad=payload('A','worker',10,14,'COND:worker#001');bad['branches']=[dict(branch_id='then',start_line=9,end_line=9),dict(branch_id='else',start_line=13,end_line=13)]
  with self.assertRaises(Exception):
   _canonicalize_condition_sources(source_text='qa',base_name='qa',target_regions=[],external_payloads=[bad],helper_payloads=[],target_conditions=[],external_conditions=[t3],loops=[],struct_loops=[],struct_rows=[],sched_rows=[],struct_edges=[],sched_edges=[])
  # Different functions deliberately share one alias: distinct formal identities still canonicalize separately.
  t4=formal('W','worker',10,14);e4=formal('M','main',20,24);tc4=clone('I4','worker',10,14,'COND:same#001');ep4=payload('M','main',20,24,'COND:same#001')
  source4=_canonicalize_condition_sources(source_text='qa',base_name='qa',target_regions=[tc4],external_payloads=[ep4],helper_payloads=[],target_conditions=[t4],external_conditions=[e4],loops=[],struct_loops=[],struct_rows=[],sched_rows=[],struct_edges=[],sched_edges=[])
  self.assertEqual(2,len(source4));self.assertNotEqual(tc4['source_condition_id'],ep4['condition_id'])
 def test_rule_isolation(self):
  text=(SOURCES/'then.c').read_text()
  for rule in ('composition_c4b','composition_c3b'):
   with self.subTest(rule=rule),tempfile.TemporaryDirectory() as tmp:
    from Code2DAG.pipeline.runner import run_blocks
    src=Path(tmp)/'x.c';src.write_text(text);base,p=self.h.native(tmp,src)
    with self.assertRaises(ValueError):run_blocks(base_dir=base,base_name='x',source_file=src,level='level2',rule_name=rule)
    self.assertFalse((p/'blocks/level2'/rule/'segments.json').exists())
class C5fInvalidTests(unittest.TestCase):
 def test_partial_and_crossing_conditions_are_rejected_without_output(self):
  h=c1.CompositionC1Tests()
  for source in sorted(INVALID.glob('*.c')):
   with self.subTest(name=source.stem),tempfile.TemporaryDirectory() as tmp:
    text=source.read_text();rule=select_rule(text);src=Path(tmp)/source.name;src.write_text(text);base,p=h.native(tmp,src)
    with self.assertRaises(Exception):run_composition_blocks(base_dir=base,base_name=source.stem,source_file=src)
    self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists())
