"""Absorbed intra witnesses follow concrete one-/two-dimensional source ports."""
import copy,unittest
from Code2DAG.tests import test_composition_c4a as c4a
from Code2DAG.pipeline.composition_c4a_model import build_loop_lock_views


def split_fixture(name,n=None):
 text,raw,dag=c4a.fixture(name)
 if n is not None:text=text.replace('i < 3','i < '+str(n)).replace('i < 0','i < '+str(n))
 lines=text.splitlines();start=raw['segments'][0]['start_line'];end=raw['segments'][0]['end_line'];raw['segments']=raw['segments'][1:];chunks=[];cursor=start
 locks=[i for i,l in enumerate(lines,1) if 'pthread_mutex_lock(&' in l];unlocks=[i for i,l in enumerate(lines,1) if 'pthread_mutex_unlock(&' in l]
 for a,b in zip(locks,unlocks):
  if cursor<a:chunks.append(dict(seg_id='P'+str(len(chunks)),function='task',kind='compute',start_line=cursor,end_line=a-1))
  chunks.append(dict(seg_id='P'+str(len(chunks)),function='task',kind='mutex_cs',start_line=a,end_line=b));cursor=b+1
 if cursor<=end:chunks.append(dict(seg_id='P'+str(len(chunks)),function='task',kind='compute',start_line=cursor,end_line=end))
 raw['segments']+=chunks;edges=[dict(src='MAIN',dst=chunks[0]['seg_id'],kind='create')]
 for i,(a,b) in enumerate(zip(chunks,chunks[1:])):edges.append(dict(src=a['seg_id'],dst=b['seg_id'],kind='intra',edge_id='original-'+str(i),custom={'nested':[i,{'source':['qa.c',i]}]},source_location={'line':a['end_line']},provenance=[{'previous':{'id':[i]}}]))
 return text,raw,dict(nodes=[r['seg_id'] for r in raw['segments']],edges=edges)

def build(t,r,d):return build_loop_lock_views(base_name='fixture',source_text=t,base_segments_json=r,base_dag_json=d)

class InternalIntraTests(unittest.TestCase):
 def test_l1_l2_l3_instances_retain_complete_sources(self):
  for name in ['many','l2_same','l3']:
   with self.subTest(name=name):
    text,raw,dag=split_fixture(name);saved=copy.deepcopy((raw,dag));st,sd,s,d,_=build(text,raw,dag);self.assertEqual(saved,(raw,dag));self.assertEqual((st,sd,s,d),build(text,raw,dag)[:4])
    originals=[e for e in dag['edges'] if 'edge_id' in e];derived=[e for e in d['edges'] if e.get('edge_instance_id')]
    self.assertTrue(derived);self.assertEqual(len(derived),len({e['edge_instance_id'] for e in derived}))
    for edge in derived:
     original=next(e for e in originals if e['edge_id']==edge['edge_id'])
     for key,value in original.items():
      if key not in ['src','dst','provenance']:self.assertEqual(value,edge[key])
     for previous in original['provenance']:self.assertIn(previous,edge['provenance'])
     self.assertTrue(any(w.get('edge')==original for w in edge['provenance']))
    expected=6 if name=='l3' else 3
    first=[e for e in derived if e['edge_id']=='original-0'];self.assertEqual(expected,len(first))
    self.assertEqual(len(originals),len(s['absorbed_intra_dependencies']))
    if name=='l3':self.assertEqual({(i,j) for i in range(3) for j in range(2)},{(row['outer_iteration_index'],row['inner_iteration_index']) for row in s['segments'] if row['kind']=='lock_group'})
 def test_parallel_sources_and_mutable_objects_are_independent(self):
  text,raw,dag=split_fixture('many');extra=copy.deepcopy(dag['edges'][1]);extra['edge_id']='parallel';extra['custom']['nested'][0]=99;dag['edges'].append(extra);saved=copy.deepcopy((raw,dag));first=build(text,raw,dag);self.assertEqual(first,build(text,raw,dag));edges=[e for e in first[3]['edges'] if e.get('edge_id') in ['original-0','parallel']]
  self.assertEqual(6,len(edges));self.assertEqual(6,len({e['edge_instance_id'] for e in edges}));edges[0]['custom']['nested'][1]['source'].append('mutated');self.assertEqual(saved,(raw,dag));self.assertTrue(all('mutated' not in e['custom']['nested'][1]['source'] for e in edges[1:]))
 def test_zero_keeps_sources_without_runtime_edges(self):
  text,raw,dag=split_fixture('zero');st,sd,s,d,_=build(text,raw,dag);self.assertFalse(s['lock_groups']);self.assertFalse(any(e.get('edge_instance_id') for e in d['edges']));self.assertTrue(s['absorbed_intra_dependencies'])
  for t in s['source_component_templates']:self.assertEqual([],s['mappings']['template_to_execution_segments'][t['template_id']])
