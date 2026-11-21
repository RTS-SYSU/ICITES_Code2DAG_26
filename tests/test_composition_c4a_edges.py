"""Complete C4a dependency cloning, parallel origins and independent copies."""
import copy,json,unittest
from Code2DAG.tests import test_composition_c4a as c4a
from Code2DAG.pipeline.composition_c4a_model import build_loop_lock_views


def fixture():
 text,raw,_=c4a.fixture('many');main=raw['segments'].pop();a=copy.deepcopy(main);b=copy.deepcopy(main);a.update(seg_id='PRE',end_line=main['start_line']+1);b.update(seg_id='POST',start_line=a['end_line']+1);raw['segments'] += [a,b]
 pairs=[('PRE','OLD','create'),('OLD','POST','join'),('PRE','POST','sem_dep'),('PRE','POST','intra'),('PRE','POST','dependency')]
 edges=[]
 for i,(src,dst,kind) in enumerate(pairs):edges.append(dict(src=src,dst=dst,kind=kind,edge_id='original-'+str(i),source={'file':'input.c','line':[i+1]},custom={'keep':[i,{'nested':['unchanged']}]},provenance=[{'earlier':{'ids':[i]}}]))
 return text,raw,dict(nodes=[r['seg_id'] for r in raw['segments']],edges=edges)

def build(text,raw,dag):return build_loop_lock_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)

class C4aEdgeTests(unittest.TestCase):
 def test_all_dependency_fields_and_input_unchanged(self):
  text,raw,dag=fixture();saved=copy.deepcopy((raw,dag));st,sd,s,d,_=build(text,raw,dag);self.assertEqual(saved,(raw,dag))
  for original in dag['edges']:
   found=[e for e in d['edges'] if e.get('edge_id')==original['edge_id']];self.assertTrue(found,original)
   for edge in found:
    for key,value in original.items():
     if key in ['src','dst','provenance']:continue
     self.assertEqual(value,edge[key],key)
    for prior in original['provenance']:self.assertIn(prior,edge['provenance'])
    self.assertTrue(any(w.get('edge')==original for w in edge['provenance']))
  self.assertEqual(dag['edges'],[x['edge'] for x in s['composition_edge_provenance']]);self.assertEqual((st,sd,s,d),build(text,raw,dag)[:4])
 def test_parallel_edges_preserve_all_sources_in_order(self):
  text,raw,dag=fixture();base=copy.deepcopy(dag['edges'][0]);other=copy.deepcopy(base);other['edge_id']='different-id';other['custom']['keep'][0]=99;dag['edges'] += [other,copy.deepcopy(base)]
  first=build(text,raw,dag);second=build(text,raw,dag);self.assertEqual(first,second);edges=[e for e in first[3]['edges'] if e.get('edge_id') in ['original-0','different-id']]
  self.assertEqual({'original-0','different-id'},{e['edge_id'] for e in edges});self.assertEqual(1,len({(e['src'],e['dst']) for e in edges}))
  origins=[w['edge'] for e in edges for w in e['provenance'] if type(w.get('base_edge_index')) is int and 'edge' in w]
  self.assertIn(other,origins);self.assertIn(base,origins)
  expected={0,len(dag['edges'])-2,len(dag['edges'])-1}
  indices={w['base_edge_index'] for e in edges for w in e['provenance'] if w.get('edge') in [base,other]};self.assertEqual(expected,indices)
 def test_nested_source_chains_are_not_aliased(self):
  text,raw,dag=fixture();saved=copy.deepcopy((raw,dag));st,sd,s,d,_=build(text,raw,dag);edge=next(e for e in d['edges'] if e.get('edge_id')=='original-0');edge['custom']['keep'][1]['nested'].append('changed');edge['source']['line'].append(99);edge['provenance'][0]['earlier']['ids'].append(99)
  self.assertEqual(saved,(raw,dag));self.assertEqual('unchanged',next(e for e in build(text,raw,dag)[3]['edges'] if e.get('edge_id')=='original-0')['custom']['keep'][1]['nested'][0])
