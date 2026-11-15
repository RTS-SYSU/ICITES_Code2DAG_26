"""Regress only C1 source-position filtering across split Loop owners."""
import copy
import unittest
from Code2DAG.tests.test_composition_c1 import fixture, SOURCES, ORDERS, line
from Code2DAG.pipeline.composition_model import build_composition_views


def example(order='ckl',n=3,multi=False,separate=False):
    text=(SOURCES/(order+'.c')).read_text().replace('i < 3','i < '+str(n))
    text=text.replace('void *worker_a', 'void body_step(int x) { value_a += x; }\nvoid after_step(void) { value_a += 23; }\nvoid *worker_a')
    text=text.replace('        value_a += i; /* BODY */','        body_step(i); /* BODY */'+('\n        body_step(i+1); /* BODY_SECOND */' if multi else ''))
    text=text.replace('    value_a += 23; /* SUFFIX */','    after_step(); /* SUFFIX */')
    text,s,d=fixture(order,text);body=[i for i,l in enumerate(text.splitlines(),1) if '/* BODY' in l];after=line(text,'/* SUFFIX */')
    if separate:
        # The complete Loop remains in its supported owner; after has its own
        # contiguous old segment. No Loop body crosses a raw segment boundary.
        old=next(r for r in s['segments'] if r['function']=='worker_a' and r['start_line']<=after<=r['end_line'])
        before_end=old['end_line'];old['end_line']=after-1
        suffix=dict(seg_id='AFTER',function='worker_a',kind='compute',start_line=after,end_line=before_end)
        s['segments'].insert(s['segments'].index(old)+1,suffix);d['nodes'].append('AFTER')
        for edge in d['edges']:
            if edge['src']==old['seg_id']:edge['src']='AFTER'
        d['edges'].append(dict(src=old['seg_id'],dst='AFTER',kind='intra'))
    def owner(l,fn='worker_a'):
        return next(r['seg_id'] for r in s['segments'] if r['function']==fn and r['start_line']<=l<=r['end_line'])
    for i,l in enumerate(body):
        key='worker_a/body_step'+str(i+1);s['node_locations'][key]=dict(function='worker_a',line=l);s['node_to_segment'][key]=owner(l)
    s['node_locations']['worker_a/after_step1']=dict(function='worker_a',line=after);s['node_to_segment']['worker_a/after_step1']=owner(after)
    labels=[*body,after,line(text,'/* PREFIX */'),line(text,'/* THEN */'),line(text,'/* ELSE */'),line(text,'/* CS_A */')]
    s['mappings']={'source_position_to_execution_segments':{f'LOOPSRC:worker_a:line:{l}':[owner(l)] for l in labels}}
    return text,s,d,body,after


class CompositionSourceMappingTests(unittest.TestCase):
    def check(self,order='ckl',n=3,multi=False,separate=False):
        text,s,d,body,after=example(order,n,multi,separate);frozen=copy.deepcopy((text,s,d))
        result=build_composition_views(base_name='fixture',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual(frozen,(text,s,d));st,sd,ss,dd,_=result
        h=line(text,'for (int i');ids=[f'LOOP:worker_a@{h}:iteration:{i}' for i in range(n)]
        mapping=ss['mappings'];positions=mapping['source_position_to_execution_segments'];generation=mapping['generation_node_to_execution_segments']
        after_ids=[r['seg_id'] for r in ss['segments'] if r['function']=='worker_a' and r['start_line']<=after<=r['end_line']]
        self.assertEqual(1,len(after_ids));self.assertEqual(after_ids,positions[f'LOOPSRC:worker_a:line:{after}']);self.assertEqual(after_ids,generation['worker_a/after_step1'])
        self.assertTrue(set(ids).isdisjoint(after_ids))
        for i,l in enumerate(body):
            self.assertEqual(ids,positions[f'LOOPSRC:worker_a:line:{l}']);self.assertEqual(ids,generation['worker_a/body_step'+str(i+1)])
        for r in ss['segments']:
            if r.get('role')=='iteration':
                self.assertEqual({'worker_a/body_step'+str(i+1) for i in range(len(body))},set(r['generation_origin']))
                self.assertNotIn('worker_a/after_step1',r['generation_origin'])
        for l in [line(text,'/* PREFIX */'),line(text,'/* THEN */'),line(text,'/* ELSE */')]:
            targets=[r['seg_id'] for r in ss['segments'] if r['function']=='worker_a' and r['start_line']<=l<=r['end_line']]
            self.assertEqual(targets,positions[f'LOOPSRC:worker_a:line:{l}'])
        self.assertEqual(['LOCKGROUP:shared_mutex'],positions[f"LOOPSRC:worker_a:line:{line(text,'/* CS_A */')}"])
        self.assertEqual(set(d['nodes']),set(mapping['original_segment_to_execution_segments']))
        # The entire old owner relation is kept: position filtering must not
        # remove suffix or other executions from this separate coarse mapping.
        for l in body:
            raw_owner=s['mappings']['source_position_to_execution_segments'][f'LOOPSRC:worker_a:line:{l}'][0]
            self.assertTrue(set(ids)<=set(mapping['original_segment_to_execution_segments'][raw_owner]))
            if not separate:self.assertTrue(set(after_ids)<=set(mapping['original_segment_to_execution_segments'][raw_owner]) if order=='ckl' else True)
        return result,s

    def test_zero_iterations_keep_empty_body_and_after(self):self.check(n=0)
    def test_one_iteration_shared_owner(self):self.check(n=1)
    def test_many_iterations_shared_owner(self):self.check(n=4)
    def test_separate_after_owner_zero_one_many(self):
        for n in [0,1,4]:
            with self.subTest(n=n):self.check(n=n,separate=True)
    def test_multiline_body_maps_every_call_and_line(self):self.check(multi=True)
    def test_all_six_orders_keep_body_prefix_suffix_and_condition(self):
        for order in ORDERS:
            with self.subTest(order=order):self.check(order=order,n=2,multi=True)
    def test_unchanged_other_positions_and_opaque_mapping_survive(self):
        text,s,d,body,after=example();s['mappings']['source_position_to_execution_segments'].update({'LOOPSRC:main:line:'+str(line(text,'pthread_t a, b;')):['M0'],'legacy:opaque':['M0']})
        before=copy.deepcopy(s);out=build_composition_views(base_name='fixture',source_text=text,base_segments_json=s,base_dag_json=d)[2];p=out['mappings']['source_position_to_execution_segments']
        self.assertEqual(['M0'],p['legacy:opaque']);self.assertEqual(['M0'],p['LOOPSRC:main:line:'+str(line(text,'pthread_t a, b;'))]);self.assertEqual(before,s)
    def test_old_segment_and_generation_maps_not_erased(self):
        text,s,d,body,after=example(n=0);plain=copy.deepcopy(s);plain.pop('mappings')
        a=build_composition_views(base_name='fixture',source_text=text,base_segments_json=s,base_dag_json=d)[2]
        b=build_composition_views(base_name='fixture',source_text=text,base_segments_json=plain,base_dag_json=d)[2]
        for key in ['original_segment_to_execution_segments','generation_node_to_execution_segments','grouped_node_to_member_segments']:
            self.assertEqual(b['mappings'][key],a['mappings'][key])


if __name__=='__main__':unittest.main()
