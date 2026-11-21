"""Direct regressions for the unique middle region across old segment boundaries."""
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from Code2DAG.pipeline.loop_pair_model import build_loop_pair_views
from Code2DAG.pipeline.loop_model import build_loop_views,copy_loop_timing,LoopModelError
from Code2DAG.pipeline.loop_timing import _validate_loop_segments
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.pipeline.errors import ValidationError
from Code2DAG.tests.test_loop_l2 import build as canonical_build
from Code2DAG.tests.test_loop_l1 import fixture as single_fixture


def fixture(count=2,tail=1,head=0,n1=1,n2=1,origins=False):
    lines=[f'void step{i}(void);' for i in range(count)] if origins else []
    lines += ['int main(void) {','    int sum = 0;'];pre=len(lines)
    h1=len(lines)+1;lines += [f'    for (int i=0; i<{n1}; i++) {{','        sum += i;','    }'];x1=len(lines)
    mid=list(range(len(lines)+1,len(lines)+count+1));lines += [f'    step{i}();' if origins else f'    sum += {10+i};' for i in range(count)]
    h2=len(lines)+1;lines += [f'    for (int j=0; j<{n2}; j++) {{','        sum += j;','    }','    sum += 100;'];post=len(lines);lines += ['    return 0;','}']
    rows=[dict(seg_id='A',function='main',kind='compute',start_line=pre,end_line=mid[tail-1] if tail else x1)]
    for i,l in enumerate(mid[tail:count-head if head else count]):rows.append(dict(seg_id='MID' if count-tail-head==1 else 'M'+str(i),function='main',kind='compute',start_line=l,end_line=l,custom_origin={'id':i}))
    rows.append(dict(seg_id='B',function='main',kind='compute',start_line=mid[count-head] if head else h2,end_line=post))
    locs={f'main/step{i}':dict(function='main',line=l) for i,l in enumerate(mid)} if origins else {}
    mapping={k:next(r['seg_id'] for r in rows if r['start_line']<=v['line']<=r['end_line']) for k,v in locs.items()}
    s=dict(base_name='merge',segments=rows,node_locations=locs,node_to_segment=mapping)
    d=dict(nodes=[r['seg_id'] for r in rows],edges=[dict(src=a['seg_id'],dst=b['seg_id'],kind='intra',source='original order') for a,b in zip(rows,rows[1:])])
    return '\n'.join(lines)+'\n',s,d,dict(mid=mid,h1=h1,h2=h2,pre=pre,post=post)


def build_case(*args,**kwargs):
    text,s,d,m=fixture(*args,**kwargs)
    return build_loop_pair_views(base_name='merge',source_text=text,base_segments_json=s,base_dag_json=d),s,d,m


def middle(views,m):
    rows=[r for r in views[2]['segments'] if any(r['start_line']<=l<=r['end_line'] for l in m['mid'])]
    if len(rows)!=1:raise AssertionError('middle must have exactly one execution segment: '+str([(r['seg_id'],r['start_line'],r['end_line']) for r in rows]))
    return rows[0]


class LoopL2MiddleMergeTests(unittest.TestCase):
    def test_01_minimal_A_MID_B_lines6_and7(self):
        v,s,d,m=build_case();r=middle(v,m);self.assertEqual((6,7),(r['start_line'],r['end_line']));self.assertEqual(5,len(v[2]['segments']));self.assertEqual('LOOP:main@3:after',r['seg_id'])

    def test_02_middle_across_three_or_more_old_segments(self):
        v,s,d,m=build_case(count=4,tail=1);r=middle(v,m);self.assertEqual(['A','M0','M1','M2'],r['source_segments']);self.assertEqual((m['mid'][0],m['mid'][-1]),(r['start_line'],r['end_line']))

    def test_03_tail_multiple_complete_segments_and_second_owner_head(self):
        v,s,d,m=build_case(count=5,tail=1,head=1);r=middle(v,m);self.assertEqual(['A','M0','M1','M2','B'],r['source_segments']);self.assertEqual(5,len(v[2]['segments']));_validate_loop_segments(v[2])

    def test_04_all_contributing_original_segments_map_to_same_middle(self):
        v,s,d,m=build_case(count=5,tail=1,head=1);r=middle(v,m);mapping=v[2]['mappings']['original_segment_to_execution_segments']
        for sid in r['source_segments']:self.assertIn(r['seg_id'],mapping[sid]);self.assertEqual(1,mapping[sid].count(r['seg_id']))
        self.assertEqual(1,v[3]['nodes'].count(r['seg_id']))

    def test_05_generation_origins_stably_merge_without_fabrication(self):
        v,s,d,m=build_case(count=5,tail=1,head=1,origins=True);r=middle(v,m);self.assertEqual([f'main/step{i}' for i in range(5)],r['generation_origin'])
        for node in s['node_locations']:self.assertEqual([r['seg_id']],v[2]['mappings']['generation_node_to_execution_segments'][node])
        self.assertEqual(set(s['node_locations']),set(v[2]['mappings']['generation_node_to_execution_segments']))

    def test_06_every_middle_source_position_maps_only_once(self):
        v,s,d,m=build_case(count=5,tail=1,head=1);r=middle(v,m)
        for line in m['mid']:self.assertEqual([r['seg_id']],v[2]['mappings']['source_position_to_execution_segments'][f'LOOPSRC:main:line:{line}'])
        self.assertEqual([f'LOOPSRC:main:line:{line}' for line in m['mid']],r['source_position_ids'])

    def test_07_external_in_and_out_edge_attributes_preserved(self):
        text,s,d,m=fixture(count=4);s['segments'] += [dict(seg_id='P',function='external',kind='compute',start_line=1,end_line=1),dict(seg_id='Q',function='external',kind='compute',start_line=1,end_line=1)];d['nodes']+=['P','Q']
        incoming=dict(src='P',dst='MID' if any(r['seg_id']=='MID' for r in s['segments']) else 'M0',kind='create',custom={'source':'left'});outgoing=dict(src='M1',dst='Q',kind='join',custom={'source':'right'});d['edges'] += [incoming,outgoing]
        v=build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d);r=middle(v,m);self.assertIn(dict(incoming,dst=r['seg_id']),v[1]['edges']);self.assertIn(dict(outgoing,src=r['seg_id']),v[1]['edges'])

    def test_08_all_edge_kinds_and_attribute_distinctions_survive(self):
        text,s,d,m=fixture(count=3);s['segments'].append(dict(seg_id='P',function='external',kind='compute',start_line=1,end_line=1));d['nodes'].append('P')
        original=[dict(src='P',dst=target,kind=kind,tag=tag) for target,kind,tag in [('M0','create','a'),('M1','join','b'),('M0','sem_dep','c'),('M1','intra','d'),('M0','custom','e'),('M1','custom','f')]];d['edges']+=original
        v=build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d);r=middle(v,m)
        for e in original:self.assertIn(dict(e,dst=r['seg_id']),v[1]['edges'])

    def test_09_internal_order_edges_removed_and_identical_duplicates_only_deduped(self):
        text,s,d,m=fixture(count=3);s['segments'].append(dict(seg_id='P',function='external',kind='compute',start_line=1,end_line=1));d['nodes'].append('P')
        d['edges'] += [dict(src='P',dst='M0',kind='intra',tag='same'),dict(src='P',dst='M1',kind='intra',tag='same'),dict(src='P',dst='M1',kind='intra',tag='different')]
        v=build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d);r=middle(v,m)
        self.assertFalse(any(e['src']==e['dst'] for e in v[1]['edges']));projected=[e for e in v[1]['edges'] if e['src']=='P'];self.assertEqual(2,len(projected));self.assertEqual({'same','different'},{e['tag'] for e in projected})

    def test_10_contraction_cycle_rejected_without_deleting_cycle_edges(self):
        text,s,d,m=fixture();s['segments'].append(dict(seg_id='X',function='external',kind='compute',start_line=1,end_line=1));d['nodes'].append('X');d['edges'] += [dict(src='A',dst='X',kind='create'),dict(src='X',dst='MID',kind='sem_dep')]
        seen=[]
        def check(nodes,edges):seen.append((nodes,edges));return topo_sort(nodes,edges)
        with patch('Code2DAG.pipeline.loop_pair_model.topo_sort',side_effect=check):
            with self.assertRaisesRegex(ValidationError,'cycle'):build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertIn(('LOOP:main@3:after','X'),seen[-1][1]);self.assertIn(('X','LOOP:main@3:after'),seen[-1][1])

    def test_11_middle_timing_is_one_explicit_template_not_legacy_sum(self):
        v,s,d,m=build_case(count=5,tail=1,head=1);r=middle(v,m);_validate_loop_segments(v[2]);data={'weights':{t['template_id']:dict(avg_ns=7,total_ns=70,count=10,min_ns=5,max_ns=9) for t in v[2]['templates']}};out=copy_loop_timing(v[2],data)
        self.assertEqual(1,sum(sid==r['seg_id'] for sid in out['weights']));self.assertEqual(data['weights'][r['template_id']],out['weights'][r['seg_id']])
        with self.assertRaises(LoopModelError):copy_loop_timing(v[2],{'weights':{**data['weights'],'M0':{'avg_ns':99}}})

    def test_12_existing_same_segment_standalone_and_four_count_pairs_unchanged(self):
        for n1,n2 in [(2,3),(0,3),(2,0),(0,0)]:
            v=canonical_build(n1,n2);self.assertEqual(n1+n2+3,len(v[2]['segments']));self.assertEqual(1,len([r for r in v[2]['segments'] if r['start_line']<=8<=r['end_line']]))
        v,s,d,m=build_case(count=2,tail=0,head=0)
        # This layout has two middle old segments, so it must now normalize them.
        self.assertEqual(1,len([r for r in v[2]['segments'] if any(r['start_line']<=l<=r['end_line'] for l in m['mid'])]))

    def test_13_no_loop_and_single_loop_delegation_bytes_unchanged(self):
        text,s,d=single_fixture()
        for text in [text,'int main(void) {\n    int before = 1;\n}\n']:
            kwargs=dict(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
            self.assertEqual(json.dumps(build_loop_views(**kwargs),sort_keys=True).encode(),json.dumps(build_loop_pair_views(**kwargs),sort_keys=True).encode())

    def test_14_inputs_and_original_C_unchanged(self):
        text,s,d,m=fixture(count=5,tail=1,head=1);frozen=copy.deepcopy((text,s,d));p=Path(__file__).resolve().parents[2]/'Code2DAG/source_files/loop_l2/loop_l2.c';before=p.read_bytes();build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d);self.assertEqual(frozen,(text,s,d));self.assertEqual(before,p.read_bytes())

    def test_15_no_generation_origin_is_explicit_null(self):
        v,s,d,m=build_case();self.assertIsNone(middle(v,m)['generation_origin']);self.assertEqual({},v[2]['mappings']['generation_node_to_execution_segments'])

    def test_16_no_first_owner_tail_but_second_owner_head_merges(self):
        v,s,d,m=build_case(count=3,tail=0,head=1);r=middle(v,m);self.assertEqual(['M0','M1','B'],r['source_segments']);self.assertEqual('after',r['role']);self.assertEqual(v[2]['loops'][0]['loop_id'],r['loop_id']);_validate_loop_segments(v[2])

    def test_17_zero_iterations_keep_one_middle_and_exact_sequential_projection(self):
        for n1,n2 in [(0,0),(0,3),(3,0),(1,1)]:
            v,s,d,m=build_case(count=5,tail=1,head=1,n1=n1,n2=n2);r=middle(v,m);ids=v[3]['nodes'];self.assertEqual(n1+n2+3,len(ids));self.assertEqual(set(zip(ids,ids[1:])),{(e['src'],e['dst']) for e in v[3]['edges']});self.assertEqual(1,ids.count(r['seg_id']))

    def test_18_other_fragment_trace_attributes_are_retained_in_provenance(self):
        v,s,d,m=build_case(count=4,tail=1);r=middle(v,m);sources=r['source_fragment_provenance'];self.assertTrue(any(x.get('custom_origin')=={'id':0} for x in sources));self.assertTrue(any(x.get('custom_origin')=={'id':1} for x in sources))
