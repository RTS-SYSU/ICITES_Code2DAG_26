"""QA only. Independent oracle + properties; preserve every production file."""
import copy
import importlib
import json
import math
import os
from pathlib import Path
import tempfile
import traceback
import unittest
from unittest.mock import patch

from Code2DAG.tests.lock_qa_oracle import (SEED, random_case, weights_for, reference,
                                         assert_candidate, assert_weights, acyclic)
from Code2DAG.pipeline.lock_model import (contract_lock_groups, build_lock_views,
                                        analyze_lock_l2, aggregate_lock_timing, LockModelError)
from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.pipeline.errors import ValidationError, StageError
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.tests.test_lock_l2 import fixture as real_fixture, build as real_build, ALGOS, SOURCES
from Code2DAG.tests.test_lock_l1 import timing_for

FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')


def record(name,value):
    folder=os.environ.get('LOCK_QA_OUTPUT_DIR')
    if folder:
        p=Path(folder);p.mkdir(parents=True,exist_ok=True)
        (p/(name+'.json')).write_text(json.dumps(value,indent=2,allow_nan=True)+'\n')


def production(case):
    # Graph tests supply source-location metadata directly, isolating graph validation
    # from the separately tested C recognizer. No synthetic C is compiled or run.
    with patch('Code2DAG.pipeline.lock_model.analyze_lock_l2',return_value=(copy.deepcopy(case['mutexes']),copy.deepcopy(case['regions']))):
        return build_lock_views(base_name='qa',source_text=case['source'],base_segments_json=case['segments'],base_dag_json=case['dag'])


def expect_incompatible_schedule(segments,dag):
    with tempfile.TemporaryDirectory() as tmp:
        base=Path(tmp);p=base/'intermediate_results/qa/pipeline';b=p/'blocks/level2/lock_grouped';t=p/'timing/level2/lock_grouped'
        b.mkdir(parents=True);t.mkdir(parents=True)
        (b/'segments.json').write_text(json.dumps(segments));(b/'dag_seg.json').write_text(json.dumps(dag));(t/'timing.json').write_text('{"weights":{}}')
        with patch('Code2DAG.pipeline.schedule.get_algo',side_effect=AssertionError('must reject before algorithm')) as spy:
            for algo in ALGOS:
                try:run_schedule(base_dir=base,base_name='qa',level='level2',rule_name='lock_grouped',algo_name=algo)
                except StageError as e:assert 'not scheduler-compatible' in str(e)
                else:raise AssertionError('cyclic graph scheduled')
            spy.assert_not_called()
        assert not list(p.rglob('schedule.json'))


class LockQATests(unittest.TestCase):
    def test_01_independent_reference_200_random_dags_and_properties(self):
        cases=[random_case(i) for i in range(200)];record('random_graph_inputs',{'master_seed':SEED,'cases':cases})
        results=[]
        for case in cases:
            actual=None;expected=None
            try:
                self.assertTrue(acyclic(case['dag']['nodes'],case['dag']['edges']))
                expected=reference(case['dag'],case['groups']);st,sd,ss,dd,meta=production(case);actual={'segments':ss,'dag':dd,'meta':meta}
                assert_candidate(case['dag'],case['groups'],ss['mappings']['original_segment_to_grouped_node'],dd)
                self.assertEqual(expected['compatible'],meta['scheduler_compatible'])
                owner={m for g in case['groups'] for m in g['member_node_ids']}
                self.assertEqual(len(owner),sum(len(g['member_node_ids']) for g in case['groups']))
                for row in case['segments']['segments']:
                    if row['seg_id'] not in owner:self.assertEqual(row,next(r for r in ss['segments'] if r['seg_id']==row['seg_id']))
                for edge in case['dag']['edges']:
                    if edge['src'] not in owner and edge['dst'] not in owner:
                        e=next(e for e in dd['edges'] if (e['src'],e['dst'],e['kind'])==(edge['src'],edge['dst'],edge['kind']))
                        self.assertEqual(edge,{k:v for k,v in e.items() if k!='provenance'})
                groups=copy.deepcopy(case['groups']);groups.reverse()
                for g in groups:
                    for field in ('member_node_ids','member_segment_ids','members'):g[field].reverse()
                first=contract_lock_groups(case['dag'],case['groups']);other=contract_lock_groups(case['dag'],groups)
                self.assertEqual(first,other)
                overlap=copy.deepcopy(case['groups'][0]);overlap['mutex_id']='zz_overlap';overlap['group_id']='LOCKGROUP:zz_overlap'
                for r in overlap['members']:r['mutex_id']='zz_overlap'
                with self.assertRaisesRegex(LockModelError,'multiple mutex groups'):
                    contract_lock_groups(case['dag'],case['groups']+[overlap])
                if expected['compatible']:
                    topo_sort(dd['nodes'],[(e['src'],e['dst']) for e in dd['edges']])
                else:
                    with self.assertRaisesRegex(ValidationError,'cycle'):topo_sort(dd['nodes'],[(e['src'],e['dst']) for e in dd['edges']])
                    expect_incompatible_schedule(ss,dd)
                results.append({'index':case['index'],'seed':case['seed'],'nodes':len(case['dag']['nodes']),
                                'group_sizes':[len(g['member_node_ids']) for g in case['groups']],
                                'contracted_compatible':expected['compatible'],'all_properties_passed':True})
            except Exception:
                record('failure',{'test':'random graph differential','case':case,'expected':expected,'actual':actual,'traceback':traceback.format_exc()});raise
        self.assertTrue(any(x['contracted_compatible'] for x in results));self.assertTrue(any(not x['contracted_compatible'] for x in results))
        record('random_graph_results',results)

    def test_02_random_timing_200_cases_and_invalid_field_types(self):
        results=[]
        for i in range(200):
            case=random_case(2*i);st,sd,ss,dd,meta=production(case);self.assertTrue(meta['scheduler_compatible'])
            timing=weights_for(case);out=aggregate_lock_timing(ss,timing)
            try:
                assert_weights(case['groups'],timing['weights'],out['weights'])
                members={n for g in case['groups'] for n in g['member_node_ids']}
                for node in case['dag']['nodes']:
                    if node not in members:self.assertEqual(timing['weights'][node],out['weights'][node])
                if len(case['groups'])>1:
                    mutated=copy.deepcopy(timing);target=case['groups'][-1]['member_segment_ids'][0]
                    for field in FIELDS:mutated['weights'][target][field]*=3
                    isolated=aggregate_lock_timing(ss,mutated)
                    for group in case['groups'][:-1]:self.assertEqual(out['weights'][group['group_id']],isolated['weights'][group['group_id']])
            except Exception:
                record('failure',{'test':'random timing sums','case':case,'timing':timing,'actual':out,'traceback':traceback.format_exc()});raise
            results.append({'seed':case['seed'],'group_sizes':[len(g['member_node_ids']) for g in case['groups']],
                            'member_timing':timing,'group_weights':{g['group_id']:out['weights'][g['group_id']] for g in case['groups']}})
        record('random_timing_results',{'master_seed':SEED,'cases':results})
        st,sd,ss,dd,meta=production(random_case(0));timing=weights_for(random_case(0));member=ss['lock_groups'][0]['member_segment_ids'][0]
        rejections=[]
        for field in FIELDS:
            for label,value in [('float_integral',1.0),('float_fraction',1.25),('nan',math.nan),('inf',math.inf),('negative',-1),('boolean',True)]:
                t=copy.deepcopy(timing);t['weights'][member][field]=value
                with self.subTest(field=field,value=label),self.assertRaisesRegex(LockModelError,'timing fields must be nonnegative integers'):
                    aggregate_lock_timing(ss,t)
                rejections.append([field,label])
            t=copy.deepcopy(timing);del t['weights'][member][field]
            with self.assertRaisesRegex(LockModelError,'missing or unsupported timing fields'):aggregate_lock_timing(ss,t)
        t=copy.deepcopy(timing);del t['weights'][member]
        with self.assertRaisesRegex(LockModelError,'every original execution segment exactly once'):aggregate_lock_timing(ss,t)
        t=copy.deepcopy(timing);t['weights'][member]['unknown']=1
        with self.assertRaisesRegex(LockModelError,'missing or unsupported timing fields'):aggregate_lock_timing(ss,t)
        t=copy.deepcopy(timing);t['weights'][member]['count']=0
        with self.assertRaisesRegex(LockModelError,'missing member samples'):aggregate_lock_timing(ss,t)
        record('invalid_timing_results',{'rejected_types':rejections,'missing_fields':list(FIELDS),'missing_node':True,'unknown_field':True,'zero_member_samples':True})

    def test_03_all_five_algorithms_consume_avg_ns_not_other_metrics(self):
        st,sd,ss,dd,_=real_build();timing=aggregate_lock_timing(ss,timing_for(st));trace=[]
        for name in ALGOS:
            algo=get_algo(name);module=importlib.import_module(type(algo).__module__);loader=module.load_avg_weights
            def spy(t,nodes):
                result=loader(t,nodes);self.assertEqual({n:t['weights'][n]['avg_ns'] for n in nodes},result);return result
            with patch.object(module,'load_avg_weights',side_effect=spy) as hook:
                result=algo.compute(segments_json=ss,dag_json=dd,timing_json=timing);self.assertEqual(1,hook.call_count)
            only={'weights':{n:{'avg_ns':w['avg_ns']} for n,w in timing['weights'].items()}}
            self.assertEqual(result,algo.compute(segments_json=ss,dag_json=dd,timing_json=only))
            broken=copy.deepcopy(timing);del broken['weights'][dd['nodes'][0]]['avg_ns']
            with self.assertRaisesRegex(ValidationError,'missing avg_ns'):algo.compute(segments_json=ss,dag_json=dd,timing_json=broken)
            trace.append({'algorithm':name,'module':module.__name__,'weight_field':'avg_ns','loader_calls':1,'other_fields_not_consumed':True})
        record('algorithm_weight_fields',trace)

    def test_04_supported_source_boundary_corpus(self):
        source=(SOURCES/'two_pairs.c').read_text();decl='static pthread_mutex_t mutex_a = PTHREAD_MUTEX_INITIALIZER;\nstatic pthread_mutex_t mutex_b = PTHREAD_MUTEX_INITIALIZER;'
        start=source.index('static void *worker_0');middle=source.index('static void *worker_1');end=source.index('int main')
        gap='    pthread_create(&t1, NULL, worker_1, NULL);\n    pthread_join(t0, NULL);'
        def with_gap(blank):
            return source.replace(gap,gap.replace('\n','\n'+blank+'\n'))
        table={
            'one_blank_between_create_join':with_gap(''),
            'several_blank_between_create_join':with_gap('\n\n'),
            'space_only_blank':with_gap('    '),
            'tab_only_blank':with_gap('\t\t'),
            'mixed_blank':with_gap(' \t \n\t\n   '),
            'crlf_blank':with_gap(' \t ').replace('\n','\r\n'),
            'line_comment_blank':with_gap('    // whitespace regression'),
            'block_comment_blank':with_gap('    /* first\n       second */'),
            'internal_call_whitespace':source.replace('pthread_create(&t1, NULL, worker_1, NULL);','pthread_create(\n        &t1,\n        NULL, worker_1,\n        NULL\n    );').replace('pthread_join(t0, NULL);','pthread_join(\n        t0,\n        NULL\n    );'),
            'spaces':source.replace('pthread_mutex_lock(&','pthread_mutex_lock ( &').replace('pthread_mutex_unlock(&','pthread_mutex_unlock ( &'),
            'tabs':source.replace('    ','\t'),
            'blank_lines':source.replace(';\n',';\n\n'),
            'crlf':source.replace('\n','\r\n'),
            'line_comments':source.replace(';\n','; // boundary comment\n'),
            'block_comments':source.replace('    v0 += 10;','    /* first\n       second */ v0 += 10;'),
            'declaration_order':source.replace(decl,'\n'.join(reversed(decl.splitlines()))),
            'worker_order':source[:start]+source[middle:end]+source[start:middle]+source[end:],
            'multi_mutex':source,
            'three_members':(SOURCES/'single_three.c').read_text(),
        }
        results=[]
        for label,text in table.items():
            try:
                mutexes,regions=analyze_lock_l2(text)
                self.assertEqual(3 if label=='three_members' else 4,len(regions))
                self.assertEqual(1 if label=='three_members' else 2,len(mutexes))
            except Exception:
                record('failure',{'test':'accepted source corpus','label':label,'source':text,'expected':'accept within declared scope','actual':traceback.format_exc()});raise
            results.append({'label':label,'accepted':True,'regions':regions})
        record('source_accepted_results',results)

    def test_05_rejected_source_and_segment_corpus_stable_reasons(self):
        source=(SOURCES/'two_pairs.c').read_text()
        table=[
            ('join_before_last_create',source.replace('    pthread_create(&t1, NULL, worker_1, NULL);\n    pthread_join(t0, NULL);','    pthread_join(t0, NULL);\n    pthread_create(&t1, NULL, worker_1, NULL);'),'all creates must precede joins'),
            ('macro',source.replace('#include <pthread.h>','#include <pthread.h>\n#define LOCK(x) pthread_mutex_lock(x)'), 'L1 rejects macros and unsupported includes'),
            ('pointer',source.replace('pthread_mutex_lock(&mutex_a)','pthread_mutex_lock(ptr)',1),'L1 requires standalone direct lock/unlock(&mutex); trylock/wrappers/aliases unsupported'),
            ('alias',source.replace('    v0 = 1;','    void *alias = &mutex_a;\n    v0 = 1;',1),'mutex aliases, shadowing, or additional uses unsupported'),
            ('wrapper',source.replace('pthread_mutex_lock(&mutex_a)','wrapped_lock(&mutex_a)',1),'mutex aliases, shadowing, or additional uses unsupported'),
            ('conditional',source.replace('    v0 = 1;','    if (v0) { v0 = 1; }',1),'L1 requires unconditional straight-line functions; control transfers are unsupported'),
            ('loop',source.replace('    v0 = 1;','    while (v0) { v0 = 1; }',1),'L1 requires unconditional straight-line functions; control transfers are unsupported'),
            ('nested',source.replace('    v0 += 10;','    pthread_mutex_lock(&mutex_b);',1),'nested or crossing mutex regions unsupported'),
            ('crossing',source.replace('pthread_mutex_unlock(&mutex_a)','pthread_mutex_unlock(&mutex_b)',1),'unpaired or crossing mutex unlock'),
            ('trylock',source.replace('pthread_mutex_lock(&mutex_a)','pthread_mutex_trylock(&mutex_a)',1),'L1 requires standalone direct lock/unlock(&mutex); trylock/wrappers/aliases unsupported'),
            ('unmatched_unlock',source.replace('    pthread_mutex_lock(&mutex_a);','',1),'unpaired or crossing mutex unlock'),
            ('empty',source.replace('    v0 += 10;\n    v0 += 20;','',1),'Lock requires a nonempty multiline critical section'),
            ('single_member',source.replace('    pthread_mutex_lock(&mutex_a);\n    v0 += 10;\n    v0 += 20;\n    pthread_mutex_unlock(&mutex_a);','    v0 += 10;',1),'each mutex group requires at least two complete regions'),
            ('external',source.replace('    v0 = 1;','    external_work();',1),'L1 rejects unknown/wrapped/indirect calls'),
        ]
        results=[]
        for label,text,reason in table:
            try:
                with self.assertRaises(LockModelError) as ctx:analyze_lock_l2(text)
                self.assertEqual(reason,str(ctx.exception))
            except Exception:
                record('failure',{'test':'rejected source corpus','label':label,'source':text,'expected':reason,'actual':traceback.format_exc()});raise
            results.append({'label':label,'reason':reason})
        for label in ('split_segment','one_segment_multiple_locks'):
            text,s,d,groups=real_fixture()
            row=s['segments'][0]
            if label=='split_segment':
                last=row['end_line'];row['end_line']=row['start_line']
                part=dict(row,seg_id='SPLIT',kind='compute',start_line=row['start_line']+1,end_line=last)
                s['segments'].append(part);d['nodes'].append('SPLIT')
                reason='critical section must correspond to exactly one complete existing segment'
            else:
                rows=[r for r in s['segments'] if r['function']==row['function']]
                row['end_line']=max(r['end_line'] for r in rows)
                reason='existing mutex segment includes outside code or is not a complete critical section'
            with self.assertRaises(LockModelError) as ctx:build_lock_views(base_name='lock_l2',source_text=text,base_segments_json=s,base_dag_json=d)
            self.assertEqual(reason,str(ctx.exception));results.append({'label':label,'reason':reason})
        record('source_rejected_results',results)

    def test_06_fault_sensitivity_of_independent_artifact_checks(self):
        case=random_case(0);st,sd,ss,dd,meta=production(case);mapping=ss['mappings']['original_segment_to_grouped_node'];results=[]
        corrupted=copy.deepcopy(dd);corrupted['edges'][0]['provenance'].pop()
        with self.assertRaisesRegex(AssertionError,'provenance'):assert_candidate(case['dag'],case['groups'],mapping,corrupted)
        results.append('deleted provenance')
        corrupted_map=copy.deepcopy(mapping);member=case['groups'][0]['member_node_ids'][0];corrupted_map[member]='FAKE_SECOND_GROUP'
        with self.assertRaisesRegex(AssertionError,'mapping'):assert_candidate(case['dag'],case['groups'],corrupted_map,dd)
        results.append('conflicting member mapping')
        timing=weights_for(case)
        # All-positive distinct fields make max versus sum necessarily distinguishable.
        for i,n in enumerate(timing['weights']):timing['weights'][n]={f:i+1 for f in FIELDS}
        grouped=aggregate_lock_timing(ss,timing);bad=copy.deepcopy(grouped['weights'])
        for g in case['groups']:
            for f in FIELDS:bad[g['group_id']][f]=max(timing['weights'][m][f] for m in g['member_segment_ids'])
        with self.assertRaisesRegex(AssertionError,'weight sum'):assert_weights(case['groups'],timing['weights'],bad)
        results.append('max instead of sum')
        cycle=next(random_case(i) for i in range(1,200) if not reference(random_case(i)['dag'],random_case(i)['groups'])['compatible'])
        st,sd,ss,dd,meta=production(cycle);bad=copy.deepcopy(dd)
        # Forge an acyclic artifact by erasing every backward projected edge.
        rank={n:i for i,n in enumerate(sorted(bad['nodes']))};bad['edges']=[e for e in bad['edges'] if rank[e['src']]<rank[e['dst']]]
        self.assertTrue(acyclic(bad['nodes'],bad['edges']))
        with self.assertRaisesRegex(AssertionError,'provenance'):assert_candidate(cycle['dag'],cycle['groups'],ss['mappings']['original_segment_to_grouped_node'],bad)
        results.append('cycle edges erased to forge compatibility')
        multi=random_case(next(i for i in range(200) if len(random_case(i)['groups'])>1));st,sd,ss,dd,meta=production(multi)
        wrong=copy.deepcopy(multi['groups']);first=wrong[0];second=wrong.pop(1)
        for key in ('member_node_ids','member_segment_ids','members'):first[key]+=second[key]
        for r in first['members']:r['mutex_id']=first['mutex_id']
        _,wrongmap,wrongdag=contract_lock_groups(multi['dag'],wrong)
        with self.assertRaises(AssertionError):assert_candidate(multi['dag'],multi['groups'],wrongmap,wrongdag)
        results.append('different mutex groups falsely merged')
        record('fault_sensitivity',{'five_mutations_rejected_by_test_oracle':results,'not_a_claim_of_production_artifact_authentication':True})

    def test_07_same_input_20_repetitions_semantically_identical(self):
        expected=real_build();timing=timing_for(expected[0]);weights=aggregate_lock_timing(expected[2],timing)
        schedules={a:get_algo(a).compute(segments_json=expected[2],dag_json=expected[3],timing_json=weights) for a in ALGOS}
        for i in range(20):
            actual=real_build();self.assertEqual(expected,actual);self.assertEqual(weights,aggregate_lock_timing(actual[2],timing))
            for a in ALGOS:self.assertEqual(schedules[a],get_algo(a).compute(segments_json=actual[2],dag_json=actual[3],timing_json=weights))
        record('repeat_stability',{'repetitions':20,'views_mappings_weights_and_five_schedules_equal':True})
