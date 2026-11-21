from __future__ import annotations
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.pipeline.condition_model import build_condition_views
from Code2DAG.pipeline.errors import ValidationError
from Code2DAG.tests.test_condition_model import SIMPLE_SOURCE, _base_payload


def fixture():
    bs, bd = _base_payload()
    _st, _sd, segments, dag, _meta = build_condition_views(base_name='test', source_text=SIMPLE_SOURCE,
        base_segments_json=bs, base_dag_json=bd)
    profiles = {}
    for branch in ('then', 'else'):
        weights = {}
        for row in segments['segments']:
            active = not row.get('condition_memberships') or row['condition_memberships'][0]['branch_id'] == branch
            weights[row['seg_id']] = {'count': int(active), 'avg_ns': 10 if active else 0, 'total_ns': 10 if active else 0}
        profiles[branch] = {'weights': weights}
    pooled = {row['seg_id']: {'avg_ns': 10} for row in segments['segments']}
    timing = {'weights': pooled, 'condition_profiles': profiles,
              'condition_config': {'cores': 2, 'seed_algo': 'FIFO', 'max_passes': 12}}
    return segments, dag, timing


class ConditionalAlgorithmTests(unittest.TestCase):
    def test_both_registered_algorithms_keep_complete_static_priority_maps(self):
        segments, dag, timing = fixture()
        for name in ('condition_pareto', 'condition_union'):
            out = get_algo(name).compute(segments_json=segments, dag_json=dag, timing_json=timing)
            self.assertEqual(set(dag['nodes']), set(out['priorities']))
            self.assertFalse(out['meta']['is_response_time_bound'])
            sim = out['meta']['simulation']
            for b in ('then', 'else'):
                active = set(sim['scenario_traces'][b]['active'])
                for row in segments['segments']:
                    for member in row.get('condition_memberships', []):
                        self.assertEqual(member['branch_id'] == b, row['seg_id'] in active)
            if name == 'condition_pareto':
                self.assertTrue(all(a <= b for a,b in zip(sim['final'], sim['initial'])))

    def test_missing_or_unobserved_paths_and_invalid_options_are_rejected(self):
        segments, dag, original = fixture()
        branch = next(row['seg_id'] for row in segments['segments'] if row.get('condition_memberships'))
        for mutation in ('no_profiles', 'missing_else', 'unobserved_then', 'wrong_else', 'cores', 'recursive_seed', 'passes'):
            timing = copy.deepcopy(original)
            if mutation == 'no_profiles': timing.pop('condition_profiles')
            if mutation == 'missing_else': timing['condition_profiles'].pop('else')
            if mutation == 'unobserved_then': timing['condition_profiles']['then']['weights'][branch]['count'] = 0
            if mutation == 'wrong_else': timing['condition_profiles']['else']['weights'][branch]['count'] = 1
            if mutation == 'cores': timing['condition_config']['cores'] = 0
            if mutation == 'recursive_seed': timing['condition_config']['seed_algo'] = 'condition_pareto'
            if mutation == 'passes': timing['condition_config']['max_passes'] = 0
            with self.subTest(mutation=mutation), self.assertRaises(ValidationError):
                get_algo('condition_pareto').compute(segments_json=segments, dag_json=dag, timing_json=timing)

    def test_ordinary_algorithms_still_ignore_exclusivity_without_changing_outputs(self):
        segments, dag, timing = fixture()
        stripped = copy.deepcopy(segments)
        for row in stripped['segments']: row.pop('condition_memberships', None)
        for name in ('FIFO', 'LPF', 'heft', 't_level', 'zhao2020'):
            self.assertEqual(get_algo(name).compute(segments_json=segments,dag_json=dag,timing_json=timing),
                             get_algo(name).compute(segments_json=stripped,dag_json=dag,timing_json=timing))


class NativePipelineTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('gcc-13'), 'native GCC13 required')
    def test_native_pipeline_mapping_runtime_and_both_paths(self):
        from Code2DAG.pipeline.condition_pipeline import prepare_condition_run
        from Code2DAG.pipeline.schedule import run_schedule
        from Code2DAG.pipeline.errors import StageError
        root = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)
            source_dir = temp / 'source'
            source_dir.mkdir()
            source = source_dir / 'source.c'
            source.write_text('''#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
static void *worker(void *arg) {
    if (getenv("CODE2DAG_CONDITION")) {
        puts("then");
    } else {
        puts("else");
    }
    return NULL;
}
int main(void) {
    pthread_t t;
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    return 0;
}
''')
            out = temp / 'prepared'
            prepared = prepare_condition_run(source=source, output_dir=out, runtime_base=root/'Code2DAG', cores=2, repeats=1)
            pipeline = Path(prepared['base_dir'])/'intermediate_results/source/pipeline'
            blocks = pipeline/'blocks/level2/conditional_branches'
            segments = json.loads((blocks/'segments.json').read_text())
            for group in prepared['groups']:
                app = Path(prepared['programs'][group])
                mapping = json.loads((app.parent/'priority_thread_mapping.json').read_text())
                for branch in ('then','else'):
                    env = dict(os.environ);env.pop('CODE2DAG_CONDITION',None)
                    if branch == 'then': env['CODE2DAG_CONDITION']='1'
                    env['CODE2DAG_PRIORITY_MODE']='none';env['CODE2DAG_AUDIT']='1'
                    p = subprocess.run([str(app)],env=env,capture_output=True,text=True,timeout=10)
                    self.assertEqual(0,p.returncode,p.stderr);self.assertEqual(branch+'\n',p.stdout)
                    metrics = json.loads(next(l[len('C2D_METRICS '):] for l in p.stderr.splitlines() if l.startswith('C2D_METRICS ')))
                    events = [json.loads(l[len('C2D_EVENT '):]) for l in p.stderr.splitlines() if l.startswith('C2D_EVENT ')]
                    self.assertGreater(metrics['response_ns'],0)
                    self.assertLessEqual(metrics['node_makespan_ns'], metrics['response_ns'])
                    self.assertEqual(0,metrics['probe_errors']);self.assertEqual(0,metrics['priority_enabled'])
                    expected = {r['seg_id']:r for r in mapping['nodes'] if not r['condition_memberships'] or r['condition_memberships'][0]['branch_id']==branch}
                    self.assertEqual(set(expected),{e['node'] for e in events})
                    self.assertEqual(len(expected),len(events))
                    owners = {}
                    for e in events:
                        row = expected[e['node']]
                        self.assertEqual(row['priority'],e['requested'])
                        self.assertLessEqual(e['begin_ns'],e['end_ns'])
                        owners.setdefault(row['function'],set()).add(e['tid'])
                    self.assertTrue(all(len(v)==1 for v in owners.values()))
                    text = (app.parent/'program.c').read_text()
                    restored = '\n'.join(l for l in text.splitlines() if l!='#include "condition_runtime.h"' and not l.startswith(('c2d_node_begin(', 'c2d_node_end(')))+'\n'
                    self.assertEqual(source.read_text(),restored)
            from Code2DAG.pipeline.condition_experiment import run_comparison
            result = run_comparison(prepared_file=out/'prepared.json', output_dir=temp/'control_run',
                                    repeats=2, warmups=0, control_only=True)
            self.assertEqual('completed', result['status'])
            self.assertFalse(result['hardware_fifo_verified'])
            self.assertFalse(result['strict_ideal_DAG_equivalence_established'])
            self.assertEqual(12,result['run_count'])
            broken = copy.deepcopy(prepared)
            broken['binary_sha256']['FIFO'] = '0'*64
            (temp/'broken.json').write_text(json.dumps(broken))
            with self.assertRaisesRegex(StageError, 'binary changed'):
                run_comparison(prepared_file=temp/'broken.json',output_dir=temp/'must_not_run',repeats=2)
            with self.assertRaisesRegex(StageError, 'outside the source'):
                prepare_condition_run(source=source, output_dir=source_dir/'bad_output',
                                      runtime_base=root/'Code2DAG', cores=2, repeats=1)

            # Fail closed when actual realtime permission is unavailable.
            env = dict(os.environ);env.pop('CODE2DAG_CONDITION',None);env['CODE2DAG_PRIORITY_MODE']='set';env['CODE2DAG_AUDIT']='1'
            p = subprocess.run([prepared['programs']['FIFO']],env=env,capture_output=True,text=True,timeout=10)
            metrics = json.loads(next(l[len('C2D_METRICS '):] for l in p.stderr.splitlines() if l.startswith('C2D_METRICS ')))
            self.assertEqual(86 if metrics['set_failures'] or metrics['verification_failures'] else 0,p.returncode)
            # Stale profiles must not be silently applied to a new model.
            changed = (blocks/'segments.json').read_text()+'\n';(blocks/'segments.json').write_text(changed)
            with self.assertRaisesRegex(StageError,'profiles do not match'):
                run_schedule(base_dir=Path(prepared['base_dir']),base_name='source',level='level2',rule_name='conditional_branches',algo_name='condition_pareto')

class RuntimeValidationTests(unittest.TestCase):
    def test_runtime_records_must_match_node_priority_and_thread_ownership(self):
        from Code2DAG.pipeline.condition_experiment import validate_events
        from Code2DAG.pipeline.errors import StageError
        mapping = {'nodes': [
            {'seg_id':'common','function':'worker','priority':50,'condition_memberships':[]},
            {'seg_id':'then','function':'worker','priority':40,'condition_memberships':[{'branch_id':'then'}]},
            {'seg_id':'else','function':'worker','priority':30,'condition_memberships':[{'branch_id':'else'}]},
        ]}
        good = [dict(node=n,tid=123,requested=p,rc=0,query=0,policy=1,actual=p,begin_ns=i,end_ns=i+1)
                for i,(n,p) in enumerate([('common',50),('then',40)])]
        self.assertEqual({'worker':123},validate_events(mapping,good,'then',True))
        for fault in ('missing','duplicate','unselected','owner','actual','rc'):
            events=copy.deepcopy(good)
            if fault=='missing':events.pop()
            if fault=='duplicate':events.append(events[0])
            if fault=='unselected':events[1]['node']='else'
            if fault=='owner':events[1]['tid']=456
            if fault=='actual':events[1]['actual']=1
            if fault=='rc':events[1]['rc']=1
            with self.subTest(fault=fault),self.assertRaises(StageError):
                validate_events(mapping,events,'then',True)

    def test_missing_or_duplicate_summary_records_are_rejected(self):
        from Code2DAG.pipeline.condition_experiment import parse_runtime
        from Code2DAG.pipeline.errors import StageError
        for text in ('', 'C2D_METRICS {}\nC2D_METRICS {}\n'):
            with self.assertRaises(StageError):parse_runtime(text)

if __name__=='__main__':unittest.main()
