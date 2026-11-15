"""Controlled Linux experiments; reports measured outcomes, never assumes improvement."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import random
import resource
import statistics
import subprocess
import time

from .errors import StageError
from .io_utils import read_json, write_json


def parse_runtime(stderr):
    metrics = [json.loads(line[len('C2D_METRICS '):]) for line in stderr.splitlines() if line.startswith('C2D_METRICS ')]
    if len(metrics) != 1:
        raise StageError('expected exactly one runtime metrics record')
    events = [json.loads(line[len('C2D_EVENT '):]) for line in stderr.splitlines() if line.startswith('C2D_EVENT ')]
    return metrics[0], events


def validate_events(mapping, events, branch, require_fifo):
    expected = {row['seg_id']: row for row in mapping['nodes'] if not row['condition_memberships'] or row['condition_memberships'][0]['branch_id'] == branch}
    if len(events) != len(expected) or {event['node'] for event in events} != set(expected):
        raise StageError('missing, duplicate or unselected node executed')
    owners = {}
    for event in events:
        row = expected[event['node']]
        if event['requested'] != row['priority'] or event['begin_ns'] > event['end_ns']:
            raise StageError('priority mapping or timestamps are invalid')
        owners.setdefault(row['function'], set()).add(event['tid'])
        if require_fifo and (event['rc'] or event['query'] or event['policy'] != 1 or event['actual'] != row['priority']):
            raise StageError('FIFO policy/priority was not actually applied')
    if any(len(ids) != 1 for ids in owners.values()):
        raise StageError('one source owner function ran in multiple threads; current mapping contract is unsupported')
    return {fn: next(iter(ids)) for fn,ids in owners.items()}


def summarize(values):
    ordered = sorted(values)
    return {'count': len(values), 'mean': statistics.mean(values), 'median': statistics.median(values),
            'stdev': statistics.stdev(values) if len(values) > 1 else 0,
            'min': min(values), 'max': max(values), 'p95': ordered[min(len(ordered)-1, int(.95*len(ordered)))]}


def paired_interval(deltas, seed=1937):
    rng = random.Random(seed)
    draws = sorted(statistics.mean(rng.choices(deltas, k=len(deltas))) for _ in range(2000))
    return {'mean_delta_ns': statistics.mean(deltas), 'bootstrap_95_interval_ns': [draws[50], draws[1949]],
            'negative_favors_candidate': True, 'samples': len(deltas)}


def run_comparison(*, prepared_file, output_dir, repeats=30, warmups=3, random_seed=20261027, control_only=False):
    if os.geteuid() == 0:
        raise StageError('drop to a regular user with an inherited RTPRIO limit before running the workload')
    if repeats < 2 or warmups < 0 or output_dir.exists():
        raise StageError('new output directory, >=2 repeats and >=0 warmups required')
    prepared = read_json(prepared_file)
    for group,path in prepared['programs'].items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != prepared['binary_sha256'][group]:
            raise StageError('prepared binary changed: ' + group)
    cpus = prepared.get('cpu_affinity', sorted(os.sched_getaffinity(0))[:prepared['cores']])
    if not set(cpus) <= os.sched_getaffinity(0):
        raise StageError('the profiling CPU affinity is no longer available')
    if len(cpus) != prepared['cores']:
        raise StageError('insufficient allowed CPUs for the prepared core count')
    output_dir.mkdir(parents=True)
    logs = output_dir / 'logs'
    logs.mkdir()
    base = Path(prepared['base_dir']) / 'intermediate_results' / prepared['base_name'] / 'pipeline'
    dag = read_json(base/'blocks/level2/conditional_branches/dag_sched.json')
    baseline = prepared['seed_algo']
    groups = ([baseline,'condition_pareto','condition_union'] if not control_only else []) + ['trace_no_priority','uninstrumented']
    records = []
    commands = []
    summary = {'status': 'running', 'hardware_fifo_verified': False, 'performance_improvement_proven': False,
               'prepared_file': str(prepared_file), 'cpu_affinity': cpus, 'cores': len(cpus), 'repeats': repeats,
               'warmups': warmups, 'random_seed': random_seed, 'control_only': control_only,
               'environment': {'uname': list(os.uname()), 'euid': os.geteuid(), 'rtprio': resource.getrlimit(resource.RLIMIT_RTPRIO),
                               'loadavg_start': list(os.getloadavg()), 'monotonic_clock': vars(time.get_clock_info('monotonic')),
                               'process_status': [line for line in Path('/proc/self/status').read_text().splitlines() if line.startswith(('CapEff:','NoNewPrivs:','Seccomp:'))]},
               'definitions': {'response_ns': 'main entry to return: one job released at main entry, NOT an analytical response-time bound',
                               'node_makespan_ns': 'first full-source segment begin to last segment end, including blocking intervals; only trace-enabled groups',
                               'set_cpu_ns': 'sum of measured pthread_setschedparam thread CPU intervals, includes clock sampling cost',
                               'set_wall_ns': 'sum of call elapsed intervals; may include descheduling and overlap across threads',
                               'probe_cpu_ns': 'sampled entry/exit probe CPU cost; not all kernel scheduling overhead',
                               'process_wall_ns': 'parent launch to reap, including process startup and log emission'},
               'limitations': ['Small fixed workload; existing shared-data races unchanged.',
                              'Node priorities are sequential thread-priority changes, not independent node jobs.',
                              'Mutex blocking, priority inheritance and source gaps are not equivalent to the abstract simulator.',
                              'Bootstrap intervals are descriptive exploratory intervals, without multiple-comparison correction.',
                              'End-to-start diagnostics compare whole source ranges including waits, while profiling may time only computation inside mutexes; overlap is not by itself a logical synchronization error.']}
    summary['environment']['cpus'] = {str(cpu): {name: (Path('/sys/devices/system/cpu')/('cpu%d'%cpu)/rel).read_text().strip() for name,rel in [('core_id','topology/core_id'),('package_id','topology/physical_package_id'),('siblings','topology/thread_siblings_list'),('governor','cpufreq/scaling_governor'),('current_freq','cpufreq/scaling_cur_freq')] if (Path('/sys/devices/system/cpu')/('cpu%d'%cpu)/rel).exists()} for cpu in cpus}
    summary['environment']['realtime_throttling'] = {name: (Path('/proc/sys/kernel')/name).read_text().strip() for name in ('sched_rt_runtime_us','sched_rt_period_us') if (Path('/proc/sys/kernel')/name).exists()}
    def save():
        write_json(output_dir/'summary.json', summary)
    def child_setup():
        os.sched_setaffinity(0, set(cpus))
        resource.setrlimit(resource.RLIMIT_CPU, (5,6))
        resource.setrlimit(resource.RLIMIT_RTTIME, (500000,1000000))
    originals = {}
    serial = 0
    def execute(group, branch, phase, block):
        nonlocal serial
        serial += 1
        program_group = baseline if group == 'trace_no_priority' else group
        app = Path(prepared['programs'][program_group])
        env = dict(os.environ)
        env.pop(prepared['condition_env'], None)
        if branch == 'then': env[prepared['condition_env']] = '1'
        env['CODE2DAG_PRIORITY_MODE'] = 'none' if group in ('trace_no_priority','uninstrumented') else 'set'
        env['CODE2DAG_AUDIT'] = '1' if phase == 'audit' else '0'
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        start = time.perf_counter_ns()
        proc = subprocess.run([str(app)], cwd=app.parent, env=env, capture_output=True, text=True,
                              timeout=15, preexec_fn=child_setup)
        elapsed = time.perf_counter_ns() - start
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        label = '%04d_%s_%s_%s_%d' % (serial,phase,group,branch,block)
        (logs/(label+'.stdout')).write_text(proc.stdout)
        (logs/(label+'.stderr')).write_text(proc.stderr)
        commands.append({'label': label, 'argv': [str(app)], 'returncode': proc.returncode, 'condition': env.get(prepared['condition_env']), 'priority_mode': env['CODE2DAG_PRIORITY_MODE'], 'audit': env['CODE2DAG_AUDIT']})
        write_json(output_dir/'commands.json', commands)
        metrics, events = parse_runtime(proc.stderr)
        row = {'label': label, 'group': group, 'branch': branch, 'phase': phase, 'block': block,
               'argv': [str(app)], 'returncode': proc.returncode, 'process_wall_ns': elapsed,
               'child_cpu_ns': int(1e9*((after.ru_utime+after.ru_stime)-(before.ru_utime+before.ru_stime))),
               'voluntary_switches': after.ru_nvcsw-before.ru_nvcsw,
               'involuntary_switches': after.ru_nivcsw-before.ru_nivcsw,
               'metrics': metrics, 'events': events}
        records.append(row)
        with (output_dir/'runs.jsonl').open('a') as f: f.write(json.dumps(row)+'\n')
        if proc.returncode != 0 or metrics['probe_errors']:
            raise StageError('run failed (including unavailable FIFO): ' + label)
        if group == 'uninstrumented':
            if phase == 'audit': originals[branch] = proc.stdout
            elif proc.stdout != originals[branch]:
                raise StageError('business output changed in uninstrumented control')
        else:
            if proc.stdout != originals[branch]:
                raise StageError('business output changed: ' + label)
            mapping = read_json(app.parent/'priority_thread_mapping.json')
            owners = validate_events(mapping, events, branch, require_fifo=phase=='audit' and group!='trace_no_priority')
            row['owner_tids'] = owners
            event_map = {event['node']: event for event in events}
            violations = []
            for edge in dag['edges']:
                u,v = edge['src'],edge['dst']
                if u in event_map and v in event_map and event_map[u]['end_ns'] > event_map[v]['begin_ns']:
                    violations.append({'src':u,'dst':v,'kind':edge['kind'],
                                       'overlap_ns':event_map[u]['end_ns']-event_map[v]['begin_ns']})
            # Diagnostic: sync inside merged nodes can release another thread
            # before the full source segment ends. Do not conceal this mismatch.
            row['strict_end_to_start_violations'] = violations
            if metrics['priority_enabled'] and metrics['set_failures']:
                raise StageError('priority transition failed: ' + label)
        return row
    try:
        save()
        for branch in ('then','else'): execute('uninstrumented',branch,'audit',-1)
        for group in groups:
            if group == 'uninstrumented':continue
            for branch in ('then','else'):execute(group,branch,'audit',-1)
        summary['hardware_fifo_verified'] = not control_only
        rng = random.Random(random_seed)
        for phase,count in (('warmup',warmups),('measure',repeats)):
            for block in range(count):
                tasks = [(group,branch) for group in groups for branch in ('then','else')]
                rng.shuffle(tasks)
                for group,branch in tasks: execute(group,branch,phase,block)
        measured = [row for row in records if row['phase']=='measure']
        metrics_keys = ('response_ns','node_makespan_ns','set_cpu_ns','set_wall_ns','probe_cpu_ns')
        stats = {}
        for group in groups:
            stats[group] = {}
            for branch in ('then','else'):
                rows = [row for row in measured if row['group']==group and row['branch']==branch]
                stats[group][branch] = {key:summarize([r['metrics'][key] for r in rows]) for key in metrics_keys}
                if group == 'uninstrumented':
                    stats[group][branch]['node_makespan_ns'] = None
                for key in ('process_wall_ns','child_cpu_ns','voluntary_switches','involuntary_switches'):
                    stats[group][branch][key] = summarize([r[key] for r in rows])
        summary['statistics'] = stats
        if not control_only:
            pairs = {}
            for candidate in ('condition_pareto','condition_union'):
                pairs[candidate] = {}
                for branch in ('then','else'):
                    a = {r['block']:r for r in measured if r['group']==baseline and r['branch']==branch}
                    b = {r['block']:r for r in measured if r['group']==candidate and r['branch']==branch}
                    pairs[candidate][branch] = {key:paired_interval([b[i]['metrics'][key]-a[i]['metrics'][key] for i in range(repeats)])
                                                for key in ('response_ns','node_makespan_ns')}
            summary['paired_vs_baseline'] = pairs
            # Describe evidence instead of requiring a speedup to pass execution validation.
            summary['candidate_response_interval_below_zero_on_both_paths'] = all(
                pairs['condition_pareto'][branch]['response_ns']['bootstrap_95_interval_ns'][1] < 0
                for branch in ('then','else'))
        summary['strict_end_to_start_violations'] = [
            {'label':r['label'],'group':r['group'],'branch':r['branch'],'violations':r['strict_end_to_start_violations']}
            for r in records if r.get('strict_end_to_start_violations')]
        summary['strict_ideal_DAG_equivalence_established'] = False
        # Re-emit enriched records, including owner mapping and overlap diagnostics.
        write_json(output_dir/'runs.json', records)
        summary.update(status='completed', run_count=len(records), loadavg_end=list(os.getloadavg()))
        save()
        return summary
    except Exception as exc:
        summary.update(status='failed',error=str(exc),run_count=len(records))
        write_json(output_dir/'runs.json',records)
        save()
        raise
