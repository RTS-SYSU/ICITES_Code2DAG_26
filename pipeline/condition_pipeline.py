"""Explicit opt-in pipeline for the frozen single-if/else research case."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

from .errors import StageError
from .io_utils import read_json, write_json
from .timing import run_timing
from .algo.conditional import ORDINARY, profile_model


def run_condition_timing(*, base_dir, base_name, cores, seed_algo='FIFO', repeats=5,
                         condition_env='CODE2DAG_CONDITION', max_passes=12):
    if cores < 1 or seed_algo not in ORDINARY or not 1 <= max_passes <= 100:
        raise StageError('positive cores, ordinary seed algorithm and 1..100 search passes required')
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', condition_env):
        raise StageError('invalid branch-control environment variable')
    level, rule = 'level2', 'conditional_branches'
    pipeline = base_dir / 'intermediate_results' / base_name / 'pipeline'
    blocks = pipeline / 'blocks' / level / rule
    segments = read_json(blocks / 'segments.json')
    dag = read_json(blocks / 'dag_seg.json')
    regions = segments.get('condition_regions', [])
    if len(regions) != 1 or segments.get('capabilities', {}).get('scheduler_compatible') is False:
        raise StageError('one scheduler-compatible if/else is required')
    cpus = sorted(os.sched_getaffinity(0))[:cores]
    if len(cpus) != cores:
        raise StageError('insufficient allowed CPUs for the requested core count')
    profile_root = pipeline / 'condition_profiles' / level / rule
    profiles = {}
    for branch in ('then', 'else'):
        profiles[branch] = run_timing(base_dir=base_dir, base_name=base_name, level=level,
                                    rule_name=rule, repeats=repeats,
                                    environment={condition_env: '1' if branch == 'then' else None}, cpu_affinity=set(cpus))
        timing_root = pipeline / 'timing' / level / rule
        report = read_json(timing_root / 'logs/instrument_report.json')
        if report['warnings'] or report['skipped']:
            raise StageError('profiling skipped or warned about source probes')
        target = profile_root / branch
        if target.exists():
            shutil.rmtree(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(timing_root, target)
    pooled = {}
    for row in segments['segments']:
        n = row['seg_id']
        samples = [profiles[b]['weights'][n] for b in ('then', 'else')]
        count = sum(v['count'] for v in samples)
        total = sum(v['total_ns'] for v in samples)
        pooled[n] = {'count': count, 'total_ns': total, 'avg_ns': total // count if count else 0}
    payload = {'base_name': base_name, 'weights': pooled, 'condition_profiles': profiles,
               'condition_config': {'cores': cores, 'seed_algo': seed_algo, 'max_passes': max_passes,
                                    'condition_env': condition_env, 'cpu_affinity': cpus},
               'segments_sha256': hashlib.sha256((blocks / 'segments.json').read_bytes()).hexdigest()}
    # Verify that selecting a path really ran only that branch before publishing.
    profile_model(dag, segments, payload)
    write_json(pipeline / 'timing' / level / rule / 'timing.json', payload)
    write_json(profile_root / 'combined.json', payload)
    return payload


def prepare_condition_run(*, source, output_dir, runtime_base, cores=2, seed_algo='FIFO',
                          repeats=5, condition_env='CODE2DAG_CONDITION'):
    from .runner import run_collector, run_blocks, run_schedule_stage, run_instrument_stage
    if output_dir.resolve().is_relative_to(source.parent.resolve()):
        raise StageError('output directory must be outside the source project to avoid recursive project copying')
    if not source.is_file() or output_dir.exists():
        raise StageError('source must exist and output directory must be new')
    output_dir.mkdir(parents=True)
    base = output_dir / 'native'
    shutil.copytree(runtime_base / 'level1', base / 'level1',
                    ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    commands = []
    def command(label, argv):
        p = subprocess.run([str(v) for v in argv], cwd=runtime_base.parent,
                           capture_output=True, text=True, timeout=90)
        (output_dir / (label + '.stdout')).write_text(p.stdout)
        (output_dir / (label + '.stderr')).write_text(p.stderr)
        commands.append({'label': label, 'argv': [str(v) for v in argv], 'returncode': p.returncode})
        write_json(output_dir / 'commands.json', commands)
        if p.returncode:
            raise StageError(label + ' failed; see output logs')
    name = source.stem
    expand_root = output_dir / 'expand'
    expand_root.mkdir()
    command('gcc_version', ['gcc-13', '--version'])
    command('expand', ['gcc-13', '-O0', '-g', '-fdump-rtl-expand', '-c', source,
                       '-o', expand_root / (name + '.o')])
    expand = next(expand_root.glob('*.expand'))
    for mode in ('threads', 'full'):
        argv = [sys.executable, '-m', 'Code2DAG.generation.legacy', expand, '--source-file', source,
                '--output-base', base, '--force']
        if mode == 'threads':
            argv.append('--threads-only')
        command('generation_' + mode, argv)
    result = base / 'intermediate_results' / name
    shutil.copy2(result / 'config_files' / (name + '.dot'), result / 'dag_generation/dag.dot')
    run_collector(base_dir=base, base_name=name, source_file=source)
    run_blocks(base_dir=base, base_name=name, source_file=source, level='level2', rule_name='conditional_branches')
    timing = run_condition_timing(base_dir=base, base_name=name, cores=cores, seed_algo=seed_algo,
                                  repeats=repeats, condition_env=condition_env)
    groups = [seed_algo, 'condition_pareto', 'condition_union']
    programs = {}
    for algo in groups:
        args = dict(base_dir=base, base_name=name, level='level2', rule_name='conditional_branches', algo_name=algo)
        run_schedule_stage(**args)
        instrument = run_instrument_stage(**args, instrument_mode='generic', segment_trace=True)
        project = output_dir / 'programs' / algo
        project.mkdir(parents=True)
        # Freeze each result before the existing instrument stage cleans its previous scratch directory.
        shutil.copy2(instrument['source_instrumented'], project / 'program.c')
        for filename in ('condition_runtime.h', 'condition_runtime.c'):
            shutil.copy2(runtime_base / 'level1' / filename, project / filename)
        mapping = Path(instrument['source_instrumented']).parent / 'priority_thread_mapping.json'
        shutil.copy2(mapping, project / mapping.name)
        command('compile_' + algo, ['gcc-13', '-O2', '-g', '-std=c11', '-pthread', project / 'program.c',
                                    project / 'condition_runtime.c', '-Wl,--wrap=main', '-o', project / 'app'])
        programs[algo] = str(project / 'app')
    plain = output_dir / 'programs/uninstrumented'
    plain.mkdir(parents=True)
    command('compile_uninstrumented', ['gcc-13', '-O2', '-g', '-std=c11', '-pthread', source,
                                       runtime_base / 'level1/condition_runtime.c', '-Wl,--wrap=main', '-o', plain / 'app'])
    programs['uninstrumented'] = str(plain / 'app')
    payload = {'status': 'prepared', 'source': str(source),
               'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(), 'base_name': name,
               'base_dir': str(base), 'cores': cores, 'cpu_affinity': timing['condition_config']['cpu_affinity'], 'seed_algo': seed_algo, 'programs': programs,
               'condition_env': condition_env, 'groups': groups,
               'profiles': str(result / 'pipeline/condition_profiles/level2/conditional_branches/combined.json'),
               'binary_sha256': {k: hashlib.sha256(Path(v).read_bytes()).hexdigest() for k,v in programs.items()},
               'runtime_contract': 'segment-entry priority changes on pthread_self; no independent node threads; priority persists through gaps',
               'hardware_test_completed': False}
    write_json(output_dir / 'prepared.json', payload)
    return payload
