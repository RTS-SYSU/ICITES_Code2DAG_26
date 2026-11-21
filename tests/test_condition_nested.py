"""Acceptance for two braced levels, independently tested in either outer arm."""
from __future__ import annotations
import collections
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

from Code2DAG.pipeline.condition_model import build_condition_views
from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.pipeline.algo.common import topo_sort

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'Code2DAG/source_files/condition_nested_then/condition_nested_then.c'
ALGOS = ('FIFO', 'LPF', 'heft', 't_level', 'zhao2020')
PATHS = {'TT': (True, True), 'TF': (True, False), 'F0': (False, False), 'F1': (False, True)}
LABELS = ('outer_then_before', 'inner_then', 'inner_else', 'outer_then_after', 'outer_else')


def reachable(edges, src, dst):
    seen = set()
    todo = [src]
    while todo:
        n = todo.pop()
        if n == dst:
            return True
        if n in seen:
            continue
        seen.add(n)
        todo.extend(e['dst'] for e in edges if e['src'] == n)
    return False


def case_labels(child_branch):
    assert child_branch in ('then', 'else')
    other = 'else' if child_branch == 'then' else 'then'
    return (f'outer_{child_branch}_before', 'inner_then', 'inner_else', f'outer_{child_branch}_after', f'outer_{other}')


def call_lines(source, child_branch='then'):
    return {label: next(i for i, line in enumerate(source.splitlines(), 1)
                        if 'puts("' + label + '")' in line) for label in case_labels(child_branch)}


def validate_structure(source, st, sd, ss, dd, child_branch='then'):
    before, _, _, after, other = case_labels(child_branch)
    regions = ss['condition_regions']
    assert len(regions) == 2
    outer = next(r for r in regions if r['parent_id'] is None)
    inner = next(r for r in regions if r['parent_id'] is not None)
    assert inner['parent_id'] == outer['condition_id'] and inner['parent_branch_id'] == child_branch
    assert outer['condition_id'] != inner['condition_id']
    assert len(sd['exclusive_groups']) == len(dd['exclusive_groups']) == 2
    assert ss['capabilities']['scheduler_compatible']
    topo_sort(sd['nodes'], [(e['src'], e['dst']) for e in sd['edges']])
    topo_sort(dd['nodes'], [(e['src'], e['dst']) for e in dd['edges']])
    by_line = {}
    for label, line in call_lines(source, child_branch).items():
        candidates = [s for s in ss['segments'] if s['function'] == 'worker' and s['start_line'] <= line <= s['end_line']]
        assert len(candidates) == 1, (label, candidates)
        by_line[label] = candidates[0]
    assert len({s['seg_id'] for s in by_line.values()}) == 5
    for label, row in by_line.items():
        memberships = {(m['exclusive_group'], m['branch_id']) for m in row.get('condition_memberships', [])}
        expected = {(outer['condition_id'], ('else' if child_branch == 'then' else 'then') if label == other else child_branch)}
        if label in ('inner_then', 'inner_else'):
            expected.add((inner['condition_id'], 'then' if label == 'inner_then' else 'else'))
        assert memberships == expected, (label, memberships)
    control = {}
    for region in (outer, inner):
        for kind in ('condition_entry', 'condition_exit'):
            rows = [s for s in st['segments'] if s['kind'] == kind and s['condition_id'] == region['condition_id']]
            assert len(rows) == 1
            control[(region['condition_id'], kind)] = rows[0]['seg_id']
    oe, ox = (control[(outer['condition_id'], k)] for k in ('condition_entry', 'condition_exit'))
    ie, ix = (control[(inner['condition_id'], k)] for k in ('condition_entry', 'condition_exit'))
    nodes = {k: v['seg_id'] for k, v in by_line.items()}
    for a, b in [(oe, nodes[before]), (nodes[before], ie),
                 (ie, nodes['inner_then']), (ie, nodes['inner_else']),
                 (nodes['inner_then'], ix), (nodes['inner_else'], ix),
                 (ix, nodes[after]), (nodes[after], ox),
                 (oe, nodes[other]), (nodes[other], ox)]:
        assert reachable(sd['edges'], a, b), (a, b)
    for graph in (sd, dd):
        for a, b in [('inner_then', 'inner_else'), (other, 'inner_then'),
                     (other, 'inner_else'), (other, before),
                     (other, after)]:
            assert not reachable(graph['edges'], nodes[a], nodes[b])
            assert not reachable(graph['edges'], nodes[b], nodes[a])
    # Inner entry/exit must stay in the chosen nesting arm, not its sibling.
    assert not reachable(sd['edges'], nodes[other], ie)
    assert not reachable(sd['edges'], ix, nodes[other])
    return {'outer': outer, 'inner': inner, 'labels': nodes,
            'control_nodes': {'outer_entry': oe, 'outer_exit': ox, 'inner_entry': ie, 'inner_exit': ix}}


def projected_structure(st, sd, structure, flags, child_branch='then'):
    before, _, _, after, other = case_labels(child_branch)
    nested_selected = flags[0] == (child_branch == 'then')
    outer, inner = structure['outer'], structure['inner']
    choices = {outer['condition_id']: 'then' if flags[0] else 'else',
               inner['condition_id']: 'then' if flags[1] else 'else'}
    active = set()
    regions = [outer, inner]
    for row in st['segments']:
        if row.get('synthetic'):
            if row['condition_id'] == inner['condition_id'] and not nested_selected:
                continue
            active.add(row['seg_id'])
            continue
        contained = [(r['condition_id'], b['branch_id']) for r in regions for b in r['branches']
                     if b['start_line'] is not None and b['start_line'] <= row['start_line'] <= row['end_line'] <= b['end_line']]
        if all(choices[group] == branch for group, branch in contained):
            active.add(row['seg_id'])
    edges = [e for e in sd['edges'] if e['src'] in active and e['dst'] in active]
    topo_sort(sorted(active), [(e['src'], e['dst']) for e in edges])
    control = structure['control_nodes']
    outer_inputs = {e['src'] for e in edges if e['dst'] == control['outer_exit']}
    assert outer_inputs == {structure['labels'][after if nested_selected else other]}
    if nested_selected:
        inner_inputs = {e['src'] for e in edges if e['dst'] == control['inner_exit']}
        assert inner_inputs == {structure['labels']['inner_then' if flags[1] else 'inner_else']}
        assert reachable(edges, control['inner_exit'], control['outer_exit'])
    else:
        assert control['inner_entry'] not in active and control['inner_exit'] not in active
    return active, edges


def verify_insertions(rendered, source, ss, kind, priorities=None):
    rows = {r['seg_id']: r for r in ss['segments']}
    original_line = 1
    restored = []
    placements = []
    ordered = iter(sorted(ss['segments'], key=lambda r:r['start_line']))
    for line in rendered.splitlines(keepends=True):
        if line.strip() == '#include "' + ('segtrace.h' if kind == 'timing' else 'prio_runtime.h') + '"':
            continue
        match = re.fullmatch(r'\s*SEG_(BEGIN|END)\("([^"]+)"\);\s*', line) if kind == 'timing' else None
        prio = re.fullmatch(r'\s*l1_set_thread_prio_fifo\((\d+)\);\s*', line) if kind == 'priority' else None
        if match:
            row = rows[match[2]]
            expected = row['start_line'] if match[1] == 'BEGIN' else row['end_line'] + 1
            assert original_line == expected
            placements.append({'node': row['seg_id'], 'event': match[1], 'original_line_cursor': original_line})
            continue
        if prio:
            row = next(ordered)
            assert original_line == row['start_line'] and int(prio[1]) == priorities[row['seg_id']]
            placements.append({'node': row['seg_id'], 'event': 'priority', 'original_line_cursor': original_line})
            continue
        restored.append(line)
        original_line += 1
    assert ''.join(restored) == source
    # Matching original line cursors plus memberships places probes inside BOTH nested braces.
    return placements


def active_nodes(ss, outer, inner, flags):
    choices = {outer: 'then' if flags[0] else 'else', inner: 'then' if flags[1] else 'else'}
    return {s['seg_id'] for s in ss['segments']
            if all(choices[m['exclusive_group']] == m['branch_id'] for m in s.get('condition_memberships', []))}


def expected_output(outer, inner, child_branch='then'):
    before, _, _, after, other = case_labels(child_branch)
    nested_selected = outer == (child_branch == 'then')
    body = [before, 'inner_then' if inner else 'inner_else', after] if nested_selected else [other]
    value = (111 if inner else 121) if nested_selected else 1000
    return '\n'.join(['main:start', *body, f'value={value} outer_evaluations=1 inner_evaluations={int(nested_selected)}', 'main:end', ''])


AUDIT_H = 'void c2d_audit_node(const char *, int);\n'
AUDIT_C = r'''#define _GNU_SOURCE
#include <pthread.h>
#include <semaphore.h>
#include <sched.h>
#include <stdio.h>
static _Thread_local const char *node = "";
static sem_t *objects[2];
static int object_count;
static int sem_id(sem_t *s){ for(int i=0;i<object_count;i++) if(objects[i]==s)return i+1; return 0; }
void c2d_audit_node(const char *s,int p){ (void)p; node=s; }
int __real_pthread_setschedparam(pthread_t,int,const struct sched_param*);
int __wrap_pthread_setschedparam(pthread_t t,int policy,const struct sched_param *sp){
 int rc=__real_pthread_setschedparam(t,policy,sp);struct sched_param actual={0};int got=-1;
 int q=pthread_getschedparam(t,&got,&actual);
 fprintf(stderr,"PRIO {\"node\":\"%s\",\"requested\":%d,\"rc\":%d,\"query\":%d,\"policy\":%d,\"actual\":%d}\n",node,sp->sched_priority,rc,q,got,actual.sched_priority);return rc;
}
int __real_sem_init(sem_t*,int,unsigned);
int __wrap_sem_init(sem_t*s,int shared,unsigned value){int rc=__real_sem_init(s,shared,value);if(!rc&&object_count<2)objects[object_count++]=s;fprintf(stderr,"SYNC sem_init %d %d\n",sem_id(s),rc);return rc;}
#define SEM_WRAP(name) int __real_##name(sem_t*); int __wrap_##name(sem_t*s){int rc=__real_##name(s);fprintf(stderr,"SYNC " #name " %d %d\n",sem_id(s),rc);return rc;}
SEM_WRAP(sem_post)
SEM_WRAP(sem_wait)
int __real_pthread_create(pthread_t*,const pthread_attr_t*,void *(*)(void*),void*);
int __wrap_pthread_create(pthread_t*t,const pthread_attr_t*a,void *(*f)(void*),void*v){int rc=__real_pthread_create(t,a,f,v);fprintf(stderr,"SYNC pthread_create 0 %d\n",rc);return rc;}
int __real_pthread_join(pthread_t,void**);
int __wrap_pthread_join(pthread_t t,void**v){int rc=__real_pthread_join(t,v);fprintf(stderr,"SYNC pthread_join 0 %d\n",rc);return rc;}
'''
WRAPS = ('pthread_setschedparam', 'sem_init', 'sem_post', 'sem_wait', 'pthread_create', 'pthread_join')


def native_acceptance(output, *, source_file=SOURCE, child_branch='then', case=None):
    """Generate fresh native metadata and verify only the declared nested case."""
    from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
    source_file = Path(source_file).resolve()
    paths_config = case['paths'] if case else (PATHS if child_branch == 'then' else {'FT': (False, True), 'FF': (False, False), 'T0': (True, False), 'T1': (True, True)})
    lines_for_case = case['call_lines'] if case else lambda text: call_lines(text, child_branch)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    native = output / 'native'
    shutil.copytree(ROOT / 'Code2DAG/level1', native / 'level1', ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    (output / 'source.c').write_bytes(source_file.read_bytes())
    source = source_file.read_text()
    name = source_file.stem
    commands = []
    logs = output / 'logs'
    logs.mkdir()
    def command(label, argv, flags=None, cwd=ROOT):
        env = dict(os.environ)
        overrides = {}
        if flags is not None:
            overrides = case['environment'](flags) if case else {key: '1' if yes else None for key,yes in zip(('C2D_OUTER','C2D_INNER'),flags)}
            for key,value in overrides.items():
                env.pop(key,None)
                if value is not None:
                    env[key]=value
        p = subprocess.run(list(map(str, argv)), cwd=cwd, env=env, capture_output=True, text=True, timeout=90)
        (logs / (label + '.stdout')).write_text(p.stdout)
        (logs / (label + '.stderr')).write_text(p.stderr)
        commands.append({'label': label, 'argv': list(map(str, argv)), 'cwd': str(cwd), 'flags': flags, 'environment_overrides': overrides, 'returncode': p.returncode})
        (output / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        assert p.returncode == 0, (label, p.returncode, p.stderr)
        return p
    def cli(label, stage, args=(), flags=None):
        return command(label, [sys.executable, '-m', 'Code2DAG.pipeline.cli', '--base-dir', native,
                       stage, '--base-name', name, *args], flags)
    command('gcc_version', ['gcc-13', '--version'])
    expand = output / 'expand'
    expand.mkdir()
    command('expand', ['gcc-13', '-O0', '-g', '-fdump-rtl-expand', '-c', source_file, '-o', expand / (name + '.o')])
    dump = next(expand.glob('*.expand'))
    for mode in ('threads', 'full'):
        command('generation_' + mode, [sys.executable, '-m', 'Code2DAG.generation.legacy', dump,
                '--source-file', source_file, '--output-base', native, '--force', *(['--threads-only'] if mode == 'threads' else [])])
    result = native / 'intermediate_results' / name
    shutil.copy2(result / 'config_files' / (name + '.dot'), result / 'dag_generation/dag.dot')
    cli('collect', 'collect', ['--source', source_file])
    cli('blocks', 'blocks', ['--source', source_file, '--level', 'level2', '--rule', 'conditional_branches'])
    pipeline = result / 'pipeline'
    blocks = pipeline / 'blocks/level2/conditional_branches'
    st, sd, ss, dd = [json.loads((blocks / n).read_text()) for n in ('segments_struct.json', 'dag_struct.json', 'segments_sched.json', 'dag_sched.json')]
    structure = case['validate_structure'](source, st, sd, ss, dd) if case else validate_structure(source, st, sd, ss, dd, child_branch)
    project = case['project'] if case else lambda st,sd,structure,flags: projected_structure(st,sd,structure,flags,child_branch)
    projections = {label: project(st,sd,structure,flags) for label,flags in paths_config.items()}
    invariant = None if case else (('F0','F1') if child_branch == 'then' else ('T0','T1'))
    if invariant:
        assert projections[invariant[0]] == projections[invariant[1]]
    (output / 'projected_paths.json').write_text(json.dumps({label: {'nodes': sorted(v[0]), 'edges': v[1]} for label,v in projections.items()}, indent=2))
    if shutil.which('dot'):
        command('render_structural_svg', ['dot', '-Tsvg', blocks / 'dag_struct.dot', '-o', output / 'structure.svg'])
    base_segments, base_dag = build_level2_segments_and_dag(base_dir=native, base_name=name, source_file=source_file)
    (output / 'base_segments.json').write_text(json.dumps(base_segments, indent=2))
    (output / 'base_dag.json').write_text(json.dumps(base_dag, indent=2))
    base_counts = collections.Counter(e['kind'] for e in base_dag['edges'])
    counts = collections.Counter(e['kind'] for e in dd['edges'])
    assert base_counts['create'] == counts['create'] == 1
    assert base_counts['join'] == counts['join'] == 1
    assert base_counts['sem_dep'] == counts['sem_dep'] == 2
    assert counts['intra'] > 0
    mapping = ss['mappings']['original_node_to_execution_segment']
    struct_mapping = ss['mappings']['original_node_to_structural_node']
    assert set(struct_mapping) == set(base_segments['node_locations'])
    regions = [(structure[key], key + '_entry') for key in ('outer', 'inner') if structure.get(key)]
    entry_lines = {region['entry_line'] for region, _ in regions}
    assert set(mapping) == {n for n,loc in base_segments['node_locations'].items() if loc['line'] not in entry_lines or loc['function'] != 'worker'}
    dependency_checks = []
    base_map = ss['mappings']['original_segment_to_execution_segments']
    for edge in base_dag['edges']:
        srcs, dsts = base_map[edge['src']], base_map[edge['dst']]
        if edge['kind'] == 'intra':
            assert reachable(dd['edges'], srcs[-1], dsts[0])
        else:
            assert any(e['src'] == srcs[-1] and e['dst'] == dsts[0] and e['kind'] == edge['kind'] for e in dd['edges'])
        dependency_checks.append({'base_edge': edge, 'mapped_src': srcs[-1], 'mapped_dst': dsts[0], 'preserved': True})
    (output / 'dependency_checks.json').write_text(json.dumps(dependency_checks, indent=2))
    schedule_rows = {s['seg_id']: s for s in ss['segments']}
    structural_rows = {s['seg_id']: s for s in st['segments']}
    for node, loc in base_segments['node_locations'].items():
        if node in mapping:
            row = schedule_rows[mapping[node]]
            assert row['function'] == loc['function'] and row['start_line'] <= loc['line'] <= row['end_line']
        if node in struct_mapping:
            row = structural_rows[struct_mapping[node]]
            assert row['function'] == loc['function'] and row['start_line'] <= loc['line'] <= row['end_line']
    for label, line in lines_for_case(source).items():
        originals = [n for n, loc in base_segments['node_locations'].items() if loc['function'] == 'worker' and loc['line'] == line]
        assert originals and all(mapping[n] == structure['labels'][label] for n in originals)
    # Compiler control-expression calls resolve to their respective synthetic entry.
    for region, control_name in regions:
        originals = [n for n, loc in base_segments['node_locations'].items() if loc['function'] == 'worker' and loc['line'] == region['entry_line']]
        assert (originals or case is not None) and all(struct_mapping[n] == structure['control_nodes'][control_name] for n in originals)
    audit = output / 'audit'
    audit.mkdir()
    (audit / 'audit.h').write_text(AUDIT_H)
    (audit / 'audit.c').write_text(AUDIT_C)
    wrap_flags = ['-Wl,--wrap=' + w for w in WRAPS]
    command('compile_original', ['gcc-13', '-O2', '-g', '-std=c11', '-pthread', source_file, '-o', audit / 'original'])
    command('compile_original_audit', ['gcc-13', '-O2', '-g', '-std=c11', '-pthread', source_file, audit / 'audit.c', *wrap_flags, '-o', audit / 'original_audit'])
    originals = {}
    sync = {}
    expected_active = {}
    paths = {}
    for label, flags in paths_config.items():
        p = command('original_' + label, [audit / 'original'], flags)
        assert p.stdout == (case['expected_output'](flags) if case else expected_output(*flags,child_branch))
        originals[label] = p.stdout
        p = command('original_audit_' + label, [audit / 'original_audit'], flags)
        assert p.stdout == originals[label]
        sync[label] = collections.Counter(l for l in p.stderr.splitlines() if l.startswith('SYNC '))
        assert sum(sync[label].values()) == 8 and all(l.endswith(' 0') for l in sync[label])
        expected_active[label] = (case['active_nodes'](ss, structure, flags) if case and 'active_nodes' in case else
                                  active_nodes(ss, structure['outer']['condition_id'], structure['inner']['condition_id'], flags))
        cli('timing_' + label, 'timing', ['--level', 'level2', '--rule', 'conditional_branches', '--repeats', '1'], flags)
        timing_root = pipeline / 'timing/level2/conditional_branches'
        snapshot = output / ('timing_' + label)
        shutil.copytree(timing_root, snapshot)
        assert (snapshot / 'logs/run.01.stdout.log').read_text() == originals[label]
        meta = json.loads((snapshot / 'timing_meta.json').read_text())
        assert meta['compile_returncode'] == meta['run_returncode'] == 0
        report = json.loads((snapshot / 'logs/instrument_report.json').read_text())
        assert not report['warnings'] and not report['skipped']
        positions = verify_insertions((snapshot / 'project' / source_file.name).read_text(), source, ss, 'timing')
        (snapshot / 'probe_positions.json').write_text(json.dumps(positions, indent=2))
        weights = json.loads((snapshot / 'timing.json').read_text())['weights']
        for name_label, sid in structure['labels'].items():
            selected = sid in expected_active[label]
            assert weights[sid]['count'] == int(selected), (label, name_label, weights[sid])
        assert all(w['count'] == 0 for sid,w in weights.items() if sid not in expected_active[label])
        # Audit the actual timed source while retaining the real segtrace runtime.
        project = snapshot / 'project'
        original_trace = snapshot / 'original_timing_trace'
        shutil.copytree(project / 'trace', original_trace)
        command('compile_timing_audit_' + label, ['gcc-13', '-O2', '-std=c11', '-pthread', '-I' + str(project),
                *sorted(project.glob('*.c')), audit / 'audit.c', *wrap_flags, '-Wl,--wrap=main', '-ldl', '-lm', '-o', audit / ('timing_' + label)])
        p = command('timing_audit_' + label, [audit / ('timing_' + label)], flags, cwd=project)
        assert p.stdout == originals[label]
        assert collections.Counter(l for l in p.stderr.splitlines() if l.startswith('SYNC ')) == sync[label]
        paths[label] = {'flags': flags, 'stdout': originals[label], 'selected_label_nodes': [n for n,sid in structure['labels'].items() if sid in expected_active[label]],
                        'timing_counts': {n: weights[sid]['count'] for n,sid in structure['labels'].items()}, 'sync_events': dict(sync[label])}
    if invariant:
        assert originals[invariant[0]] == originals[invariant[1]]
    # Supply ordinary algorithms actual per-node averages pooled from separate paths, no branch WCET merge.
    weights_by_path = [json.loads((output / ('timing_' + label) / 'timing.json').read_text())['weights'] for label in paths_config]
    pooled = {}
    for sid in dd['nodes']:
        total = sum(w[sid]['total_ns'] for w in weights_by_path)
        count = sum(w[sid]['count'] for w in weights_by_path)
        pooled[sid] = {'total_ns': total, 'count': count, 'avg_ns': total // count if count else 0}
    timing_file = pipeline / 'timing/level2/conditional_branches/timing.json'
    timing_payload = json.loads(timing_file.read_text())
    timing_payload['weights'] = pooled
    timing_file.write_text(json.dumps(timing_payload, indent=2))
    (output / 'schedule_timing_inputs.json').write_text(json.dumps(timing_payload, indent=2))
    algorithms = {}
    for algo in ALGOS:
        cli('schedule_' + algo, 'schedule', ['--level', 'level2', '--rule', 'conditional_branches', '--algo', algo])
        cli('instrument_' + algo, 'instrument', ['--level', 'level2', '--rule', 'conditional_branches', '--algo', algo, '--mode', 'generic'])
        instrument_result = json.loads((logs / ('instrument_' + algo + '.stdout')).read_text())
        rendered_path = Path(instrument_result['source_instrumented'])
        folder = output / 'programs' / algo
        folder.mkdir(parents=True)
        shutil.copy2(rendered_path, folder / 'production.c')
        shutil.copy2(native / 'level1/prio_runtime.h', folder / 'prio_runtime.h')
        priorities = json.loads((pipeline / ('schedule/level2/conditional_branches/' + algo + '/schedule.json')).read_text())['priorities']
        assert set(priorities) == set(dd['nodes'])
        rendered = (folder / 'production.c').read_text()
        positions = verify_insertions(rendered, source, ss, 'priority', priorities)
        (folder / 'probe_positions.json').write_text(json.dumps(positions, indent=2))
        # Removing only generated statements/includes must restore the original source byte-for-byte.
        restored = re.sub(r'^.*l1_set_thread_prio_fifo\(\d+\);\s*\n', '', rendered, flags=re.M)
        restored = re.sub(r'^#include "prio_runtime.h"\n', '', restored, flags=re.M)
        assert restored == source
        # Inject diagnostic node labels immediately before the unchanged real priority calls.
        ordered = sorted(ss['segments'], key=lambda s: s['start_line'])
        calls = list(re.finditer(r'l1_set_thread_prio_fifo\((\d+)\);', rendered))
        assert len(calls) == len(ordered)
        index = 0
        def label_call(match):
            nonlocal index
            row = ordered[index]
            index += 1
            assert int(match[1]) == priorities[row['seg_id']]
            return 'c2d_audit_node(' + json.dumps(row['seg_id']) + ', ' + match[1] + '); ' + match[0]
        diagnosed = re.sub(r'l1_set_thread_prio_fifo\((\d+)\);', label_call, rendered)
        diagnosed = diagnosed.replace('#include "prio_runtime.h"', '#include "prio_runtime.h"\n#include "audit.h"')
        (folder / 'diagnostic.c').write_text(diagnosed)
        command('compile_production_' + algo, ['gcc-13', '-O2', '-std=c11', '-pthread', folder / 'production.c', '-o', folder / 'production'])
        command('compile_diagnostic_' + algo, ['gcc-13', '-O2', '-std=c11', '-pthread', '-I' + str(audit), folder / 'diagnostic.c', audit / 'audit.c', *wrap_flags, '-o', folder / 'diagnostic'])
        traces = {}
        for label, flags in paths_config.items():
            p = command('production_' + algo + '_' + label, [folder / 'production'], flags)
            assert p.stdout == originals[label]
            p = command('diagnostic_' + algo + '_' + label, [folder / 'diagnostic'], flags)
            assert p.stdout == originals[label]
            assert collections.Counter(l for l in p.stderr.splitlines() if l.startswith('SYNC ')) == sync[label]
            events = [json.loads(l.removeprefix('PRIO ')) for l in p.stderr.splitlines() if l.startswith('PRIO ')]
            assert len(events) == len(expected_active[label]) and {e['node'] for e in events} == expected_active[label]
            for event in events:
                assert event['requested'] == priorities[event['node']]
            traces[label] = events
        algorithms[algo] = {'priorities': priorities, 'priority_events': traces}
    summary = {'passed': True, 'scope': case['scope'] if case else f'two levels, child in outer {child_branch}, multiline braced, existing ordinary algorithms only',
               'structural': structure, 'node_count': len(dd['nodes']), 'edge_counts': dict(counts), 'base_edge_counts': dict(base_counts),
               'mapping': ss['mappings'], 'paths': paths, 'algorithms': algorithms, 'command_count': len(commands),
               'actual_fifo_required_by_this_acceptance': False,
               'priority_api_results': dict(collections.Counter(str(e['rc']) for a in algorithms.values() for ev in a['priority_events'].values() for e in ev))}
    (output / 'acceptance.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary


class NestedConditionTests(unittest.TestCase):
    def test_recursive_structure_memberships_and_no_false_dependencies(self):
        source = SOURCE.read_text()
        lines = source.splitlines()
        worker_start = next(i for i,l in enumerate(lines,1) if '(void)unused;' in l)
        worker_end = next(i for i,l in enumerate(lines,1) if 'return NULL;' in l)
        main_start = next(i for i,l in enumerate(lines,1) if 'puts("main:start")' in l)
        payload = {'segments': [
            {'seg_id': 'W', 'function': 'worker', 'kind': 'compute', 'start_line': worker_start, 'end_line': worker_end},
            {'seg_id': 'M', 'function': 'main', 'kind': 'compute', 'start_line': main_start, 'end_line': len(lines)-1}],
            'node_locations': {label: {'function': 'worker', 'line': line} for label,line in call_lines(source).items()}}
        st, sd, ss, dd, _ = build_condition_views(base_name='nested', source_text=source, base_segments_json=payload, base_dag_json={'nodes': ['W','M'], 'edges': []})
        result = validate_structure(source, st, sd, ss, dd)
        for flags in PATHS.values():
            projected_structure(st, sd, result, flags)
        for label, sid in result['labels'].items():
            self.assertEqual(sid, ss['mappings']['original_node_to_execution_segment'][label])
        for algo in ALGOS:
            schedule = get_algo(algo).compute(segments_json=ss, dag_json=dd, timing_json={'weights': {n: {'avg_ns': 1} for n in dd['nodes']}})
            self.assertEqual(set(dd['nodes']), set(schedule['priorities']))

    @unittest.skipUnless(shutil.which('gcc-13'), 'Ubuntu GCC13 required')
    def test_native_three_paths_plus_false_inner_invariance(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = native_acceptance(Path(tmp) / 'acceptance')
            self.assertTrue(summary['passed'])
            self.assertEqual(summary['paths']['F0']['stdout'], summary['paths']['F1']['stdout'])


if __name__ == '__main__':
    unittest.main()
