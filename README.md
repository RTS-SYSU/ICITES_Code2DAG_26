# Code2DAG

<p align="center">
    <img src="./assets/logo.svg" alt="Code2DAG" width="30%">
</p>

<p align="center">
    <a href="https://github.com" rel="nofollow">
        <img src="https://img.shields.io/badge/Parallel_Programs-DAG_Construction-orange.svg">
    </a>
    <a href="https://github.com" rel="nofollow">
        <img src="https://img.shields.io/badge/Platform-Linux%20%7C%20POSIX-brightgreen.svg">
    </a>
    <a href="https://github.com" rel="nofollow">
        <img src="https://img.shields.io/badge/Python-%E2%89%A53.8-yellow.svg">
    </a>
</p>

<p align="center">
    <a href="README.md">English</a> | <a href="README_zh.md">中文</a>
</p>

**Code2DAG** constructs scheduling-oriented DAG models from parallel C programs. It
starts from a source file and its GCC RTL expansion, extracts inter-thread
dependencies, merges synchronization-aware segments, and emits graph views,
mapping tables, timing imports, scheduling priorities, and instrumented programs.

The repository contains two related capabilities:

- the original **traditional DAG** pipeline for straight-line concurrency and
  POSIX synchronization (`pthread_create` / `pthread_join`, semaphores, mutexes);
- the **extended DAG** pipeline for `if/else` condition paths, mutex identity and
  branch membership, nested condition/lock/loop shapes, and the C2c
  condition-plus-independent-lock rule.

The extended pipeline is a graph-construction and analysis toolchain. The full
schedule executor used by the current performance experiments is a separate
component and is **not** included in this repository. In particular, C2c and the
`composition_*` nested rules are analysis-only: standard `timing` and
`instrument` reject them, and their graphs are consumed through the exported
JSON/mapping files and an external cost/schedule executor.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Traditional DAG vs Extended DAG](#traditional-dag-vs-extended-dag)
- [Extended DAG Rules](#extended-dag-rules)
- [Supported Source Shapes and Limitations](#supported-source-shapes-and-limitations)
- [Quick Start](#quick-start)
- [Pipeline Overview](#pipeline-overview)
- [Requirements and Installation](#requirements-and-installation)
- [CLI Reference](#cli-reference)
- [Output Structure](#output-structure)
- [Old Node-Priority Runtime vs Full Schedule Executor](#old-node-priority-runtime-vs-full-schedule-executor)
- [Evaluation Summary](#evaluation-summary)
- [Reproduction and Test Results](#reproduction-and-test-results)
- [Terminology](#terminology)
- [Scope and Limitations](#scope-and-limitations)
- [Repository Layout](#repository-layout)
- [Data Availability](#data-availability)

---

## Overview

Modern parallel programs expose dependencies that are not limited to a single
call chain: thread lifecycle events, semaphores, mutex critical sections, and
mutually exclusive `if/else` branches all affect the feasible execution order.
A DAG that ignores any of them can expose an infeasible schedule.

Code2DAG performs a joint static analysis of the C source and GCC RTL. The
traditional pipeline extracts sequential call chains, `pthread_create` /
`pthread_join` relations, and `sem_post` / `sem_wait` / mutex constraints. The
extended rules additionally model:

- **condition paths**: paired `condition_entry` / `condition_exit` structural
  nodes, separate then/else execution nodes, `condition_branch` /
  `condition_merge` edges, and `exclusive_group` / `branch_id` metadata;
- **mutex identity and branch membership**: complete critical sections become
  `LOCKGROUP` (or branch-local `LOCKPATH`) nodes; each retains its `mutex_id`,
  member execution segments, lock/unlock ranges, and source mapping;
- **nested structures**: bounded condition trees inside locks, locks inside
  condition branches, and the validated condition/lock/loop composition family;
- **C2c**: condition paths plus independent lock regions without a static loop,
  where each critical section stays a separate lock group even when the same
  mutex is used by several workers.

The result is a pair of views: a **structural** view for inspecting control
semantics, and a **scheduling** view containing only executable nodes (plus
synthetic lock groups where applicable).

---

## Key Features

- Automated DAG construction from parallel C source and GCC RTL, without manual
  graph annotation.
- Three traditional dependency families: sequential call chains, thread
  lifecycle (`pthread_create` / `pthread_join`), and synchronization
  (`sem_post` / `sem_wait`, `pthread_mutex_lock` / `pthread_mutex_unlock`).
- Synchronization-aware node merging for the default `effective_line_merge`
  rule, plus optional condition, lock, and loop rules.
- Extended condition model with explicit mutually exclusive branches and
  structural entry/exit nodes.
- Mutex-aware composition with `mutex_id`, lock groups, branch membership, and
  complete source/segment mappings.
- C2c analysis for condition plus independent lock regions, with no static loop
  required.
- Dual structural/scheduling JSON views and backward-compatible
  `segments.json` / `dag_seg.json` aliases.
- Existing priority algorithms (`FIFO`, `LPF`, `HEFT`, `T_LEVEL`, `Zhao2020`)
  consume the scheduling view.
- Optional condition-priority algorithms (`condition_pareto`,
  `condition_union`) for two-profile conditional inputs.
- Native tests for the fixed 27-program corpus, G1–G4 structural mappings, and
  the C2c rule.

---

## Traditional DAG vs Extended DAG

| Aspect | Traditional DAG | Extended DAG |
|:---|:---|:---|
| Input shape | Straight-line thread bodies, create/join, semaphores, mutexes | Adds `if/else` trees, nested conditions, mutex identities, bounded static loops, and their validated compositions |
| Default rule | `effective_line_merge` | `conditional_branches`, `lock_grouped`, `loop_static*`, `composition_*` (opt-in) |
| Control representation | No separate condition nodes | Structural view has `CE:` / `CX:` nodes and branch edges; scheduling view has separate branch execution nodes |
| Mutex representation | `mutex_cs` segment | `mutex_cs` member plus `LOCKGROUP` / `LOCKPATH` with `mutex_id`, lock ranges, and branch memberships |
| Mutual exclusion | Implicit in merged segment | Explicit `exclusive_group` / `branch_id`; condition branches are not connected to each other |
| Output views | One unified view | `segments_struct.json` / `dag_struct.json` and `segments_sched.json` / `dag_sched.json`; legacy aliases remain |
| Standard timing/instrument | Supported | `conditional_branches` supported; lock/composition rules are analysis-only or use imported timing; loop rules use explicit template timing |
| Full schedule executor | Not included | Not included; external executor consumes exported graphs/plans |

The extended rules do **not** change the default pipeline. Existing DAG 1–6
cases and the `effective_line_merge` entry point keep their previous behaviour.

---

## Extended DAG Rules

All extended rules are registered as level-2 block rules and selected explicitly
with `--rule` or through the composition facade.

| Rule | Source shape | Main output | Standard timing / instrument |
|:---|:---|:---|:---|
| `effective_line_merge` | Traditional straight-line + sync | Merged execution segments | Yes |
| `conditional_branches` | One or nested `if/else` trees; no lock/loop mixing | `CE:` / `CX:` structural nodes; separate then/else execution nodes | Yes |
| `lock_grouped` | Independent complete mutex regions; no condition/loop | One `LOCKGROUP` per independent region; `mutex_id` retained | Analysis-only (imported member timing) |
| `loop_static`, `loop_static_pair`, `loop_static_nested` | Bounded static `for` loops | Loop/template/iteration nodes | Explicit template timing; no runtime C instrumentation |
| `composition_c1` | Disjoint condition + lock + loop modules | Composition analysis graph | Analysis-only |
| `composition_c2a` | Complete lock inside a condition branch | Branch-local `LOCKGROUP` / `LOCKPATH` | Analysis-only |
| `composition_c2b` | Condition tree inside a lock (including bounded helper/worker forms) | `LOCKPATH` candidates and lock groups | Analysis-only |
| `composition_c2c` | Condition + independent lock regions, **no static loop** | Separate condition branches and one `LOCKGROUP` per critical section | Analysis-only |
| `composition_c3a`, `composition_c3b` | Condition/loop nesting | Composition analysis graph | Analysis-only |
| `composition_c4a`, `composition_c4b` | Loop/lock nesting | Composition analysis graph | Analysis-only |
| `composition_c5a`…`composition_c5f` | Validated three-layer condition/loop/lock nesting | Composition analysis graph | Analysis-only |

`composition_c2c` is the rule used by the G4 structural example. It:

- requires both a Condition and a Lock, and rejects static loops;
- requires each critical section to map to exactly one complete `mutex_cs`
  base segment;
- keeps each independent critical section as its own `LOCKGROUP`, even when
  several groups share the same `mutex_id`;
- rejects unpaired/nested mutexes, lock/condition overlap, dynamic mutex
  initialization, aliases, and unsupported thread-call forms;
- sets `capabilities.scheduler_compatible=true` and
  `capabilities.analysis_only=true`; standard `timing` and `instrument` are
  intentionally disabled.

---

## Supported Source Shapes and Limitations

Supported (within the tested subset):

- C programs using POSIX threads, `pthread_create` / `pthread_join`, semaphores,
  and statically initialized `pthread_mutex_t` objects;
- paired `pthread_mutex_lock` / `pthread_mutex_unlock` critical sections;
- bounded `if/else` trees with braced, multi-line branch bodies;
- bounded static `for` loops in the loop/composition rules;
- the fixed 27-program corpus in `tests/validation27/programs/` and the G1–G4
  sources in `examples/g1_g4/sources/`.

Not supported or explicitly rejected:

- dynamic mutex initialization, mutex aliases, function-pointer calls, and
  `trylock` / wrapper-based locking;
- nested or unpaired mutex operations;
- condition branches with `return`, `break`, `continue`, or `goto` when the
  branch must be independently scheduled; same-line branch code is also
  rejected for independent instrumentation;
- unbounded loops or loop bounds derived from runtime variables;
- active non-`#include` preprocessor directives, macros in supported control
  positions, and unsupported static-loop forms;
- C2c inputs containing any static loop, lock/condition overlap, or more than
  the bounded condition-tree shape;
- C2c and the `composition_*` rules through the standard `timing` /
  `instrument` stages: they are analysis-only and must be consumed by an
  external cost/schedule executor.

The extended rules preserve original source files. They build analysis copies
and never rewrite the input C program.

---

## Quick Start

Run commands from the repository parent so that `Code2DAG` is importable as a
package:

```bash
cd <repo-parent>
```

### 1. Traditional DAG (full pipeline)

```bash
python3 -m Code2DAG.pipeline.cli run_all \
  --source Code2DAG/source_files/dag1/dag1.c \
  --level level2 \
  --rule effective_line_merge \
  --repeats 1
```

Observed result on the reference machine: all eight stages complete, the
scheduling view has 21 nodes, and each of the five algorithms
(`FIFO`, `LPF`, `heft`, `t_level`, `zhao2020`) instruments 21 priorities.

### 2. Extended condition DAG (full pipeline)

```bash
python3 -m Code2DAG.pipeline.cli run_all \
  --source Code2DAG/source_files/condition_nested_then/condition_nested_then.c \
  --level level2 \
  --rule conditional_branches \
  --repeats 1
```

Observed result: all eight stages complete; the scheduling view has 12 nodes and
each of the five algorithms instruments 12 priorities. The structural view adds
the condition entry/exit nodes and branch edges.

### 3. C2c analysis (no standard timing/instrument)

```bash
python3 Code2DAG/examples/run_c2c.py
```

This builds `Code2DAG/source_files/composition_c2c/benchmark.c` in a temporary
directory and prints the C2c graph summary. It does not run `timing`,
`instrument`, or a runtime workload.

### 4. G1–G4 structural reproduction

```bash
python3 Code2DAG/examples/g1_g4/reproduce.py
```

This is the entry point for the four-graph structural cases. The sources are in
`Code2DAG/examples/g1_g4/sources/`.

---

## Pipeline Overview

The traditional pipeline is:

1. **Source/RTL analysis** — parse the C source and the GCC RTL `.expand` dump.
2. **Dependency extraction** — sequential, thread-lifecycle, and synchronization
   relations.
3. **DAG construction** — original DAG with call/sync/control nodes.
4. **Node merging** — level-2 synchronization-aware segmentation.
5. **Timing** — instrument segment boundaries and measure costs.
6. **Scheduling** — assign node priorities with the selected algorithm.
7. **Instrumentation** — write the priority-instrumented C program.

The extended rules replace step 4 with a composition rule. They first build the
base segments/DAG, then add condition, lock, loop, or composition nodes and
mappings. The scheduling view is consumed by the same priority algorithms; the
structural view is for inspection and for external analysis.

---

## Requirements and Installation

| Dependency | Version | Purpose |
|:---|:---|:---|
| Python | ≥ 3.8 | Pipeline runtime |
| GCC | modern (GCC 13 used by the native tests) | RTL dump generation and instrumented compilation |
| Graphviz (`dot`) | modern | DAG rendering |

```bash
python3 --version
gcc --version
dot -V
```

Python dependencies (`requirements.txt`):

```text
networkx>=2.6
pillow>=9.0.0
pydot>=1.4.2
matplotlib>=3.5
pandas>=1.5
seaborn>=0.12
flask>=2.2
```

```bash
git clone https://github.com/RTS-SYSU/Code2DAG.git
cd Code2DAG
pip install -r requirements.txt
```

The core pipeline and the examples do not require a GUI or a real-time
scheduler. The native tests invoke `gcc-13`; set `CC=gcc` to use another GCC
binary with the same RTL-expand behaviour.

---

## CLI Reference

```text
python3 -m Code2DAG.pipeline.cli <subcommand> [options]
```

| Subcommand | Description |
|:---|:---|
| `run_all` | Full pipeline: collect → blocks → timing → schedule × algorithms → instrument × algorithms |
| `collect` | Parse source/RTL and construct the original DAG |
| `blocks` | Apply a level-2 block rule |
| `timing` | Instrument and profile segment costs |
| `schedule` | Compute priorities for one algorithm |
| `instrument` | Generate the priority-instrumented C source |
| `list` | List registered rules and algorithms |

Key options:

| Option | Description |
|:---|:---|
| `--source <path>` | Input C source |
| `--level <level1\|level2\|level3>` | Segmentation level |
| `--rule <rule>` | Block rule; default is `effective_line_merge` |
| `--algo <name>` | Priority algorithm for `schedule` / `instrument` |
| `--repeats <n>` | Timing repetitions |
| `--segment-trace` | Record node/TID/priority boundaries (generic instrumentation only) |

List the extended rules with:

```bash
python3 -m Code2DAG.pipeline.cli list --level level2
```

Observed result: 23 level-2 rules are listed, including
`conditional_branches`, `lock_grouped`, `loop_static_nested`,
`composition_c1`, `composition_c2a`, `composition_c2b`, `composition_c2c`,
and the `composition_c3`–`composition_c5f` family.

---

## Output Structure

For each case, the pipeline publishes final outputs under `results/` and keeps
intermediate artifacts under `intermediate_results/`.

Traditional / condition rules:

```text
results/<case_name>/
├── graphs/
│   ├── original_dag.png
│   ├── block_dag.png
│   └── original_dag.round.dot
└── algorithms/
    ├── FIFO/source/FIFO.c
    ├── LPF/source/LPF.c
    ├── heft/source/heft.c
    └── ...
```

Per-rule block outputs:

```text
intermediate_results/<case>/pipeline/blocks/level2/<rule>/
├── segments_struct.json      # structural view (CE:/CX:, branch edges)
├── dag_struct.json
├── segments_sched.json       # scheduling view (executable nodes + lock groups)
├── dag_sched.json
├── segments.json             # scheduling-view alias
├── dag_seg.json              # scheduling-view alias
├── dag_seg.dot / .png
├── dag_struct.dot / .png     # when structural and scheduling views differ
└── rule_meta.json
```

Extended mapping fields include:

- `condition_regions` — condition id, entry/exit lines, parent id, branches;
- `condition_memberships` on segments — `exclusive_group` and `branch_id`;
- `lock_groups` — `group_id`, `mutex_id`, members, member segments,
  lock/unlock/critical ranges;
- `mappings.original_node_to_execution_segment`,
  `generation_node_to_execution_segments`,
  `source_position_to_execution_segments`,
  `grouped_node_to_member_segments`,
  and the structural counterparts;
- `composition_edge_provenance` for composition rules.

---

## Old Node-Priority Runtime vs Full Schedule Executor

The **old node-priority runtime** is the path implemented by this repository's
`schedule` / `instrument` stages:

- a priority algorithm assigns one priority per execution node;
- `instrument` inserts `sp.sched_priority = prior_n` and
  `pthread_setschedparam(...)` at node entries;
- the Linux scheduler still decides when and where each thread/node runs.

The **current performance experiments** use a different execution model:

- the exported scheduling graph and measured segment costs are consumed by an
  external full schedule executor (HEFT / CHEFT / FIFO / LPF / Zhao plan
  generation followed by a frozen per-CPU plan);
- the executor enforces CPU assignment, per-CPU order, and the plan trace;
- that executor, its G4-specific cost/execution adaptation, and the raw
  experiment records are **not** part of this repository.

Do not read the extended performance numbers as the result of the old
node-priority instrumentation. In particular, the gains are not attributable to
mutex modeling alone: condition-path fidelity, mutex identity, segment costs,
probability-weighted CHEFT, and the full executor all contribute. G4 contains
both positive and negative configuration-level results; no single factor should
be credited with the eight-configuration average.

---

## Evaluation Summary

### Traditional DAG (historical result)

The original evaluation used six DAG structures (DAG 1–6), two CPU-affinity
configurations (2-core `0–1`, 4-core `0–1–2–3`), 100 repetitions, a VMware
Ubuntu 20.04.6 guest, an Intel Core i5-13400F, and a 5.15.96-rt61 kernel. The
reported traditional result is **13.73% average makespan reduction vs CFS**
(best case **30.87%**, DAG 3, 4-core).

### Extended DAG (current same-program G3 result)

The current extended-DAG paper table reports:

| Comparison | Result |
|:---|---:|
| Extended scheme vs CFS | **+8.80%** |
| Extended scheme vs traditional DAG + HEFT | **+4.92%** |

Configuration and data provenance:

- G1–G4, each at 2-core and 4-core, 100 blocks per path; G1/G2/G4 use the
  frozen two-path weights, G3 uses its native 16-path probabilities;
- G3 uses the **same-program 6-task** version for both core counts (the older
  4-task G3_2c is historical and is not mixed into this table);
- reference machine: original i5-12600K, Linux
  `6.1.192-0601192-generic`, CFS/SCHED_OTHER;
- source batches: the unified work2 100-block formal batch for G1/G2/G4 and the
  G3 same-program 100-block update for G3; the aggregate is the current
  same-program G3 eight-config table;
- the full schedule executor (not this repository) produced the runtime
  measurements. Raw 100-block records are not published in this repository.

The two evaluation setups are not directly comparable: they use different
programs, kernels, machines, and execution models. The extended numbers are not
a mutex-only ablation and should not be presented as one.

---

## Reproduction and Test Results

The following commands were run on the reference Linux host from the repository
parent. The observed results are included so the README is not a set of
unverified commands.

| Command | Observed result |
|:---|:---|
| `python3 -m Code2DAG.pipeline.cli list --level level2` | 23 level-2 rules, 7 algorithms |
| `python3 -m Code2DAG.pipeline.cli run_all --source Code2DAG/source_files/dag1/dag1.c --level level2 --rule effective_line_merge --repeats 1` | 8/8 stages complete; 21 priorities for each of FIFO/LPF/heft/t_level/zhao2020 |
| `python3 -m Code2DAG.pipeline.cli run_all --source Code2DAG/source_files/condition_nested_then/condition_nested_then.c --level level2 --rule conditional_branches --repeats 1` | 8/8 stages complete; 12 priorities for each of the five algorithms |
| `python3 Code2DAG/examples/run_c2c.py` | C2c: 31 nodes, 42 edges, 5 condition regions, 2 `LOCKGROUP` nodes (`m1`), 7 create and 12 join edges; `analysis_only=true` |
| `python3 Code2DAG/examples/g1_g4/reproduce.py` | G1 19/25, G2 22/29, G3 26/36, G4 31/42 scheduling nodes/edges; G4 has 2 lock groups (`task_5`, `task_7`) |
| `python3 -m unittest -v Code2DAG.tests.test_composition_c2c` | 4 tests passed |
| `python3 -m unittest -v Code2DAG.tests.test_validation27` | 27-program structural regression passed |
| `python3 -m unittest -v Code2DAG.tests.test_g1_g4_mapping` | G1–G4 mapping regression passed |

The extended-DAG targeted suite (condition model, nested conditions, C1, C2a,
C2b, C2c, lock L1/L2/QA, 27-program corpus, and G1–G4) is:

```bash
python3 -m unittest -v \
  Code2DAG.tests.test_condition_model \
  Code2DAG.tests.test_condition_nested \
  Code2DAG.tests.test_condition_nested_else \
  Code2DAG.tests.test_composition_c1 \
  Code2DAG.tests.test_composition_c2a \
  Code2DAG.tests.test_composition_c2a_branch_segments \
  Code2DAG.tests.test_composition_c2b \
  Code2DAG.tests.test_composition_c2b_basic \
  Code2DAG.tests.test_composition_c2b_repeated_entries \
  Code2DAG.tests.test_composition_c2c \
  Code2DAG.tests.test_lock_l1 \
  Code2DAG.tests.test_lock_l2 \
  Code2DAG.tests.test_lock_qa \
  Code2DAG.tests.test_validation27 \
  Code2DAG.tests.test_g1_g4_mapping
```

Observed: **116 tests passed** (114 existing extended-DAG tests plus the
27-program and G1–G4 regressions). The full checked-in suite also passes:

```bash
python3 -m unittest discover -q Code2DAG/tests
```

Observed: **600 tests passed** in 137 s. The native tests require `gcc-13`; they
do not require real-time privileges and do not run the performance workload.

---

## Terminology

| Term | Definition |
|:---|:---|
| **Traditional DAG** | DAG built from call chains, create/join, semaphores, and mutex critical sections with the default merge rule |
| **Extended DAG** | DAG with condition paths, mutex identity/branch membership, nested composition, or C2c |
| **Structural view** | Graph with `CE:` / `CX:` (and loop control) nodes for inspecting control semantics |
| **Scheduling view** | Executable nodes plus synthetic lock groups; consumed by priority algorithms |
| **LOCKGROUP** | Scheduling node representing a complete critical section; retains `mutex_id` and member segments |
| **LOCKPATH** | Branch-local lock candidate used when a lock region contains a condition |
| **C2c** | Condition + independent lock regions, no static loop; analysis-only |

---

## Scope and Limitations

Code2DAG targets static POSIX-threaded C programs. The supported subset is
defined by the tests in `tests/` and the 27-program corpus; it is not a general
C front end. In particular:

- C2c and `composition_*` do not provide standard C timing/instrument support;
  use the exported JSON with an external cost/schedule executor;
- the public repository does not include the full schedule executor, the G4
  cost/execution adapter, or the raw 100-block measurement data;
- the extended performance table is from the named experiment batches, not from
  the public examples;
- per-node priority instrumentation is not the same execution model as the
  frozen full schedule executor used for the paper numbers.

The `ASAP_DAG/` directory retains the legacy ASAP benchmark sources and
artifacts used by the original repository; it is separate from the extended
rules.

---

## Repository Layout

```text
Code2DAG/
├── pipeline/                 # CLI, pipeline stages, rules, algorithms
│   ├── condition_model.py    # Condition paths, structural/scheduling views
│   ├── lock_model.py         # Lock groups and mutex identity
│   ├── lock_regions.py       # Function-scoped lock-region matcher
│   ├── composition_c2a_model.py
│   ├── composition_c2b_model.py
│   ├── composition_c2c_model.py
│   ├── composition_c3a_model.py … composition_c5f_model.py
│   ├── rules/                # Registered level-2 rules
│   └── algo/                 # FIFO, LPF, HEFT, T_LEVEL, Zhao2020, condition*
├── generation/               # Source/RTL analysis and DAG construction
├── core/                     # Parsing and range utilities
├── level1/                   # Level-1 segmentation and instrumentation
├── level2/                   # Level-2 segmentation and composition
├── source_files/             # DAG 1–6, condition, lock, loop, composition fixtures
├── examples/
│   ├── run_c2c.py            # C2c public example
│   └── g1_g4/                # G1–G4 sources and reproduction script
├── tests/
│   ├── validation27/         # Fixed 27-program corpus and manifest
│   ├── test_composition_c2c.py
│   ├── test_validation27.py
│   └── test_g1_g4_mapping.py
├── ASAP_DAG/                 # Legacy ASAP benchmark sources/artifacts
├── results/                  # Published example outputs (DAG 1–6)
├── intermediate_results/     # Pipeline intermediates (DAG 1–6)
├── requirements.txt
├── README.md
└── README_zh.md
```

---

## Data Availability

This repository publishes source code, tests, small source fixtures, and
reproduction entry points. It does not publish the raw 100-block performance
records, instrumented binaries, or the external full schedule executor. The
extended performance numbers are reported with the batch names and configuration
above so they are not confused with the public structural examples.
