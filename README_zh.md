# Code2DAG

<p align="center">
    <img src="./assets/logo.svg" alt="Code2DAG" width="30%">
</p>

<p align="center">
    <a href="https://github.com" rel="nofollow">
        <img src="https://img.shields.io/badge/并行程序-DAG构建-orange.svg">
    </a>
    <a href="https://github.com" rel="nofollow">
        <img src="https://img.shields.io/badge/平台-Linux%20%7C%20POSIX-brightgreen.svg">
    </a>
    <a href="https://github.com" rel="nofollow">
        <img src="https://img.shields.io/badge/Python-%E2%89%A53.8-yellow.svg">
    </a>
</p>

<p align="center">
    <a href="README.md">English</a> | <a href="README_zh.md">中文</a>
</p>

**Code2DAG** 从并行 C 程序构建面向调度的 DAG 模型。工具以源文件和 GCC RTL
展开为输入，提取线程间依赖、执行同步感知的节点合并，并输出图视图、映射表、
计时导入、调度优先级和插桩程序。

仓库包含两类相关能力：

- 原始**传统 DAG**流水线：面向直线型线程体、`pthread_create` /
  `pthread_join`、信号量和互斥锁；
- **扩展 DAG**流水线：面向 `if/else` 条件路径、mutex identity 与分支成员关系、
  嵌套 condition/lock/loop 结构，以及 C2c（Condition + 独立锁区、无静态 Loop）
  规则。

扩展流水线是图构建与分析工具链。当前性能实验使用的完整排程执行器是独立组件，
**不包含在本仓库中**。特别是 C2c 与 `composition_*` 嵌套规则属于
analysis-only：标准 `timing` / `instrument` 会拒绝它们，其图通过导出的
JSON/映射文件交给外部成本/排程执行器使用。

---

## 目录

- [概述](#概述)
- [核心特性](#核心特性)
- [传统 DAG 与扩展 DAG](#传统-dag-与扩展-dag)
- [扩展 DAG 规则](#扩展-dag-规则)
- [支持的源码形态与限制](#支持的源码形态与限制)
- [快速开始](#快速开始)
- [流水线概述](#流水线概述)
- [环境要求与安装](#环境要求与安装)
- [命令行参考](#命令行参考)
- [输出结构](#输出结构)
- [旧节点优先级运行方式与完整排程执行器](#旧节点优先级运行方式与完整排程执行器)
- [实验摘要](#实验摘要)
- [复现与测试结果](#复现与测试结果)
- [术语说明](#术语说明)
- [适用范围与限制](#适用范围与限制)
- [仓库结构](#仓库结构)
- [数据可用性](#数据可用性)

---

## 概述

现代并行程序的依赖并不局限于单条调用链：线程生命周期、信号量、互斥临界区和
互斥的 `if/else` 分支都会影响可行执行顺序。忽略其中任何一类依赖的 DAG 都可能
给出不可行的调度。

Code2DAG 对 C 源码与 GCC RTL 做联合静态分析。传统流水线提取顺序调用链、
`pthread_create` / `pthread_join` 关系，以及 `sem_post` / `sem_wait` / mutex
约束。扩展规则额外建模：

- **条件路径**：成对的 `condition_entry` / `condition_exit` 结构节点，独立的
  then/else 执行节点，`condition_branch` / `condition_merge` 边，以及
  `exclusive_group` / `branch_id` 元数据；
- **mutex identity 与分支成员关系**：完整临界区成为 `LOCKGROUP`（或分支内的
  `LOCKPATH`）节点；每个节点保留 `mutex_id`、成员执行段、lock/unlock 范围和
  源映射；
- **嵌套结构**：锁内条件树、条件分支内锁，以及经过验证的 condition/lock/loop
  组合族；
- **C2c**：条件路径加独立锁区、不含静态 Loop；即使多个 worker 使用同一
  mutex，每个临界区仍保持为独立锁组。

输出包含两套视图：用于检查控制语义的**结构视图**，以及只含可执行节点（以及
必要的合成锁组）的**调度视图**。

---

## 核心特性

- 从并行 C 源码与 GCC RTL 自动构建 DAG，无需手工标注图。
- 传统三类依赖：顺序调用链、线程生命周期（`pthread_create` /
  `pthread_join`）、同步（`sem_post` / `sem_wait`、`pthread_mutex_lock` /
  `pthread_mutex_unlock`）。
- 默认 `effective_line_merge` 规则的同步感知节点合并，以及可选 condition、
  lock、loop 规则。
- 扩展条件模型：显式互斥分支，以及结构入口/出口节点。
- mutex 感知组合：`mutex_id`、锁组、分支成员关系和完整的源码/段映射。
- C2c：条件加独立锁区分析，不要求静态 Loop。
- 结构/调度双 JSON 视图，并保留 `segments.json` / `dag_seg.json` 兼容别名。
- 现有优先级算法（`FIFO`、`LPF`、`HEFT`、`T_LEVEL`、`Zhao2020`）消费调度视图。
- 可选条件优先级算法（`condition_pareto`、`condition_union`），用于两 profile
  条件输入。
- 固定 27 程序语料、G1–G4 结构映射和 C2c 规则的原生测试。

---

## 传统 DAG 与扩展 DAG

| 维度 | 传统 DAG | 扩展 DAG |
|:---|:---|:---|
| 输入形态 | 直线型线程体、create/join、信号量、互斥锁 | 增加 `if/else` 树、嵌套条件、mutex identity、有界静态循环及其验证过的组合 |
| 默认规则 | `effective_line_merge` | `conditional_branches`、`lock_grouped`、`loop_static*`、`composition_*`（显式启用） |
| 控制表示 | 无独立条件节点 | 结构视图含 `CE:` / `CX:` 节点和分支边；调度视图含独立分支执行节点 |
| mutex 表示 | `mutex_cs` 段 | `mutex_cs` 成员加 `LOCKGROUP` / `LOCKPATH`，保留 `mutex_id`、锁范围和分支成员关系 |
| 互斥语义 | 合并段内隐式表达 | 显式 `exclusive_group` / `branch_id`；then/else 分支互不连接 |
| 输出视图 | 单一统一视图 | `segments_struct.json` / `dag_struct.json` 与 `segments_sched.json` / `dag_sched.json`；保留兼容别名 |
| 标准 timing/instrument | 支持 | `conditional_branches` 支持；lock/composition 规则为 analysis-only 或导入计时；loop 规则使用显式模板计时 |
| 完整排程执行器 | 不在本仓库 | 不在本仓库；外部执行器消费导出的图/计划 |

扩展规则**不改变**默认流水线。原有 DAG 1–6 用例和 `effective_line_merge`
入口保持原有行为。

---

## 扩展 DAG 规则

所有扩展规则都注册为 level-2 block 规则，通过 `--rule` 或 composition facade
显式选择。

| 规则 | 源码形态 | 主要输出 | 标准 timing / instrument |
|:---|:---|:---|:---|
| `effective_line_merge` | 传统直线型 + 同步 | 合并执行段 | 支持 |
| `conditional_branches` | 单个或嵌套 `if/else` 树；不与 lock/loop 混用 | `CE:` / `CX:` 结构节点；独立 then/else 执行节点 | 支持 |
| `lock_grouped` | 独立完整 mutex 区；无 condition/loop | 每个独立区一个 `LOCKGROUP`；保留 `mutex_id` | Analysis-only（导入成员计时） |
| `loop_static`、`loop_static_pair`、`loop_static_nested` | 有界静态 `for` 循环 | Loop/template/iteration 节点 | 显式模板计时；无运行时 C 插桩 |
| `composition_c1` | 条件 + 锁 + 循环三者互不嵌套 | 组合分析图 | Analysis-only |
| `composition_c2a` | 条件分支内完整锁 | 分支局部 `LOCKGROUP` / `LOCKPATH` | Analysis-only |
| `composition_c2b` | 锁内条件树（含有界 helper/worker 形态） | `LOCKPATH` 候选与锁组 | Analysis-only |
| `composition_c2c` | 条件 + 独立锁区，**无静态 Loop** | 独立条件分支，每个临界区一个 `LOCKGROUP` | Analysis-only |
| `composition_c3a`、`composition_c3b` | 条件/循环嵌套 | 组合分析图 | Analysis-only |
| `composition_c4a`、`composition_c4b` | 循环/锁嵌套 | 组合分析图 | Analysis-only |
| `composition_c5a`…`composition_c5f` | 已验证的三层 condition/loop/lock 嵌套 | 组合分析图 | Analysis-only |

G4 结构示例使用的 `composition_c2c` 规则：

- 要求同时存在 Condition 与 Lock，并拒绝静态 Loop；
- 每个临界区必须对应恰好一个完整的 `mutex_cs` 基础段；
- 即使多个锁组共享同一 `mutex_id`，每个独立临界区仍保持自己的 `LOCKGROUP`；
- 拒绝未配对/嵌套 mutex、lock/condition 重叠、动态 mutex 初始化、别名和不支持
  的线程调用形态；
- 设置 `capabilities.scheduler_compatible=true` 和
  `capabilities.analysis_only=true`；标准 `timing` / `instrument` 被显式禁用。

---

## 支持的源码形态与限制

已测试支持：

- 使用 POSIX 线程、`pthread_create` / `pthread_join`、信号量和静态初始化
  `pthread_mutex_t` 的 C 程序；
- 成对的 `pthread_mutex_lock` / `pthread_mutex_unlock` 临界区；
- 带花括号、多行分支体的有界 `if/else` 树；
- loop/composition 规则中的有界静态 `for` 循环；
- `tests/validation27/programs/` 的固定 27 程序语料，以及
  `examples/g1_g4/sources/` 的 G1–G4 源码。

不支持或显式拒绝：

- 动态 mutex 初始化、mutex 别名、函数指针调用、`trylock` / wrapper 锁；
- 嵌套或未配对 mutex 操作；
- 条件分支中的 `return`、`break`、`continue`、`goto`（当分支需要独立调度时）；
  同一源码行上的分支代码也会因独立插桩而被拒绝；
- 无界循环或由运行期变量决定的循环边界；
- 有效非 `#include` 预处理指令、受控控制位置中的宏，以及不支持的静态循环形态；
- C2c 输入中的任何静态 Loop、lock/condition 重叠，或超出有界条件树形态的结构；
- 通过标准 `timing` / `instrument` 使用 C2c 与 `composition_*` 规则：它们是
  analysis-only，必须由外部成本/排程执行器消费。

扩展规则保留原始源文件；它们只构建分析副本，不重写输入 C 程序。

---

## 快速开始

请在仓库父目录执行命令，使 `Code2DAG` 可作为包导入：

```bash
cd <仓库父目录>
```

### 1. 传统 DAG（完整流水线）

```bash
python3 -m Code2DAG.pipeline.cli run_all \
  --source Code2DAG/source_files/dag1/dag1.c \
  --level level2 \
  --rule effective_line_merge \
  --repeats 1
```

参考机器上的实测结果：8 个阶段全部完成；调度视图 21 个节点；
`FIFO`、`LPF`、`heft`、`t_level`、`zhao2020` 五种算法各为 21 个节点插桩优先级。

### 2. 扩展条件 DAG（完整流水线）

```bash
python3 -m Code2DAG.pipeline.cli run_all \
  --source Code2DAG/source_files/condition_nested_then/condition_nested_then.c \
  --level level2 \
  --rule conditional_branches \
  --repeats 1
```

实测结果：8 个阶段全部完成；调度视图 12 个节点；五种算法各为 12 个节点插桩
优先级。结构视图额外包含条件入口/出口节点和分支边。

### 3. C2c 分析（不运行标准 timing/instrument）

```bash
python3 Code2DAG/examples/run_c2c.py
```

该命令在临时目录中构建
`Code2DAG/source_files/composition_c2c/benchmark.c`，输出 C2c 图摘要；不运行
`timing`、`instrument` 或运行时工作负载。

### 4. G1–G4 结构复现

```bash
python3 Code2DAG/examples/g1_g4/reproduce.py
```

四图结构用例的源码位于 `Code2DAG/examples/g1_g4/sources/`。

---

## 流水线概述

传统流水线为：

1. **源码/RTL 分析**：解析 C 源码与 GCC RTL `.expand` 转储。
2. **依赖提取**：顺序、线程生命周期与同步关系。
3. **DAG 构建**：含调用/同步/控制节点的原始 DAG。
4. **节点合并**：level-2 同步感知分段。
5. **计时**：对段边界插桩并测量成本。
6. **调度**：使用所选算法分配节点优先级。
7. **插桩**：生成带优先级插桩的 C 程序。

扩展规则替换第 4 步。它们先构建基础段/DAG，再加入 condition、lock、loop 或
composition 节点及映射。调度视图仍由同一组优先级算法消费；结构视图用于检查
和外部分析。

---

## 环境要求与安装

| 依赖 | 版本 | 用途 |
|:---|:---|:---|
| Python | ≥ 3.8 | 流水线运行时 |
| GCC | 现代版本（原生测试使用 GCC 13） | RTL 转储与插桩编译 |
| Graphviz（`dot`） | 现代版本 | DAG 渲染 |

```bash
python3 --version
gcc --version
dot -V
```

Python 依赖（`requirements.txt`）：

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

核心流水线与示例不需要 GUI 或实时调度权限。原生测试调用 `gcc-13`；若使用其他
GCC，可设置 `CC=gcc`（需保持相同的 RTL expand 行为）。

---

## 命令行参考

```text
python3 -m Code2DAG.pipeline.cli <子命令> [选项]
```

| 子命令 | 说明 |
|:---|:---|
| `run_all` | 完整流水线：collect → blocks → timing → schedule × 算法 → instrument × 算法 |
| `collect` | 解析源码/RTL，构建原始 DAG |
| `blocks` | 应用 level-2 block 规则 |
| `timing` | 插桩并剖析段成本 |
| `schedule` | 为单个算法计算优先级 |
| `instrument` | 生成带优先级插桩的 C 源码 |
| `list` | 列出已注册规则与算法 |

主要选项：

| 选项 | 说明 |
|:---|:---|
| `--source <路径>` | 输入 C 源码 |
| `--level <level1\|level2\|level3>` | 分段层级 |
| `--rule <规则>` | block 规则；默认 `effective_line_merge` |
| `--algo <名称>` | `schedule` / `instrument` 使用的优先级算法 |
| `--repeats <n>` | 计时重复次数 |
| `--segment-trace` | 记录节点/TID/优先级边界（仅 generic 插桩） |

列出扩展规则：

```bash
python3 -m Code2DAG.pipeline.cli list --level level2
```

实测结果：列出 23 个 level-2 规则，包括 `conditional_branches`、
`lock_grouped`、`loop_static_nested`、`composition_c1`、`composition_c2a`、
`composition_c2b`、`composition_c2c`，以及 `composition_c3`–`composition_c5f`
族。

---

## 输出结构

每个用例的最终产物发布到 `results/`，中间产物保留在 `intermediate_results/`。

传统/条件规则：

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

逐规则 block 输出：

```text
intermediate_results/<case>/pipeline/blocks/level2/<rule>/
├── segments_struct.json      # 结构视图（CE:/CX:、分支边）
├── dag_struct.json
├── segments_sched.json       # 调度视图（可执行节点 + 锁组）
├── dag_sched.json
├── segments.json             # 调度视图别名
├── dag_seg.json              # 调度视图别名
├── dag_seg.dot / .png
├── dag_struct.dot / .png     # 当结构视图与调度视图不同时
└── rule_meta.json
```

扩展映射字段包括：

- `condition_regions`：条件 id、入口/出口行、父条件 id、分支；
- 段上的 `condition_memberships`：`exclusive_group` 与 `branch_id`；
- `lock_groups`：`group_id`、`mutex_id`、成员、成员段、lock/unlock/critical
  范围；
- `mappings.original_node_to_execution_segment`、
  `generation_node_to_execution_segments`、
  `source_position_to_execution_segments`、
  `grouped_node_to_member_segments` 及对应结构映射；
- composition 规则的 `composition_edge_provenance`。

---

## 旧节点优先级运行方式与完整排程执行器

**旧节点优先级运行方式**是本仓库 `schedule` / `instrument` 阶段实现的路径：

- 优先级算法为每个执行节点分配一个优先级；
- `instrument` 在节点入口插入 `sp.sched_priority = prior_n` 和
  `pthread_setschedparam(...)`；
- 线程何时、在哪个 CPU 上运行仍由 Linux 调度器决定。

**当前性能实验**使用不同的执行模型：

- 导出的调度图与实测段成本交给外部完整排程执行器（HEFT / CHEFT / FIFO /
  LPF / Zhao 计划生成，以及冻结的逐 CPU 计划）；
- 执行器强制 CPU 分配、同核顺序和计划 trace；
- 该执行器、G4 专用成本/执行适配以及原始实验记录**不在**本仓库中。

不要把扩展性能数字理解为旧节点优先级插桩的结果。特别是收益不能单独归因于
mutex 建模：条件路径保真、mutex identity、段成本、概率加权 CHEFT 和完整执行器
都有贡献。G4 的逐配置结果有正有负，不能把八配置平均单独归因于锁。

---

## 实验摘要

### 传统 DAG（历史结果）

原始实验使用六种 DAG 结构（DAG 1–6）、两种 CPU 亲和性配置（2 核 `0–1`、
4 核 `0–1–2–3`）、100 次重复、VMware Ubuntu 20.04.6 虚拟机、Intel Core
i5-13400F、5.15.96-rt61 内核。传统结果报告为**相对 CFS 平均 makespan 降低
13.73%**（最优 **30.87%**，DAG 3，4 核）。

### 扩展 DAG（当前同程序 G3 结果）

当前扩展 DAG 论文主表报告：

| 对照 | 结果 |
|:---|---:|
| 扩展方案相对 CFS | **+8.80%** |
| 扩展方案相对传统 DAG + HEFT | **+4.92%** |

配置与数据出处：

- G1–G4，各 2 核与 4 核，每路径 100 个 block；G1/G2/G4 使用冻结的两路径概率，
  G3 使用原生 16 路径概率；
- G3 在两种核数下使用**同一程序 6 任务版本**（旧的 4 任务 G3_2c 只作历史结果，
  不混入本表）；
- 参考机器：原 i5-12600K，Linux `6.1.192-0601192-generic`，CFS/SCHED_OTHER；
- 数据来源：G1/G2/G4 来自 unified work2 100-block 正式批次，G3 来自 G3
  同程序 100-block 更新批次；汇总为当前同程序 G3 八配置主表；
- 运行时测量由完整排程执行器（不在本仓库）产生；原始 100-block 记录不在本仓库
  发布。

两套实验设置不可直接比较：程序、内核、机器和执行模型均不同。扩展数字不是
mutex-only 消融，也不应被这样表述。

---

## 复现与测试结果

以下命令在参考 Linux 主机的仓库父目录执行。实测结果一并列出，避免 README 出现
未经验证的命令。

| 命令 | 实测结果 |
|:---|:---|
| `python3 -m Code2DAG.pipeline.cli list --level level2` | 23 个 level-2 规则，7 个算法 |
| `python3 -m Code2DAG.pipeline.cli run_all --source Code2DAG/source_files/dag1/dag1.c --level level2 --rule effective_line_merge --repeats 1` | 8/8 阶段完成；FIFO/LPF/heft/t_level/zhao2020 各 21 个优先级 |
| `python3 -m Code2DAG.pipeline.cli run_all --source Code2DAG/source_files/condition_nested_then/condition_nested_then.c --level level2 --rule conditional_branches --repeats 1` | 8/8 阶段完成；五种算法各 12 个优先级 |
| `python3 Code2DAG/examples/run_c2c.py` | C2c：31 节点、42 边、5 个条件区、2 个 `LOCKGROUP`（`m1`）、7 条 create、12 条 join；`analysis_only=true` |
| `python3 Code2DAG/examples/g1_g4/reproduce.py` | G1 19/25、G2 22/29、G3 26/36、G4 31/42 调度节点/边；G4 有 2 个锁组（`task_5`、`task_7`） |
| `python3 -m unittest -v Code2DAG.tests.test_composition_c2c` | 4 项测试通过 |
| `python3 -m unittest -v Code2DAG.tests.test_validation27` | 27 程序结构回归通过 |
| `python3 -m unittest -v Code2DAG.tests.test_g1_g4_mapping` | G1–G4 映射回归通过 |

扩展 DAG 定向测试套件（condition model、嵌套条件、C1、C2a、C2b、C2c、
lock L1/L2/QA、27 程序语料和 G1–G4）：

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

实测：**116 项测试通过**（114 项既有扩展 DAG 测试 + 27 程序与 G1–G4 回归）。
完整仓库测试套件同样通过：

```bash
python3 -m unittest discover -q Code2DAG/tests
```

实测：**600 项测试通过**，耗时 137 s。原生测试需要 `gcc-13`；不需要实时权限，
也不运行性能工作负载。

---

## 术语说明

| 术语 | 定义 |
|:---|:---|
| **传统 DAG** | 由调用链、create/join、信号量和 mutex 临界区经默认合并规则构建的 DAG |
| **扩展 DAG** | 含条件路径、mutex identity/分支成员关系、嵌套组合或 C2c 的 DAG |
| **结构视图** | 含 `CE:` / `CX:`（及 loop 控制）节点、用于检查控制语义的图 |
| **调度视图** | 可执行节点加合成锁组；由优先级算法消费 |
| **LOCKGROUP** | 表示完整临界区的调度节点；保留 `mutex_id` 与成员段 |
| **LOCKPATH** | 锁区内含条件时使用的分支局部锁候选 |
| **C2c** | 条件 + 独立锁区、无静态 Loop；analysis-only |

---

## 适用范围与限制

Code2DAG 面向静态 POSIX 线程 C 程序。支持范围由 `tests/` 与 27 程序语料定义，
并非通用 C 前端。特别地：

- C2c 与 `composition_*` 不提供标准 C timing/instrument 支持；请用导出的 JSON
  配合外部成本/排程执行器；
- 公共仓库不包含完整排程执行器、G4 成本/执行适配或原始 100-block 测量数据；
- 扩展性能表来自上述实验批次，不由公共示例产生；
- 逐节点优先级插桩与论文数字使用的冻结完整排程执行器不是同一种执行模型。

`ASAP_DAG/` 目录保留原仓库使用的 ASAP 基准源码与产物，与扩展规则相互独立。

---

## 仓库结构

```text
Code2DAG/
├── pipeline/                 # CLI、流水线阶段、规则与算法
│   ├── condition_model.py    # 条件路径、结构/调度视图
│   ├── lock_model.py         # 锁组与 mutex identity
│   ├── lock_regions.py       # 函数作用域锁区匹配
│   ├── composition_c2a_model.py
│   ├── composition_c2b_model.py
│   ├── composition_c2c_model.py
│   ├── composition_c3a_model.py … composition_c5f_model.py
│   ├── rules/                # 已注册 level-2 规则
│   └── algo/                 # FIFO、LPF、HEFT、T_LEVEL、Zhao2020、condition*
├── generation/               # 源码/RTL 分析与 DAG 构建
├── core/                     # 解析与范围工具
├── level1/                   # Level-1 分段与插桩
├── level2/                   # Level-2 分段与组合
├── source_files/             # DAG 1–6、condition、lock、loop、composition 夹具
├── examples/
│   ├── run_c2c.py            # C2c 公共示例
│   └── g1_g4/                # G1–G4 源码与复现脚本
├── tests/
│   ├── validation27/         # 固定 27 程序语料与 manifest
│   ├── test_composition_c2c.py
│   ├── test_validation27.py
│   └── test_g1_g4_mapping.py
├── ASAP_DAG/                 # 原 ASAP 基准源码/产物
├── results/                  # 已发布示例输出（DAG 1–6）
├── intermediate_results/     # 流水线中间产物（DAG 1–6）
├── requirements.txt
├── README.md
└── README_zh.md
```

---

## 数据可用性

本仓库发布源码、测试、小型源码夹具与复现入口，不发布原始 100-block 性能记录、
插桩二进制或外部完整排程执行器。扩展性能数字连同批次名与配置一并给出，避免与
公共结构示例混淆。
