# 单个 if/else Ubuntu 原生验收（2026-10-27）

结论：当前单个、多行、带花括号 if/else 的普通权限功能闭环通过。真实 SCHED_FIFO 优先级切换没有成功，不能声称已验证实时调度效果。没有开始下一项 Condition 功能。

## 假设、环境和输入

工作目录 `/home/spcae/code2dag-condition`。Ubuntu 24.04.5 LTS，GCC/gcc-13 13.3.0。执行环境为本机 Codex 沙箱，euid=1000、CapEff=0、RLIMIT_RTPRIO=(0,0)、NoNewPrivs=1、Seccomp=2；见 `work/condition_ubuntu_20261027/environment.json`。没有 sudo、setcap、修改 limits 或系统权限。

输入为 `Code2DAG/source_files/dag5_condition_regression/dag5_condition_regression.c`，SHA256 `1a0a85fee32ab49521e78d1e17d02c1ed5d5c883d932fe3a85d9fa6c89bd6a4d`。保持 WORK_SCALE=1。true 为 CODE2DAG_CONDITION=1，false 必须 unset；字符串 0 仍为 true。原程序没有业务 stdout。

开始前完整读取 README_FIRST.md、两篇 Condition 文档、现有测试，以及 condition_model、conditional_rule、runner、collector、Level2 分段、timing、schedule、instrument 及相关插桩调用。参考论文未作为新增算法要求。

## 实际最小修改

- `Code2DAG/generation/exporters.py`：`CircleTxtExporter._extract_sem_record` 传入调用点列号，`_extract_source_arg_name` 区分同一行多个信号量调用；无列号且有多个匹配时，不再盲选第一个。
- `Code2DAG/tests/test_condition_model.py`：新增 `PipelineIntegrationTests.test_native_generation_preserves_same_line_semaphore_dependencies`。使用 GCC 13 真正重新编译回归源码，生成 DAG、collect、blocks，断言 8 条 sem_dep、17 create、17 join、独立 then/else 以及同一行参数消歧。
- 本验收文档和 `work/condition_ubuntu_20261027/{run.py,verify.py}` 为验收辅助，不改变研究模型或调度算法。
- 原回归 C 源码、Condition 模型和五种调度算法均未修改。补丁见 `work/condition_ubuntu_20261027/changes.patch`。

首次原生生成虽各阶段退出 0，但只有 42 条边/3 条 sem_dep，故判定验收失败。根因：第194行6次 sem_post 均被错误绑定至第一个 sem_mid_01。失败生成物保存在 `work/condition_ubuntu_20261027/before_fix/native/`，旧 circle.txt 可直接核查。修复后从源码重新生成 expand 和全部 DAG 元数据；新 native/ 原先不存在，没有读取旧 dag5 或 Windows expand。

## 命令和结果

完整逐条 argv、cwd、CODE2DAG_CONDITION、退出码保存于 `work/condition_ubuntu_20261027/commands.json`；每条命令有同名 `.stdout`、`.stderr`。

从工作目录执行过：

```bash
python3 -m unittest Code2DAG.tests.test_condition_model -v
python3 work/condition_ubuntu_20261027/run.py
python3 work/condition_ubuntu_20261027/verify.py
```

最初11项测试全部通过，修复后12项全部通过；最终 run.py 和 verify.py 退出码均0，`acceptance.json` 为 passed=true。run.py 当前要求 native/ 不存在；复跑请将两个脚本复制到新的空验收目录，避免覆盖本轮证据。

以下是脚本实际执行的命令形式，精确绝对路径见 commands.json：

```bash
gcc-13 -O0 -g -fdump-rtl-expand -c <回归源码> -o <全新expand目录>/dag5_condition_regression.o
python3 -m Code2DAG.generation.legacy <新expand> --source-file <回归源码> --output-base <native目录> --force --threads-only
python3 -m Code2DAG.generation.legacy <新expand> --source-file <回归源码> --output-base <native目录> --force
python3 -m Code2DAG.pipeline.cli --base-dir <native目录> collect --base-name dag5_condition_regression --source <回归源码>
python3 -m Code2DAG.pipeline.cli --base-dir <native目录> blocks --base-name dag5_condition_regression --level level2 --rule conditional_branches --source <回归源码>
# 分别设置1、取消环境变量，执行 timing，repeats=1
python3 -m Code2DAG.pipeline.cli --base-dir <native目录> timing --base-name dag5_condition_regression --level level2 --rule conditional_branches --repeats 1
# 对 FIFO、LPF、heft、t_level、zhao2020 分别执行
python3 -m Code2DAG.pipeline.cli --base-dir <native目录> schedule --base-name dag5_condition_regression --level level2 --rule conditional_branches --algo <算法>
python3 -m Code2DAG.pipeline.cli --base-dir <native目录> instrument --base-name dag5_condition_regression --level level2 --rule conditional_branches --algo <算法> --mode generic
gcc-13 -O2 -g -std=c11 -pthread <各算法instrumented.c> -o <app>
# 每个 app 分别设置1和取消 CODE2DAG_CONDITION 后运行
```

generation 导出的新 config_files/dag5_condition_regression.dot 被复制到同次生成的 dag_generation/dag.dot 供 collector 使用。此步骤只复制本次新图，不复用旧图。timing 内部 gcc 也已确认是13.3.0。上述编译、两个 generation、collect、blocks、两次 timing、5次 schedule、5次 instrument、5次编译、10次生产源码执行均退出0。

verify.py 另外编译运行无插桩原程序和诊断版本。诊断头只增加位置/返回值观测，仍调用生产 prio_runtime.h 的真实实现；链接 wrap 同步函数时仍调用 __real_* 并返回原返回值。原程序及五种诊断版本各运行两条路径，均退出0。原始生产插桩版本也单独编译运行，不以诊断替身代替。

## 十项验收证据

1. 22个调度节点/47条边，then=`BR:a_gate#004@188-188`、else=`BR:a_gate#005@190-191`。不存在打包最坏节点。
2. 结构图各有一个 condition_entry、condition_exit，4条条件边；共同 exclusive_group=`COND:a_gate#001`。
3. 119个原节点有结构映射，118个有执行段映射。getenv 条件表达式映射到入口结构节点，不作为分支执行段。then调用映射到then，else两次调用均映射到else。逐项核验所有已映射调用的函数及行范围。
4. 原基础DAG全部依赖逐条检查：非intra边保留类型及对应端点，intra边保留可达关系。最终 create=17、join=17、sem_dep=8、intra=1、condition_branch=2、condition_merge=2。基础图快照为 base_segments.json/base_dag.json。
5. 五种算法全部为22个节点生成优先级，then/else分别如下：FIFO 78/93，LPF 78/97，heft 96/97，t_level 85/84，zhao2020 91/97。这些是本轮else计时样本的结果，非固定值或保证。
6. true timing：then count=1、else count=0。
7. false timing：then count=0、else count=1。原始 trace 文件也保存于各 timing_<路径>/project/trace/。
8. timing 的 BEGIN/END 和 priority调用均由源码范围断言确认在各自分支内部；诊断日志中每条路径只出现该分支的一次优先级调用。两次 else 计算调用属于同一else节点。详见 audit_results.json 的 line、requested。
9. 移除新增探针/priority语句及新增include后，插桩源码与原源码逐字一致；原程序、timing、五个priority版本 stdout均为空。诊断同步事件按符号归一化后与原程序完全一致（忽略并发顺序），包括17次create、17次join、8次sem_init、8次sem_post、8次sem_wait、19次lock、19次unlock，全部返回0。不据此声称相同线程交错。
10. 五种算法两路径每次21次优先级尝试，全为 rc=1/EPERM；pthread_getschedparam 返回0且 policy=0/SCHED_OTHER。生产版本同样报告失败但退出0。没有为测试放宽权限，也未执行特权测试。

## 关键新产物（均在工作目录下）

- `work/condition_ubuntu_20261027/expand/dag5_condition_regression.c.255r.expand`
- `work/condition_ubuntu_20261027/native/intermediate_results/dag5_condition_regression/dag_generation/`：新基础调用和位置元数据。
- `work/condition_ubuntu_20261027/native/intermediate_results/dag5_condition_regression/pipeline/blocks/level2/conditional_branches/`：四个结构/调度JSON及兼容别名、图。
- `work/condition_ubuntu_20261027/timing_then/`、`timing_else/`：两路径完整计时项目、trace、JSON、编译和运行日志。
- `work/condition_ubuntu_20261027/native/intermediate_results/dag5_condition_regression/pipeline/schedule/level2/conditional_branches/<算法>/schedule.json`
- `work/condition_ubuntu_20261027/<算法>/instrumented.c`、`app`：五种算法均有独立快照。pipeline instrument会清理之前算法的临时目录，故验收不依赖那些会被覆盖的位置。
- `work/condition_ubuntu_20261027/audit/`、`audit_results.json`：真实调度API返回值、分支调用位置及同步事件证据。
- `work/condition_ubuntu_20261027/{commands.json,acceptance.json,sha256.json,environment.json,changes.patch}`。

## 尚未完成的风险和限制

- 完成的是原生源码生成到双路径执行的普通权限功能闭环；成功启用SCHED_FIFO及其运行效果没有验证。工具运行受沙箱约束，已完整记录环境，不外推特权行为。
- 单路径 timing 给未选择节点填0；本轮调度使用最后一次else样本，没有用伪造数据填then时间，没有合并分支最坏时间。两路径实测分别保存。单次采样不是WCET或性能结论。
- 原程序本来共享 mat_c/g_busy_sink，可能存在数据竞争；本轮未改变源码或修复它，也未证明所有并发交错/数值结果等价。业务stdout为空，语义证据限于源码恢复、分支观测、同步事件和正常退出。
- 旧生成器将else中两次同名计算调用的精确行号均记录为191；两次都正确映射到190–191的同一个else执行段。本轮未扩展精确调用点恢复。
- 没有实现或验证 else if、循环、switch、早退、复杂嵌套、新的论文优先级算法、响应时间分析或分支最坏时间合并。
