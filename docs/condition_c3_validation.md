# Condition C3：if / else if / else 限定闭环完成报告

日期：2026-10-30。**C3限定功能闭环通过；完成报告提交后停止，不开始C4。**

## 1. 范围、基线与恢复

C1已完成；C2两层内嵌then/else的两种形式已完成。开始前重新运行现有25项回归，全部通过、退出0，见baseline_tests.stderr。baseline_before_c3.zip和baseline_manifest.json保存本轮修改前的可恢复源码快照，原交接包未覆盖。

唯一新增支持形式：一个if、一个else if、最终else；三个业务分支均为非空、多行、带花括号代码；整条链后有after_condition。未支持两个以上连续else if、分支内部继续嵌套、同行业务语句、循环/switch、分支内早期控制转移、无else的C4。没有锁、PI、就绪、WCET、首次内核派发或新算法研究。

真实程序source_files/condition_c3/condition_c3.c含main+一个worker、两个信号量，测试输入由C2D_MODE提供。三条路径分别使用0、1、7；7是其他值的代表。outer_evaluations/inner_evaluations是原测试源码中的条件求值计数，用于证明第二个条件仅在外层else到达，不用于替代条件选择。

## 2. 实际失败与最小修复

首先复现了失败：

```bash
cd /home/spcae/code2dag-condition
python3 -m unittest Code2DAG.tests.test_condition_c3 -v
```

修复前退出1，日志before_fix_tests.stderr；before_fix_model.json记录scheduler_compatible=false，原因是`conditional branch code shares a control boundary line`。当时没有执行被拒绝模型的timing或schedule，不将其写成已执行失败。

根因：_parse_statement已经把else if当作else中的内层条件，父子归属和两套图本身已建好。但外层else的源码范围从内层if开始，兼容性检查看到同一行前缀`} else `，误把纯控制头当作业务代码共享边界。执行节点实际均在三个花括号内部，控制头不应产生执行探针。

最小修复仅在pipeline/condition_model.py的build_condition_views中增加局部is_c3_control_prefix判断：只对顶层if加一个直接else-if、三个非空braced body、同函数恰好两个条件区域、子区域起止与外层else语句一致的C3形态，允许纯`} else `前缀。其他前缀和尾部业务代码仍拒绝，长else-if链不获豁免。没有重构解析器、扁平多路条件节点、结构建图、依赖定义或算法。

## 3. 修改文件与新增测试

|文件|内容|
|---|---|
|Code2DAG/pipeline/condition_model.py|build_condition_views / is_c3_control_prefix，仅修正上述严格C3头部误拒绝|
|Code2DAG/tests/test_condition_nested.py|native_acceptance测试辅助增加可选case描述，复用已有原生编译/计时/同步审计；C2默认行为不变|
|Code2DAG/tests/test_condition_c3.py|C3结构、路径投影、边界拒绝、五算法原生闭环测试|
|Code2DAG/source_files/condition_c3/condition_c3.c|真实三分支链用例|
|Code2DAG/docs/condition_c3_validation.md|本报告副本|

仅前两个为原有文件修改，其他原文件哈希不变。完整补丁见changes.patch。

新增5项测试：

1. test_c3_header_is_not_business_code_sharing_a_boundary
2. test_hierarchical_views_global_exclusivity_and_five_algorithms
3. test_actual_inline_business_code_still_rejected（三个业务分支分别验证）
4. test_longer_chain_does_not_receive_c3_header_exemption（只验证不扩大支持）
5. test_native_c3_three_paths

原25项加5项，共30项通过，退出0，见release_tests.stderr。C1及C2 then/else两个原生闭环均重新通过。

## 4. 入口、出口、互斥组和父子归属

|区域|入口|出口|互斥组|归属|
|---|---|---|---|---|
|外层|CE:worker#003@14-14|CX:worker#004@23-23|COND:worker#001|顶层|
|内层else-if|CE:worker#006@17-17|CX:worker#007@23-23|COND:worker#002|parent_id=COND:worker#001，parent_branch_id=else|

两个出口的源码行相同，但结构节点ID不同，不能混为一个节点。内层出口再连接外层出口，外层出口之后才是after_condition；没有把after_condition放入内层else。

|业务|可追溯执行节点|成员|
|---|---|---|
|branch_zero|BR:worker#005@15-16|外层then|
|branch_one|BR:worker#008@18-19|外层else + 内层then|
|branch_other|BR:worker#009@21-22|外层else + 内层else|
|after_condition|BR:worker#010@24-26|无条件成员，始终执行|

三条完整路径的选择条件两两互斥。各投影只保留一个业务分支；mode=0剔除内层CE/CX，第二个条件不求值。三个业务节点彼此之间没有可达先后关系；内层CE只由外层else到达。两套图均通过拓扑排序。

[结构图](/home/spcae/Documents/Codex/2026-10-27/new-chat/outputs/condition_c3/release_acceptance/structure.svg)；全部模型位于release_acceptance/native/intermediate_results/condition_c3/pipeline/blocks/level2/conditional_branches/。

共有10个调度执行节点、14条调度边，结构视图另含4个控制节点。结构边分类为{"intra": 6, "condition_branch": 4, "condition_merge": 4, "create": 1, "join": 1, "sem_dep": 2}；调度视图收缩CE/CX后branch/merge各3条，通过condition_transitions保留跨两层的完整转移，未丢失外层else语义。见projected_paths.json。

## 5. 原始映射与依赖

17个原始调用节点均有结构和执行段映射，逐项验证原函数、源码行范围和三分支对应关系。完整表见original_mapping.csv。两个mode比较不是基础调用图中的函数调用节点，不伪造其原始调用ID；CE/CX仍有明确源码行及条件区域归属。

原基础图8条依赖逐条验证：create=1、join=1、sem_dep=2、intra=4均保留类型/端点或原可达关系；没有引入新的同步语义。见base_segments.json、base_dag.json、dependency_checks.json。原节点名称中的legacy while/switch0片段不代表本次输入使用这些C语法。

## 6. 三条运行路径及关键事件

|输入|唯一业务分支|timing业务count（zero/one/other）|after_condition count|结果value|条件求值次数 outer/inner|每算法priority事件数|
|---|---|---|---:|---:|---|---:|
|C2D_MODE=0|branch_zero|1/0/0|1|110|1/0|8|
|C2D_MODE=1|branch_one|0/1/0|1|120|1/1|8|
|C2D_MODE=7|branch_other|0/0/1|1|130|1/1|8|

每条stdout顺序为main:start → 选中业务分支 → after_condition → value/求值计数 → main:end。原程序、timing、五算法生产版及诊断版逐字一致。模式0的inner_evaluations=0证明else-if条件入口没有到达；控制CE/CX不额外伪造运行事件。

每个业务timing count与实际选择一致；priority日志的节点集合和请求值逐条等于实际路径与静态优先级表。原始计时CSV保存在各timing_ZERO/ONE/OTHER的original_timing_trace，project/trace可能另外含同步审计执行的trace，不能混算。

probe_positions.json逐条验证宏和priority调用位于对应原始行，因而处在正确花括号内；移除新增语句/include后，timing与priority源码均逐字恢复到原C。同步审计仍调用真实API，所有路径均与原程序相同：1 create、1 join、2 sem_init、2 sem_post、2 sem_wait，8个事件全部返回0。比较不宣称线程交错相同。

## 7. 五种普通算法与权限结果

每种算法为全部10个节点生成优先级，并完成插桩、编译、三路径生产/诊断执行。

|算法|branch_zero|branch_one|branch_other|after_condition|
|---|---:|---:|---:|---:|
| FIFO | 96 | 94 | 95 | 97 |
| LPF | 96 | 92 | 93 | 95 |
| heft | 95 | 93 | 94 | 92 |
| t_level | 95 | 94 | 93 | 97 |
| zhao2020 | 96 | 90 | 91 | 95 |

这些是本批计时输入的结果，不是固定优先级或性能结论。仅汇总同一节点在各路径的平均计时，未合并分支最坏耗时。

**本轮120次真实pthread_setschedparam均返回rc=1/EPERM，query=0、实际SCHED_OTHER/0。** 已验证分配值和触发路径，不声称真实FIFO生效。没有权限改动、sudo实验或新运行机制，符合本项验收边界。

## 8. 命令、退出码和产物索引

最终全回归：

```bash
cd /home/spcae/code2dag-condition
python3 -m unittest discover -s Code2DAG/tests -v
```

30项通过、退出0。持久闭环执行：

```bash
python3 /home/spcae/Documents/Codex/2026-10-27/new-chat/outputs/condition_c3/run_release_acceptance.py
```

退出0；已有目录拒绝覆盖。release_acceptance/commands.json中73条子命令全部退出0，逐条含argv、cwd、flags和C2D_MODE覆盖值；logs/保存同名stdout/stderr。包含GCC13新expand、两个generation阶段、collect/blocks、三次timing、五次schedule/instrument和原程序/生产/诊断编译及执行。未复用旧图或Windows展开。

三路径精确执行形式：C2D_MODE=0/1/7 <release_acceptance/programs/算法/production>。对应日志production_FIFO_ZERO、production_LPF_ONE、production_heft_OTHER等，所有算法和模式的完整列表见commands.json。

核心产物：

- evidence_report.md：本报告；verification_summary.json：完成状态。
- before_fix_tests.stderr / before_fix_model.json：最小失败及根因数据。
- changes.patch / baseline_before_c3.zip：修复审阅和恢复基线。
- release_tests.stderr：全部30项通过。
- release_acceptance/acceptance.json：全部优先级、路径、事件和映射。
- release_acceptance/projected_paths.json：三条活动图与汇合证据。
- release_acceptance/timing_ZERO、timing_ONE、timing_OTHER：计时、trace、插桩源码和日志。
- release_acceptance/programs/算法/：生产与诊断源码、二进制、探针位置。
- original_mapping.csv / ordinary_priorities.csv：可追溯映射与优先级表。

## 9. 完成判定与停止边界

本限定C3的结构、全局互斥路径、层次归属、映射、依赖、源码插桩、同步与输出、五算法三路径及全部回归均通过，C3可记为完成（普通权限功能闭环）。真实FIFO、性能改善、WCET和响应时间不在本轮结论中。

不扩大为多个else-if、链内部嵌套、同行业务、循环/switch、早期跳转、无else的C4或任意C条件。C3豁免之外的通用解析能力不等同已完成支持。本报告提交后立即停止，不开始C4或其他下一项。
