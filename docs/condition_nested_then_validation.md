# Code2DAG Condition 第二项：两层嵌套最小闭环

日期：2026-10-30。结论：**本轮明确范围内的两层嵌套Condition功能闭环通过；现有核心实现已经正确支持，无需修改建模或调度代码。** 三条语义路径全部通过，外层false还额外验证了inner的两种取值。阶段一回归保持通过。

## 1. 假设与精确支持范围

仅两个多行、带花括号的if/else，内层只位于外层then。外层then在内层前后均保留独立执行段，外层else独立。运行时仍由原C表达式选择分支，不计算合并WCET，不打包嵌套条件，不修改普通算法、依赖定义、锁、PI、就绪或调度协议。

真实用例是pthread+sem的可编译C程序：主线程与一个工作线程，start/done两个信号量，5个可区分业务段，结果在join后读取，无需新增锁。环境变量按“存在/不存在”取值；false必须unset，而不是字符串0。条件表达式自身递增outer_evaluations/inner_evaluations，用于证明条件入口是否真正被求值。

本轮支持的是上述源码布局和普通pipeline。历史单层研究命令condition-run及条件感知搜索不在本轮使用或扩展；完整闭环使用现有generation/collect/blocks/timing/schedule/instrument命令。

## 2. 基线、快照与实际修改

开始前已完整阅读README_FIRST.md、condition_phase1.md、condition_real_program_validation.md、阶段一完成审计，以及现有Condition实现、测试和调用方。旧文档中的Windows/未验收文字是历史记录，以阶段一完成审计及已有Ubuntu报告为准。

阶段一及已有pipeline回归：21项通过、退出0，见baseline_tests.stderr。新快照baseline_before_nested.zip含208个已有源码/文档文件，已验证zip完整性并保存SHA清单baseline_manifest.json。没有覆盖阶段一交接包或冻结压缩包。

**所有208个原有文件哈希保持不变。** 本轮项目内仅新增：

|文件|函数/职责|
|---|---|
|Code2DAG/source_files/condition_nested_then/condition_nested_then.c|worker、main；两层条件、三条业务路径及既有pthread/sem同步类型的真实用例|
|Code2DAG/tests/test_condition_nested.py|validate_structure、projected_structure、verify_insertions、native_acceptance及两个回归测试；结构、映射、原生全流程、源码恢复和事件验收|
|Code2DAG/docs/condition_nested_then_validation.md|本报告的项目内副本|

outputs中的脚本、快照、CSV和原始日志为验收辅助。condition_model.py、conditional_rule.py、timing、instrument、五种算法及已有测试均未修改。原生初步检查已显示递归建图、父子归属和双重成员信息正确；本轮没有需要先修复的Condition失败。早期initial_native为检查已有能力的草稿输入，最终结论只采用release_native_acceptance。

## 3. 结构与互斥验收

|区域|条件入口|条件出口|exclusive_group|父子归属|
|---|---|---|---|---|
|外层|CE:worker#003@14-14|CX:worker#004@29-29|COND:worker#001|顶层|
|内层|CE:worker#006@17-17|CX:worker#007@23-23|COND:worker#002|parent_id=COND:worker#001，parent_branch_id=then|

共有12个调度执行节点、16条调度边；另有4个结构控制节点。调度视图保留5个条件内业务执行段：

|业务段|执行节点|成员信息|
|---|---|---|
|outer_then_before|BR:worker#005@15-16|外层then|
|inner_then|BR:worker#008@18-19|外层then + 内层then|
|inner_else|BR:worker#009@21-22|外层then + 内层else|
|outer_then_after|BR:worker#010@24-25|外层then|
|outer_else|BR:worker#011@27-28|外层else|

完整结构：[结构图](/home/spcae/Documents/Codex/2026-10-27/new-chat/outputs/condition_nested_phase2/release_native_acceptance/structure.svg)，源DOT/JSON位于release_native_acceptance/native/intermediate_results/condition_nested_then/pipeline/blocks/level2/conditional_branches/。

逐路径投影检查了：内层出口位于外层then路径，随后到outer_then_after再到外层出口；内层汇合只来自选中内层分支，外层汇合只来自实际选中路径。outer=false投影删除内层入口/出口及所有内层分支，两种inner取值投影相同。完整结构/调度图均无互斥分支间的虚假可达依赖，且拓扑排序通过。见projected_paths.json。

CE/CX是结构控制节点，不是额外运行线程或计时节点。本轮没有伪造其“执行事件”：outer=false时inner_evaluations=0，证明内层C条件入口没有求值；两个内层执行段的timing/priority事件也均为0。

## 4. 映射与依赖

18个原始调用节点均有结构映射；其中16个有执行段映射，两个条件getenv调用分别映射到外层/内层CE，不作为计算分支节点。所有映射逐项验证函数和源码范围；5个业务调用分别指向表中的对应段，完整追溯表见original_mapping.csv。

原始基础DAG的8条依赖逐条核验，非intra保留类型和端点、intra保留可达关系：

|依赖|基础图|最终调度图|
|---|---:|---:|
|create|1|1|
|join|1|1|
|sem_dep|2|2|
|intra|4|4|
|condition_branch|0|4|
|condition_merge|0|4|

见base_segments.json、base_dag.json和dependency_checks.json。原始节点名称中偶有legacy导出的while/switch0标签；本用例源码没有循环或switch，也没有扩展这些语法。

## 5. 三条路径与inner不敏感性

|配置|环境|实际有timing事件的条件业务段|每算法priority事件数（全部节点）|程序退出码|
|---|---|---|---:|---:|
| TT | C2D_OUTER=1, C2D_INNER=1 | outer_then_before, inner_then, outer_then_after | 10 | 0 |
| TF | C2D_OUTER=1, unset C2D_INNER | outer_then_before, inner_else, outer_then_after | 10 | 0 |
| F0 | unset C2D_OUTER, unset C2D_INNER | outer_else | 8 | 0 |
| F1 | unset C2D_OUTER, C2D_INNER=1 | outer_else | 8 | 0 |

TT输出包含outer_then_before → inner_then → outer_then_after，value=111；TF为outer_then_before → inner_else → outer_then_after，value=121。两者outer_evaluations=1、inner_evaluations=1。

F0/F1均只有outer_else，value=1000、outer_evaluations=1、inner_evaluations=0；完整stdout逐字一致。全部输出都有固定main:start/main:end。未选分支及外层false时内层条件均不执行。

每个timing业务段在被选中时count=1，未选中时count=0。完整计时权重、原始trace、编译/运行日志分别保存在timing_TT、timing_TF、timing_F0、timing_F1。original_timing_trace保留正式一次采样的原始CSV，project/trace还可能含随后同步审计运行产生的额外trace，不能混算。

## 6. 五种普通算法

本次每算法为全部12个执行节点分配优先级；相关5段如下。这些是本次真实计时输入下的结果，不是固定值或优越性结论。

|算法|outer_then_before|inner_then|inner_else|outer_then_after|outer_else|
|---|---:|---:|---:|---:|---:|
| FIFO | 96 | 94 | 95 | 93 | 97 |
| LPF | 96 | 90 | 95 | 94 | 91 |
| heft | 95 | 92 | 93 | 91 | 94 |
| t_level | 92 | 95 | 94 | 96 | 91 |
| zhao2020 | 96 | 89 | 95 | 94 | 88 |

调度输入仅将同一节点在分别保存的路径样本中做总和/计数平均；没有把互斥分支合并、取最坏时间或增加优先级规则。见schedule_timing_inputs.json及各算法schedule.json。

每算法四个输入各编译、运行原生产插桩版和诊断版，均退出0。诊断版本只增加节点标签和真实API返回值观测，调用的仍是原prio_runtime.h与实际pthread_setschedparam；没有用成功桩替换真实设置。

**权限边界：本轮180次优先级设置尝试均为rc=1/EPERM，查询为SCHED_OTHER/0。** 已验证的是优先级表完整、对应调用只在正确路径发生；未验证这个新嵌套用例的真实FIFO生效，不把程序退出0当作设置成功。未改系统权限，也未运行sudo实验。本轮不以新增调度机制来绕过权限。

## 7. 花括号内插桩及语义对比

每个timing BEGIN/END与priority调用都核对插入时的原始行游标；含双重成员信息的内层段自然位于两层正确花括号内部。probe_positions.json保存逐点位置。移除新增探针语句/include后，timing及priority源码均与原C源码逐字一致。

原程序、timing程序、五种生产插桩版、五种诊断版的每条输入stdout逐字相同。同步审计仍调用真实API，按信号量对象归一化比较：1次create、1次join、2次sem_init、2次sem_post、2次sem_wait，共8个成功事件，与无插桩原程序一致。比较的是事件内容/计数/返回值，不宣称线程交错或性能不变。没有新增锁。

## 8. 实际命令与退出码

工作目录/home/spcae/code2dag-condition：

```bash
python3 -m unittest Code2DAG.tests.test_condition_model Code2DAG.tests.test_condition_pipeline -v
python3 -m unittest discover -s Code2DAG/tests -v
```

分别21项和23项通过，退出0；最终日志release_tests.stderr。新增测试为：

- NestedConditionTests.test_recursive_structure_memberships_and_no_false_dependencies
- NestedConditionTests.test_native_three_paths_plus_false_inner_invariance

持久验收通过native_acceptance执行，release_native_acceptance/commands.json中的**88条实际子命令全部退出0**，逐条含argv/cwd/输入布尔值及对应stdout/stderr。包含GCC13版本查询、新expand、两个generation模式、collect、blocks、四次timing、五次schedule/instrument、原程序/审计/生产编译与四输入执行。GCC展开使用-O0 -g -fdump-rtl-expand；各运行程序使用-O2 -std=c11 -pthread。基础图只复制同次新生成的dot，未复用Windows或旧dag5图。

三条路径的直接生产执行命令形式（每个算法均已执行）：

```bash
# TT
C2D_OUTER=1 C2D_INNER=1 <programs/算法/production>
# TF
unset C2D_INNER; C2D_OUTER=1 <programs/算法/production>
# F0
unset C2D_OUTER C2D_INNER; <programs/算法/production>
# F1，证明inner不影响outer=false
unset C2D_OUTER; C2D_INNER=1 <programs/算法/production>
```

上述为可读形式，实际执行通过subprocess独立环境设置，没有依赖前一条shell的环境残留。各精确绝对路径和退出码以commands.json为准。日志名如production_FIFO_TT.stdout、diagnostic_LPF_F1.stderr、timing_TF.stdout等。

复跑：运行新增unittest会在新临时目录重建全部流程；要保留产物，可从Code2DAG.tests.test_condition_nested导入native_acceptance，传入一个**不存在的新目录**，不得覆盖本轮证据。

## 9. 结论与未支持边界

本轮唯一目标的结构、嵌套成员、原始映射、依赖、三条运行路径、普通算法、插桩位置和语义对比均通过，全部回归通过，因此可以将**本限定范围的两层嵌套Condition功能闭环记为完成（普通权限执行）**。不声称新用例已成功运行SCHED_FIFO、性能提升、WCET/响应时间界或所有线程交错等价。

未支持/未验收：内层位于外层else、else if、三层及以上、同行分支、循环、switch、分支内return/break/continue/goto，以及其他复杂布局。C函数末尾正常return是普通函数结束，不是条件分支早退。本轮没有扩展这些行为，也未为强制收缩范围而改写已有通用解析器；已有结构识别能力不等同完整执行支持声明。

没有开始下一项功能、锁/PI、计算就绪或新优先级算法研究。baseline源码清单全部未变；只新增本轮用例、测试和文档。
