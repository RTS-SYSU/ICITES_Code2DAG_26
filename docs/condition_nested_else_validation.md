# 两层嵌套Condition：外层else对称验收

日期：2026-10-30。结论：**本次else对称形式通过。结合此前then形式的通过记录，支持结论可扩大为：单个内层、多行带花括号if/else可位于外层then或else之一，两种形式均完成普通权限下的结构、映射、语义路径和五算法执行闭环。** 不代表两侧同时有多个内层、任意C布局或真实FIFO已经验收。

## 基线与改动

开始前保存baseline_before_else.zip及baseline_manifest.json，没有覆盖阶段一/then形式的交接包。基线23项通过，退出0；最终25项全部通过，退出0，其中then原生闭环也重新运行通过。

生产建模和算法代码仍无需修改。项目内变化只有：

- 新增source_files/condition_nested_else/condition_nested_else.c：worker/main；将内层放到外层else，保留嵌套前后两个独立业务段，outer=true执行独立outer_then。
- 新增tests/test_condition_nested_else.py：test_else_parent_memberships_and_projected_merges、test_native_false_paths_and_true_inner_invariance。
- 修改tests/test_condition_nested.py中的测试辅助：case_labels、call_lines、validate_structure、projected_structure、expected_output、native_acceptance接受明确的child_branch参数；默认then及其原测试不变，避免复制整套验收器。补丁见test_helper_changes.patch。
- 新增docs/condition_nested_else_validation.md：本报告副本。

快照中其余已有文件哈希不变。没有修改锁协议、同步依赖、现有优先级算法或启动/就绪机制，没有开始else if。

## 结构、归属和映射

|区域|入口|出口|互斥组|归属|
|---|---|---|---|---|
|外层|CE:worker#003@14-14|CX:worker#004@29-29|COND:worker#001|顶层|
|内层|CE:worker#007@20-20|CX:worker#008@26-26|COND:worker#002|parent_id=COND:worker#001，parent_branch_id=else|

inner_then=BR:worker#009@21-22，成员为外层else+内层then；inner_else=BR:worker#010@24-25，成员为外层else+内层else。outer_else_before/after仅属于外层else；outer_then仅属于外层then。

12个调度节点、16条边；结构图另有4个控制节点。逐路径投影确认内层汇合留在外层else内部，然后经过outer_else_after到外层汇合。互斥分支间不存在虚假可达依赖，两个图均为无环图。outer=true投影剔除内层入口、出口及分支；inner两个值投影一致。

18个原节点均有结构映射，16个有执行映射，另两个getenv条件调用映射各自入口。完整追溯见original_mapping.csv。原基础图8条依赖逐条检查：create=1、join=1、sem_dep=2、intra=4均保留；新增条件branch/merge各4条。见native_acceptance/dependency_checks.json和原始base_dag.json。legacy原节点名中的while/switch0只是旧导出标签，源码未使用这些语法。

[完整结构图](/home/spcae/Documents/Codex/2026-10-27/new-chat/outputs/condition_nested_else/native_acceptance/structure.svg)；结构/调度JSON、DOT位于native_acceptance/native/intermediate_results/condition_nested_else/pipeline/blocks/level2/conditional_branches/。

## 三条路径及inner不敏感性

|配置|outer|inner|实际条件业务顺序|结果|条件求值次数 outer/inner|每算法priority事件数|
|---|---|---|---|---:|---|---:|
|FT|false|true|outer_else_before → inner_then → outer_else_after|111|1/1|10|
|FF|false|false|outer_else_before → inner_else → outer_else_after|121|1/1|10|
|T0|true|false|outer_then|1000|1/0|8|
|T1|true|true|outer_then|1000|1/0|8|

四种输入全部退出0；T0/T1完整stdout相同。每个实际选中业务段timing count=1，未选段为0；五算法的priority事件集合等于对应实际路径节点集合，未选分支没有事件。CE/CX为结构节点，不伪造运行事件；inner求值次数为0证明未进入原C内层条件。

## 五算法结果

五种普通算法均覆盖全部12个节点，相关段的本次优先级如下：

|算法|outer_else_before|inner_then|inner_else|outer_else_after|outer_then|
|---|---:|---:|---:|---:|---:|
| FIFO | 97 | 95 | 94 | 93 | 96 |
| LPF | 96 | 95 | 90 | 94 | 91 |
| heft | 95 | 93 | 92 | 91 | 94 |
| t_level | 91 | 95 | 94 | 96 | 92 |
| zhao2020 | 96 | 95 | 89 | 94 | 88 |

各段保持独立。调度使用同一节点在已保存各路径中的平均计时，不合并分支、不取分支最坏时间。源码花括号位置由原始行游标逐条验证；删去新增探针/include后源码逐字恢复。原程序、timing、五种生产版和诊断版的stdout逐字一致，同步事件归一化后均为8个成功调用（1 create、1 join、2 sem_init、2 sem_post、2 sem_wait）；不声称线程交错一致。

## 命令与退出码证据

基线及全部回归：

```bash
cd /home/spcae/code2dag-condition
python3 -m unittest discover -s Code2DAG/tests -v
```

分别23/25项通过，日志baseline_tests.stderr、final_tests.stderr。持久验收：

```bash
python3 /home/spcae/Documents/Codex/2026-10-27/new-chat/outputs/condition_nested_else/run_acceptance.py
```

已执行退出0；目录存在时脚本拒绝覆盖，复跑需给native_acceptance传入新目录。native_acceptance/commands.json中88条实际子命令全部退出0，含GCC13重新生成expand和基础图、collect/blocks、四次timing、五算法schedule/instrument、编译及各输入执行。逐条argv/cwd/布尔输入、stdout/stderr均保留。

环境取值按“存在/不存在”：FT取消C2D_OUTER并设置C2D_INNER=1；FF取消两者；T0设置C2D_OUTER=1并取消C2D_INNER；T1两者均设置1。脚本为每次运行构建独立环境，不用字符串0表示false。

## 权限与范围边界

本次180次真实pthread_setschedparam调用均为rc=1/EPERM，查询仍为SCHED_OTHER/0。因此只验证了分配值、插桩和触发路径，**未验证新else用例真实FIFO生效**，不将程序退出0解释为设置成功。未改系统权限，不要求额外sudo测试。

通过范围是单个内层if/else分别位于外层then或else之一、多行带花括号、普通权限执行。仍不声称支持else if、三层及以上、同行分支、两侧同时多个内层、复杂控制流、循环/switch或分支内早退。既有解析器可能识别其他结构，不等同已完成其执行验收。
