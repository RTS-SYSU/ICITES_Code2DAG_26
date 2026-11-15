# Condition 真实程序验收记录

## 验收对象

- 基线：仓库现有 `source_files/dag5/dag5.c` 多线程 DAG 工作负载。
- 回归源码：`source_files/dag5_condition_regression/dag5_condition_regression.c`。
- 保留结构：17 个工作线程、19 个互斥锁、8 个信号量，以及原有 create/join/sem 依赖。
- 唯一功能性变化：在 `a_gate` 的真实计算段加入由 `CODE2DAG_CONDITION` 控制的
  多行 `if/else`，两个分支执行不同的计算调用组合。
- 为缩短回归时间，将测试变体的 `WORK_SCALE` 从 100 调整为 1；原始 `dag5` 不修改。

## 条件图结果

- 条件区域：1。
- 可调度执行节点：22。
- 调度边：47。
- 条件边：4（两条 `condition_branch`、两条 `condition_merge`）。
- 原依赖类型保留：`create`、`join`、`sem_dep`、`intra`。
- then 节点：`BR:a_gate#004@188-188`。
- else 节点：`BR:a_gate#005@190-191`。
- 两个节点属于同一个 `exclusive_group`，且分别只有 then/else 成员信息。

## 运行验证

使用正式 pipeline 计时阶段分别运行两条路径：

- 设置 `CODE2DAG_CONDITION=1`：then 节点 `count=1`，else 节点 `count=0`。
- 不设置 `CODE2DAG_CONDITION`：then 节点 `count=0`，else 节点 `count=1`。
- 两次运行均正常退出，其余真实线程节点均产生计时记录。

保存的证据：

- `intermediate_results/dag5_condition_regression/pipeline/validation/condition_branches/timing_then.json`
- `intermediate_results/dag5_condition_regression/pipeline/validation/condition_branches/timing_else.json`

## 调度和插桩验证

- `FIFO`、`LPF`、`heft`、`t_level`、`zhao2020` 均成功为全部 22 个节点生成优先级。
- then/else 两个节点均获得独立优先级。
- 五种算法的 generic 插桩阶段均成功且无警告。
- `zhao2020` 插桩源码使用 MinGW GCC 成功编译。
- 编译后的插桩程序分别运行 then/else 两条路径，退出码均为 0。
- 源码检查确认两个分支的优先级设置分别位于对应花括号内部。

## 环境修复和限制

- `segtrace.c` 增加了 Windows 高精度计时和线程 ID 实现，Linux 路径保持不变。
- Windows 链接时不再添加 Linux 专用的 `-ldl`。
- 未安装 Graphviz 时发布 DOT 文件，不再阻断插桩结果发布。
- 当前 Windows GCC 的 RTL 展开结果会丢失源码调用行号。此次 Condition 验收复用了仓库
  已提供的 `dag5` DAG 元数据，并保持回归源码总行数及后续函数行号不变；这项生成器的
  Windows 兼容问题不属于本次 Condition 功能，尚未修复。
