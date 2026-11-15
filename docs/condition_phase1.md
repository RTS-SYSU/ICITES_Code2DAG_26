# Condition 第一阶段

`conditional_branches` 是一个可选的 Level-2 建模规则。它建立两套有明确用途的输出：

- `segments_struct.json` / `dag_struct.json`：条件 DAG 结构视图。每个 `if` 都有成对的
  `condition_entry` 和 `condition_exit` 节点；入口的 `condition_branch` 边共享
  `exclusive_group`，表示一次执行只选择一个分支。
- `segments_sched.json` / `dag_sched.json`：现有计时和普通 DAG 调度算法使用的兼容视图。
  then/else 中的可执行段分别作为节点参与优先级分配；入口/出口只保留在结构视图，
  调度视图通过带 `exclusive_group` 的条件边和节点上的 `condition_memberships` 保留互斥信息。
  运行时仍由原 C 条件表达式选择分支，调度器不替程序选择分支。
- `segments.json` / `dag_seg.json`：兼容旧流程的别名，内容等同于调度视图。

运行示例：

```bash
python -m Code2DAG.pipeline.cli --base-dir Code2DAG blocks \
  --base-name <case> \
  --source Code2DAG/source_files/<case>/<case>.c \
  --level level2 \
  --rule conditional_branches
```

`segments_struct.json` 和 `segments_sched.json` 都包含 `mappings`：

- `original_node_to_execution_segment`
- `original_node_to_structural_node`
- `original_segment_to_execution_segments`
- `structural_node_to_execution_segment`

## 已完成的第一项闭环

单层、带花括号的 `if/else` 已完成端到端验证：then/else 分别生成执行节点，
分别获得现有算法分配的优先级，计时和优先级插桩只在运行时实际选中的分支触发，
插桩前后的程序输出保持一致。该闭环测试会实际编译并分别运行 true/false 两种输入。

仓库 `dag5` 多线程工作负载的真实回归变体也已通过：保留原有 17 个工作线程、
19 个互斥锁、8 个信号量和 create/join/sem 依赖，在 `a_gate` 的真实计算段加入
双分支。详细记录见 `docs/condition_real_program_validation.md`。

在继续扩展其他条件语法前，这一行为作为回归基线保留。

## 当前边界

本阶段只完成条件结构识别和现有流程适配，不实现条件 DAG 的响应时间分析或新的
优先级算法，也不计算两个分支合并后的最坏执行时间。分支节点直接使用现有调度算法
进行优先级分配。若分支内包含 `return`、`break`、`continue`、`goto`，输出仍保留结构视图，但
`capabilities.scheduler_compatible` 为 `false`；计时和调度阶段会明确拒绝继续。

条件分支本身可以使用单条语句；若分支直接以 `for`、`while`、`switch` 或 `do`
开头，第一阶段要求给该分支加花括号，以避免在不完整的 C 语法分析下误判语句边界。
then/else 的可执行语句还必须位于不同源码行；若两个分支共享同一行，结构视图仍会
生成，但计时和调度会明确拒绝继续，避免按行插桩把两个优先级同时放到条件语句之前。

这项限制用于避免插桩跨越已经离开当前控制流的语句。条件分支的互斥选择始终沿用
原始 C 程序语义。
