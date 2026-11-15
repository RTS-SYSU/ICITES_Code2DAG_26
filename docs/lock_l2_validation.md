# Code2DAG Lock L2 完成证据报告

日期：2026-10-31。结论：**在本报告精确范围内，单组多成员与多 mutex 同时保守收缩闭环通过。Lock 功能扩展到此停止。** 本轮仅扩展原有可选 lock_grouped 规则，没有新增另一套规则或算法。

## 1. 基线与实际修改

修改前执行 `python3 -B -m unittest discover -s Code2DAG/tests -v`：现有 **51 项全部通过、退出 0**，见 [baseline_tests.stderr](baseline_tests.stderr)。本轮保存 [L1 全部证据快照](lock_l1_evidence_snapshot.zip)，115 个文件，ZIP 完整性和 SHA 已校验；对应 .sha256 和 .manifest.json 同目录。L1 原证据全部哈希保持不变；另存 baseline_before_l2.zip 与逐文件 SHA 清单，未覆盖 L1 或 Condition 交接包。

Condition C1–C4 最终 ZIP SHA-256 仍为 `d14d987ac3a97b7fda04c00d0615a82978615bfcd5f6deca0fea2b935b6c1f4c`。L1 变更包 SHA 仍为 `7a70d8527c8b1dfe9d93153b6cca7415b3d9aaf29a4b47187333c7c67c8e3daa`。

唯一修改的原有生产文件为 `Code2DAG/pipeline/lock_model.py`：

| 函数 | 修改 |
|---|---|
| _analyze_lock_source（由原 analyze_lock_l1 的共用检查形成） | 保留原词法/单次调用限制；L2 接受多个具名 mutex、多个 worker、一个 worker 中多个非嵌套临界区；逐函数维护一个活动锁，拒绝嵌套/交叉/不配对 |
| analyze_lock_l1 | 保留原严格单 mutex、双线程/双临界区验证入口及返回格式，L1 测试源码不改 |
| analyze_lock_l2（新增） | 使用同一受限检查，返回所有 mutex/临界区 |
| _canonical_lock_groups（新增） | 在任何收缩或权重计算前检查组/成员 ID、源码元数据和唯一归属；稳定排列仅用于序列化 |
| contract_lock_groups（新增） | 从原始 DAG 一次建立全体成员映射，一次投影所有边；同组内边删除并存来源，跨组边保留投影，去重保留完整来源；检查节点数公式 |
| build_lock_views | 绑定每个临界区与完整原段，调用同时收缩，生成全组映射与结构标注；全图拓扑失败则整个调度视图不兼容 |
| aggregate_lock_timing | 求和前再次检查全体唯一归属及映射一致性；每组单独逐字段求和，保持 L1 字段语义 |

新增文件：tests/test_lock_l2.py（14 项测试及 fixture/build/cross_edges/native_acceptance 辅助）；source_files/lock_l2/single_three.c、two_pairs.c、unequal.c（三份只读分析用例）；docs/lock_l2_validation.md（本报告镜像）。原有规则注册、lock_rule.py、lock_timing.py、默认选择、所有 Condition 模型/测试、Lock L1 测试、五算法、schedule/runner、原有 C 文件均未修改。见 [最小补丁](changes.patch)、[文件/函数清单](change_audit.json)、[不变量核对](unchanged_baseline_checks.json)。

## 2. 精确支持范围

- 同一个直接命名的全局 pthread_mutex_t 对应两个或以上完整临界区，包含三个或更多成员；一个程序可有多个 mutex，每组独立收缩为 LOCKGROUP:mutex_name。
- 每个临界区恰好对应一个完整现有 mutex_cs 执行段；每个成员只属于一个组。一个线程可按源程序顺序访问多个不嵌套的 mutex，但本规则不推断或安排实际锁顺序。
- 所有 worker 通过 main 中各一次直接 pthread_create 启动，在 pthread_create、join 和 mutex API 正常成功且程序正常运行的假设下，静态直线代码每个词法临界区到达一次；所有 create 在 join 之前，每 worker 对应一次 join。每个被创建的 worker 都需有受支持的临界区，额外无锁 worker 未纳入本轮源码范围。本轮不以运行轨迹证明实际次数或测量顺序。
- 继承 L1 词法边界：每个 mutex 单独一行直接以 PTHREAD_MUTEX_INITIALIZER 静态初始化；lock/unlock 是独立行的直接 &name 调用；非空多行区域；只接受已知标准头文件 include、同文件可见 helper 和已知 puts/printf，拒绝不明间接/外部调用、任意宏/typedef、嵌套词法块及控制流。末尾普通 return NULL/0 可用。
- 每个 mutex 至少两个成员；单成员 mutex、动态初始化/属性或上述范围外的布局未支持，不能默默漏掉。声明但未满足组要求的 mutex 明确拒绝。

不支持：嵌套/交叉锁、同段多锁归属、条件/循环中的锁、别名/包装/指针分析、trylock、递归/读写/属性锁、信号量、早期控制转移、跨多个原段的临界区；不增加 PI、优先级冻结、死锁处理、运行时插桩或性能实验。不开始 Loop。

## 3. 同时收缩方法与顺序无关性

先验证所有成员，建立唯一全局映射 f(original_segment)=LOCKGROUP 或自身，再仅遍历**原始 DAG**中的每条边 e=(u,v,kind)：

1. u/v 同属一个组时，作为组内边删除并保存 removed_internal_edges 来源。
2. 否则生成 (f(u),f(v),kind)；这同时处理普通入/出边与不同组之间的原始边。
3. 按 (src,dst,kind) 去重，不同类型不合并。provenance 包含所有原始边索引和完整原 edge 字典，不遗失来源。
4. 未参与节点直接保留；未参与边的原 src/dst/kind/其他属性保留，只增来源审计信息。
5. 所有组映射完成后执行全图拓扑检查；不是逐组修改工作图。

节点数在实现和每个验收用例中均断言：

`grouped_nodes = original_nodes - Σ(member_count - 1)`。

[order_invariance.json](order_invariance.json) 对相同源码/基础图运行两种 mutex 输入顺序 × 两种成员顺序，共 4 种组合：**完整五份模型输出、映射、权重数据完全相同**，并保存一致的规范 JSON SHA。测试还颠倒组列表与各组成员列表，验证 contract_lock_groups 的结果相同。

另在内存交换 mutex 声明顺序，节点、边、映射及权重完全相同；源码哈希当然随文本变化，不要求其相同。再重命名 mutex 使字典序反转，按名称对应关系还原 ID 后，图和原始节点映射相同。名称仅用于 ID 和稳定展示，不能选择某组先执行；五算法本身可能按 ID 处理同分值，此处不扩大为算法重命名不变性声明。

## 4. 单组多成员及多组原生结果

用三份本轮新增 C，经 Ubuntu GCC13 从源码新生成 RTL expand 和基础元数据。没有使用旧 Condition/dag5 DAG。L2 原生流程仅编译对象、生成元数据、blocks、导入已有 schema 的合成 timing 和 schedule；没有执行 L2 C 程序。

| 原生用例 | 各组成员数 | 原节点数 | 分组节点数 | 边数前→后 | 子命令/退出 |
|---|---|---:|---:|---|---|
| single_three | [3] | 15 | 13 | 17 → 17 | 11 × 0 |
| two_pairs | [2, 2] | 14 | 12 | 15 → 15 | 11 × 0 |
| unequal | [3, 2] | 19 | 16 | 21 → 21 | 11 × 0 |

single_three 为三个 worker 各一次 mutex_a。two_pairs 为两个 worker 各按原 C 顺序访问 mutex_a 后 mutex_b，两个区域之间保留普通执行段。unequal 为前两 worker 各 A/B、第三 worker 仅 A，因此 A=3、B=2。每个组保留成员函数、锁/解锁行、计算行、完整原段范围；结构图保留所有成员，分组图只替换这些成员。

完整原生数据在 `native_acceptance/<用例>/native/intermediate_results/<用例>/pipeline/blocks/level2/lock_grouped/`；segments_struct.json、dag_struct.json、segments_sched.json、dag_sched.json、兼容别名和 DOT 均保留。analysis_only/timing_compatible=false/instrumentation_compatible=false 保护沿用 L1，汇总包络行号不作为可执行 C 范围。

[model_examples.json](model_examples.json) 另外保留小型确定性结构/映射/权重用例：成员数 3 的图 5→3 节点，2+2 为 6→4，3+2 为 7→4，方便逐项复核。

## 5. 组间依赖、去重和全图环拒绝

[intergroup_edges.json](intergroup_edges.json) 保存原图及分组图：三条来自不同成员的 A→B 边，其中两条 kind=intra、另一条 sem_dep。两条 intra 去重为一条 A组→B组 intra，仍保存两个不同 callsite；sem_dep 独立保留。因此组间结果是两条类型边、三个完整原始来源。全图合法，五算法均在测试中接受该图。

这个 sem_dep 是检验既有 DAG 边类型保留的分析输入，**不增加 sem 源码支持**。外部 create/join、非成员 P→Q 的原属性、全部来源索引也逐项核对。原图 14 边→分组 7 条唯一边；所有来源可追溯，组内删除只涉及同 mutex 成员。

[cycle_rejection.json](cycle_rejection.json) 保存跨组环反例：原图 A1→B1 和 B2→A2，无环；同时收缩后得到 A组→B组→A组。两条边及完整来源均保留，整个模型 scheduler_compatible=false 并记录原因；FIFO、LPF、heft、t_level、zhao2020 的 schedule 入口均明确失败，没有生成 schedule.json，权重发布也拒绝不兼容模型。没有删依赖、合并 mutex 组或猜测锁顺序。

## 6. 权重独立求和

本轮只使用确定性**合成 timing JSON**验证运算，不测量运行时间。每个组分别按全部成员求和，不混入其他组；不加等待时间、不取 max 代替和、不作 WCET 声明。

| 原生用例 | 组 | total_ns | count | avg_ns | min_ns | max_ns |
|---|---|---:|---:|---:|---:|---:|
| single_three | LOCKGROUP:mutex_a | 660 | 6 | 330 | 264 | 396 |
| two_pairs | LOCKGROUP:mutex_a | 340 | 4 | 170 | 136 | 204 |
| two_pairs | LOCKGROUP:mutex_b | 420 | 4 | 210 | 168 | 252 |
| unequal | LOCKGROUP:mutex_a | 780 | 6 | 390 | 312 | 468 |
| unequal | LOCKGROUP:mutex_b | 500 | 4 | 250 | 200 | 300 |

[weight_calculation.json](weight_calculation.json) 保存每个原成员字段及各组结果，可以逐项重算。沿用 L1：count 是贡献的真实成员样本数量之和，不是虚拟组的实测执行次数；avg_ns 是成员均值之和，不重新以汇总 total_ns/count 计算。min/max 是成员样本极值之和，不是实测组极值或经过证明的 WCET。

测试修改 B 组一个成员的所有字段后，B 权重改变、A 权重完全不变。对 5 个成员逐个删除整份 timing 或五个字段之一，共 30 个缺失反例均明确拒绝；不静默使用 0。L1 对非法数值/零样本等检查仍通过。权重入口再次验证所有组唯一归属和原段→组映射一致性，防止篡改模型造成重复求和。未参与分组的原权重不变。

## 7. 映射与唯一归属

[mapping_evidence.json](mapping_evidence.json) 保存每个原生用例的全部映射：

- original_node_to_grouped_node：原始调用/函数节点→对应组或未改节点。
- original_segment_to_grouped_node：原执行段→唯一组或自身。
- grouped_node_to_member_nodes / grouped_node_to_member_segments：每组→全部原成员。
- grouped_node_to_mutex_and_source：每组→mutex 名称、所有成员函数、lock/unlock、临界计算和原段范围。
- original_node_to_execution_segment：原执行段追溯仍保留；不相关 mappings 元数据不变。

组成员中的 member_node_id 与 member_segment_id 在当前 DAG schema 中均为现有执行段 ID，但原始调用节点仍逐一映射。一个成员只能被分配一次：跨组重复、组内重复、node/segment 列表不一致、成员来源缺失或映射矛盾都拒绝，不能静默去重或遗漏临界区。明确的重复归属和各成员缺权重错误见 [rejection_cases.json](rejection_cases.json)。

## 8. 五算法结果

所有结果仅属于分析图；**不写回真实线程，不生成或运行 L2 插桩程序**。

| 用例 | 算法 | mutex_a 组优先级 | mutex_b 组优先级 | 全图优先级数 |
|---|---|---:|---:|---:|
| single_three | FIFO | 99 | — | 13 |
| single_three | LPF | 95 | — | 13 |
| single_three | heft | 93 | — | 13 |
| single_three | t_level | 93 | — | 13 |
| single_three | zhao2020 | 95 | — | 13 |
| two_pairs | FIFO | 98 | 99 | 12 |
| two_pairs | LPF | 96 | 94 | 12 |
| two_pairs | heft | 95 | 92 | 12 |
| two_pairs | t_level | 92 | 95 | 12 |
| two_pairs | zhao2020 | 96 | 94 | 12 |
| unequal | FIFO | 99 | 98 | 16 |
| unequal | LPF | 95 | 93 | 16 |
| unequal | heft | 93 | 90 | 16 |
| unequal | t_level | 90 | 94 | 16 |
| unequal | zhao2020 | 95 | 93 | 16 |

每种算法输出覆盖相应分组图全部节点，原成员已不在调度节点/优先级表中。原生多组例保留 A组→普通段→B组 的合法源程序因果路径；确定性图另覆盖直接组间边。全部 15 次原生 schedule 子命令退出 0。五算法文件哈希与本轮前一致，未改算法来适配分组。

## 9. 执行命令、退出码与全部回归

工作目录 `/home/spcae/code2dag-condition`：

| 已执行命令 | 结果 |
|---|---|
| python3 -B -m unittest discover -s Code2DAG/tests -v（修改前） | 51 项通过、退出 0；baseline_tests.stderr |
| python3 -B -m unittest Code2DAG.tests.test_lock_l1 -v（扩展后检查） | 17 项通过、退出 0；l1_after_extension.stderr |
| python3 -B -m unittest Code2DAG.tests.test_lock_l2 -v（新增初验） | 14 项通过、退出 0；initial_tests.stderr |
| python3 -B -m unittest discover -s Code2DAG/tests -v（最终） | **65 项全部通过、退出 0，无跳过，13.060 s**；final_tests.stderr |

最终构成 Condition 34 + Lock L1 17 + Lock L2 14。新增测试覆盖全部 12 类要求：3 成员、2+2、3+2、合法跨组边、去重来源、输入/成员顺序、重复归属、跨组环、逐成员缺权重、默认/L1/Condition 兼容、五算法、原 C SHA。

持久验收命令，cwd 为当前交付工作目录：

```bash
python3 -B /home/spcae/Documents/Codex/2026-10-27/new-chat/outputs/lock_l2/run_acceptance.py
```

退出 0。三用例各 11 条子命令，总 33 条全部退出 0，逐条 argv/cwd/returncode 保存在 native_acceptance/<用例>/commands.json，原 stdout/stderr 在 logs。包括新 GCC13 expand、generation threads/full、collect、lock_grouped blocks、lock_timing 导入及五算法 schedule。目录已存在时拒绝覆盖，复跑应给脚本一个新目录。

除原有 Condition 回归按原行为执行其既有程序外，L1/L2 原生验收均只进行分析和调度；没有新增 C 运行、插桩或性能实验。不得把全回归所含既有 Condition 行为表述成 L2 运行时验证。

## 10. C 文件 SHA-256 与不可变证据

| 原生输入 | 分析前 SHA-256 | 分析后 |
|---|---|---|
| single_three.c | `8c25c738c4ad87852a6629cb04c744b36302e8518742ac24412dd88bfacd99da` | 相同 |
| two_pairs.c | `c4c1ea3bc00b757b60f6f6c28e98096679693b98936485e6de45b113b362abbe` | 相同 |
| unequal.c | `ab95c176e6e58a8870853669311001410b7a23d0719aa568aa8d03e3ad5db1b1` | 相同 |

全部原有 C 文件与本轮基线哈希相同，三份新输入各自前后相同；generation/collector 仅可保存未修改的原源码配置副本。没有修改原线程、mutex 调用、临界区或次数。没有运行 L2 C 证明实际锁顺序；这里只核验源码不变和静态直线/单次范围。

L1 证据 ZIP 和本轮基线 ZIP 的哈希文件已保存，原 L1 的 115 份证据也再次逐个核对，完全未变。Condition 最终 ZIP 未改变。L2 变更包 lock_l2_changes.zip 保存唯一修改文件和新增用例/测试/文档，恢复时应在 Condition 最终快照 + L1 变更包的基础上叠加；不覆盖旧快照。

## 11. 边界与停止

完成的是受限静态词法输入、多成员/多组、同时保守商图与现有算法适配。输入范围之外一律不声明完成；单成员 mutex、任意 C 控制结构、动态初始化、锁别名/包装、嵌套/交叉、同段多锁、递归/读写/trylock、PI、冻结、死锁处理仍不支持。

收缩可能给原本无环的程序图引入保守环；明确拒绝调度不等于证明程序死锁。该汇总节点是分析抽象，不代表运行时合并执行、节点优先级真实生效、最优调度、WCET、性能提升或任意线程交错等价。

本轮没有改 Condition、五算法或运行时机制，没有做锁顺序推断、插桩/性能实验；不自动开始 Loop、论文写作或其他锁功能。**L2 报告交付后停止全部代码扩展。**
