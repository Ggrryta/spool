# 全量审阅路线图（review guide）

> 用途：对 spool 全库做一次系统性人工审阅。按依赖顺序从地基往上审，
> 每站先读契约、再看实现、然后对照作证材料（测试/规约/基准），
> 带着"已知薄弱点"清单重点攻击。
>
> 产出约定：每站记录 findings，按 **严重 / 建议 / 存疑** 三级标注，
> 汇总到 `docs/review-findings.md`（审阅完成后创建）。

## 总时间预算

约 7~8 小时，建议分 2~3 个会话。每站末尾的"攻击问题"是本站的
核心思考题——审阅的价值在于回答它们，而不是逐行读完。

---

## Stage 0 · 定位与文档（30 分钟）

**读**：`README.md` → `docs/roadmap.md` → `docs/CONTRACTS.md`

**审**：
- [ ] README 的性能声称是否都能在 docs/benchmarks/ 找到出处
- [ ] CONTRACTS.md 六大类契约有无歧义、有无遗漏的实现未覆盖
- [ ] roadmap 的"不做清单"与当前代码是否仍然自洽（mpsc 实验性
      标注、bounded 的诚实声明）

---

## Stage 1 · unbounded 地基（1.5 小时）

**读**：`unbounded/unbounded.go`（手动版核心，~110 行）→ `chan.go`（泵）→
`doc.go`

**作证材料**：`fuzz_test.go`（影子模型对账）、`contract_test.go`、
`docs/proofs/UnboundedDrain.tla`（TLC 已验证）

**攻击问题**：
1. `backlog = backlog[1:]` 的 re-slice：底层数组在高水位后会驻留
   多久？长期运行的生产-消费平衡场景下，内存是否只涨不跌？
   （gRPC 原版同样如此——确认这是权衡而非缺陷，评估是否值得
   用环形缓冲改进）
2. 手动 Load 协议漏调一次的后果是什么？（停止推进——文档是否
   把这个后果说透了）
3. Close 与 Put 的竞态窗口：契约允许"生效前成功"，测试是否
   精确钉住了"生效后必失败"
4. 影子模型（FuzzUnbounded）与实现是否可能**一起漂移**——模型
   的每个分支能否在实现中指认对应代码行

---

## Stage 2 · mpsc（1.5 小时，三份实现三种策略）

**读**：`mpsc/queue.go`（无锁 Michael-Scott）→ `chan.go`（泵+唤醒协议）→
`sharded.go`（分片+批量窃取）

**作证材料**：`FuzzQueue`/`FuzzShardedChan`、`docs/proofs/ShardedWake.tla`、
`docs/benchmarks/v0.2-lockfree.md`（负结果）与 `v0.2.1-sharded.md`

**攻击问题**：
1. `Push` 的帮助推进（tail 落后时 CAS 推进）是否存在活锁场景
2. Go GC 免疫 ABA 的论证是否严密——"局部变量是 GC 根"在
   `tail := q.tail.Load()` 之后 CAS 之前的窗口内是否始终成立
3. 泵唤醒协议的时序穷举（chan.go 注释）与 TLA+ 规约是否逐条对应
4. ShardedChan 泵按分片 0→N 轮询：高压下低分片号是否会被
   优先服务（公平性未观测，roadmap 挂账项）
5. 实验性标注（Queue/Chan）与转正标注（ShardedChan）是否与
   基准数据一致

---

## Stage 3 · bounded（1 小时，刚修完真 bug 的一站）

**读**：`bounded/bounded.go`（环形索引 + closedCh 广播 + 批量窃取泵）

**作证材料**：`bounded_test.go`（含 `TestCloseWakesAllBlockedPuts`
广播回归测试）、`internal/contract` 接入

**攻击问题**：
1. 环形索引回绕：(head+count)%cap 在 int 溢出边界的正确性
2. closedCh 广播：是否覆盖所有等待者类型（阻塞 Put ✓ / 泵？
   泵睡在 data 上，Close 走 signal(data)——容量 1 够吗）
3. **容量+1 语义**：泵手中始终持有至多 1 个在途值，有效缓冲 =
   cap+1——这一点**文档未写**（修测试时发现），确认后补进
   CONTRACTS.md
4. 批量窃取的 batchLimit=64 是否合理，有无基准支撑
5. v0.5 刚修的广播缺失 bug：修复是否完整（对照回归测试）

---

## Stage 4 · serializer / pubsub（1 小时）

**读**：`serializer/serializer.go` → `pubsub/pubsub.go`

**作证材料**：`serializer_test.go`（panic 三分支、取消排空）、
`pubsub_test.go`（慢订阅者不阻塞发布者）

**攻击问题**：
1. fail-fast 默认策略会让进程崩溃——文档是否把后果说透，
   WithPanicHandler 是否覆盖了全部用户期待
2. pubsub 共享单一 serializer：慢订阅者拖慢所有人（gRPC 同款
   取舍）——是否有基准或测试证明"拖慢"而非"饿死"
3. Unsubscribe 后已调度的回调仍会执行——文档说了，测试呢
4. 发布顺序 = 订阅注册顺序（注释声称）——实现用 map 遍历无排序！
   **疑似文档与实现不符**（重点核实）

---

## Stage 5 · singleflight（45 分钟）

**读**：`singleflight/singleflight.go`，对照
`golang.org/x/sync/singleflight` 原版逐语义比对

**攻击问题**：
1. panic 路径：err 定稿 → wg.Done → 删 key → re-panic 的顺序
   （race 测试抓过的 bug）是否还有遗漏路径
2. Forget 与进行中调用的竞态：Forget 后旧调用完成时会误删
   新 key 吗（`g.m[key] == c` 守卫的充分性）
3. DoChan 未实现——x/sync 有的能力清单是否在文档里声明

---

## Stage 6 · backoff（45 分钟）

**读**：`backoff/backoff.go`

**攻击问题**：
1. `Backoff(retries)` 的乘法循环在大 retries 下的行为
   （backoff < max 才乘，有界——确认无溢出路径）
2. Runner 的 timer 复用：Reset 前的排空假设是否在所有路径成立
3. Permanent 的错误链：`permanentError.Unwrap` 只返回用户错误，
   调用方的自定义哨兵 `errors.Is(err, mySentinel)` 还能用吗
   （**疑似缺陷**：未透传用户错误链，重点核实）
4. 与 cenkalti/backoff 的对比声明（"不重复造全覆盖策略"）是否
   在文档中成立

---

## Stage 7 · 横切面（1 小时）

**读**：`internal/contract/contract.go`（一致性套件自身）→
`.github/workflows/ci.yml` + `nightly.yml` → `bench/` 各文件

**攻击问题**：
1. 契约套件的阴性对照只有"丢值"一种——顺序破坏、提前关闭、
   Done 不关的坏实现能被抓到吗（可各写一个做对照扩展）
2. 契约套件对 bounded 的适配（Put 带 ctx）是否弱化了断言
3. CI 的 TLC job：规约-实现一致性是人工评审（proofs/README），
   评审记录是否足以让第三方复核
4. bench 模块的方法学：单次运行 vs -count=10+benchstat（已知
   欠账），对比口径是否公平（chanx 初始化参数等）
5. 全部文档中的数字声称是否都能溯源到 runs

---

## 审阅完成标准

- [ ] 7 个 Stage 全部走完，findings 三级归类
- [ ] 每个"疑似缺陷"都有复现路径或明确的"非缺陷"论证
- [ ] 高水位内存、容量+1、发布顺序、Permanent 链四个疑点有结论
- [ ] findings 汇总入 docs/review-findings.md，严重项开修复单
- [ ] 修复后跑全量防线（race + fuzz 种子 + 契约套件 + TLC）
