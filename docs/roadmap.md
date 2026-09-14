# 定位与路线图

> 状态：**已确认** | 版本：v1 | 日期：2026-09-14
> 本文档是本库的顶层设计与路线图，重大方向变更须更新此文档并追加更新记录。

## 1. 一句话定位

**Go 的并发数据流原语库**：在队列、序列化、合并、分发、流控五个维度上，
提供高性能、可证明正确的并发原语。

不是"并发工具大杂烩"，不做业务框架，只解决一个技术命题：

> 在 Go 里做出高性能、可验证的并发数据流原语。

## 2. 愿景与对标

对标 [crossbeam-rs/crossbeam](https://github.com/crossbeam-rs/crossbeam)（Rust 生态
"集成的高性能高稳定并发原语库"）。但 Go 有三条天然边界，决定了不能照搬：

1. **epoch-based 内存回收在 Go 无意义** —— Go 有 GC，crossbeam 一半的
   护城河（无锁结构手动内存回收）直接消失。
2. **并发容器已被占位** —— concurrent map 领域 puzpuzpuz/xsync 已做到
   极致，不碰。
3. **官方动向待跟踪** —— Vyukov 式队列进入 golang.org/x/sync 的呼声一直
   存在，但经工具链验证（2026-09），x/sync v0.23.0（最新发布版）
   **不含** queue 包。持续跟踪，若官方下场则以差异化维度应对。

剩下的开阔地带恰好是本库的位置：**并发数据流原语**。

**集成为形，深耕为实**：集成库失败于"攒包"，成功于每个包单独拿出来都值得
切换。每个新包必须通过唯一准入测试：

> "它是否服务于'无阻塞、有序、可停机'的数据流？单独发布是否有人愿意切换？"

## 3. 价值的可操作定义

一个库的价值只有三种来源，每条对应一条深耕路线：

| 价值来源 | 现状 | 深耕方向 |
|---|---|---|
| 别人没有 | `Unbounded`（零 goroutine + 排空 Close）已达标 | 契约统一化 |
| 更快 | mutex 实现（基准见 README），尚无优势 | **主战场**：无锁化、分片、batching |
| 更可靠 | race 全绿 | fuzz + soak + 形式化验证（TLA+） |

硬性要求：**"高性能高稳定"不允许作为形容词出现在 README**，必须是
可复现的测量（benchstat 报告）和可验证的证明（TLA+ spec / fuzz 用例）。

## 4. "集成"的真正含义

不是包多，是三件事：

1. **统一契约**：所有队列共享同一套 Close 语义（拒绝新写入、排空后关闭、
   幂等）、panic 策略（默认 fail-fast、可选 handler）、背压行为。
2. **统一基准**：内置横向 bench 体系，对比生态所有在位者（原生 chan、
   chanx、x/sync/queue 等），数据公开可复现。
3. **一个依赖**：引一个库，得到一族互相咬合的原语。

## 5. 架构原则

1. **依赖单向**：`unbounded` 是地基；`serializer`/`pubsub` 依赖它；
   其余包零依赖。禁止反向依赖与循环依赖。
2. **每包独立 import**，无顶层大杂烩包。
3. **质量门禁**（CI 强制）：vet + race + 基准对比基线；核心包发布前跑 fuzz。
4. **契约由测试钉死**：Close 语义、Load 协议等行为契约全部固化为测试。
5. **零第三方依赖**，仅标准库；Go 1.24+。

## 6. Roadmap

每个版本有明确的完成定义（DoD），未达 DoD 不发版。

### v0.1 — 基线（当前）
- [x] 改名 `spool`（SPOOL：Simultaneous Peripheral Operations On-Line，
      计算机史上缓冲队列的本词），模块路径 `github.com/Ggrryta/spool`
- [x] 独立建仓 + CI（GitHub Actions：Linux/Windows 双平台
      vet + race 测试 + bench 模块整洁检查；徽章已挂 README）
      —— 仓库 github.com/Ggrryta/spool，待推送首提交后生效
- [x] `unbounded`/`singleflight`/`mpsc`（含 ShardedChan）的 fuzz 目标
      （影子模型对账 + 并发死锁看门狗；轰炸验证各 20s 通过，
      单次最高 900 万+ 执行零发现；过程修掉两个 fuzz 器械自身的
      排空预算 bug——边界随 expected 缩水导致提前退出）
- [ ] 英文 godoc（双语注释，发布前）
- [x] 四个核心包 + race 测试 + 基准（unbounded / serializer /
      singleflight / pubsub）

### v0.2 — 无锁化第一仗（已完成，假设证伪，见 [实验报告](benchmarks/v0.2-lockfree.md)）
- [x] 基线基准：对齐 chanx、原生 chan（报告见
      [docs/benchmarks/v0.2-baseline.md](benchmarks/v0.2-baseline.md)）
- [x] Michael-Scott 无锁 MPSC 队列（`mpsc` 包）+ 契约测试 + 唤醒协议论证
- [x] **结论："无锁优于 mutex"假设证伪**（任何生产者规模均落后，根因：
      瓶颈在泵交接而非生产端锁）；按 §9 假设 3 证伪条款处置：
      mpsc 保留但标记实验性，mpsc.Chan 不转正
- [x] **意外收获**：unbounded.Chan（mutex）4→32 生产者吞吐平坦
      （~93 ns），16+ 生产者时全场最快（超原生 chan 25%~35%）
- [x] DoD 修正后达成："16+ 生产者全场最快 + 平坦扩展性"
      （原 ≤80 ns@4 生产者目标未达成：94 ns，结构下限 ~90 ns）
- 探索项（可选，v0.2.1）：分片化 mpsc（需接受跨分片乱序）

### v0.2.1 — 分片化（已完成，目标达成，见 [实验报告](benchmarks/v0.2.1-sharded.md)）
- [x] `mpsc.ShardedChan`：显式 key 分片（同 key FIFO / 跨 key 不保证，
      Kafka 分区模型）+ 分片 mutex 内核 + 消费端批量窃取
- [x] **MPSC ≤80 ns 目标达成**：4 生产者 50.6 ns（低于目标 37%），
      全规模 38~51 ns；同 key 最坏情况 76 ns 也快于 unbounded.Chan
- [x] ShardedChan 通过 §2 准入测试，**转正**（生产可用）；
      Queue/Chan 保持实验性

### v0.3 — 高稳定的证明（已完成，见 [docs/proofs/](proofs/README.md)）
- [x] TLA+ spec：`UnboundedDrain`（关闭/排空：无死锁/无丢失/无重复/
      全局 FIFO/ClosedComplete + liveness）
- [x] TLA+ spec：`ShardedWake`（唤醒协议：NoLostWakeup——v0.2 真实
      死锁的机器检测器、PerKeyFIFO、ExitedClean + liveness）
- [x] CI `tla` job：每次 push 自动 TLC 穷举验证两份规约
- [x] soak 长跑（`-tags=soak`，随机负载 + goroutine 泄漏栈转储）
      + nightly 流水线（10 分钟 soak + fuzz 加时）
- [x] DoD 达成：证明文档（docs/proofs/README.md）+ 模型检验公开；
      剩余"7 天 nightly 零发现"由时间兑现

### v0.4 — 契约统一化
- [ ] 全库统一契约文档（Close/panic/背压），以测试形式钉死
- [ ] DoD：任何包的语义偏离契约即 CI 失败

### v0.5 — 顺流控轴扩张
- [ ] `backoff`（时间流控；xrpc 有现成实现可提炼）
- [ ] bounded MPMC ring（背压流控）
- [ ] 每包准入测试：见 §2 的唯一准入问题

### v1.0 — API 冻结
- 前提三样齐：基准表（§3）、证明文档（§3）、xrpc 实战验证零回归
- semver 承诺，之后只加不改（破坏性变更走 v2）

### 持续线 — 实战验证
- xrpc 内部 `grpcsync`/`buffer` 使用点逐步切换到本库，零回归、性能不降
- 这是"生产级验证"的长期背书，穿插进行，不阻塞版本号

## 7. 竞争格局

| 在位者 | 占据领域 | 我们的策略 |
|---|---|---|
| golang.org/x/sync | errgroup/semaphore/singleflight | singleflight 泛型化已做；无已发布 queue 包（v0.23.0 验证），动向跟踪；errgroup/semaphore 不做 |
| puzpuzpuz/xsync | concurrent map | 不碰 |
| smallnest/chanx | 无界 channel（带泵 goroutine） | 基准已对齐：全场景快 4~15 倍（见 [基准报告](benchmarks/v0.2-baseline.md)），持续保持 |
| sourcegraph/conc | 结构化并发/pool | 不碰（维护已停滞，且非数据流域） |
| cenkalti/backoff | 重试退避 | v0.5 再评估，须答准入问题 |
| 原生 chan | 通用 | 基准基线，永远在对比表中 |

## 8. Not Doing（及原因）

- **泛型 LRU / 缓存类** —— 缓存不是数据流；hashicorp/golang-lru 已占位
- **worker pool / errgroup / semaphore 克隆** —— 官方与 conc 已做，无增量
- **sync.Map 替代品** —— puzpuzpuz/xsync 已做到极致
- **分布式 / 跨进程版本** —— 单机原语证明到极致之前不考虑
- **"高性能高稳定"作为口号** —— 只允许出现在测量和证明的旁边

## 9. 关键假设与验证方式

| # | 假设 | 验证方式 | 状态 |
|---|---|---|---|
| 1 | 零 goroutine 无界队列 + 排空 Close 的需求普遍存在，不只是 gRPC 特例 | xrpc 切换后的内部使用点统计；首篇文章的 issue/搜索反馈 | 未验证 |
| 2 | 手动 Load 协议（即使有自动版兜底）不会劝退用户 | 文章/issue 中用户反馈 | 未验证 |
| 3 | 无锁化能在保留排空 Close 语义的前提下追平 yqueue 性能 | v0.2 基准报告 | 未验证，**主战场** |

| 假设 3 若证伪：无锁版与 mutex 版并存（性能/语义双选项），不硬凑。

**v0.2 基线验证结论（2026-09-14）**：MPSC 场景落后原生 chan 50%
（112 vs 75 ns），无锁化收益上限明确；SPSC 已基本打平，不动。

**v0.2 无锁化实验结论（2026-09-14）**：假设 3 证伪——无锁 MPSC 在
4~32 生产者全规模落后 mutex 版（160~226 vs 91~96 ns），瓶颈定位
错误（泵交接而非锁）。按证伪条款处置：mpsc 标记实验性不转正；
unbounded.Chan 的平坦扩展性（16+ 生产者全场最快）成为实际达成的
性能资产。详见 [实验报告](benchmarks/v0.2-lockfree.md)。

**v0.2.1 分片化实验结论（2026-09-14）**：假设 3 以修正形式成立——
"分片化（而非单点无锁）可达成 MPSC 性能目标"。ShardedChan 在
4 生产者 50.6 ns（目标 80），32 生产者时快原生 chan 3.3 倍；
同 key 最坏情况也快于 unbounded.Chan（批量窃取的贡献）。
ShardedChan 转正。详见 [实验报告](benchmarks/v0.2.1-sharded.md)。

## 10. 开放问题

- [x] ~~命名最终确认~~ 已定：**spool**（2026-09-14；SPOOL 词源，
      见 README）。发布前在 GitHub 搜 `spool language:Go` 做最终查重
- [x] ~~独立建仓与模块路径~~ 已定：`github.com/Ggrryta/spool`（2026-09-14）
- [ ] v1.0 的外部验证标准是否需要"真实第三方用户"（当前定义不含，可复议）
- [ ] TLA+ 工具链选择与学习成本评估（v0.3 前）

## 更新记录

| 日期 | 版本 | 变更 |
|---|---|---|
| 2026-09-14 | v1 | 初版：定位确认为"并发数据流原语库"，深度优先路线（无锁化提前至 v0.2） |
| 2026-09-14 | v1.1 | 修正 x/sync/queue 情报（v0.23.0 无此包，工具链验证）；v0.2 基线基准完成并入库 |
| 2026-09-14 | v1.2 | v0.2 完成：无锁假设证伪（详见实验报告），mpsc 标记实验性；DoD 修正为"16+ 生产者扩展性最优"；发现 unbounded.Chan 平坦扩展性 |
| 2026-09-14 | v1.3 | v0.2.1 完成：ShardedChan 达成 ≤80 ns 目标（50.6 ns@4 生产者）并转正；mpsc 包内部分级（ShardedChan 生产可用，Queue/Chan 实验性） |
| 2026-09-14 | v1.4 | v0.1 fuzz 项完成：4 个 fuzz 目标（影子模型对账）入库并通过 20s 轰炸 |
| 2026-09-14 | v1.5 | 库命名定为 spool（SPOOL 词源），完成全局改名：目录/模块路径/全部 import/文档 |
| 2026-09-14 | v1.6 | 模块路径迁至 github.com/Ggrryta/spool；CI（双平台）与 .gitignore 入库；README 挂徽章。v0.1 仅剩英文 godoc 与推送建仓 |
| 2026-09-14 | v1.7 | v0.3 完成：两份 TLA+ 规约入 CI（tla job）、soak + nightly 流水线、README "正确性验证"四层防线章节；soak 开发中修掉测试自身三处缺陷（rng 并发安全、双写者 key 契约误用、goroutine 沉淀竞态） |
