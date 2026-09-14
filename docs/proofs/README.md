# 形式化验证（v0.3）

本目录存放 spool 核心协议的 TLA+ 规约与 TLC 模型检验配置。
每次 push 由 CI 自动验证（见 `.github/workflows/ci.yml` 的 `tla` job）。

## 为什么是 TLA+

测试（单测/fuzz/soak）是**采样**：跑多少次都只是执行空间的子集。
TLC 模型检验是**穷举**：对规约的所有可达状态逐一检查不变量——
包括万亿次运行才可能撞上的时序窗口。对并发协议，这是"高稳定"
从形容词变成证据的唯一路径。

## 规约一览

### UnboundedDrain.tla — 关闭与排空协议

`unbounded.Unbounded`/`Chan` 的核心协议：容量 1 的信号通道 + backlog +
优雅排空关闭。抽象层级：2 生产者 × 各 MaxPuts=3 次，值全局唯一，
Close 可在任意时刻发生。

| 不变量 | 断言 |
|---|---|
| `ChanCap` | 信号通道容量恒 ≤ 1 |
| `NoDup` | 无重复送达 |
| `FIFO` | 接收顺序 = Put 提交顺序（全局 FIFO） |
| `NoLoss` | 每个成功 Put 的值必在 chan/backlog/delivered 之一 |
| `ClosedClean` | 通道关闭时 backlog 必已排空 |
| `ClosedComplete` | 观察到关闭 ⇒ 全部 Put 值已送达 |

时序性质：`AllDeliveredAfterClose`——closing 后（弱公平下）最终全部送达。
死锁检查：开启（Close 恒可用作逃生路径，死锁即 bug）。

### ShardedWake.tla — 唤醒协议

`mpsc.ShardedChan` 的泵唤醒协议：sig 容量 1、trySend 满则丢弃、
泵"排空→检查→入睡"原子化。抽象层级：2 key × 各 MaxPuts=3，
Out 通道与下游消费者抽象为无界汇（不参与唤醒协议）。

| 不变量 | 断言 |
|---|---|
| `NoDup` / `NoLoss` | 完备性 |
| `PerKeyFIFO` | 同 key 序号单调递增（同 key FIFO） |
| `ExitedClean` | 泵退出 ⇒ 已关闭且全部分片排空 |
| **`NoLostWakeup`** | **泵无信号入睡 ⇒ 确实无事可做（无数据且未关闭）** |

时序性质：`DrainAndExit`——closing 后全部送达且泵退出。

**`NoLostWakeup` 是 v0.2 实测死锁的机器检测器**：当时的 bug 版本里
Close 只置 `closing` 不发信号，"泵睡在 sig 上且 sig=0 且 closing"可达，
违反本不变量。换言之，先写规约再写代码，这个 bug 不会活过第一分钟。
完整的失败-修复复盘见 `docs/benchmarks/v0.2-lockfree.md`。

## 运行

```bash
# 本地（需 Java 17+）
curl -sL -o tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
cd docs/proofs
java -jar ../tla2tools.jar -cleanup -config UnboundedDrain.cfg UnboundedDrain.tla
java -jar ../tla2tools.jar -cleanup -config ShardedWake.cfg ShardedWake.tla
```

CI 中由 `tla` job 在每次 push 时执行同样命令。

## 验证记录（2026-09-14，TLC 1.8.0）

| 规约 | 穷举状态 | 深度 | 结论 |
|---|---:|---:|---|
| UnboundedDrain | 1,443 distinct / 2,617 generated | 15 | 全部不变量 + liveness 成立 |
| ShardedWake | 13,843 distinct / 25,027 generated | 17 | 全部不变量 + liveness 成立 |

（指纹碰撞概率分别为 9.2E-14 / 8.4E-12，状态空间完整覆盖。）

### 阴性对照：不变量非空洞的证明

为验证 `NoLostWakeup` 不是空洞成立，我们注入了 v0.2 的真实 bug
（Close 只置 `closing` 不发信号）到规约副本：

```
Error: Invariant NoLostWakeup is violated.
```

TLC 立即捕获。结论：**该不变量恰好是那个历史死锁的机器检测器**——
若 v0.3 的规约先于代码存在，该 bug 不会活过第一天。这也验证了
"规约-实现一致性"的置信度：不变量的语义与真实故障模式对得上。

## 建模诚实声明

- 规约建模的是**协议状态机**，不是 Go 实现：内存序、缓存一致性、
  channel 内部锁等实现细节被抽象掉（它们由 `-race`、fuzz、soak
  三层测试兜底）。
- `MaxPuts=3` 是状态空间规模与覆盖深度的折中；TLC 穷举的是该规模下
  的**全部**交错，不是实现的全部可能历史。
- Go 代码与规约的一致性靠人工评审（规约的动作逐一镜像实现分支）；
  未来可考虑用同一状态机做 Go 侧的模型驱动测试以缩小间隙。

## 配套资产

- soak 长跑：`soak/soak_test.go`（`-tags=soak`，nightly 10 分钟随机负载，
  含 goroutine 泄漏检查——失败时转储全部 goroutine 栈）
- nightly 流水线：`.github/workflows/nightly.yml`（soak + 每目标 2 分钟 fuzz 加时）
