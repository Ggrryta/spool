# 统一契约（v0.4）

本文档是 spool 全部通道化队列类型共享的语义契约。**任何偏离都会被
`internal/contract` 一致性套件在 CI 中抓住**——各包的 `contract_test.go`
以各自适配器运行同一套断言，语义偏离即 CI 红。

## 写入

- `Put` 永不阻塞（无界缓冲），生产者速率不受消费者限制
- 关闭**生效后** Put 返回非 nil 错误（各包约定为 `ErrClosed`）
- 关闭**生效前**的竞态窗口内，Put 允许成功——调用方必须以返回值为准，
  而不是以"我调用了 Close"为准

## 关闭（优雅排空）

- `Close` 幂等，重复调用无副作用
- 生效后拒绝新写入；**已缓冲的值全部送达后**，读通道才关闭
- 提供 `Done` 的实现：排空 + 泵 goroutine 退出后 `Done` 关闭

## 顺序保证

| 类型 | 保证 |
|---|---|
| `unbounded.Unbounded`（手动 Load） | 全局 FIFO |
| `unbounded.Chan` | 全局 FIFO |
| `mpsc.Chan` | 全局 FIFO |
| `mpsc.ShardedChan` | 同 key 严格 FIFO；跨 key 不保证 |
| `bounded.Chan` | 全局 FIFO（单一环形缓冲） |
| `serializer` / `pubsub` | 回调/载荷按调度顺序 FIFO 串行执行 |

## panic 策略

- 队列类（unbounded/mpsc/bounded）：只搬运数据、不执行用户代码，无用户 panic 面
- `serializer`：默认 **fail-fast**（回调 panic 在 runner goroutine 上重抛）；
  `WithPanicHandler` 可恢复并继续执行后续回调（已有测试钉死该语义）

## 背压

| 类型 | 行为 |
|---|---|
| unbounded / mpsc 全家族 | **无背压**（无界定义）：消费不及时，值驻留内存 |
| `bounded.Chan` | 缓冲满时 `Put(ctx, v)` **阻塞**，直到有空间、关闭或 ctx 取消 |

bounded 的 Put 带有 ctx：阻塞中的写入可被取消/超时打断（返回 ctx.Err()），
Close 则以广播（closedCh 关闭）唤醒全部阻塞者并返回 ErrClosed。

## 资源卫生

- 泵 goroutine 在排空后必然退出；`Done` 关闭后 goroutine 数回归基线
- nightly soak 持续断言上述性质（含泄漏时转储全部 goroutine 栈）

## 执行机制

```
internal/contract.Verify(spec) —— 全套契约断言，返回违反列表
internal/contract.Run(t, spec) —— 测试包装：任何违反即 t.Errorf
```

各包的 `contract_test.go` 以适配器接入：

| 包 | 适配的类型 | 顺序模式 |
|---|---|---|
| unbounded | `Chan` | GlobalFIFO |
| mpsc | `Chan` | GlobalFIFO |
| mpsc | `ShardedChan` | PerKeyFIFO(4) |
| bounded | `Chan` | GlobalFIFO（Put 带 ctx） |

（手动版 `unbounded.Unbounded` 的 Load 协议不适用通道表面，由
`FuzzUnbounded` 影子模型对账 + 单元测试覆盖。）

### 阴性对照

套件自带阴性对照：对"每 5 个值吞掉 1 个"的坏实现运行，必须报丢失。
这保证断言非空洞——契约测试不是一组永远绿灯的摆设。
