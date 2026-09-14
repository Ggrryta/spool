# spool

[![CI](https://github.com/Ggrryta/spool/actions/workflows/ci.yml/badge.svg)](https://github.com/Ggrryta/spool/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Ggrryta/spool.svg)](https://pkg.go.dev/github.com/Ggrryta/spool)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

并发数据流原语库，吸收了 gRPC 内部实现、`golang.org/x/sync`、`sourcegraph/conc` 等库的精华，用 Go 泛型重新实现。

名字取自 **SPOOL**（*Simultaneous Peripheral Operations On-Line*）——计算机史上为打印机缓冲队列造的词：生产者永不阻塞、任务无界排队、按消费者节奏处理、退场时优雅排空。这正是本库全部原语的共同语义。

```bash
go get github.com/Ggrryta/spool
```

要求 Go 1.24+，仅依赖标准库。

> 顶层设计、路线图与不做清单见 [docs/roadmap.md](docs/roadmap.md)。

## 包一览

### unbounded — 零 goroutine 的无界队列

源自 gRPC `internal/buffer.Unbounded`。生产者永不阻塞，消费者拿到的是一个可 `select` 的真 channel；`Close` 是优雅的：拒绝新写入，但已缓冲的值会全部送达后读通道才关闭。

```go
// 手动版（零开销）：读一个值后必须调一次 Load
b := unbounded.New[int]()
b.Put(1)
for v := range b.Get() {
    b.Load()
}

// 自动版（多一个泵 goroutine，多一跳转发）
c := unbounded.NewChan[int](ctx)
c.Put(1)
for v := range c.Out() { ... } // Close/ctx 取消后排空自动关闭
```

基准（Windows, amd64, Intel Ultra 5 225H）：

| 基准 | ns/op | allocs/op |
|---|---|---|
| Unbounded 快路径（直递消费者） | 78 | 0 |
| Unbounded 慢路径（入 backlog） | 22 | 0 |
| 原生 chan（cap=1，会阻塞） | 151 | 0 |
| 原生 chan（cap=1024，会阻塞） | 51 | 0 |
| Chan 自动泵版 | 289 | 0 |

### serializer — FIFO 串行回调执行器

源自 gRPC `internal/grpcsync.CallbackSerializer`，增加了 panic 处理策略。回调按提交顺序逐个执行，ctx 取消后拒绝新调度但已排队的全部执行完，`Done` 才关闭。

```go
s := serializer.New(ctx,
    serializer.WithPanicHandler(func(ctx context.Context, f func(context.Context), r any, stack []byte) {
        log.Printf("callback panicked: %v", r)
    }),
)
s.TrySchedule(func(ctx context.Context) { /* 串行执行 */ })
```

### singleflight — 泛型版请求合并

`golang.org/x/sync/singleflight` 的泛型重写：无 `any` 装箱，语义一致（结果共享、`Forget`、panic 转错误传播给等待者）。

```go
var g singleflight.Group[User]
u, err, _ := g.Do("user:42", func() (User, error) { return loadUser(42) })
```

### mpsc — 无锁队列（实验性）+ 分片管道（生产可用）

**`ShardedChan`**（生产可用）：显式 key 分片的多生产者管道——同 key 严格 FIFO、跨 key 顺序不保证（Kafka 分区模型）。**基准成绩**（详见 [实验报告](docs/benchmarks/v0.2.1-sharded.md)）：4 生产者 50.6ns（快 `unbounded.Chan` 46%、快原生 chan 36%），32 生产者时快原生 chan 3.3 倍，全程零分配。适合有天然 key 的并发汇聚（连接事件、按 ID 聚合）。

```go
c := mpsc.NewShardedChan[Event](ctx, 8)
c.Put(connID, event)   // 同一连接的事件严格有序
for v := range c.Out() { ... }
```

**`Queue` / `Chan`**（实验性）：Michael-Scott 无锁队列。诚实声明：我们的[实验](docs/benchmarks/v0.2-lockfree.md)显示它在 4~32 生产者全规模吞吐低于 mutex 版，保留作为协议正确性的文档化样本（含 Go GC 免疫 ABA 的论证）。

### pubsub — 基于 serializer 的发布订阅

源自 gRPC `internal/grpcsync.PubSub`。`Publish` 永不阻塞，回调在共享 serializer 上按序执行；ctx 取消即优雅停机。

```go
p := pubsub.New(ctx)
p.Subscribe("logger", func(payload any) { log.Println(payload) })
p.Publish("hello")
```

## 设计取舍

- **每包独立 import**，按需拉取，无大而全的顶层包。
- **不提供 errgroup/pool**：`golang.org/x/sync` 和 `sourcegraph/conc` 已经覆盖，本库专注“队列/序列化/合并”这条线。
- **`Unbounded` 的手动 `Load()` 协议**是刻意的零开销设计；不确定就用 `NewChan` 自动版。
- **Close 语义契约**：Close/ctx 取消生效前 `Put` 仍可能成功（返回 `nil`），生效后返回 `unbounded.ErrClosed`；已缓冲数据保证排空。

## 致谢

`unbounded`、`serializer`、`pubsub` 源自 [gRPC-Go](https://github.com/grpc/grpc-go)（Apache License 2.0）内部包的泛型重写；`singleflight` 源自 `golang.org/x/sync`（BSD-3）。相应版权声明保留在文件头注释与 NOTICE 中。

## License

Apache License 2.0，见 [LICENSE](LICENSE)。
