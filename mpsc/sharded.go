package mpsc

import (
	"context"
	"sync"
	"sync/atomic"
)

// ShardedChan 是分片化的 MPSC 泵封装：生产者按 key 路由到独立分片，
// 消除多生产者在单一同步点上的竞争。
//
// 排序契约（与 Chan 的关键差异，调用方必须理解）：
//
//   - 同一 key 的值严格 FIFO；
//   - 不同 key 之间顺序不保证。
//
// 这是 Kafka 分区式的取舍：需要全局 FIFO 用 unbounded.Chan；
// 每个生产者（或每个事件流）有天然 key、且要求高并发写入时用本类型。
//
// 设计要点（依据 docs/benchmarks/v0.2-lockfree.md 的实验教训）：
//
//   - 分片内核用 mutex + slice 而非无锁链表——临界区只有一次 append
//     时，无竞争 mutex 快路径（~15ns）远比 CAS 失败重试便宜；
//   - 消费端批量窃取：泵一次加锁取走整个批次，锁成本按批摊销，
//     缓解"泵逐值交接"的结构性瓶颈；
//   - 每分片维护精确计数（锁内更新），泵的空扫描与 Drained 判断
//     完全免锁。
type ShardedChan[T any] struct {
	shards  []shard[T]
	mask    uint64
	closing atomic.Bool
	sig     chan struct{}
	out     chan T
	done    chan struct{}
}

// shard 是单分片：mutex 保护的 backlog。所有状态变更（append 与
// 批量窃取）都在锁内完成，count 恒等于 len(backlog)，因此对泵而言
// count.Load() 是精确的空判断。
type shard[T any] struct {
	mu      sync.Mutex
	backlog []T
	count   atomic.Int64
}

func (s *shard[T]) push(v T) {
	s.mu.Lock()
	s.backlog = append(s.backlog, v)
	s.count.Add(1)
	s.mu.Unlock()
}

// stealAll 一次加锁取走整个 backlog。批量语义：锁成本由整批均摊。
func (s *shard[T]) stealAll() []T {
	s.mu.Lock()
	b := s.backlog
	s.backlog = nil
	s.count.Add(-int64(len(b)))
	s.mu.Unlock()
	return b
}

// NewShardedChan 返回一个新的 ShardedChan 并启动泵 goroutine。
// shardCount 会被向上取整到 2 的幂（最小 1）；0 表示默认 8。
// 取消 ctx 等价于 Close（拒绝新写入，已入队值排空送达）。
func NewShardedChan[T any](ctx context.Context, shardCount int) *ShardedChan[T] {
	if shardCount <= 0 {
		shardCount = 8
	}
	n := 1
	for n < shardCount {
		n <<= 1
	}
	c := &ShardedChan[T]{
		shards: make([]shard[T], n),
		mask:   uint64(n - 1),
		sig:    make(chan struct{}, 1),
		out:    make(chan T),
		done:   make(chan struct{}),
	}
	go c.pump(ctx)
	return c
}

// Put 把 v 路由到 key 对应的分片，永不阻塞；关闭后返回 ErrClosed。
// 同 key 严格 FIFO；跨 key 顺序不保证。
func (c *ShardedChan[T]) Put(key uint64, v T) error {
	if c.closing.Load() {
		return ErrClosed
	}
	c.shards[key&c.mask].push(v)
	c.signal()
	return nil
}

// Out 返回读通道。关闭并完全排空后该通道关闭。
func (c *ShardedChan[T]) Out() <-chan T { return c.out }

// Close 拒绝后续 Put；已入队的值仍会送达 Out。
// 必须唤醒泵：它可能正睡在 sig 上（与 Chan 相同的唤醒协议）。
func (c *ShardedChan[T]) Close() {
	c.closing.Store(true)
	c.signal()
}

// Done 在 Out 关闭且泵 goroutine 退出后关闭。
func (c *ShardedChan[T]) Done() <-chan struct{} { return c.done }

func (c *ShardedChan[T]) signal() {
	select {
	case c.sig <- struct{}{}:
	default:
	}
}

func (c *ShardedChan[T]) pump(ctx context.Context) {
	defer close(c.done)
	defer close(c.out)

	// ctx 取消 → 关闭（含唤醒信号），排空后退出。
	context.AfterFunc(ctx, c.Close)

	for {
		// 一轮：扫描全部分片，批量窃取非空分片。
		for i := range c.shards {
			if c.shards[i].count.Load() == 0 {
				continue // 精确空判断，免锁
			}
			for _, v := range c.shards[i].stealAll() {
				c.out <- v
			}
		}
		if c.closing.Load() && c.allEmpty() {
			return
		}
		<-c.sig
	}
}

func (c *ShardedChan[T]) allEmpty() bool {
	for i := range c.shards {
		if c.shards[i].count.Load() != 0 {
			return false
		}
	}
	return true
}
