// Package bounded 提供有界的多生产者/多消费者（MPMC）通道——
// 背压维度的流控原语。
//
// 与无界家族（unbounded/mpsc）的唯一契约差异：缓冲满时 Put 阻塞
// （可用 ctx 取消）。这正是本类型的功能而非缺陷：消费者借此限速
// 生产者，防止内存膨胀。其余语义与 spool 家族完全一致——幂等 Close、
// 优雅排空、Out/Done（统一契约见 docs/CONTRACTS.md）。
//
// 与原生 chan 的关系：原生 chan 是"有界 + 阻塞 + close 即关"；
// 本类型在其上补齐 spool 家族契约——关闭后已入队值全部送达 Out
// 才关闭（排空式关闭），并提供 Done 终止信号。
package bounded

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed 在通道已关闭后调用 Put 时返回。
var ErrClosed = errors.New("bounded: Put called on closed channel")

// Chan 是固定容量的 MPMC 通道。
//
//   - Put：任意多 goroutine 并发调用；缓冲满时阻塞直到有空间、关闭
//     或 ctx 取消（三者先到为准）。
//   - Out：任意多消费者并发读取；值按入队顺序（全局 FIFO）分发。
//   - Close：幂等。生效后拒绝新 Put；已入队的值仍会全部送达 Out，
//     排空后 Out 关闭、泵退出、Done 关闭。注意：若 Close 后无人
//     继续消费 Out，泵会阻塞在送达上——与原生 chan 的 close/接收
//     语义一致，Done 的关闭以"有消费者排空"为前提。
type Chan[T any] struct {
	mu      sync.Mutex
	ring    []T // 固定容量环形缓冲
	head    int // 出队位置
	count   int // 当前元素数；入队位置 = (head+count) % cap
	closing bool

	// data/space 各为容量 1 的通知通道（trySend 满则丢弃），
	// 唤醒协议与 mpsc.Chan 同源并经 TLA+ 验证：
	//   Put:   入环   → signal(data)
	//   出队:  出环   → signal(space)
	//   泵:    排空   → 检查退出 → <-data 睡眠
	// closedCh 在 Close 时关闭：对"多个阻塞中的 Put"实现广播唤醒——
	// 容量 1 的 trySend 只能唤醒一个，其余会永久睡死（实测抓出的真 bug）。
	data     chan struct{}
	space    chan struct{}
	closedCh chan struct{}

	out  chan T
	done chan struct{}
}

// NewChan 返回一个容量为 capacity 的有界通道。capacity 必须 > 0。
func NewChan[T any](capacity int) *Chan[T] {
	if capacity <= 0 {
		panic("bounded: capacity must be positive")
	}
	c := &Chan[T]{
		ring:     make([]T, capacity),
		data:     make(chan struct{}, 1),
		space:    make(chan struct{}, 1),
		closedCh: make(chan struct{}),
		out:      make(chan T),
		done:     make(chan struct{}),
	}
	go c.pump()
	return c
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Put 把 v 入队。缓冲满时阻塞，直到有空间、通道关闭或 ctx 取消
// （先到为准）。关闭后返回 ErrClosed；Close 生效前竞态窗口内的
// Put 允许成功（与 unbounded/mpsc 的既有契约一致）。
func (c *Chan[T]) Put(ctx context.Context, v T) error {
	for {
		c.mu.Lock()
		if c.closing {
			c.mu.Unlock()
			return ErrClosed
		}
		if c.count < len(c.ring) {
			c.ring[(c.head+c.count)%len(c.ring)] = v
			c.count++
			c.mu.Unlock()
			signal(c.data)
			return nil
		}
		c.mu.Unlock() // 满：等待空间、关闭或取消
		select {
		case <-c.space:
		case <-c.closedCh: // 广播唤醒：Close 已生效
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Out 返回读通道。关闭并完全排空后该通道关闭。
// 支持任意多消费者并发读取（MPMC 的 C）。
func (c *Chan[T]) Out() <-chan T { return c.out }

// Close 拒绝后续 Put；已入队的值仍会全部送达 Out
// （前提是有消费者继续排空，见类型文档）。幂等。
// 必须唤醒泵（data）并广播唤醒所有阻塞中的 Put（closedCh）。
func (c *Chan[T]) Close() {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return
	}
	c.closing = true
	close(c.closedCh) // 广播：唤醒全部阻塞中的 Put
	c.mu.Unlock()
	signal(c.data)
}

// Done 在 Out 关闭且泵 goroutine 退出后关闭。
func (c *Chan[T]) Done() <-chan struct{} { return c.done }

func (c *Chan[T]) pump() {
	defer close(c.done)
	defer close(c.out)

	const batchLimit = 64
	for {
		// 批量窃取：一次加锁取走最多 batchLimit 个值，
		// 锁与空间释放的成本按批摊销（对比逐值加锁吞吐提升数倍）。
		for {
			c.mu.Lock()
			if c.count == 0 {
				c.mu.Unlock()
				break
			}
			n := c.count
			if n > batchLimit {
				n = batchLimit
			}
			batch := make([]T, n)
			for i := 0; i < n; i++ {
				batch[i] = c.ring[c.head]
				var zero T
				c.ring[c.head] = zero // 释放引用，便于 GC
				c.head = (c.head + 1) % len(c.ring)
			}
			c.count -= n
			c.mu.Unlock()

			signal(c.space) // 批量释放空间名额，放行阻塞中的 Put
			for _, v := range batch {
				c.out <- v
			}
		}

		c.mu.Lock()
		exit := c.closing && c.count == 0
		c.mu.Unlock()
		if exit {
			return
		}
		<-c.data
	}
}
