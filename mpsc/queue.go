package mpsc

import (
	"errors"
	"sync/atomic"
)

// ErrClosed 在队列已关闭后调用 Push 时返回。
var ErrClosed = errors.New("mpsc: Push called on closed queue")

// node 是链表节点。生产者只写自己的节点；通过 CAS 发布 next 即完成入队。
type node[T any] struct {
	next atomic.Pointer[node[T]]
	v    T
}

// Queue 是无锁 MPSC 无界队列。
//
//   - Push：任意多 goroutine 并发调用，永不阻塞。
//   - Peek/Pop：仅允许单个消费者 goroutine 调用，且必须先 Peek 确认
//     存在值、再 Pop 移除同一值（MPSC 契约）。
//
// 关闭语义（与 unbounded.Unbounded 一致）：Close 后拒绝新 Push（返回
// ErrClosed）；Close 生效前已在途的 Push 仍会成功入队；已入队的数据
// 可继续消费直至排空。
//
// 字段布局：head（消费者独占）、tail（生产者 CAS 竞争点）、closing
// 分别以填充隔离到不同缓存行——否则多生产者的 tail CAS 与消费者的
// head 访问在同一缓存行上乒乓，实测 MPSC 吞吐劣化近一倍。
type Queue[T any] struct {
	head atomic.Pointer[node[T]] // 消费端，仅消费者写（哨兵起点）
	_    [56]byte                // 缓存行隔离

	tail atomic.Pointer[node[T]] // 生产端，多生产者的 CAS 竞争点
	_    [56]byte                // 缓存行隔离

	closing atomic.Bool
}

// New 返回一个新的 Queue，内部以哨兵节点初始化。
func New[T any]() *Queue[T] {
	stub := &node[T]{}
	q := &Queue[T]{}
	q.head.Store(stub)
	q.tail.Store(stub)
	return q
}

// Push 把 v 入队，永不阻塞。队列已关闭时返回 ErrClosed。
//
// 协议（链接式 Michael-Scott 入队）：
//
//  1. 读 tail 与 tail.next；
//  2. 若 tail.next 非空，说明 tail 落后于实际链尾，先帮忙推进再重试；
//  3. CAS 链接 tail.next == 新节点（全局串行点，即 FIFO 顺序）；
//  4. 尽力推进 tail；失败无害，后来者会继续推进。
//
// 退出前若发现 closing 已置位，本次 Push 仍成功（Close 生效前在途的
// 写入允许入队，与 unbounded 的既有契约一致）。
func (q *Queue[T]) Push(v T) error {
	if q.closing.Load() {
		return ErrClosed
	}
	n := &node[T]{v: v}
	for {
		tail := q.tail.Load()
		next := tail.next.Load()
		if next != nil {
			// tail 落后：帮忙推进后重试。
			q.tail.CompareAndSwap(tail, next)
			continue
		}
		if tail.next.CompareAndSwap(nil, n) {
			// 推进失败没关系：下一个 Push 或消费者会看到链接并推进。
			q.tail.CompareAndSwap(tail, n)
			return nil
		}
	}
}

// Peek 返回队首元素但不移除。仅单消费者调用。
// 队列为空时返回 false。
func (q *Queue[T]) Peek() (T, bool) {
	if first := q.head.Load().next.Load(); first != nil {
		return first.v, true
	}
	var zero T
	return zero, false
}

// Pop 移除队首元素。仅单消费者调用，且必须紧跟一次返回 true 的 Peek。
func (q *Queue[T]) Pop() {
	h := q.head.Load()
	first := h.next.Load() // 契约保证非 nil（调用方刚 Peek 到）
	q.head.Store(first)    // head 仅消费者写，普通 Store 即可
}

// Close 关闭队列：之后 Push 返回 ErrClosed，已入队数据不受影响。
// Close 幂等。
func (q *Queue[T]) Close() { q.closing.Store(true) }

// Drained 报告队列是否已关闭且排空。
// 泵封装用它决定退出时机。
func (q *Queue[T]) Drained() bool {
	return q.closing.Load() && q.head.Load().next.Load() == nil
}
