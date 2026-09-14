package unbounded

import (
	"errors"
	"sync"
)

// ErrClosed 在缓冲区已关闭（或正在关闭）后调用 Put 时返回。
var ErrClosed = errors.New("unbounded: Put called on closed buffer")

// Unbounded 是一个不启动任何 goroutine 的无界队列。
//
// 生产者永不阻塞：当信号通道满时，Put 把值追加到内部 backlog。
// 消费者阻塞在 Get 返回的 channel 上，可以在 select 语句中使用。
//
// 从 Get 成功接收一个值之后，消费者必须调用 Load 把下一条缓冲值推进通道。
// 忘记调用 Load 会让队列停摆（或者使用自动泵版的 NewChan）。
//
// Close 是优雅的：调用 Close 后 Put 返回 ErrClosed，但已缓冲的值仍会全部
// 送达；Get 返回的 channel 只有在完全排空之后（最后一次 Load 触发）才会
// 关闭。
//
// 所有方法都可以被多个 goroutine 并发调用。
type Unbounded[T any] struct {
	mu      sync.Mutex
	c       chan T
	backlog []T
	closing bool
	closed  bool
}

// New 返回一个新的 Unbounded 缓冲区。
func New[T any]() *Unbounded[T] {
	return &Unbounded[T]{c: make(chan T, 1)}
}

// Put 把 t 加入缓冲区，永不阻塞。缓冲区已关闭时返回 ErrClosed。
func (b *Unbounded[T]) Put(t T) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing {
		return ErrClosed
	}
	if len(b.backlog) == 0 {
		// 快路径：直接把值递交给正在等待的消费者。
		select {
		case b.c <- t:
			return nil
		default:
		}
	}
	b.backlog = append(b.backlog, t)
	return nil
}

// Load 把最早的缓冲值（如果有的话）推进 Get 返回的读通道。消费者每次成功
// 接收一个值后都必须调用本方法。Close 之后调用 Load 也是安全（且必要）的：
// 如果 backlog 已排空，Load 会关闭读通道。
func (b *Unbounded[T]) Load() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.backlog) > 0 {
		select {
		case b.c <- b.backlog[0]:
			var zero T
			b.backlog[0] = zero // 释放引用，便于 GC 回收
			b.backlog = b.backlog[1:]
		default:
		}
	} else if b.closing && !b.closed {
		b.closed = true
		close(b.c)
	}
}

// Get 返回读通道，Put 加入的值通过它送达。
//
// 接收到一个值后，调用方必须调用 Load 把下一条缓冲值推进通道。如果缓冲区
// 已被 Close，读通道会在所有缓冲值送达之后关闭。
func (b *Unbounded[T]) Get() <-chan T {
	return b.c
}

// Close 关闭缓冲区。之后不能再 Put；Get 返回的通道要等所有缓冲值被读完、
// 且最后一次 Load 被调用之后才会关闭。Close 幂等，重复调用无副作用。
func (b *Unbounded[T]) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing {
		return
	}
	b.closing = true
	if len(b.backlog) == 0 {
		b.closed = true
		close(b.c)
	}
}
