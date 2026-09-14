package unbounded

import "context"

// Chan 是 Unbounded 的易用封装，外观像一个普通 channel：提供永不阻塞的
// Put 和一个 Out 读通道，由一个泵 goroutine 按 FIFO 顺序把内部缓冲的值
// 转发到 Out。
//
// 即使消费者很慢，Put 也永不阻塞：值会堆积在无界 backlog 里。Out 是一个
// 真 channel，可以在 select 语句中使用。
//
// 关闭语义（优雅）：Close——或取消传给 NewChan 的 context——之后拒绝新的
// Put（返回 ErrClosed），已缓冲的值仍会全部送达 Out，完全排空后 Out 关闭。
// 泵 goroutine 随之退出，Done 关闭。
type Chan[T any] struct {
	buf  *Unbounded[T]
	out  chan T
	done chan struct{}
}

// NewChan 返回一个新的 Chan 并启动泵 goroutine。取消 ctx 会关闭底层缓冲；
// 泵在排空后退出。
func NewChan[T any](ctx context.Context) *Chan[T] {
	c := &Chan[T]{
		buf:  New[T](),
		out:  make(chan T),
		done: make(chan struct{}),
	}
	go func() {
		defer close(c.done)
		defer close(c.out)
		// 把缓冲值搬运到 Out。先发 Out 再 Load，保持背压：当前值递交给
		// 消费者之后，才从 backlog 拉取下一个值。
		for v := range c.buf.Get() {
			c.out <- v
			c.buf.Load()
		}
	}()
	context.AfterFunc(ctx, c.buf.Close)
	return c
}

// Put 把 t 加入缓冲区，永不阻塞；Close 之后返回 ErrClosed。
func (c *Chan[T]) Put(t T) error { return c.buf.Put(t) }

// Out 返回读通道。缓冲区关闭并完全排空后该通道关闭。
func (c *Chan[T]) Out() <-chan T { return c.out }

// Close 拒绝后续 Put；已缓冲的值仍会送达 Out。
func (c *Chan[T]) Close() { c.buf.Close() }

// Done 在 Out 关闭且泵 goroutine 退出后关闭。
func (c *Chan[T]) Done() <-chan struct{} { return c.done }
