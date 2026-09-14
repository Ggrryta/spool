package mpsc

import "context"

// Chan 是 Queue 的泵封装：一个泵 goroutine 把队列中的值持续搬运到
// Out 通道，API 与语义与 unbounded.Chan 完全一致（Put / Out / Close /
// Done，优雅排空关闭），但生产端无锁。
//
// **实验性**：本库自己的基准（docs/benchmarks/v0.2-lockfree.md）显示，
// 在 4~32 个生产者的全部规模下，本实现的吞吐都低于 mutex 版的
// unbounded.Chan——管道瓶颈在泵的逐值交接而非生产端锁。除非你需要
// Queue 本身（Peek/Pop 形态的纯无锁数据结构），否则请使用
// unbounded.Chan。保留本类型是为了：协议正确性的文档化样本，
// 以及未来分片化改进的基线。
//
// 唤醒协议（保证泵睡眠时不丢数据、关闭时不漏醒）：
//
//	Put:   链接入队   → signal      [链接必先于信号]
//	Close: 置关闭标志 → signal      [标志必先于信号]
//	泵:    排空队列   → 检查 Drained → <-sig 睡眠  [排空必先于睡眠]
//
// 时序穷举可证不丢唤醒：任何在"泵排空之后"完成的链接，其信号要么在
// 泵睡眠前送达 sig（容量 1，泵会醒），要么泵的下一轮排空直接看到该值。
// 信号满被丢弃的场景，必有泵尚未消费的旧信号，泵醒来后排空即覆盖。
// 关闭同理：泵若已睡，Close 的信号将其唤醒复查 Drained。
type Chan[T any] struct {
	q    *Queue[T]
	sig  chan struct{}
	out  chan T
	done chan struct{}
}

// NewChan 返回一个新的 Chan 并启动泵 goroutine。取消 ctx 会关闭底层
// 队列；已缓冲的值仍会全部送达 Out，排空后 Out 关闭、泵退出、Done 关闭。
func NewChan[T any](ctx context.Context) *Chan[T] {
	c := &Chan[T]{
		q:    New[T](),
		sig:  make(chan struct{}, 1),
		out:  make(chan T, 64), // 与 unbounded.Chan 一致：泵批量搬运的吞吐缓冲
		done: make(chan struct{}),
	}
	go c.pump(ctx)
	return c
}

// Put 把 t 入队，永不阻塞；关闭后返回 ErrClosed。
func (c *Chan[T]) Put(t T) error {
	if err := c.q.Push(t); err != nil {
		return err
	}
	c.signal()
	return nil
}

// Close 拒绝后续 Put；已入队的值仍会送达 Out。
// 必须唤醒泵：它可能正睡在 sig 上等待复查 Drained。
func (c *Chan[T]) Close() {
	c.q.Close()
	c.signal()
}

func (c *Chan[T]) signal() {
	select {
	case c.sig <- struct{}{}:
	default:
	}
}

// Out 返回读通道。队列关闭并完全排空后该通道关闭。
func (c *Chan[T]) Out() <-chan T { return c.out }

// Done 在 Out 关闭且泵 goroutine 退出后关闭。
func (c *Chan[T]) Done() <-chan struct{} { return c.done }

func (c *Chan[T]) pump(ctx context.Context) {
	defer close(c.done)
	defer close(c.out)

	// ctx 取消 → 关闭（含唤醒信号），排空后退出。
	context.AfterFunc(ctx, c.Close)

	for {
		// 排空：持续搬运直到队列空。
		for {
			v, ok := c.q.Peek()
			if !ok {
				break
			}
			c.out <- v
			c.q.Pop()
		}
		if c.q.Drained() {
			return
		}
		<-c.sig
	}
}
