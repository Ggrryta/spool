// Package serializer 提供回调的 FIFO 串行执行机制。
//
// 通过 TrySchedule 提交的回调由单个后台 goroutine 按提交顺序逐个执行。
// 这是 gRPC 内部（internal/grpcsync/callback_serializer.go）用来串行化
// resolver / balancer 状态更新的模式。
//
// 与"锁保护的临界区"相比：被调度的回调可以长时间运行，也可以再次调度新的
// 回调而不会死锁；与"任务队列库"相比：serializer 保证严格的 FIFO 顺序和
// 优雅停机——取消 context 后不再接受新调度，但所有已排队的回调都会在
// Done 关闭之前执行完毕。
//
// panic 安全：默认情况下，回调中的 panic 会在 runner goroutine 上重新
// 抛出（快速失败）。使用 WithPanicHandler 可以改为恢复 panic 并继续处理
// 剩余的回调。
package serializer

import (
	"context"
	"runtime/debug"

	"github.com/Ggrryta/spool/unbounded"
)

// PanicHandler 在设置了 WithPanicHandler 且某个已调度回调 panic 时收到
// 回调函数、恢复出的值和调用栈。
type PanicHandler func(ctx context.Context, f func(context.Context), recovered any, stack []byte)

type options struct {
	onPanic PanicHandler
}

// Option 配置 Serializer。
type Option func(*options)

// WithPanicHandler 安装 panic 处理器。不设置时，回调的 panic 会在
// runner goroutine 上重新抛出并使进程崩溃。
func WithPanicHandler(h PanicHandler) Option {
	return func(o *options) { o.onPanic = h }
}

// Serializer 按提交顺序逐个执行已调度的回调。
//
// 所有方法都可以被多个 goroutine 并发调用。
type Serializer struct {
	done      chan struct{}
	callbacks *unbounded.Unbounded[func(context.Context)]
	onPanic   PanicHandler
}

// New 返回一个新的 Serializer 并启动 runner goroutine。取消 ctx 后不再
// 接受新的调度（TrySchedule 返回错误）；已调度的回调仍会全部执行完毕，
// 之后 Done 才关闭。
func New(ctx context.Context, opts ...Option) *Serializer {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	s := &Serializer{
		done:      make(chan struct{}),
		callbacks: unbounded.New[func(context.Context)](),
		onPanic:   o.onPanic,
	}
	go s.run(ctx)
	return s
}

// TrySchedule 把 f 按提交顺序加入执行队列。这是尽力而为的操作：如果传给
// New 的 context 已被取消（或 serializer 正在关闭），f 不会被调度并返回
// ErrClosed。
//
// 回调执行任何阻塞操作时应尊重传入的 context，取消后尽快返回。
func (s *Serializer) TrySchedule(f func(context.Context)) error {
	return s.callbacks.Put(f)
}

// ScheduleOr 与 TrySchedule 一样调度 f；如果无法调度，则改为在当前
// goroutine 内联执行 onFailure。
func (s *Serializer) ScheduleOr(f func(context.Context), onFailure func()) {
	if s.callbacks.Put(f) != nil {
		onFailure()
	}
}

// Done 在传给 New 的 context 被取消、且所有已调度的回调执行完毕后关闭。
func (s *Serializer) Done() <-chan struct{} { return s.done }

func (s *Serializer) run(ctx context.Context) {
	defer close(s.done)

	// ctx 取消时关闭缓冲区：不再接受新的回调，
	// 待 backlog 排空后 Get 通道随之关闭。
	context.AfterFunc(ctx, s.callbacks.Close)

	for cb := range s.callbacks.Get() {
		s.callbacks.Load()
		s.safeCall(ctx, cb)
	}
}

func (s *Serializer) safeCall(ctx context.Context, f func(context.Context)) {
	defer func() {
		if r := recover(); r != nil {
			if s.onPanic != nil {
				s.onPanic(ctx, f, r, debug.Stack())
				return
			}
			// 快速失败：在 runner goroutine 上重新抛出。
			panic(r)
		}
	}()
	f(ctx)
}
