// Package pubsub 提供轻量的发布/订阅机制：订阅者回调在共享的 serializer
// goroutine 上按 FIFO 顺序执行。
//
// 它是 gRPC 内部 grpcsync.PubSub 的重写版本，底层使用本库的 serializer
// 而非 grpcsync.CallbackSerializer。
//
// 语义：
//   - Subscribe 注册一个具名回调；名称必须唯一。
//   - Publish 把载荷分发给所有已注册的回调，按订阅注册顺序调度。回调
//     逐个执行，慢的订阅者会拖慢其他订阅者（与 gRPC 相同的取舍，
//     换取每个订阅者零额外开销）。
//   - 取消传给 New 的 context 即停止分发；已调度的回调仍可能在 Done
//     关闭之前执行。
package pubsub

import (
	"context"
	"sync"

	"github.com/Ggrryta/spool/serializer"
)

type subscription struct {
	name     string
	callback func(payload any)
}

// PubSub 把发布的载荷分发给已注册的订阅者回调。
//
// 所有方法都可以被多个 goroutine 并发调用。
type PubSub struct {
	cs *serializer.Serializer

	mu            sync.Mutex
	subscriptions map[string]*subscription
}

// New 返回一个新的 PubSub。取消 ctx 即关闭 PubSub：后续的 Publish 会被
// 丢弃，Done 在进行中的回调执行完毕后关闭。
func New(ctx context.Context) *PubSub {
	return &PubSub{
		cs:            serializer.New(ctx),
		subscriptions: make(map[string]*subscription),
	}
}

// Subscribe 以给定名称注册 callback。同名订阅已存在时返回 false。
func (p *PubSub) Subscribe(name string, callback func(payload any)) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.subscriptions[name]; ok {
		return false
	}
	p.subscriptions[name] = &subscription{name: name, callback: callback}
	return true
}

// Unsubscribe 移除以 name 注册的订阅（如果存在）。
// 已被调度的回调仍会执行。
func (p *PubSub) Unsubscribe(name string) {
	p.mu.Lock()
	delete(p.subscriptions, name)
	p.mu.Unlock()
}

// Publish 把 payload 分发给所有已注册的订阅者回调。永不阻塞：载荷在
// serializer 上排队并按 FIFO 顺序分发。
// PubSub 的 context 取消后，Publish 不做任何事。
func (p *PubSub) Publish(payload any) {
	p.mu.Lock()
	subs := make([]*subscription, 0, len(p.subscriptions))
	for _, sub := range p.subscriptions {
		subs = append(subs, sub)
	}
	p.mu.Unlock()

	for _, sub := range subs {
		sub := sub
		p.cs.ScheduleOr(func(context.Context) { sub.callback(payload) }, func() {})
	}
}

// Done 在传给 New 的 context 被取消、且所有进行中的回调执行完毕后关闭。
func (p *PubSub) Done() <-chan struct{} { return p.cs.Done() }
