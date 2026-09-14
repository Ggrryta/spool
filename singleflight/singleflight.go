// Package singleflight 提供泛型的重复调用抑制机制：多个调用者用相同的 key
// 并发调用 Do 时，函数只执行一次，所有调用者共享其结果（和错误）。
//
// 它是 golang.org/x/sync/singleflight 的泛型重写。官方版本返回 (any, error)，
// 每个结果都要装箱；本版本结果类型是类型参数，值类型返回不产生接口分配。
//
// 与 x/sync 一致的语义：
//   - 第一个调用者执行 fn；相同 key 的其他调用者阻塞并拿到相同的结果和错误。
//   - Forget 移除一个 key，即使该 key 的调用仍在进行中，下一次 Do 也会
//     重新执行 fn。
//   - fn 中的 panic 会被转换为错误传递给等待中的调用者，并在发起者的
//     goroutine 上重新抛出。
package singleflight

import (
	"fmt"
	"sync"
)

// call 表示一次进行中或已完成的 Do 调用。
type call[T any] struct {
	wg sync.WaitGroup

	// val 和 err 只写入一次、在 wg.Done 之前写入，调用者只在
	// wg.Wait 返回之后读取。
	val T
	err error

	// dups 记录加入本次调用（而非发起新调用）的调用者数量。
	dups int
}

type panicError struct {
	value any
}

func (p *panicError) Error() string {
	return fmt.Sprintf("singleflight: panic in called function: %v", p.value)
}

// Group 按 key 对函数调用进行合并。零值即可使用，首次使用后不可拷贝。
type Group[T any] struct {
	mu sync.Mutex
	m  map[string]*call[T]
}

// Do 执行 fn，确保同一 key 同时只有一个执行中的调用。重复的调用者等待
// 原始调用完成后，收到相同的结果。
//
// shared 表示该结果是否被多个调用者共享。
func (g *Group[T]) Do(key string, fn func() (T, error)) (v T, err error, shared bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call[T])
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := &call[T]{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	g.doCall(c, key, fn)
	return c.val, c.err, c.dups > 0
}

// doCall 为发起者执行 fn。即使 fn panic，也保证等待者被释放、key 恰好
// 清理一次。
func (g *Group[T]) doCall(c *call[T], key string, fn func() (T, error)) {
	defer func() {
		// 必须先把 c.err 定稿、再释放等待者，否则加入进行中调用的调用者
		// 会读到未写完的结果（数据竞争）。
		var repanic any
		if r := recover(); r != nil {
			// 等待者不能永远阻塞：把 panic 作为错误交给它们，
			// 然后在下面重新抛出，保持普通 panic 的行为。
			c.err = &panicError{value: r}
			repanic = r
		}

		c.wg.Done()

		g.mu.Lock()
		if g.m[key] == c {
			delete(g.m, key)
		}
		g.mu.Unlock()

		if repanic != nil {
			panic(repanic)
		}
	}()
	c.val, c.err = fn()
}

// Forget 让 Group 忘记某个 key。之后对该 key 的 Do 调用会重新执行 fn，
// 即使该 key 的调用仍在进行中。
func (g *Group[T]) Forget(key string) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}
