package unbounded

import (
	"context"
	"testing"
)

// BenchmarkUnboundedPutFastPath 测量快路径：消费者正在 Get 上等待，
// Put 直接把值递交出去，不经过 backlog。
//
//	go test -bench=. -benchmem ./unbounded/
func BenchmarkUnboundedPutFastPath(b *testing.B) {
	buf := New[int]()
	done := make(chan struct{})
	go func() {
		for range buf.Get() {
			buf.Load()
		}
		close(done)
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buf.Put(i)
	}
	buf.Close()
	<-done
}

// BenchmarkUnboundedPutBacklog 测量慢路径：消费者跟不上生产者，值堆积在
// backlog 中（均摊的 slice append 开销）。
func BenchmarkUnboundedPutBacklog(b *testing.B) {
	buf := New[int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buf.Put(i)
	}
	b.StopTimer()
}

// BenchmarkNativeChan 基线：同元素类型的普通缓冲 channel（容量 1），
// 带专职消费者。
func BenchmarkNativeChan(b *testing.B) {
	ch := make(chan int, 1)
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- i
	}
	close(ch)
	<-done
}

// BenchmarkNativeChanBigBuffer 展示一个很大的预分配缓冲（1024）能做到什么；
// Unbounded 用容量为 1 的 channel 加一次 slice append 达到了同样的
// 永不阻塞行为。
func BenchmarkNativeChanBigBuffer(b *testing.B) {
	ch := make(chan int, 1024)
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- i
	}
	close(ch)
	<-done
}

func BenchmarkChanAutoPump(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewChan[int](ctx)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range c.Out() {
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Put(i)
	}
	c.Close()
	<-drained
	<-c.Done()
}
