// Package bench 是独立于主库的基准模块：主库保持零第三方依赖，
// 竞品的对比基准放在这里（依赖 chanx 等），通过 replace 引用主库。
//
// 三个场景：
//
//   - Handoff：交接快路径（SPSC，消费者就位等待）——测量"生产者到消费者"
//     的单次交接成本。
//   - Backlog：堆积路径（无消费者立即读取）——测量纯入队成本。
//     原生 chan 会阻塞，无法参与此场景。
//   - MPSC：4 生产者 + 1 消费者——测量生产端竞争下的吞吐。
//
// 运行：
//
//	go test -bench=. -benchmem -benchtime=1s -timeout 30m
package bench

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/smallnest/chanx"
	"github.com/Ggrryta/spool/unbounded"
)

const producers = 4

// ---------------- 场景 A：交接快路径（SPSC） ----------------

func BenchmarkHandoff_OursUnbounded(b *testing.B) {
	buf := unbounded.New[int]()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range buf.Get() {
			buf.Load()
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buf.Put(i)
	}
	b.StopTimer()
	buf.Close()
	<-done
}

func BenchmarkHandoff_OursChan(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := unbounded.NewChan[int](ctx)
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
	b.StopTimer()
	c.Close()
	<-drained
	<-c.Done()
}

func BenchmarkHandoff_Chanx(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := chanx.NewUnboundedChan[int](ctx, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch.Out {
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.In <- i
	}
	b.StopTimer()
	close(ch.In)
	<-done
}

func BenchmarkHandoff_NativeChan1(b *testing.B) {
	ch := make(chan int, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- i
	}
	b.StopTimer()
	close(ch)
	<-done
}

func BenchmarkHandoff_NativeChan1024(b *testing.B) {
	ch := make(chan int, 1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- i
	}
	b.StopTimer()
	close(ch)
	<-done
}

// ---------------- 场景 B：堆积路径（入队成本，无即时消费者） ----------------

func BenchmarkBacklog_OursUnbounded(b *testing.B) {
	buf := unbounded.New[int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buf.Put(i)
	}
	b.StopTimer()
	buf.Close()
	for range buf.Get() {
		buf.Load()
	}
}

func BenchmarkBacklog_OursChan(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := unbounded.NewChan[int](ctx)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Put(i)
	}
	b.StopTimer()
	c.Close()
	for range c.Out() {
	}
	<-c.Done()
}

func BenchmarkBacklog_Chanx(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := chanx.NewUnboundedChan[int](ctx, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.In <- i
	}
	b.StopTimer()
	close(ch.In)
	for range ch.Out {
	}
}

// ---------------- 场景 C：MPSC（4 生产者 + 1 消费者） ----------------

func BenchmarkMPSC_OursUnbounded(b *testing.B) {
	buf := unbounded.New[int]()
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for range buf.Get() {
			buf.Load()
		}
	}()

	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	var wg sync.WaitGroup
	b.ResetTimer()
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				_ = buf.Put(1)
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	buf.Close()
	<-consumed
}

func BenchmarkMPSC_OursChan(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := unbounded.NewChan[int](ctx)
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for range c.Out() {
		}
	}()

	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	var wg sync.WaitGroup
	b.ResetTimer()
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				_ = c.Put(1)
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	c.Close()
	<-consumed
	<-c.Done()
}

func BenchmarkMPSC_Chanx(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := chanx.NewUnboundedChan[int](ctx, 1024)
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for range ch.Out {
		}
	}()

	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	var wg sync.WaitGroup
	b.ResetTimer()
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				ch.In <- 1
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	close(ch.In)
	<-consumed
}

func BenchmarkMPSC_NativeChan1024(b *testing.B) {
	ch := make(chan int, 1024)
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for range ch {
		}
	}()

	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	var wg sync.WaitGroup
	b.ResetTimer()
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				ch <- 1
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	close(ch)
	<-consumed
}
