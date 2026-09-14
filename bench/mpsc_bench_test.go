package bench

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Ggrryta/spool/mpsc"
)

// ---------------- mpsc（无锁 MPSC 内核）与既有实现对比 ----------------

// BenchmarkQueueMPSC_MpscPushPop：纯队列吞吐（4 生产者 Push + 1 消费者
// Peek/Pop，无任何 channel 开销）——展示无锁内核本身的极限。
func BenchmarkQueueMPSC_MpscPushPop(b *testing.B) {
	q := mpsc.New[int]()
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for {
			if _, ok := q.Peek(); ok {
				q.Pop()
			} else {
				if q.Drained() {
					return
				}
			}
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
				_ = q.Push(1)
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	q.Close()
	<-consumed
}

// BenchmarkMPSC_MpscChan：无锁内核 + 泵，对标 unbounded.Chan
// （基线 93.6 ns）与原生 chan（74.8 ns）。
func BenchmarkMPSC_MpscChan(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := mpsc.NewChan[int](ctx)
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

// BenchmarkHandoff_MpscChan：SPSC 快路径对比（对标 unbounded.Chan
// 基线 32.8 ns）。
func BenchmarkHandoff_MpscChan(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := mpsc.NewChan[int](ctx)
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
