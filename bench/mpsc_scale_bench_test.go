package bench

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Ggrryta/spool/mpsc"
	"github.com/Ggrryta/spool/unbounded"
)

// 生产者规模扩展性对比：验证"无锁在高生产者数下反超 mutex"的假设，
// 寻找交叉点。
//
// go test -bench="MPSCScale" -benchmem -benchtime=1s ./...
func BenchmarkMPSCScale(b *testing.B) {
	for _, n := range []int{4, 8, 16, 32} {
		b.Run(fmt.Sprintf("producers=%d/mpsc.Chan(无锁)", n), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := mpsc.NewChan[int](ctx)
			consumed := make(chan struct{})
			go func() {
				defer close(consumed)
				for range c.Out() {
				}
			}()
			runProducers(b, n, func(v int) error { return c.Put(v) })
			b.StopTimer()
			c.Close()
			<-consumed
			<-c.Done()
		})
		b.Run(fmt.Sprintf("producers=%d/unbounded.Chan(mutex)", n), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := unbounded.NewChan[int](ctx)
			consumed := make(chan struct{})
			go func() {
				defer close(consumed)
				for range c.Out() {
				}
			}()
			runProducers(b, n, func(v int) error { return c.Put(v) })
			b.StopTimer()
			c.Close()
			<-consumed
			<-c.Done()
		})
		b.Run(fmt.Sprintf("producers=%d/原生chan(1024)", n), func(b *testing.B) {
			ch := make(chan int, 1024)
			consumed := make(chan struct{})
			go func() {
				defer close(consumed)
				for range ch {
				}
			}()
			runProducers(b, n, func(v int) error { ch <- v; return nil })
			b.StopTimer()
			close(ch)
			<-consumed
		})
	}
}

// runProducers 启动 n 个生产者共写入 b.N 个值，等待全部完成。
// 计时由调用方用 b.ResetTimer/StopTimer 控制。
func runProducers(b *testing.B, n int, put func(int) error) {
	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	var wg sync.WaitGroup
	b.ResetTimer()
	for p := 0; p < n; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				_ = put(1)
			}
		}()
	}
	wg.Wait()
}
