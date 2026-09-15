package bench

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Ggrryta/spool/bounded"
	"github.com/Ggrryta/spool/mpsc"
	"github.com/Ggrryta/spool/unbounded"
)

// MPMC 背压通道对比：bounded.Chan vs 原生 chan（同等容量）。
// 4 生产者 + 2 消费者。
//
// go test -bench="MPMC" -benchmem -benchtime=1s ./...
func BenchmarkMPMC(b *testing.B) {
	for _, cap := range []int{16, 1024} {
		b.Run(fmt.Sprintf("cap=%d/bounded.Chan", cap), func(b *testing.B) {
			c := bounded.NewChan[int](cap)
			var cwg sync.WaitGroup
			for k := 0; k < 2; k++ {
				cwg.Add(1)
				go func() {
					defer cwg.Done()
					for range c.Out() {
					}
				}()
			}
			runProducers(b, 4, func(v int) error { return c.Put(context.Background(), v) })
			b.StopTimer()
			c.Close()
			cwg.Wait()
			<-c.Done()
		})
		b.Run(fmt.Sprintf("cap=%d/原生chan", cap), func(b *testing.B) {
			ch := make(chan int, cap)
			var cwg sync.WaitGroup
			for k := 0; k < 2; k++ {
				cwg.Add(1)
				go func() {
					defer cwg.Done()
					for range ch {
					}
				}()
			}
			runProducers(b, 4, func(v int) error { ch <- v; return nil })
			b.StopTimer()
			close(ch)
			cwg.Wait()
		})
	}
}

// 生产者规模扫描扩展（偿还 v0.2.1 的挂账项：64/128 生产者行为）。
// 对标数据见 BenchmarkMPSCScale（4/8/16/32）。
func BenchmarkMPSCScaleLarge(b *testing.B) {
	for _, n := range []int{64, 128} {
		b.Run(fmt.Sprintf("producers=%d/ShardedChan8", n), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := mpsc.NewShardedChan[int](ctx, 8)
			consumed := make(chan struct{})
			go func() {
				defer close(consumed)
				for range c.Out() {
				}
			}()
			runProducers(b, n, func(v int) error { return c.Put(1, v) })
			b.StopTimer()
			c.Close()
			<-consumed
			<-c.Done()
		})
		b.Run(fmt.Sprintf("producers=%d/unbounded.Chan", n), func(b *testing.B) {
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
	}
}
