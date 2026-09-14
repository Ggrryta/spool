package bench

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Ggrryta/spool/mpsc"
)

// ShardedChan 规模扩展对比：v0.2.1 的核心实验。
//
// "独立key" = 每个生产者用自己的序号做 key（最坏情况：值撒满全部分片，
// 无同 key 批量效应）；"同key" = 全部生产者挤同一分片（顺序模式上限）。
// 对标数据见 BenchmarkMPSCScale（unbounded.Chan 与原生 chan）。
//
// go test -bench="ShardedScale" -benchmem -benchtime=1s ./...
func BenchmarkShardedScale(b *testing.B) {
	for _, n := range []int{4, 8, 16, 32} {
		b.Run(fmt.Sprintf("producers=%d/ShardedChan8(独立key)", n), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := mpsc.NewShardedChan[int](ctx, 8)
			consumed := make(chan struct{})
			go func() {
				defer close(consumed)
				for range c.Out() {
				}
			}()
			runProducersKeyed(b, n, func(pid int) error {
				return c.Put(uint64(pid), 1)
			})
			b.StopTimer()
			c.Close()
			<-consumed
			<-c.Done()
		})
		b.Run(fmt.Sprintf("producers=%d/ShardedChan8(同key)", n), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := mpsc.NewShardedChan[int](ctx, 8)
			consumed := make(chan struct{})
			go func() {
				defer close(consumed)
				for range c.Out() {
				}
			}()
			runProducersKeyed(b, n, func(pid int) error {
				return c.Put(7, 1) // 所有生产者同一 key：同分片竞争
			})
			b.StopTimer()
			c.Close()
			<-consumed
			<-c.Done()
		})
	}
}

// runProducersKeyed 启动 n 个生产者共写入 b.N 个值，回调收到生产者
// 序号（供分片路由使用），等待全部完成。
func runProducersKeyed(b *testing.B, n int, put func(pid int) error) {
	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	var wg sync.WaitGroup
	b.ResetTimer()
	for p := 0; p < n; p++ {
		pid := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				_ = put(pid)
			}
		}()
	}
	wg.Wait()
}
