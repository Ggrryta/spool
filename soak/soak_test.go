//go:build soak

// Package soak 提供小时级长跑压测（roadmap v0.3）。
//
// 与 fuzz 的分工：fuzz 找逻辑组合错误，soak 找时间累积效应——
// goroutine 泄漏、内存增长、随机负载下的偶发丢失。
//
// 默认不参与常规 CI（构建标签隔离），nightly 以如下方式运行：
//
//	SPOOL_SOAK_SECONDS=600 go test -tags=soak -run TestSoak -timeout 30m -v ./soak/
//
// 每轮断言：无丢失、无重复、（分片版）同 key 序号严格递增（即 FIFO）、
// 泵 goroutine 全部退出（goroutine 基线回归）。
package soak

import (
	"context"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ggrryta/spool/mpsc"
	"github.com/Ggrryta/spool/unbounded"
)

// awaitGoroutineBaseline 等待 goroutine 数回落到基线附近。
// goroutine 的退出相对于 wg.Done / 通道接收是异步的，瞬时采样必然
// 偶发误报；沉淀轮询让瞬时者落盘。等待是进度感知的（计数仍在下降
// 就继续等）；真泄漏则等满超时后转储全部 goroutine 栈再失败，
// 便于定位元凶。
func awaitGoroutineBaseline(t *testing.T, base, round, slack int) {
	t.Helper()
	const settleTimeout = 5 * time.Second
	deadline := time.Now().Add(settleTimeout)
	last, stable := runtime.NumGoroutine(), 0
	for g := last; g > base+slack; {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("round %d: goroutine 泄漏（基线 %d，%.0fs 后仍为 %d）\n%s",
				round, base, settleTimeout.Seconds(), g, buf[:n])
		}
		time.Sleep(5 * time.Millisecond)
		g = runtime.NumGoroutine()
		if g < last {
			last, stable = g, 0 // 有退出进度，重置稳定计数
		} else if stable++; stable > 100 { // 500ms 无任何退出进度
			break // 停在高位且不再变化，提前进入超时判定
		}
	}
	if g := runtime.NumGoroutine(); g > base+slack {
		t.Fatalf("round %d: goroutine 数高位停滞（基线 %d，当前 %d）",
			round, base, g)
	}
}

func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	if s := os.Getenv("SPOOL_SOAK_SECONDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("SPOOL_SOAK_SECONDS=%q 无效: %v", s, err)
		}
		return time.Duration(n) * time.Second
	}
	return 30 * time.Second
}

// TestSoakUnboundedChan：随机生产者数 × 随机写入量 × 随机停顿，
// 多轮执行 unbounded.Chan 的完整生命周期（含 Close/ctx 取消两条关闭路径，
// 以及小概率的提前关闭竞争窗口），验证完备性与 goroutine 卫生。
func TestSoakUnboundedChan(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	deadline := time.Now().Add(soakDuration(t))
	base := runtime.NumGoroutine()

	for round := 1; time.Now().Before(deadline); round++ {
		ctx, cancel := context.WithCancel(context.Background())
		c := unbounded.NewChan[int](ctx)

		producers := 1 + rng.Intn(7) // 1..7
		var success atomic.Int64
		var nextVal atomic.Int64
		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				prng := rand.New(rand.NewSource(seed)) // 每生产者独立 RNG
				n := prng.Intn(2000)
				for i := 0; i < n; i++ {
					v := int(nextVal.Add(1))
					if err := c.Put(v); err != nil {
						return // Close 竞争窗口内的拒绝是契约允许的
					}
					success.Add(1)
					if prng.Intn(1000) == 0 {
						time.Sleep(time.Duration(prng.Intn(100)) * time.Microsecond)
					}
				}
			}(rng.Int63())
		}

		seen := make(map[int]bool)
		var mu sync.Mutex
		consumed := make(chan int)
		go func() {
			n := 0
			for v := range c.Out() {
				mu.Lock()
				if seen[v] {
					t.Errorf("round %d: 值 %d 重复送达", round, v)
				}
				seen[v] = true
				mu.Unlock()
				n++
			}
			consumed <- n
		}()

		// 小概率提前关闭：制造 Put 与 Close 的竞争窗口。
		if rng.Intn(20) == 0 {
			c.Close()
		}

		go func() {
			wg.Wait()
			if rng.Intn(2) == 0 {
				cancel() // ctx 取消路径
			} else {
				c.Close() // 显式 Close 路径
			}
		}()

		got := <-consumed
		<-c.Done()

		mu.Lock()
		seenCount := len(seen)
		mu.Unlock()
		if int64(got) != success.Load() || int64(seenCount) != success.Load() {
			t.Fatalf("round %d: 送达 %d / 去重 %d / Put 成功 %d（丢失或重复）",
				round, got, seenCount, success.Load())
		}

		// goroutine 卫生：等泵等瞬时 goroutine 沉淀后对账。
		awaitGoroutineBaseline(t, base, round, 8)
		cancel() // 释放本轮 ctx（若走 Close 路径则取消是幂等善后）
	}
}

// TestSoakShardedChan：每生产者独占一个 key（单写者）的随机写入。
// 验证方法：值为 key*10M + 该 key 的递增序号；单消费者断言每 key 序号
// 严格递增（蕴含 同 key FIFO + 无重复——单写者下这是契约保证的），
// 总量对账（无丢失）。
func TestSoakShardedChan(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	deadline := time.Now().Add(soakDuration(t))
	base := runtime.NumGoroutine()

	for round := 1; time.Now().Before(deadline); round++ {
		ctx, cancel := context.WithCancel(context.Background())

		producers := 2 + rng.Intn(7) // 2..8，每生产者独占 key
		c := mpsc.NewShardedChan[int](ctx, 8)

		var success atomic.Int64
		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(key int, seed int64) {
				defer wg.Done()
				prng := rand.New(rand.NewSource(seed))
				n := prng.Intn(3000)
				for i := 0; i < n; i++ {
					v := key*10_000_000 + i
					if err := c.Put(uint64(key), v); err != nil {
						return // 序号空洞由总量对账兜底
					}
					success.Add(1)
					if prng.Intn(1000) == 0 {
						time.Sleep(time.Duration(prng.Intn(100)) * time.Microsecond)
					}
				}
			}(p, rng.Int63())
		}

		lastSeq := make([]int, producers)
		for i := range lastSeq {
			lastSeq[i] = -1 // 首值 i=0 必须能通过严格递增检查
		}
		consumed := make(chan int)
		go func() {
			n := 0
			for v := range c.Out() {
				k := v / 10_000_000
				s := v % 10_000_000
				if s <= lastSeq[k] {
					t.Errorf("round %d: key %d 序号非严格递增（%d <= %d）",
						round, k, s, lastSeq[k])
				}
				lastSeq[k] = s
				n++
			}
			consumed <- n
		}()

		if rng.Intn(20) == 0 {
			c.Close() // 提前关闭竞争窗口
		}

		go func() {
			wg.Wait()
			c.Close()
		}()

		got := <-consumed
		<-c.Done()

		if int64(got) != success.Load() {
			t.Fatalf("round %d: 送达 %d / Put 成功 %d（丢失或重复）",
				round, got, success.Load())
		}
		awaitGoroutineBaseline(t, base, round, 8)
		cancel()
	}
}
