package backoff

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExponentialGrowthAndClamp(t *testing.T) {
	cfg := Config{BaseDelay: 100 * time.Millisecond, Multiplier: 2, Jitter: 0, MaxDelay: 1 * time.Second}
	e := NewExponential(cfg)

	for retries, want := range map[int]time.Duration{
		0: 100 * time.Millisecond,
		1: 200 * time.Millisecond,
		2: 400 * time.Millisecond,
		3: 800 * time.Millisecond,
		4: 1 * time.Second,  // 撞上限
		9: 1 * time.Second,  // 稳定在上限
	} {
		if got := e.Backoff(retries); got != want {
			t.Fatalf("Backoff(%d) = %v, 期望 %v", retries, got, want)
		}
	}
}

func TestExponentialJitterBounds(t *testing.T) {
	cfg := Config{BaseDelay: 1 * time.Second, Multiplier: 1, Jitter: 0.2, MaxDelay: 10 * time.Second}
	e := NewExponential(cfg)

	for retries := 0; retries < 5; retries++ {
		for i := 0; i < 200; i++ {
			got := e.Backoff(retries)
			if got < 800*time.Millisecond || got > 1200*time.Millisecond {
				t.Fatalf("Backoff(%d) = %v, 超出 ±20%% 抖动范围", retries, got)
			}
		}
	}
}

func TestExponentialConcurrentSafe(t *testing.T) {
	e := NewExponential(DefaultConfig())
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_ = e.Backoff(i % 20)
			}
		}()
	}
	wg.Wait() // 无数据竞争（配合 -race 运行）
}

func TestSleepCompletesAndCancels(t *testing.T) {
	// 睡满：返回 nil，耗时 >= d。
	start := time.Now()
	if err := Sleep(context.Background(), 50*time.Millisecond); err != nil {
		t.Fatalf("Sleep = %v, 期望 nil", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("Sleep 提前返回（%v）", elapsed)
	}

	// ctx 取消：立即返回 ctx.Err()。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start = time.Now()
	if err := Sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep = %v, 期望 context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Sleep 未被取消及时打断（%v）", elapsed)
	}
}

func TestRunnerRetriesUntilSuccess(t *testing.T) {
	cfg := Config{BaseDelay: 5 * time.Millisecond, Multiplier: 2, Jitter: 0, MaxDelay: 50 * time.Millisecond}
	r := NewRunner(NewExponential(cfg))

	var calls atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	err := r.Run(ctx, func() error {
		if calls.Add(1) < 4 {
			return errors.New("transient")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Run = %v, 期望 nil", err)
	}
	if calls.Load() != 4 {
		t.Fatalf("执行 %d 次, 期望 4 次", calls.Load())
	}
	// 3 次失败 → 退避 5+10+20 = 35ms（零抖动，确定性）。
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("Run 过快返回（%v），退避未生效", elapsed)
	}
}

func TestRunnerReset(t *testing.T) {
	cfg := Config{BaseDelay: 20 * time.Millisecond, Multiplier: 10, Jitter: 0, MaxDelay: time.Second}
	r := NewRunner(NewExponential(cfg))

	target := errors.New("final failure")
	var calls atomic.Int64
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := r.Run(ctx, func() error {
		n := calls.Add(1)
		switch {
		case n <= 3:
			return ErrReset // 重置退避：前三次都不积累退避
		case n == 4:
			return Permanent(target) // 终止性错误
		default:
			return nil
		}
	})
	if !errors.Is(err, target) {
		t.Fatalf("Run = %v, 期望终态返回 target", err)
	}
	// 若重置语义正确：前三次退避均为 BaseDelay（20ms）×3 = 60ms 左右；
	// 若重置失效：20×10^2 = 2s+，早已超时。
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("重置语义未生效（总耗时 %v）", elapsed)
	}
}

func TestRunnerContextCancelDuringBackoff(t *testing.T) {
	cfg := Config{BaseDelay: time.Hour, Multiplier: 1, Jitter: 0, MaxDelay: time.Hour}
	r := NewRunner(NewExponential(cfg))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := r.Run(ctx, func() error { return errors.New("always fail") })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, 期望 DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("退避中的取消未及时生效（%v）", elapsed)
	}
}

func TestDefaultConfigSane(t *testing.T) {
	c := DefaultConfig()
	if c.BaseDelay <= 0 || c.MaxDelay < c.BaseDelay || c.Multiplier <= 1 || c.Jitter < 0 || c.Jitter > 1 {
		t.Fatalf("默认配置不合理: %+v", c)
	}
	// 函数化防篡改：两次调用返回独立副本。
	a, b := DefaultConfig(), DefaultConfig()
	a.BaseDelay = -1
	if b.BaseDelay <= 0 {
		t.Fatal("DefaultConfig 返回了共享可变状态")
	}
}
