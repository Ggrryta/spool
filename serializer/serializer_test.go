package serializer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Ggrryta/spool/unbounded"
)

func TestFIFOOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := New(ctx)
	const n = 1000
	var mu sync.Mutex
	order := make([]int, 0, n)
	for i := 0; i < n; i++ {
		i := i
		// 从多个 goroutine 提交，验证单个 runner 依然逐个、无数据竞争地执行。
		go func() {
			_ = s.TrySchedule(func(ctx context.Context) {
				mu.Lock()
				defer mu.Unlock()
				order = append(order, i)
			})
		}()
	}

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		done := len(order) >= n
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			mu.Lock()
			defer mu.Unlock()
			t.Fatalf("超时；已执行 %d/%d 个回调", len(order), n)
		}
	}

	seen := make(map[int]bool, n)
	for _, v := range order {
		if seen[v] {
			t.Fatalf("回调 %d 被执行了多次", v)
		}
		seen[v] = true
	}
}

func TestCancelDrainsPendingCallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	s := New(ctx)
	var mu sync.Mutex
	executed := 0
	const n = 20
	for i := 0; i < n; i++ {
		_ = s.TrySchedule(func(ctx context.Context) {
			mu.Lock()
			executed++
			mu.Unlock()
		})
	}
	cancel() // 不再接受新调度，但已排队的必须全部执行

	// context.AfterFunc 是异步的：轮询直到 Close 生效。
	deadline := time.After(2 * time.Second)
	for {
		err := s.TrySchedule(func(context.Context) {})
		if errors.Is(err, unbounded.ErrClosed) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("取消后 TrySchedule = %v, 期望 ErrClosed", err)
		case <-time.After(time.Millisecond):
		}
	}

	select {
	case <-s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("取消并排空后 Done 未关闭")
	}
	mu.Lock()
	defer mu.Unlock()
	if executed != n {
		t.Fatalf("执行了 %d 个回调, 期望 %d", executed, n)
	}
}

func TestScheduleOrRunsOnFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := New(ctx)
	defer cancel()

	called := false
	s.ScheduleOr(func(context.Context) {}, func() { called = true })
	if called {
		t.Fatal("serializer 未关闭时 onFailure 不应执行")
	}

	cancelled, cancel2 := context.WithCancel(context.Background())
	cancel2()
	s2 := New(cancelled)
	fallback := false
	// AfterFunc 是异步的：轮询直到调度被拒绝。
	deadline := time.After(2 * time.Second)
	for {
		s2.ScheduleOr(func(context.Context) {}, func() { fallback = true })
		if fallback {
			break
		}
		select {
		case <-deadline:
			t.Fatal("context 取消后 onFailure 未执行")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestPanicHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicked := make(chan any, 1)
	var mu sync.Mutex
	afterRan := false

	s := New(ctx, WithPanicHandler(func(_ context.Context, _ func(context.Context), r any, stack []byte) {
		panicked <- r
	}))

	_ = s.TrySchedule(func(context.Context) { panic("boom") })
	_ = s.TrySchedule(func(context.Context) {
		mu.Lock()
		afterRan = true
		mu.Unlock()
	})

	select {
	case r := <-panicked:
		if r != "boom" {
			t.Fatalf("恢复到 %v, 期望 \"boom\"", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("panic 处理器未被调用")
	}

	// 恢复 panic 之后 serializer 必须继续运行。
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		done := afterRan
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatal("panic 之后的回调未被执行")
		}
	}
}
