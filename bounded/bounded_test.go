package bounded

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func mustTimeoutCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestPutGetFIFO(t *testing.T) {
	c := NewChan[int](8)
	for i := 0; i < 8; i++ {
		if err := c.Put(context.Background(), i); err != nil {
			t.Fatalf("Put(%d) = %v, 期望 nil", i, err)
		}
	}
	for i := 0; i < 8; i++ {
		v, ok := <-c.Out()
		if !ok || v != i {
			t.Fatalf("收到 %d (ok=%v), 期望 %d", v, ok, i)
		}
	}
}

func TestPutBlocksWhenFull(t *testing.T) {
	c := NewChan[int](2)

	// 持续写入直到首次阻塞。阻塞点取决于泵的批量窃取时序（泵手中
	// 可持有至多 batchLimit 个在途值），因此不断言具体第几次阻塞，
	// 只断言背压语义本身：满时必然阻塞，且超时放弃的写入不入队。
	var put []int
	blocked := false
	for i := 0; i < 1000 && !blocked; i++ {
		err := c.Put(mustTimeoutCtx(t, 20*time.Millisecond), i)
		switch {
		case err == nil:
			put = append(put, i)
		case errors.Is(err, context.DeadlineExceeded):
			blocked = true
		default:
			t.Fatalf("Put(%d) = %v, 意外错误", i, err)
		}
	}
	if !blocked {
		t.Fatal("写入 1000 次从未阻塞：背压语义缺失")
	}

	// 排空：已成功写入的值按序全部送达，超时放弃的不出现。
	c.Close()
	i := 0
	for v := range c.Out() {
		if v != put[i] {
			t.Fatalf("FIFO 被破坏：位置 %d 收到 %d, 期望 %d", i, v, put[i])
		}
		i++
	}
	if i != len(put) {
		t.Fatalf("送达 %d 个值, 期望 %d", i, len(put))
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done 未关闭")
	}
}

func TestPutAfterClose(t *testing.T) {
	c := NewChan[int](4)
	c.Close()
	if err := c.Put(context.Background(), 1); err != ErrClosed {
		t.Fatalf("Close 后 Put = %v, 期望 ErrClosed", err)
	}
	c.Close() // 幂等
}

func TestCloseDrains(t *testing.T) {
	c := NewChan[int](4)
	const n = 100
	// 写入放 goroutine：环满时 Put 会阻塞，需要消费端并发排空才能推进。
	go func() {
		for i := 0; i < n; i++ {
			if err := c.Put(context.Background(), i); err != nil {
				return
			}
		}
		c.Close()
	}()

	count := 0
	seen := make([]bool, n)
	for v := range c.Out() {
		if seen[v] {
			t.Fatalf("值 %d 重复", v)
		}
		seen[v] = true
		count++
	}
	if count != n {
		t.Fatalf("排空 %d 个值, 期望 %d", count, n)
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done 未关闭")
	}
}

func TestCloseWakesBlockedPut(t *testing.T) {
	c := NewChan[int](1)
	_ = c.Put(context.Background(), 1) // 泵取走 1（阻塞在 Out 交付）
	_ = c.Put(context.Background(), 2) // 环满

	putDone := make(chan error)
	go func() {
		putDone <- c.Put(context.Background(), 3) // 阻塞中
	}()

	time.Sleep(30 * time.Millisecond)
	c.Close() // 必须唤醒阻塞中的 Put

	select {
	case err := <-putDone:
		if err != ErrClosed {
			t.Fatalf("阻塞中的 Put = %v, 期望 ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close 未唤醒阻塞中的 Put")
	}
	// 已入队的 1、2 仍会送达（排空关闭语义）。
	if v, ok := <-c.Out(); !ok || v != 1 {
		t.Fatalf("收到 %d (ok=%v), 期望 1", v, ok)
	}
	if v, ok := <-c.Out(); !ok || v != 2 {
		t.Fatalf("收到 %d (ok=%v), 期望 2", v, ok)
	}
}

func TestPutContextCancel(t *testing.T) {
	c := NewChan[int](1)
	_ = c.Put(context.Background(), 1) // 泵取走
	_ = c.Put(context.Background(), 2) // 环满

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.Put(ctx, 3); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Put = %v, 期望 DeadlineExceeded", err)
	}
}

// TestCloseWakesAllBlockedPuts：多个生产者同时阻塞在满缓冲上时，
// Close 必须广播唤醒全部（容量 1 的 trySend 只能唤醒一个——这是
// 实测抓出的广播缺失 bug 的回归测试）。
func TestCloseWakesAllBlockedPuts(t *testing.T) {
	c := NewChan[int](1)
	_ = c.Put(context.Background(), 1) // 泵取走
	_ = c.Put(context.Background(), 2) // 环满

	const blocked = 4
	var wg sync.WaitGroup
	errs := make(chan error, blocked)
	for i := 0; i < blocked; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- c.Put(context.Background(), 99) // 全部阻塞
		}()
	}

	time.Sleep(50 * time.Millisecond) // 让 4 个 Put 全部睡进 space 等待
	c.Close()

	wg.Wait()
	close(errs)
	n := 0
	for err := range errs {
		if err != ErrClosed {
			t.Fatalf("阻塞中的 Put = %v, 期望 ErrClosed", err)
		}
		n++
	}
	if n != blocked {
		t.Fatalf("仅 %d/%d 个阻塞 Put 被唤醒", n, blocked)
	}
	// 已入队的 1、2 仍送达。
	if v, ok := <-c.Out(); !ok || v != 1 {
		t.Fatalf("收到 %d (ok=%v), 期望 1", v, ok)
	}
	if v, ok := <-c.Out(); !ok || v != 2 {
		t.Fatalf("收到 %d (ok=%v), 期望 2", v, ok)
	}
}

// TestConcurrentMPMC：多生产者多消费者的完备性压力测试。
func TestConcurrentMPMC(t *testing.T) {
	c := NewChan[int](16)
	const producers, consumers, perProducer = 4, 3, 2000
	total := producers * perProducer

	var success atomic.Int64
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if err := c.Put(context.Background(), p*perProducer+i); err != nil {
					t.Errorf("Put = %v, 期望 nil", err)
					return
				}
				success.Add(1)
			}
		}(p)
	}

	seen := make(map[int]bool)
	var mu sync.Mutex
	var cwg sync.WaitGroup
	for k := 0; k < consumers; k++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for v := range c.Out() {
				mu.Lock()
				if seen[v] {
					t.Errorf("值 %d 重复送达", v)
				}
				seen[v] = true
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	c.Close()
	cwg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total {
		t.Fatalf("送达 %d 个唯一值, 期望 %d", len(seen), total)
	}
	if int64(success.Load()) != int64(total) {
		t.Fatalf("Put 成功 %d, 期望 %d", success.Load(), total)
	}
}
