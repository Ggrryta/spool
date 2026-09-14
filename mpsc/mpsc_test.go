package mpsc

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// ---------------- Queue：纯数据结构契约 ----------------

func TestQueueFIFO(t *testing.T) {
	q := New[int]()
	for i := 0; i < 100; i++ {
		if err := q.Push(i); err != nil {
			t.Fatalf("Push(%d) = %v, 期望 nil", i, err)
		}
	}
	for i := 0; i < 100; i++ {
		v, ok := q.Peek()
		if !ok || v != i {
			t.Fatalf("Peek 得到 %d (ok=%v), 期望 %d", v, ok, i)
		}
		q.Pop()
	}
	if _, ok := q.Peek(); ok {
		t.Fatal("排空后 Peek 应返回 false")
	}
}

func TestQueuePushAfterClose(t *testing.T) {
	q := New[int]()
	q.Close()
	if err := q.Push(1); err != ErrClosed {
		t.Fatalf("Close 后 Push = %v, 期望 ErrClosed", err)
	}
	q.Close() // 幂等
}

func TestQueueCloseDrains(t *testing.T) {
	q := New[int]()
	for i := 0; i < 50; i++ {
		if err := q.Push(i); err != nil {
			t.Fatalf("Push = %v, 期望 nil", err)
		}
	}
	q.Close()
	if q.Drained() {
		t.Fatal("还有 50 个值未消费，Drained 应为 false")
	}

	for i := 0; i < 50; i++ {
		v, ok := q.Peek()
		if !ok || v != i {
			t.Fatalf("Peek 得到 %d (ok=%v), 期望 %d", v, ok, i)
		}
		q.Pop()
	}
	if !q.Drained() {
		t.Fatal("关闭且排空后 Drained 应为 true")
	}
}

// TestQueueConcurrentProducers：MPSC 压力测试——无丢失、无重复。
// （多生产者间的全局 FIFO 顺序也由链表单一串行点保证，但为避免测试
// 对调度顺序的脆弱依赖，这里验证的是完备性契约。）
func TestQueueConcurrentProducers(t *testing.T) {
	const producers = 8
	const perProducer = 5000

	q := New[int]()
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if err := q.Push(p*perProducer + i); err != nil {
					t.Errorf("Push = %v, 期望 nil", err)
				}
			}
		}(p)
	}

	received := make([]bool, producers*perProducer)
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for {
			v, ok := q.Peek()
			if !ok {
				// 队列暂时为空：生产者还在写入，让出后继续直到全部收齐。
				if allReceived(received) {
					return
				}
				runtime.Gosched()
				continue
			}
			if received[v] {
				t.Errorf("值 %d 被重复消费", v)
			}
			received[v] = true
			q.Pop()
		}
	}()

	wg.Wait()
	<-consumed

	for v, ok := range received {
		if !ok {
			t.Fatalf("值 %d 从未被消费", v)
		}
	}
}

func allReceived(received []bool) bool {
	for _, ok := range received {
		if !ok {
			return false
		}
	}
	return true
}

// ---------------- Chan：泵封装契约（与 unbounded.Chan 对齐） ----------------

func TestChanDeliversInOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewChan[int](ctx)
	for i := 0; i < 1000; i++ {
		if err := c.Put(i); err != nil {
			t.Fatalf("Put = %v, 期望 nil", err)
		}
	}

	// 收满 1000 个后关闭。
	go func() {
		for i := 0; i < 1000; i++ {
			v, ok := <-c.Out()
			if !ok {
				t.Errorf("通道提前关闭，第 %d 个值未送达", i)
				return
			}
			if v != i {
				t.Errorf("收到 %d, 期望 %d", v, i)
			}
		}
		c.Close()
	}()

	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done 未在超时内关闭")
	}
}

func TestChanContextCancelDrains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	c := NewChan[int](ctx)
	for i := 0; i < 50; i++ {
		if err := c.Put(i); err != nil {
			t.Fatalf("Put = %v, 期望 nil", err)
		}
	}
	cancel() // 不允许新的 Put，但已入队的 50 个值必须排空

	// context.AfterFunc 是异步的：轮询直到 Close 生效。
	// 与 Close 竞争的 Put 仍可能成功——这是允许的行为——记录多进的值。
	deadline := time.After(2 * time.Second)
	extra := 0
	for {
		if err := c.Put(100 + extra); err == ErrClosed {
			break
		}
		extra++
		select {
		case <-deadline:
			t.Fatal("ctx 取消后 Close 未生效")
		case <-time.After(time.Millisecond):
		}
	}

	count := 0
	for range c.Out() {
		count++
	}
	if want := 50 + extra; count != want {
		t.Fatalf("排空 %d 个值, 期望 %d", count, want)
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("ctx 取消并排空后 Done 未关闭")
	}
}

// TestChanSlowConsumer：慢消费者不能阻塞生产者（无界契约）。
func TestChanSlowConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewChan[int](ctx)
	release := make(chan struct{})
	go func() {
		for range c.Out() {
			<-release
		}
	}()

	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := c.Put(i); err != nil {
			t.Fatalf("Put = %v, 期望 nil", err)
		}
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Put 被慢消费者阻塞了 %v", d)
	}
}

// TestChanCloseWithNoDataWakes：泵可能在睡，Close 必须能唤醒它复查
// Drained 并退出。这是 Close 必须发信号的回归测试（基准测试抓出的
// 死锁：Close 只置标志不唤醒 → Done 永不关闭）。
func TestChanCloseWithNoDataWakes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewChan[int](ctx)
	// 给泵时间睡进 <-sig（若 Close 不发信号，此测试将超时）。
	time.Sleep(50 * time.Millisecond)
	c.Close()

	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Close 后泵未被唤醒，Done 未关闭")
	}
}
