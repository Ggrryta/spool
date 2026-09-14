package mpsc

import (
	"context"
	"sync"
	"testing"
	"time"
)

// 排序契约：同 key 严格 FIFO——这是 ShardedChan 存在的前提，
// 必须最先钉死。
func TestShardedFIFOWithinKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewShardedChan[int](ctx, 4)
	const n = 2000
	for i := 0; i < n; i++ {
		if err := c.Put(42, i); err != nil {
			t.Fatalf("Put = %v, 期望 nil", err)
		}
	}

	for i := 0; i < n; i++ {
		v, ok := <-c.Out()
		if !ok {
			t.Fatalf("通道提前关闭，第 %d 个值未送达", i)
		}
		if v != i {
			t.Fatalf("同 key 顺序被破坏：收到 %d, 期望 %d", v, i)
		}
	}
	c.Close()
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done 未关闭")
	}
}

// 完备性契约：跨分片不查顺序（契约允许乱序），但无丢失、无重复。
func TestShardedCompleteness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const producers = 8
	const perProducer = 3000

	c := NewShardedChan[int](ctx, 8)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if err := c.Put(uint64(p), p*perProducer+i); err != nil {
					t.Errorf("Put = %v, 期望 nil", err)
				}
			}
		}(p)
	}

	received := make([]bool, producers*perProducer)
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for v := range c.Out() {
			if received[v] {
				t.Errorf("值 %d 被重复消费", v)
			}
			received[v] = true
		}
	}()

	wg.Wait()
	c.Close()
	<-consumed

	for v, ok := range received {
		if !ok {
			t.Fatalf("值 %d 从未被消费", v)
		}
	}
}

func TestShardedCloseDrains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewShardedChan[int](ctx, 4)
	const n = 100
	for i := 0; i < n; i++ {
		_ = c.Put(uint64(i%4), i) // 撒到多个分片
	}
	c.Close()

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

func TestShardedPushAfterClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewShardedChan[int](ctx, 4)
	c.Close()
	if err := c.Put(0, 1); err != ErrClosed {
		t.Fatalf("Close 后 Put = %v, 期望 ErrClosed", err)
	}
	c.Close() // 幂等
}

// 泵睡眠时 Close 必须唤醒它（v0.2 死锁的回归测试，协议同源）。
func TestShardedCloseWithNoDataWakes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewShardedChan[int](ctx, 4)
	time.Sleep(50 * time.Millisecond) // 给泵时间睡进 <-sig
	c.Close()

	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Close 后泵未被唤醒，Done 未关闭")
	}
}

func TestShardedContextCancelDrains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	c := NewShardedChan[int](ctx, 4)
	for i := 0; i < 50; i++ {
		if err := c.Put(uint64(i), i); err != nil {
			t.Fatalf("Put = %v, 期望 nil", err)
		}
	}
	cancel()

	// AfterFunc 异步生效：轮询直到拒绝。
	deadline := time.After(2 * time.Second)
	for {
		if err := c.Put(999, 999); err == ErrClosed {
			break
		}
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
	// 50 个已入队值 + 轮询期间竞争成功的写入（契约允许）。
	if count < 50 {
		t.Fatalf("排空 %d 个值, 期望至少 50", count)
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done 未关闭")
	}
}
