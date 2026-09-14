package unbounded

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPutLoadFIFO(t *testing.T) {
	b := New[int]()
	for i := 0; i < 10; i++ {
		if err := b.Put(i); err != nil {
			t.Fatalf("Put(%d) = %v, 期望 nil", i, err)
		}
	}
	for i := 0; i < 10; i++ {
		v, ok := <-b.Get()
		if !ok || v != i {
			t.Fatalf("收到 %d (ok=%v), 期望 %d", v, ok, i)
		}
		b.Load()
	}
}

func TestPutAfterClose(t *testing.T) {
	b := New[int]()
	b.Close()
	if err := b.Put(1); err != ErrClosed {
		t.Fatalf("Close 后 Put = %v, 期望 ErrClosed", err)
	}
	b.Close() // 幂等
}

func TestCloseDrainsThenClosesChannel(t *testing.T) {
	b := New[int]()
	for i := 0; i < 5; i++ {
		if err := b.Put(i); err != nil {
			t.Fatalf("Put = %v, 期望 nil", err)
		}
	}
	b.Close()

	// 所有已缓冲的值必须仍然送达，然后通道才关闭。
	for i := 0; i < 5; i++ {
		v, ok := <-b.Get()
		if !ok || v != i {
			t.Fatalf("收到 %d (ok=%v), 期望 %d", v, ok, i)
		}
		b.Load()
	}
	if _, ok := <-b.Get(); ok {
		t.Fatal("排空后 Get 通道未关闭")
	}
}

func TestCloseWithEmptyBacklogClosesImmediately(t *testing.T) {
	b := New[int]()
	b.Close()
	if _, ok := <-b.Get(); ok {
		t.Fatal("backlog 为空时 Close 应立即关闭通道")
	}
}

func TestConcurrentProducers(t *testing.T) {
	const producers = 8
	const perProducer = 2000

	b := New[int]()
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if err := b.Put(p*perProducer + i); err != nil {
					t.Errorf("Put = %v, 期望 nil", err)
				}
			}
		}(p)
	}

	received := make([]bool, producers*perProducer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for v := range b.Get() {
			if received[v] {
				t.Errorf("值 %d 被重复送达", v)
			}
			received[v] = true
			b.Load()
		}
	}()

	wg.Wait()
	b.Close()
	<-done

	for v, ok := range received {
		if !ok {
			t.Fatalf("值 %d 从未被送达", v)
		}
	}
}

func TestChanAutoPump(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewChan[int](ctx)
	for i := 0; i < 100; i++ {
		if err := c.Put(i); err != nil {
			t.Fatalf("Put = %v, 期望 nil", err)
		}
	}

	for i := 0; i < 100; i++ {
		v, ok := <-c.Out()
		if !ok || v != i {
			t.Fatalf("收到 %d (ok=%v), 期望 %d", v, ok, i)
		}
	}

	c.Close()
	select {
	case _, ok := <-c.Out():
		if ok {
			t.Fatal("backlog 为空时 Close 后收到了意外的值")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close 后 Out 通道未关闭")
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("排空后 Done 未关闭")
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
	cancel() // 不允许新的 Put，但已缓冲的 50 个值必须排空

	// context.AfterFunc 是异步的：轮询直到 Close 生效。
	// 与 Close 竞争的 Put 仍可能成功——这是允许的行为——所以记录多进了
	// 几个值，排空时也应包含它们。
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
