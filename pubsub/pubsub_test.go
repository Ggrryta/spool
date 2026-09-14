package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"
)

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !cond() {
		select {
		case <-deadline:
			t.Fatal("等待条件超时")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestPublishDeliversToAllSubscribers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := New(ctx)
	var mu sync.Mutex
	var gotA, gotB []string
	if !p.Subscribe("a", func(payload any) { mu.Lock(); gotA = append(gotA, payload.(string)); mu.Unlock() }) {
		t.Fatal("Subscribe(a) = false")
	}
	if !p.Subscribe("b", func(payload any) { mu.Lock(); gotB = append(gotB, payload.(string)); mu.Unlock() }) {
		t.Fatal("Subscribe(b) = false")
	}

	p.Publish("msg1")
	p.Publish("msg2")

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(gotA) == 2 && len(gotB) == 2
	})
	mu.Lock()
	defer mu.Unlock()
	if gotA[0] != "msg1" || gotA[1] != "msg2" || gotB[0] != "msg1" || gotB[1] != "msg2" {
		t.Fatalf("分发顺序错误: a=%v b=%v", gotA, gotB)
	}
}

func TestDuplicateSubscribeRejected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := New(ctx)
	if !p.Subscribe("x", func(any) {}) {
		t.Fatal("第一次 Subscribe(x) = false")
	}
	if p.Subscribe("x", func(any) {}) {
		t.Fatal("重复 Subscribe(x) = true")
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := New(ctx)
	var count int
	var mu sync.Mutex
	p.Subscribe("s", func(any) { mu.Lock(); count++; mu.Unlock() })

	p.Publish("1")
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return count == 1 })

	p.Unsubscribe("s")
	p.Publish("2")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("取消订阅后 count = %d, 期望 1", count)
	}
}

func TestContextCancelShutsDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	p := New(ctx)
	executed := make(chan struct{})
	p.Subscribe("s", func(any) { close(executed) })
	p.Publish("1")

	<-executed
	cancel()

	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("取消后 Done 未关闭")
	}
}

func TestSlowSubscriberDoesNotBlockPublisher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := New(ctx)
	release := make(chan struct{})
	p.Subscribe("slow", func(any) { <-release })

	start := time.Now()
	for i := 0; i < 10; i++ {
		p.Publish(i)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Publish 阻塞了 %v", d)
	}
	close(release) // 让订阅者完成，保证停机流程结束
}
