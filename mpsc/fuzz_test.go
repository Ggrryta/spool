package mpsc

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// FuzzQueue 用影子模型对账的模糊测试：随机 Push/Peek/Pop/Close 序列，
// 断言值、顺序、关闭语义与模型完全一致。
//
//	go test -fuzz=FuzzQueue -fuzztime=30s
func FuzzQueue(f *testing.F) {
	f.Add("ppkkpkck")
	f.Add("cpk")
	f.Add("pppppckkkkk")
	f.Fuzz(func(t *testing.T, script string) {
		q := New[int]()
		var (
			expected []int
			closing  bool
			next     int
		)

		for _, op := range []byte(script) {
			switch op {
			case 'p':
				err := q.Push(next)
				if closing {
					if err != ErrClosed {
						t.Fatalf("Push 在 Close 后应返回 ErrClosed，得到 %v", err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("Push = %v, 期望 nil", err)
				}
				expected = append(expected, next)
				next++
			case 'k':
				v, ok := q.Peek()
				if ok {
					if len(expected) == 0 {
						t.Fatalf("Peek 到意外值 %d：模型认为队列为空", v)
					}
					if v != expected[0] {
						t.Fatalf("FIFO 被破坏：Peek 得到 %d, 期望 %d", v, expected[0])
					}
					q.Pop()
					expected = expected[1:]
				} else if len(expected) != 0 {
					t.Fatalf("Peek 为空但模型还有 %d 个值", len(expected))
				}
			case 'c':
				q.Close()
				closing = true
			}
		}

		// 排空：Close 后 Peek/Pop 直到空。
		// 注意预算要在循环前定格：expected 在循环内缩小，
		// 若用 len(expected) 做边界会跟着缩水导致提前退出。
		q.Close()
		closing = true
		budget := len(expected) + 4
		for i := 0; i < budget; i++ {
			v, ok := q.Peek()
			if !ok {
				break
			}
			if v != expected[0] {
				t.Fatalf("排空阶段 FIFO 被破坏：%d != %d", v, expected[0])
			}
			q.Pop()
			expected = expected[1:]
		}
		if len(expected) != 0 {
			t.Fatalf("排空后仍有 %d 个值（丢失）", len(expected))
		}
		if !q.Drained() {
			t.Fatal("关闭且排空后 Drained 应为 true")
		}
	})
}

// FuzzShardedChan 在真实并发（泵 goroutine 活跃）下模糊测试：
// 随机 key 的 Put、Close、让出调度的任意序列，结束后对账——
// 每 key 的子序列严格有序、总量无丢失无重复。
//
//	go test -fuzz=FuzzShardedChan -fuzztime=30s
func FuzzShardedChan(f *testing.F) {
	f.Add("abcgdcba")
	f.Add("acac")
	f.Add("ggabbcgcd")
	f.Fuzz(func(t *testing.T, script string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const keys = 4
		c := NewShardedChan[int](ctx, 4)

		// 每 key 的期望子序列与已成功 Put 的总数。
		var expected [keys][]int
		var counters [keys]int
		putTotal := 0
		closed := false

		for _, op := range []byte(script) {
			switch {
			case op >= 'a' && op < 'a'+keys:
				k := int(op - 'a')
				v := k*1_000_000 + counters[k]
				err := c.Put(uint64(k), v)
				if closed {
					if err != ErrClosed {
						t.Fatalf("Close 后 Put 应返回 ErrClosed，得到 %v", err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("Put = %v, 期望 nil", err)
				}
				expected[k] = append(expected[k], v)
				counters[k]++
				putTotal++
			case op == 'g':
				runtime.Gosched() // 让泵有机会交错搬运
			case op == 'c':
				c.Close()
				closed = true
			}
		}

		if !closed {
			c.Close()
		}

		// 排空对账（带死锁看门狗：协议若坏，这里会挂死）。
		type item struct {
			key, v int
		}
		done := make(chan []item)
		go func() {
			var got []item
			for v := range c.Out() {
				got = append(got, item{v / 1_000_000, v})
			}
			done <- got
		}()
		select {
		case got := <-done:
			if len(got) != putTotal {
				t.Fatalf("送达 %d 个值, 期望 %d（丢失或重复）", len(got), putTotal)
			}
			var heads [keys]int
			for _, it := range got {
				if heads[it.key] >= len(expected[it.key]) {
					t.Fatalf("key %d 收到多余值 %d", it.key, it.v)
				}
				if it.v != expected[it.key][heads[it.key]] {
					t.Fatalf("key %d 顺序被破坏：%d != %d",
						it.key, it.v, expected[it.key][heads[it.key]])
				}
				heads[it.key]++
			}
		case <-time.After(5 * time.Second):
			t.Fatal("排空疑似死锁（Out 未在 5s 内关闭）")
		}

		select {
		case <-c.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("Done 未关闭")
		}
	})
}
