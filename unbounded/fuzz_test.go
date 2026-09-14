package unbounded

import (
	"testing"
)

// FuzzUnbounded 用影子模型对账的模糊测试。
//
// 随机脚本驱动 Put/Load/读通道/Close 的任意序列，影子模型精确镜像
// 实现的内部状态迁移（chanVal = 容量 1 的信号通道、backlog、
// closing/closed），每个操作后断言两者一致：
//
//	p  Put       值递增计数器（FIFO 顺序检查的依据）
//	l  Load      backlog → 通道，或触发关闭
//	r  读通道    非阻塞读（select+default），对账值或关闭状态
//	c  Close     幂等
//
// 结束后排空，断言无丢失、无重复、顺序保持。语义偏离（不只是崩溃）
// 会被立即捕获。
//
//	go test -fuzz=FuzzUnbounded -fuzztime=30s
func FuzzUnbounded(f *testing.F) {
	f.Add("pplprl")
	f.Add("plpcrlrl")
	f.Add("cplr")
	f.Add("ppppclrrlrl")
	f.Fuzz(func(t *testing.T, script string) {
		b := New[int]()

		// 影子模型：镜像实现的可见状态。
		var (
			chanVal  int  // 通道中待读的值（通道容量 1）
			hasChan  bool // 通道是否有值
			backlog  []int
			closing  bool
			closed   bool
			expected []int // 已成功 Put 的全部值，按序
			next     int   // Put 的值计数器
		)

		put := func() {
			err := b.Put(next)
			if closing {
				if err != ErrClosed {
					t.Fatalf("Put 在 Close 之后应返回 ErrClosed，得到 %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Put = %v, 期望 nil", err)
			}
			// 镜像 Put：backlog 空 且 通道空 → 直递通道；否则入 backlog。
			if len(backlog) == 0 && !hasChan {
				chanVal, hasChan = next, true
			} else {
				backlog = append(backlog, next)
			}
			expected = append(expected, next)
			next++
		}

		load := func() {
			b.Load()
			// 镜像 Load：backlog 非空且通道空 → 搬运；backlog 空且
			// closing → 关闭（通道里有值也允许关闭：Go 语义下接收者
			// 先取完缓冲值再观察到关闭）。
			if len(backlog) > 0 && !hasChan {
				chanVal, hasChan = backlog[0], true
				backlog = backlog[1:]
			} else if len(backlog) == 0 && closing && !closed {
				closed = true
			}
		}

		recv := func() {
			select {
			case v, ok := <-b.Get():
				if !ok {
					if !closed {
						t.Fatal("通道提前关闭")
					}
					return
				}
				if !hasChan {
					t.Fatalf("收到意外值 %d：模型认为通道为空", v)
				}
				if v != chanVal {
					t.Fatalf("FIFO 被破坏：收到 %d, 期望 %d", v, chanVal)
				}
				if len(expected) == 0 || expected[0] != v {
					t.Fatalf("顺序错误：收到 %d, 不在队首", v)
				}
				expected = expected[1:]
				hasChan = false
			default:
				if hasChan {
					t.Fatal("模型认为通道有值，非阻塞读却失败")
				}
			}
		}

		closeOp := func() {
			b.Close()
			if closing {
				return // 幂等
			}
			closing = true
			if len(backlog) == 0 {
				closed = true
			}
		}

		for _, op := range []byte(script) {
			switch op {
			case 'p':
				put()
			case 'l':
				load()
			case 'r':
				recv()
			case 'c':
				closeOp()
			}
		}

		// 排空阶段：关闭（若未关）→ 循环读+Load 直到观察到关闭。
		// 预算在循环前定格（expected 会缩小）；每个值需要 load+recv
		// 两次迭代，预算按 4 倍给足。
		closeOp()
		drained := false
		budget := 4*len(expected) + 16
		for i := 0; i < budget; i++ {
			if hasChan {
				recv()
				continue
			}
			if closed {
				recv() // 必须观察到通道关闭
				drained = true
				break
			}
			load()
		}
		if !drained {
			t.Fatal("排空循环超出预算（疑似状态卡死）")
		}

		if len(expected) != 0 {
			t.Fatalf("排空后仍有 %d 个值未被送达（丢失）", len(expected))
		}
		if len(backlog) != 0 || hasChan {
			t.Fatal("排空后模型状态不干净")
		}
	})
}
