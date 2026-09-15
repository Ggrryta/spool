// Package contract 定义并检验 spool 全部通道化队列共享的统一语义契约
// （v0.4 契约统一化，完整文档见 docs/CONTRACTS.md）。
//
// 任何声称实现本契约的类型，都必须通过 Run 执行的全套断言；各包的
// contract_test.go 以各自的适配器调用 Run——任何语义偏离即 CI 红。
//
// 契约要点（详见 CONTRACTS.md）：
//
//   - Put 永不阻塞；Close 生效后返回非 nil 错误，生效前竞态窗口内允许成功
//   - Close 幂等；已缓冲值全部送达后读通道关闭（优雅排空）
//   - 顺序：GlobalFIFO（全局）或 PerKeyFIFO（同 key 严格有序，跨 key 不保证）
//   - Done（如有）：排空 + 泵退出后关闭，goroutine 无泄漏
//
// 本包仅供库内各包的测试使用（internal），不构成公共 API。
package contract

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// OrderMode 声明被测对象的顺序保证强度。
type OrderMode int

const (
	GlobalFIFO OrderMode = iota // 全局 FIFO：送达顺序 = Put 提交顺序
	PerKeyFIFO                  // 同 key FIFO：跨 key 顺序不保证
)

// Channel 是被测对象须满足的最小表面。key 参数在 GlobalFIFO 模式下被
// 适配器忽略；PerKeyFIFO 模式下决定值所属的顺序域。
type Channel struct {
	// Put 写入一个值。无界实现忽略 ctx（永不阻塞）；有界实现（bounded）
	// 在缓冲满时阻塞，直到成功、关闭或 ctx 取消。
	Put   func(ctx context.Context, key, v int) error
	Close func()          // 幂等；触发优雅排空
	Out   <-chan int      // 排空后关闭
	Done  <-chan struct{} // 可选：泵退出信号（无则为 nil）
}

// Spec 描述一个被测实现。每项检查都会调用 New 获取全新实例。
type Spec struct {
	Name  string
	New   func() Channel
	Order OrderMode
	Keys  int // Order == PerKeyFIFO 时的分片数（= 并发检查的生产者数）
}

// Verify 执行全部契约断言，返回违反描述（空 = 通过）。
// 独立于 *testing.T 以支持阴性对照：对已知有缺陷的实现运行，
// 期待非空结果——证明断言本身不是空洞的。
func Verify(spec Spec) []string {
	var v []string
	violate := func(check, format string, args ...any) {
		v = append(v, fmt.Sprintf("[%s] %s", check, fmt.Sprintf(format, args...)))
	}

	// drain 读取 Out 直到关闭；5s 无进展视为疑似死锁。
	drain := func(check string, c Channel, onValue func(int)) (n int) {
		for {
			select {
			case v, ok := <-c.Out:
				if !ok {
					return n
				}
				if onValue != nil {
					onValue(v)
				}
				n++
			case <-time.After(5 * time.Second):
				violate(check, "排空超时（疑似死锁或丢失关闭信号）")
				return n
			}
		}
	}

	// waitDone 等待 Done 关闭（如有）。
	waitDone := func(check string, c Channel) {
		if c.Done == nil {
			return
		}
		select {
		case <-c.Done:
		case <-time.After(5 * time.Second):
			violate(check, "Done 未在超时内关闭")
		}
	}

	// ---- 1. 关闭后写入必须被拒绝 ----
	{
		const check = "PutAfterClose"
		ctx := context.Background()
		c := spec.New()
		c.Close()
		if err := c.Put(ctx, 0, 1); err == nil {
			violate(check, "Close 后 Put 未返回错误")
		}
		c.Close() // 幂等
	}

	// ---- 2. 幂等关闭 + 空排空 + 终止 ----
	{
		const check = "CloseIdempotent"
		c := spec.New()
		c.Close()
		c.Close()
		drain(check, c, nil)
		waitDone(check, c)
	}

	// ---- 3. 顺序排空：生产与消费并发，验证顺序与完备 ----
	// （写入放 goroutine：有界实现满时 Put 会阻塞，必须并发排空才不死锁；
	//   对无界实现两者等价。）
	{
		const check = "DrainFIFO"
		const n = 200
		c := spec.New()
		ctx := context.Background()
		var got []int
		putDone := make(chan struct{})
		go func() {
			defer close(putDone)
			for i := 0; i < n; i++ {
				key := 0
				if spec.Order == PerKeyFIFO {
					key = i % spec.Keys
				}
				if err := c.Put(ctx, key, i); err != nil {
					violate(check, "Put(%d) = %v, 期望 nil", i, err)
					return
				}
			}
			c.Close()
		}()

		drain(check, c, func(v int) { got = append(got, v) })
		<-putDone
		waitDone(check, c)

		if len(got) != n {
			violate(check, "送达 %d 个值, 期望 %d", len(got), n)
		}
		switch spec.Order {
		case GlobalFIFO:
			for i, val := range got {
				if val != i {
					violate(check, "全局 FIFO 被破坏：位置 %d 收到 %d, 期望 %d", i, val, i)
					break
				}
			}
		case PerKeyFIFO:
			last := make([]int, spec.Keys)
			for i := range last {
				last[i] = -1
			}
			for _, val := range got {
				k := val % spec.Keys
				seq := val / spec.Keys
				if seq <= last[k] {
					violate(check, "key %d 顺序被破坏：%d <= %d", k, seq, last[k])
					break
				}
				last[k] = seq
			}
		}
	}

	// ---- 4. 并发完备性：无丢失、无重复、（PerKey）同 key 有序 ----
	{
		const check = "ConcurrentNoLossNoDup"
		const perProducer = 500
		c := spec.New()

		producers, keys := 4, 1
		if spec.Order == PerKeyFIFO {
			producers, keys = spec.Keys, spec.Keys
		}

		var success atomic.Int64
		var seq atomic.Int64
		var wg sync.WaitGroup

		var mu sync.Mutex
		var received []int

		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				ctx := context.Background()
				for i := 0; i < perProducer; i++ {
					var key, v int
					switch spec.Order {
					case GlobalFIFO:
						key, v = 0, int(seq.Add(1))
					case PerKeyFIFO:
						key = p // 单写者/key：FIFO 可断言
						v = key*1_000_000 + i
					}
					if err := c.Put(ctx, key, v); err != nil {
						return // 关闭竞态窗口内的拒绝由 EarlyClose 场景覆盖
					}
					success.Add(1)
				}
			}(p)
		}

		go func() {
			wg.Wait()
			c.Close()
		}()

		drain(check, c, func(v int) {
			mu.Lock()
			received = append(received, v)
			mu.Unlock()
		})
		waitDone(check, c)

		mu.Lock()
		defer mu.Unlock()
		if len(received) != int(success.Load()) {
			violate(check, "送达 %d / Put 成功 %d（丢失或重复）",
				len(received), success.Load())
		}

		seen := make(map[int]bool, len(received))
		last := make([]int, keys)
		for i := range last {
			last[i] = -1
		}
		for _, val := range received {
			if seen[val] {
				violate(check, "值 %d 重复送达", val)
			}
			seen[val] = true
			if spec.Order == PerKeyFIFO {
				k := val / 1_000_000
				seqNum := val % 1_000_000
				if seqNum <= last[k] {
					violate(check, "key %d 顺序被破坏：%d <= %d", k, seqNum, last[k])
				}
				last[k] = seqNum
			}
		}
	}

	// ---- 5. 提前关闭竞争窗口：送达数 == 成功 Put 数 ----
	// 排空必须与 Close 并发：有界实现下，阻塞中的 Put 要等到
	// "Close 唤醒 + 消费者排空"才能返回。
	{
		const check = "EarlyCloseRace"
		c := spec.New()
		var success atomic.Int64
		var wg sync.WaitGroup

		producers := 4
		if spec.Order == PerKeyFIFO {
			producers = spec.Keys
		}
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				ctx := context.Background()
				for i := 0; ; i++ {
					var key, v int
					switch spec.Order {
					case GlobalFIFO:
						key, v = 0, p*1_000_000+i
					case PerKeyFIFO:
						key, v = p, p*1_000_000+i
					}
					if err := c.Put(ctx, key, v); err != nil {
						return // 关闭生效，停止写入
					}
					success.Add(1)
				}
			}(p)
		}
		go func() {
			time.Sleep(5 * time.Millisecond) // 让部分值入队，制造竞争窗口
			c.Close()
		}()

		var got int
		seen := make(map[int]bool)
		drain(check, c, func(v int) {
			if seen[v] {
				violate(check, "值 %d 重复送达", v)
			}
			seen[v] = true
			got++
		})
		wg.Wait()
		waitDone(check, c)

		if int64(got) != success.Load() {
			violate(check, "送达 %d / Put 成功 %d（丢失或重复）", got, success.Load())
		}
	}

	return v
}

// Run 对 spec 执行全套契约断言，任何违反即 t.Errorf。
func Run(t *testing.T, spec Spec) {
	t.Helper()
	for _, v := range Verify(spec) {
		t.Errorf("%s: %s", spec.Name, v)
	}
}
