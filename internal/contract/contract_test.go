package contract

import (
	"context"
	"testing"

	"github.com/Ggrryta/spool/unbounded"
)

// referenceSpec 用已知正确的实现（unbounded.Chan）构造 spec，
// 用于验证套件对正确实现零误报。
func referenceSpec(name string) Spec {
	return Spec{
		Name:  name,
		Order: GlobalFIFO,
		New: func() Channel {
			ctx, cancel := context.WithCancel(context.Background())
			c := unbounded.NewChan[int](ctx)
			return Channel{
				Put:   func(key, v int) error { return c.Put(v) },
				Close: func() { c.Close(); cancel() },
				Out:   c.Out(),
				Done:  c.Done(),
			}
		},
	}
}

// TestSuitePassesReferenceImplementation：套件对正确实现必须零违反。
func TestSuitePassesReferenceImplementation(t *testing.T) {
	Run(t, referenceSpec("unbounded.Chan(Global)"))
}

// TestSuiteCatchesDroppingImplementation：阴性对照——对每 5 个值吞掉
// 1 个的坏实现，套件必须报丢失。证明断言非空洞：这不是一组永远绿灯
// 的摆设测试。
func TestSuiteCatchesDroppingImplementation(t *testing.T) {
	losing := Spec{
		Name:  "dropper(每5丢1)",
		Order: GlobalFIFO,
		New: func() Channel {
			ctx, cancel := context.WithCancel(context.Background())
			c := unbounded.NewChan[int](ctx)
			out := make(chan int)
			go func() {
				i := 0
				for v := range c.Out() {
					i++
					if i%5 == 0 {
						continue // 故意吞值
					}
					out <- v
				}
				close(out)
			}()
			return Channel{
				Put:   func(key, v int) error { return c.Put(v) },
				Close: func() { c.Close(); cancel() },
				Out:   out,
			}
		},
	}
	if violations := Verify(losing); len(violations) == 0 {
		t.Fatal("阴性对照失败：丢值实现未被套件发现（断言空洞）")
	}
}
