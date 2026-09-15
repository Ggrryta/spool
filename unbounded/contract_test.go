package unbounded

import (
	"context"
	"testing"

	"github.com/Ggrryta/spool/internal/contract"
)

// TestContractUnboundedChan：unbounded.Chan 声明实现通道化无界队列契约
// （全局 FIFO），由统一一致性套件验证。
//
// 手动版 Unbounded（Put/Load/Get/Close 协议）不适用本套件的通道表面，
// 其语义由 FuzzUnbounded（影子模型对账）+ 单元测试覆盖。
func TestContractUnboundedChan(t *testing.T) {
	contract.Run(t, contract.Spec{
		Name:  "unbounded.Chan",
		Order: contract.GlobalFIFO,
		New: func() contract.Channel {
			ctx, cancel := context.WithCancel(context.Background())
			c := NewChan[int](ctx)
			return contract.Channel{
				Put:   func(_ context.Context, key, v int) error { return c.Put(v) },
				Close: func() { c.Close(); cancel() },
				Out:   c.Out(),
				Done:  c.Done(),
			}
		},
	})
}
