package bounded

import (
	"context"
	"testing"

	"github.com/Ggrryta/spool/internal/contract"
)

// TestContractBoundedChan：bounded.Chan 声明实现通道化无界队列契约中
// 除"Put 永不阻塞"外的全部条款（GlobalFIFO + 排空关闭 + Done），
// 由统一一致性套件验证。差异（缓冲满时阻塞）是本类型的功能本身。
func TestContractBoundedChan(t *testing.T) {
	contract.Run(t, contract.Spec{
		Name:  "bounded.Chan",
		Order: contract.GlobalFIFO,
		New: func() contract.Channel {
			c := NewChan[int](64)
			return contract.Channel{
				Put:   func(ctx context.Context, key, v int) error { return c.Put(ctx, v) },
				Close: c.Close,
				Out:   c.Out(),
				Done:  c.Done(),
			}
		},
	})
}
