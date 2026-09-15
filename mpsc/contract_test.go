package mpsc

import (
	"context"
	"testing"

	"github.com/Ggrryta/spool/internal/contract"
)

// TestContractMpscChan：mpsc.Chan 声明实现通道化无界队列契约
// （全局 FIFO，无锁生产端），由统一一致性套件验证。
func TestContractMpscChan(t *testing.T) {
	contract.Run(t, contract.Spec{
		Name:  "mpsc.Chan",
		Order: contract.GlobalFIFO,
		New: func() contract.Channel {
			ctx, cancel := context.WithCancel(context.Background())
			c := NewChan[int](ctx)
			return contract.Channel{
				Put:   func(key, v int) error { return c.Put(v) },
				Close: func() { c.Close(); cancel() },
				Out:   c.Out(),
				Done:  c.Done(),
			}
		},
	})
}

// TestContractShardedChan：ShardedChan 声明实现 PerKeyFIFO 契约
// （同 key 严格有序，跨 key 不保证）。
func TestContractShardedChan(t *testing.T) {
	contract.Run(t, contract.Spec{
		Name:  "mpsc.ShardedChan",
		Order: contract.PerKeyFIFO,
		Keys:  4,
		New: func() contract.Channel {
			ctx, cancel := context.WithCancel(context.Background())
			c := NewShardedChan[int](ctx, 4)
			return contract.Channel{
				Put:   func(key, v int) error { return c.Put(uint64(key), v) },
				Close: func() { c.Close(); cancel() },
				Out:   c.Out(),
				Done:  c.Done(),
			}
		},
	})
}
