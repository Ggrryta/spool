// Package unbounded 提供泛型的无界队列：生产者永不阻塞，消费者拿到的是一个
// 可 select 的原生 channel。
//
// 核心类型 Unbounded 不会启动任何 goroutine：Put 永不阻塞（内部缓冲），
// 消费者阻塞在 Get 返回的 channel 上。由于读端是真 channel，消费者可以把它
// 和 context 取消等事件一起放进 select——这是 sync.Cond 方案做不到的。
//
// 两种消费方式：
//
//   - 手动版（零开销）：调用 Get，每次成功接收一个值后必须调用 Load，
//     把下一条缓冲值推进通道。
//   - 自动版（NewChan）：由一个泵 goroutine 自动调用 Load。更好用，
//     代价是每个值多一次 goroutine 调度和一跳转发。
package unbounded
