// Package backoff 提供时间维度的流控原语：指数退避策略、可取消睡眠、
// 以及带重置语义的重试执行器。
//
// 算法提炼自 gRPC 的 connection-backoff 规范
// (https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md)，
// 与 spool 家族的共同契约：所有等待都尊重 ctx 取消——优雅停机对时间
// 流控同样适用。
//
// 与 cenkalti/backoff 的差异定位：本包不追求重试策略的全覆盖，而是
// 与 spool 队列原语组合（Put 失败 → backoff → 重试）并提供统一的
// ctx 契约。
package backoff

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// Config 是指数退避的全部配置项。
type Config struct {
	// BaseDelay 是首次失败后的退避时长。
	BaseDelay time.Duration
	// Multiplier 是每次失败后退避时长的放大系数，应大于 1。
	Multiplier float64
	// Jitter 是退避时长的随机化比例（±Jitter），避免集群同时重试
	// 造成的锁步（thundering herd）。
	Jitter float64
	// MaxDelay 是退避时长的上限。
	MaxDelay time.Duration
}

// DefaultConfig 返回按 gRPC connection-backoff 规范默认值构造的配置。
// 返回值而非导出变量：防止调用方无意修改共享默认值。
func DefaultConfig() Config {
	return Config{
		BaseDelay:  1.0 * time.Second,
		Multiplier: 1.6,
		Jitter:     0.2,
		MaxDelay:   120 * time.Second,
	}
}

// Strategy 声明退避策略：给定连续失败次数，返回下次重试前的等待时长。
// 实现必须无状态（可被任意多 goroutine 并发调用）。
type Strategy interface {
	Backoff(retries int) time.Duration
}

// Exponential 是指数退避 + 抖动的实现，零值不可用——请通过
// NewExponential 或显式赋值 Config 构造。
type Exponential struct {
	Config Config
}

// NewExponential 返回一个使用 cfg 的指数退避策略。
func NewExponential(cfg Config) Exponential {
	return Exponential{Config: cfg}
}

// Backoff 返回连续 retries 次失败后的等待时长：
//
//	min(BaseDelay * Multiplier^retries, MaxDelay) * (1 ± Jitter)
//
// 无状态，可被任意多 goroutine 并发调用。
func (e Exponential) Backoff(retries int) time.Duration {
	backoff, max := float64(e.Config.BaseDelay), float64(e.Config.MaxDelay)
	for backoff < max && retries > 0 {
		backoff *= e.Config.Multiplier
		retries--
	}
	if backoff > max {
		backoff = max
	}
	// 抖动：±Jitter，避免锁步。
	backoff *= 1 + e.Config.Jitter*(rand.Float64()*2-1)
	if backoff < 0 {
		return 0
	}
	return time.Duration(backoff)
}

// ErrReset 由重试函数 f 返回，指示 Runner 重置退避状态后继续重试
// （典型场景：f 内部发生了局部进展，如重连成功但认证失败）。
var ErrReset = errors.New("backoff: reset retry state")

var errPermanent = errors.New("backoff: permanent error")

type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Is 使 errors.Is(Permanent(e), errPermanent) 沿链匹配成立。
func (p *permanentError) Is(target error) bool { return target == errPermanent }

// Permanent 把 err 标记为终止性错误：Runner.Run 收到后立即返回该错误，
// 不再重试。用于 f 区分"可重试的暂时失败"与"不可重试的永久失败"
// （如参数校验失败、明确的 4xx）。
func Permanent(err error) error { return &permanentError{err: err} }

// Sleep 是可取消的时间流控原语：睡眠 d，或 ctx 取消/超时，先到为准。
//
// 返回 nil 表示睡满 d；返回 ctx.Err() 表示被取消。零依赖 timer 池化
// 之前的朴素实现，适用于中低频调用；热路径请配合长生命周期 Runner。
func Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Runner 按策略反复执行 f，直到成功、ctx 取消，或 f 返回终止性错误。
//
// Run 的语义（与 RunF 的"周期执行"不同，本类型面向重试）：
//
//   - f 返回 nil：成功，Run 返回 nil；
//   - f 返回 errors.Is(err, ErrReset)：重置退避计数，立即重试；
//   - f 返回 errors.Is(err, errPermanent)（即经 Permanent 包装的错误）：
//     终止重试，Run 返回该错误；
//   - f 返回其他非 nil 错误：退避 Backoff(retries) 后重试，retries++；
//   - ctx 取消：Run 返回 ctx.Err()（正在进行的退避会被立即打断）。
type Runner struct {
	Strategy Strategy
}

// NewRunner 返回使用 s 策略的 Runner。
func NewRunner(s Strategy) *Runner {
	return &Runner{Strategy: s}
}

// Run 按 r.Strategy 反复执行 f，直到成功、ctx 取消或 f 返回终止性错误。
// f 应尊重 ctx：Run 不会中断一个正在执行的 f。
func (r *Runner) Run(ctx context.Context, f func() error) error {
	retries := 0
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}

		err := f()
		switch {
		case err == nil:
			return nil
		case errors.Is(err, errPermanent):
			return err // 终止性错误：原样返回（Unwrap 可取回原始错误）
		case errors.Is(err, ErrReset):
			retries = 0
		default:
			retries++
		}

		// timer 已在循环顶部的 select 中到期并排空，直接 Reset 安全。
		timer.Reset(r.Strategy.Backoff(retries))
	}
}
