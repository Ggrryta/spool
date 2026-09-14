package singleflight

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoSameKeyExecutesOnce(t *testing.T) {
	var g Group[int]
	var executions atomic.Int64

	const n = 10
	var wg sync.WaitGroup
	results := make([]int, n)
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err, _ := g.Do("key", func() (int, error) {
				executions.Add(1)
				time.Sleep(50 * time.Millisecond) // 拉大竞争窗口
				return 42, nil
			})
			if err != nil {
				t.Errorf("Do = %v, 期望 nil", err)
			}
			results[i] = v
		}()
	}
	wg.Wait()

	if got := executions.Load(); got != 1 {
		t.Fatalf("fn 执行了 %d 次, 期望 1 次", got)
	}
	for i, v := range results {
		if v != 42 {
			t.Fatalf("调用者 %d 拿到 %d, 期望 42", i, v)
		}
	}
}

func TestDoDifferentKeysExecuteSeparately(t *testing.T) {
	var g Group[string]

	v1, err1, shared1 := g.Do("a", func() (string, error) { return "A", nil })
	v2, err2, shared2 := g.Do("b", func() (string, error) { return "B", nil })
	if v1 != "A" || v2 != "B" || err1 != nil || err2 != nil {
		t.Fatalf("得到 (%q,%v) 和 (%q,%v)", v1, err1, v2, err2)
	}
	if shared1 || shared2 {
		t.Fatal("单独调用不应报告 shared")
	}
}

func TestDoSharesError(t *testing.T) {
	var g Group[int]
	wantErr := errors.New("boom")

	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := 0; i < 5; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err, _ := g.Do("k", func() (int, error) {
				time.Sleep(50 * time.Millisecond)
				return 0, wantErr
			})
			errs[i] = err
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if !errors.Is(err, wantErr) {
			t.Fatalf("调用者 %d 得到 %v, 期望 %v", i, err, wantErr)
		}
	}
}

func TestForget(t *testing.T) {
	var g Group[int]

	started := make(chan struct{})
	release := make(chan struct{})
	fnDone := make(chan struct{})
	go func() {
		defer close(fnDone)
		g.Do("k", func() (int, error) {
			close(started)
			<-release
			return 1, nil
		})
	}()

	<-started
	g.Forget("k") // "k" 仍在进行中，但下一次 Do 必须重新发起

	v, err, shared := g.Do("k", func() (int, error) { return 2, nil })
	if v != 2 || err != nil {
		t.Fatalf("Forget 后 Do = (%d, %v), 期望 (2, nil)", v, err)
	}
	if shared {
		t.Fatal("Forget 后的 Do 不应与进行中的调用共享结果")
	}

	close(release)
	<-fnDone
}

func TestPanicReachesWaitersAsError(t *testing.T) {
	var g Group[int]

	started := make(chan struct{})
	go func() {
		defer func() { recover() }() // 发起者会重新 panic；在这里吞掉
		g.Do("k", func() (int, error) {
			close(started)
			// 给下面的等待者留出加入进行中调用的时间；在调用完成前加入的
			// 调用者必须以错误的形式收到 panic，而不是永远阻塞。
			time.Sleep(100 * time.Millisecond)
			panic("kaboom")
		})
	}()

	<-started
	// 第一次探测会加入进行中的调用（key 存在），阻塞到 panic 完成后返回
	// shared=true 和 panic 错误。之后的探测会发起新调用（err=nil），所以
	// 只接受 shared 的结果。
	for i := 0; i < 100; i++ {
		_, err, shared := g.Do("k", func() (int, error) { return 0, nil })
		if shared {
			if err == nil || !strings.Contains(err.Error(), "kaboom") {
				t.Fatalf("等待者得到 err %v, 期望包含 kaboom 的 panic 错误", err)
			}
			return
		}
	}
	t.Fatal("探测从未加入进行中的调用")
}
