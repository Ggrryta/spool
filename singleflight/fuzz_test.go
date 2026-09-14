package singleflight

import (
	"testing"
)

// FuzzGroup 模糊测试：随机 Do/Forget/panic 序列，断言状态机完整性：
//
//   - 顺序调用（无并发 join）必须每次都执行 fn（shared=false）；
//   - 返回值与 key 一致；
//   - panic 路径（key 3 触发）不破坏后续调用——验证 map 清理，
//     包括 panic 后的 key 清除。
//
//	go test -fuzz=FuzzGroup -fuzztime=30s
func FuzzGroup(f *testing.F) {
	f.Add("dfdffdf")
	f.Add("dffddfdfff")
	f.Add("ffdf")
	f.Fuzz(func(t *testing.T, script string) {
		var g Group[int]

		fn := func(key int) func() (int, error) {
			return func() (int, error) {
				if key == 3 {
					panic("boom") // 指定 key 触发 panic 路径
				}
				return key * 10, nil
			}
		}

		for i, op := range []byte(script) {
			key := i % 4 // 0..3，key 3 会 panic
			switch op {
			case 'd':
				// panic key 会在调用方重新抛出，这里必须接住，
				// 否则 fuzz 引擎把它当作崩溃。
				func() {
					defer func() {
						if key == 3 {
							if recover() == nil {
								t.Fatal("panic key 未重新抛出")
							}
						}
					}()
					v, err, shared := g.Do("k", fn(key))
					if key != 3 {
						if err != nil {
							t.Fatalf("Do = %v, 期望 nil", err)
						}
						if v != key*10 {
							t.Fatalf("Do = %d, 期望 %d", v, key*10)
						}
					} else if err == nil {
						t.Fatal("panic key 应返回错误")
					}
					if shared {
						t.Fatal("顺序调用不应报告 shared（状态泄漏：key 未清理）")
					}
				}()
			case 'f':
				g.Forget("k")
			}
		}
	})
}
