/*
 * @Description:
 * @Version: 2.0
 * @Author: kingeasternsun
 * @Date: 2021-03-02 10:10:53
 * @LastEditors: kingeasternsun
 * @LastEditTime: 2021-03-02 10:56:44
 * @FilePath: \three\timer_wheel_test.go
 */
package three

import (
	"sync"
	"testing"
	"time"
)

func TestWheelTimerInSecond(t *testing.T) {

	timeUnit := time.Second //调度的最小时间单位

	type Res struct {
		name   string
		tc     time.Time //期望触发时间
		realtc time.Time //实际执行时间
	}
	var results []Res

	type Arg struct {
		name string
		tc   time.Time //期望触发时间
	}

	tests := []Arg{
		{"a", time.Now().Add(2 * timeUnit)},
		{"b", time.Now().Add(1 * timeUnit)},
		{"c", time.Now().Add(3 * timeUnit)},
		{"d", time.Now().Add(4 * timeUnit)},
		{"e", time.Now().Add(5 * timeUnit)},
		{"f", time.Now().Add(7 * timeUnit)},
		{"g", time.Now().Add(6 * timeUnit)},
		{"h", time.Now().Add(5 * timeUnit)},
	}

	mu := sync.Mutex{}

	var task = func(par interface{}) {

		arg := par.(Arg)
		mu.Lock()
		results = append(results, Res{
			name:   arg.name,
			tc:     arg.tc,
			realtc: time.Now(),
		})
		mu.Unlock()
	}

	tt := NewWheelTask(timeUnit, 4)
	go tt.Run()

	for _, test := range tests {
		test := test
		go tt.Add(test.tc, task, test)
	}

	time.Sleep(9 * timeUnit)

	if len(results) != len(tests) {
		t.Errorf(" reulst num = %v, want %v", len(results), len(tests))
	}

	for _, res := range results {
		if abs(res.realtc.Unix()-res.tc.Unix())/int64(timeUnit/time.Second) > 1 {
			t.Errorf("realtc() = %v, want %v", res.realtc.Unix(), res.tc.Unix())
		}

	}

	return
}
