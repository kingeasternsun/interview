/*
 * @Description:
 * @Version: 2.0
 * @Author: kingeasternsun
 * @Date: 2021-03-01 11:45:42
 * @LastEditors: kingeasternsun
 * @LastEditTime: 2021-03-02 10:58:02
 * @FilePath: \three\timer_wheel.go
 */
package three

import (
	"errors"
	"sync"
	"time"
)

// 时间轮的长度，1440分钟，也就是一天
// const WheelLen = 1440 //写死不方便测试

type WTask struct {
	TS       time.Time   //时间戳
	Task     Task        //具体要执行的任务
	Par      interface{} //要传入的参数
	CycleCnt int         //超出时间轮多少圈
	Next     *WTask
}

//WheelTimer 时间轮定时器
type WheelTimer struct {
	mu       []sync.Mutex  //每一个格子对应一个lock
	Wheels   []*WTask      //
	Tail     []*WTask      // 执行链表的最后一个节点
	dur      time.Duration //定时器的检查周期
	curIndex int           //当前所在第几个格子
	wheelLen int
	once     sync.Once
}

//NewWheelTask 创建任务定时器
func NewWheelTask(dur time.Duration, wheelLen int) *WheelTimer {

	t := &WheelTimer{
		dur:      dur,
		wheelLen: wheelLen,
		Wheels:   make([]*WTask, wheelLen),
		Tail:     make([]*WTask, wheelLen),
		mu:       make([]sync.Mutex, wheelLen),
	}

	// 初始化一个冗余的头部节点，便于后面的插入 删除
	for i := range t.Wheels {
		t.Wheels[i] = &WTask{}
		t.Tail[i] = t.Wheels[i]
	}

	return t
}

//AddTimeOut
func (t *WheelTimer) AddTimeOut(d time.Duration, ts time.Time, task Task, par interface{}) {

	//距离当前位置要移动多少格子
	steps := t.curIndex + int(d/t.dur)
	newTask := &WTask{
		TS:       ts,
		Task:     task,
		Par:      par,
		CycleCnt: steps / t.wheelLen,
	}

	t.mu[steps%t.wheelLen].Lock()
	t.Tail[steps%t.wheelLen].Next = newTask
	t.Tail[steps%t.wheelLen] = newTask
	t.mu[steps%t.wheelLen].Unlock()

}

//Add 添加任务
func (t *WheelTimer) Add(ts time.Time, task Task, par interface{}) (err error) {
	d := ts.Sub(time.Now())
	if d < 0 {
		return errors.New("ts should after now()")
	}

	t.AddTimeOut(d, ts, task, par)
	return

}

//Run 执行
func (t *WheelTimer) Run() {

	t.once.Do(func() {
		tk := time.NewTicker(t.dur)
		for {
			<-tk.C
			t.checkTask()
			t.curIndex++
		}
	})

}

func (t *WheelTimer) checkTask() {
	t.mu[t.curIndex%t.wheelLen].Lock()
	defer t.mu[t.curIndex%t.wheelLen].Unlock()
	pre := t.Wheels[t.curIndex%t.wheelLen]
	p := pre.Next

	for p != nil {

		if p.CycleCnt > 0 {
			p.CycleCnt--
			pre, p = p, p.Next
			continue
		}

		go p.Task(p.Par)

		//删除这个节点
		pre.Next = p.Next
		p = p.Next
	}
	t.Tail[t.curIndex%t.wheelLen] = pre

	return
}
