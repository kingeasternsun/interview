/*
 * @Description:tasktimer
 * @Version: 2.0
 * @Author: kingeasternsun
 * @Date: 2021-02-25 17:01:05
 * @LastEditors: kingeasternsun
 * @LastEditTime: 2021-02-26 14:45:20
 * @FilePath: \three\task_timer.go

 利用最小堆，实现简单的定时任务调度器。
 在实际项目中，定时器和任务处理要解耦分开，使用专门的woker池来执行任务 。
*/
package three

import (
	"container/heap"
	"sync"
	"time"
)

type Task func(v interface{})
type TaskItem struct {
	TS   time.Time   //时间戳
	Task Task        //具体要执行的任务
	Par  interface{} //要传入的参数
}

type TaskHeap []TaskItem

func (h TaskHeap) Len() int           { return len(h) }
func (h TaskHeap) Less(i, j int) bool { return h[i].TS.Before(h[j].TS) }
func (h TaskHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *TaskHeap) Push(x interface{}) {
	*h = append(*h, x.(TaskItem))
}
func (h *TaskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type TaskTimer struct {
	Heap TaskHeap
	mu   sync.Mutex
	dur  time.Duration //定时器的检查周期
}

//NewTaskTimer 创建任务定时器
func NewTaskTimer(dur time.Duration) *TaskTimer {

	return &TaskTimer{
		Heap: TaskHeap{},
		mu:   sync.Mutex{},
		dur:  dur,
	}
}

//Add 添加任务
func (t *TaskTimer) Add(tm time.Time, task Task, par interface{}) {
	t.mu.Lock()
	heap.Push(&t.Heap, TaskItem{TS: tm, Task: task, Par: par}) //因为题目要求精度是分钟，所以这里用秒也就足够了
	t.mu.Unlock()
}

//AddTimeOut 另外一种添加方式 也方便测试
func (t *TaskTimer) AddTimeOut(d time.Duration, task Task, par interface{}) {

	t.Add(time.Now().Add(d), task, par)
}

//GetOldest 获取最早的任务
func (t *TaskTimer) GetOldest() (TaskItem, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Heap.Len() == 0 {
		return TaskItem{}, false
	}

	return t.Heap[0], true

}

//Pop 剔除最早的任务
func (t *TaskTimer) Pop() TaskItem {
	t.mu.Lock()
	defer t.mu.Unlock()

	d := heap.Pop(&t.Heap)
	return d.(TaskItem)

}

//Run 执行
func (t *TaskTimer) Run() {

	tk := time.NewTicker(t.dur)
	for {

		<-tk.C
		t.checkTask()
	}
}

func (t *TaskTimer) checkTask() {

	for {
		oldTask, exist := t.GetOldest()
		if !exist {
			return
		}

		//到时间了
		if oldTask.TS.Before(time.Now()) {
			go oldTask.Task(oldTask.Par)
			t.Pop()
		} else {
			return
		}
	}

}

func getTimeStamp() int64 {
	return time.Now().Unix()
}
