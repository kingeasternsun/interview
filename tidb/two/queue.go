/*
 * @Description:queue
 * @Version: 2.0
 * @Author: kingeasternsun
 * @Date: 2021-02-25 10:00:18
 * @LastEditors: kingeasternsun
 * @LastEditTime: 2021-02-25 13:46:06
 * @FilePath: \tidb\two\queue.go
 */
package two

import (
	"errors"
	"sync"
)

/*
● 支持多个生产者和消费者，允许 item 在处理的同时被重新添加到队列, 同一个 item 在
并发消费的情况下只能被处理一次
● item 在被处理前被添加多次，只会被处理一次
● item 按添加的顺序被处理，即使是有多个消费者
● 支持关闭队列通知
*/
type Queue interface {
	Add(item interface{})                   // 添加 item 到队列。
	Get() (item interface{}, shutdown bool) //获取 item。shutdown 如果为 true, 调用者应关闭请求。处理完 item 后应调用 Done 方法。
	Len() int                               //返回队列长度，不包含正在处理的 item。
	Done(item interface{})                  //表示 item 被处理完成。
	ShutDown()                              //关闭队列，不再接收 Add 请求，待队列 item 被处理完后关闭。
	ShuttingDown() bool                     //队列是否正在关闭。
}

type Itemer interface {
	GetID() string //得到item的唯一标识 用于去重
}

type ItemStatus = uint8

const NotExist ItemStatus = 0
const (
	Ready     ItemStatus = 1 << iota //加入到了队列中
	InProcess                        //处理中
)

/*

系统中 相同的item最多会出现两个，我们2个bit标识item的状态，4个bit就可以表示相同两个item的状态

1. 相同item出现两次 ，一个刚加入01,一个处理中 10。
+----+
|0110|
+----+
如果item处理完成，状态右移两个bit

2. item只出现一次， 刚加入的时候
+----+
|0001|
+----+

3. item进行处理 ，变为
+----+
|0010|
+----+

如果item处理完成了， 就可以直接删除掉这个

4. 相同的item进行了添加，此时系统中有两个相同的item，状态不一样 变为
+----+
|0110|
+----+
	最老的处理完，就右移动2bit ，变为
+----+
|0001|
+----+

5. 如果最老的item在处理中时，最新的也执行了Get变为了处理中
+----+
|1010|
+----+
	item处理完后，就右移动2bit,变为
+----+
|0010|
+----+

*/
type TiQueue struct {
	MaxCap int //队列最大的item数量
	Queue  chan Itemer
	sync.Mutex
	ItemStatus map[string]uint8 //记录item的状态 ，
	Closed     bool             //标记是否已经关闭
	once       sync.Once
}

var errExceedCap = errors.New("queue is full")
var errClosed = errors.New("queue is close")
var errEmpty = errors.New("queue is empty")
var errItemNotGet = errors.New("item not get") //item没有Get就Donel
var errItemExist = errors.New("item exist")    //item 已经存在

//NewTiQueue 队列初始化
func NewTiQueue(maxCap int) *TiQueue {

	return &TiQueue{
		MaxCap: maxCap,
		Queue:  make(chan Itemer, maxCap),
	}
}

//Add 添加item 到队列
func (q *TiQueue) Add(item Itemer) error {

	//快速判定，因为队列不可能从关闭变为开启
	if q.Closed {
		return errClosed
	}

	if len(q.Queue) == q.MaxCap {
		return errExceedCap
	}

	q.Lock()
	defer q.Unlock()

	status, ok := q.ItemStatus[item.GetID()]
	if !ok {
		//队列中不存在，就可以直接添加
		q.Queue <- item
		q.ItemStatus[item.GetID()] = Ready
		return nil
	}

	//已经处理中，而且此刻只有一个相同的item
	if status == InProcess {
		q.Queue <- item
		q.ItemStatus[item.GetID()] = (Ready << 2) | InProcess
		return nil
	}

	//其他情况下都不可以再进行添加了
	return errItemExist
}

//Get 从队列中获取item，block 标记是否阻塞读
func (q *TiQueue) Get(block bool) (item Itemer, shutdown bool, err error) {

	//阻塞读
	if block {
		item, shutdown = <-q.Queue
	} else {
		//非阻塞读
		select {
		case item, shutdown = <-q.Queue:
		default:
			err = errEmpty
			shutdown = false
			return
		}
	}

	q.Lock()
	defer q.Unlock()

	//更新状态
	status, ok := q.ItemStatus[item.GetID()]
	if !ok {
		//如果找不到 肯定就有大问题了
		panic("can not found status of " + item.GetID())
	}

	if status == Ready {
		q.ItemStatus[item.GetID()] = InProcess
		return
	}

	//两个相同item的情况 最早的肯定是 InProcess ，最新的肯定是 Ready
	if status == (Ready<<2)|InProcess {
		q.ItemStatus[item.GetID()] = (InProcess << 2) | InProcess
		return
	}

	//其他状态 肯定就有大问题了
	panic(" status error " + item.GetID())

}

//Len 获取Ready 状态的数据个数
func (q *TiQueue) Len() int {
	return len(q.Queue)
}

//Done 表示item处理完成了
func (q *TiQueue) Done(item Itemer) (err error) {
	q.Lock()
	defer q.Unlock()

	q.Lock()
	defer q.Unlock()

	//更新状态
	status, ok := q.ItemStatus[item.GetID()]
	if !ok {
		//幂等处理
		return
	}

	//如果没有Get就Done 了
	if status == Ready {
		err = errItemNotGet
		return
	}

	status = status >> 2
	if status == 0 {
		delete(q.ItemStatus, item.GetID())
	} else {
		q.ItemStatus[item.GetID()] = status
	}

	return

}

//ShutDown 关闭
func (q *TiQueue) ShutDown() {

	q.Lock()
	q.once.Do(func() {
		q.Closed = true
		close(q.Queue)
	})

	return
}

//ShuttingDown 判断是否在关闭中
func (q *TiQueue) ShuttingDown() bool {
	if q.Closed == true {
		//所有任务都处理完成了
		if len(q.ItemStatus) == 0 {
			return false //已经完全关闭了 ，而不是关闭中
		}

		return true //关闭中
	}

	return false //还没有关闭
}

func (q *TiQueue) getItemStatus(item Itemer) (status ItemStatus) {
	q.Lock()
	defer q.Unlock()

	status, ok := q.ItemStatus[item.GetID()]
	if !ok {
		return NotExist
	}
	return status
}
func (q *TiQueue) setItemStatus(item Itemer, status ItemStatus) {
	q.Lock()
	q.ItemStatus[item.GetID()] = status
	q.Unlock()
	return
}
