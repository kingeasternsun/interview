/*
 * @Description:
 * @Version: 2.0
 * @Author: kingeasternsun
 * @Date: 2021-02-25 14:57:51
 * @LastEditors: kingeasternsun
 * @LastEditTime: 2021-02-25 16:17:40
 * @FilePath: \tidb\two\queue_test.go
 */
package two

import (
	"strconv"
	"sync"
	"testing"
)

type StringItem string

func (s StringItem) GetID() string {
	return string(s)
}

func TestReadyStatus(t *testing.T) {

	wg := sync.WaitGroup{}
	wg.Add(2)
	q := NewTiQueue(7)
	go func() {

		for _, v := range []string{"one", "two", "three"} {
			err := q.Add(StringItem(v))
			if err != nil {
				t.Error(err)
			}
			status := q.GetItemStatus(StringItem(v))
			if status != Ready {
				t.Errorf("%v status %v not equal ready", v, status)
			}
		}
		wg.Done()

	}()

	go func() {

		for _, v := range []string{"1", "2", "3"} {
			err := q.Add(StringItem(v))
			if err != nil {
				t.Error(err)
			}
			status := q.GetItemStatus(StringItem(v))
			if status != Ready {
				t.Errorf("%v status %v not equal ready", v, status)
			}
		}
		wg.Done()

	}()

	wg.Wait()

	isShuting := q.ShuttingDown()
	if isShuting {
		t.Errorf("shutingdown %v not %v", isShuting, false)
	}

	//测试 重复
	err := q.Add(StringItem("3"))
	if err != errItemExist {
		t.Errorf("err %v not %v", err, errItemExist)
	}

	//填满队列
	err = q.Add(StringItem("4"))
	if err != nil {
		t.Error(err)
	}

	//测试溢出
	err = q.Add(StringItem("full"))
	if err != errExceedCap {
		t.Errorf("err %v not %v", err, errExceedCap)
	}

	isShuting = q.ShuttingDown()
	if isShuting {
		t.Errorf("shutingdown %v not %v", isShuting, false)
	}

	q.ShutDown()
	isShuting = q.ShuttingDown()
	if !isShuting {
		t.Errorf("shutingdown %v not %v", isShuting, true)
	}

	for {
		//非阻塞读
		item, shutdown, _ := q.Get(false)
		if shutdown {
			break
		}

		status := q.GetItemStatus(item)
		if status != InProcess {
			t.Errorf("%v status %v not equal InProcess ", item, status)
		}

		//完成任务
		q.Done(item)
		status = q.GetItemStatus(item)
		if status != NotExist {
			t.Errorf("%v status %v not equal NotExist ", item, status)
		}

	}

}

type IntItem int

func (s IntItem) GetID() string {
	return strconv.Itoa(int(s))
}

//测试 3个生产者 2个消费者
func TestMultiProduceMultiCousumer(t *testing.T) {
	q := NewTiQueue(4000)
	for v := 3000; v < 4000; v++ {
		err := q.Add(IntItem(v))
		if err != nil {
			t.Error(err)
		}
		status := q.GetItemStatus(IntItem(v))
		if status != Ready {
			t.Errorf("%v status %v not equal ready", v, status)
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(5)
	go func() {

		for v := 0; v < 1000; v++ {
			err := q.Add(IntItem(v))
			if err != nil {
				t.Error(err)
			}
			status := q.GetItemStatus(IntItem(v))
			if status != Ready {
				t.Errorf("%v status %v not equal ready", v, status)
			}
		}
		wg.Done()

	}()

	go func() {
		for v := 1000; v < 2000; v++ {
			err := q.Add(IntItem(v))
			if err != nil {
				t.Error(err)
			}
			status := q.GetItemStatus(IntItem(v))
			if status != Ready {
				t.Errorf("%v status %v not equal ready", v, status)
			}
		}
		wg.Done()

	}()

	go func() {
		for v := 2000; v < 3000; v++ {
			err := q.Add(IntItem(v))
			if err != nil {
				t.Error(err)
			}
			status := q.GetItemStatus(IntItem(v))
			if status != Ready {
				t.Errorf("%v status %v not equal ready", v, status)
			}
		}
		wg.Done()

	}()

	//构建2个 消费者
	for i := 0; i < 2; i++ {
		go func() {

			for {
				//非阻塞读
				item, shutdown, _ := q.Get(false)
				if shutdown {
					break
				}

				status := q.GetItemStatus(item)
				if status != InProcess {
					t.Errorf("%v status %v not equal InProcess ", item, status)
				}

				//完成任务
				q.Done(item)
				status = q.GetItemStatus(item)
				if status != NotExist {
					t.Errorf("%v status %v not equal NotExist ", item, status)
				}

			}

			wg.Done()

		}()
	}

	wg.Wait()

}

//TestInproduceAdd 测试处理的同时进行添加 允许 item 在处理的同时被重新添加到队列,
func TestInproduceAdd1(t *testing.T) {

	q := NewTiQueue(4)
	err := q.Add(StringItem("two"))
	if err != nil {
		t.Error(err)
	}

	//得到 two
	item, _, err := q.Get(true)
	if err != nil {
		t.Error(err)
	}

	//状态变为处理中
	status := q.GetItemStatus(item)
	if status != InProcess {
		t.Errorf("%v status %v not equal InProcess ", item, status)
	}

	// 相同的item在处理中，就添加了新的item
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = q.Add(StringItem("two"))
		if err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	wg.Wait()

	//这时候状态应该变为 ready<<2 | inproduce
	status = q.GetItemStatus(item)
	if status != (Ready<<2)|InProcess {
		t.Errorf("%v status %v not equal (Ready<<2)|InProcess ", item, status)
	}

	err = q.Done(item)
	status = q.GetItemStatus(item)
	if status != Ready {
		t.Errorf("%v status %v not equal Ready ", item, status)
	}

}

//TestInproduceAdd 测试处理的同时进行添加 允许 item 在处理的同时被重新添加到队列,
func TestInproduceAdd2(t *testing.T) {

	q := NewTiQueue(4)
	err := q.Add(StringItem("two"))
	if err != nil {
		t.Error(err)
	}

	//得到 two
	item, _, err := q.Get(true)
	if err != nil {
		t.Error(err)
	}

	//状态变为处理中
	status := q.GetItemStatus(item)
	if status != InProcess {
		t.Errorf("%v status %v not equal InProcess ", item, status)
	}

	//相同item在处理中，就新添加了相同的item，然后进行了Get
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = q.Add(StringItem("two"))
		if err != nil {
			t.Error(err)
		}
		item2, _, err := q.Get(true)
		if err != nil {
			t.Error(err)
		}

		//这时候状态应该变为 InProcess<<2 | InProcess
		status = q.GetItemStatus(item2)
		if status != (InProcess<<2)|InProcess {
			t.Errorf("%v status %v not equal (Ready<<2)|InProcess ", item2, status)
		}

		wg.Done()
	}()

	wg.Wait()

	err = q.Done(item)
	status = q.GetItemStatus(item)
	if status != InProcess {
		t.Errorf("%v status %v not equal InProcess ", item, status)
	}

}

//TestProduceOnce  测试多个消费者情况下，item只会处理一次
func TestProduceOnce(t *testing.T) {

	numMap := make(map[string]struct{}, 0)

	q := NewTiQueue(1000)
	for v := 0; v < 1000; v++ {
		err := q.Add(IntItem(v))
		if err != nil {
			t.Error(err)
		}
		status := q.GetItemStatus(IntItem(v))
		if status != Ready {
			t.Errorf("%v status %v not equal ready", v, status)
		}
	}
	consumerNumber := 8
	wg := sync.WaitGroup{}
	wg.Add(consumerNumber)

	//构建8个 消费者
	for i := 0; i < consumerNumber; i++ {
		go func() {

			for {
				//非阻塞读
				item, shutdown, _ := q.Get(false)
				if shutdown {
					break
				}

				if _, ok := numMap[item.GetID()]; ok {
					t.Errorf("%v has produce", item.GetID())
				}

				numMap[item.GetID()] = struct{}{}

				status := q.GetItemStatus(item)
				if status != InProcess {
					t.Errorf("%v status %v not equal InProcess ", item, status)
				}

				//完成任务
				q.Done(item)
				status = q.GetItemStatus(item)
				if status != NotExist {
					t.Errorf("%v status %v not equal NotExist ", item, status)
				}

			}

			wg.Done()

		}()
	}

	wg.Wait()

}

//TestAddMultiBugProduceOnce item 在被处理前被添加多次，只会被处理一次
func TestAddMultiBugProduceOnce(t *testing.T) {
	q := NewTiQueue(2)
	err := q.Add(IntItem(1))
	if err != nil {
		t.Error(err)
	}

	err = q.Add(IntItem(1))
	if err != errItemExist {
		t.Errorf(" err %v not equal %v", err, errItemExist)
	}

	item, _, err := q.Get(true)
	if err != nil {
		t.Error(err)
	}

	if item.GetID() != "1" {
		t.Errorf(" %v not 1", item.GetID())
	}

	_, _, err = q.Get(false)
	if err != errEmpty {
		t.Error(err)
	}

}
