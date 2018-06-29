package jelly

import (
	"testing"
	"sync"
	"time"
)

func TestChanPool_SendRecv(t *testing.T) {
	var w sync.WaitGroup
	pool, err := NewChanPool(DefaultOption, func(i interface{}) {
		if v := i.(int); v == 3 {
			w.Done()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	err = pool.StartRecv()
	if err != nil {
		t.Fatal(err)
	}
	var count = 1000
	w.Add(count)
	for i := 0; i < 5; i++ {
		go func() {
			for i := 0; i < 200; i++ {
				pool.Send(3, true)
			}
		}()
	}
	go func() {
		timer := time.NewTimer(5 * time.Second)
		<-timer.C
		t.Fatal("time out")
	}()
	w.Wait()
}

func TestChanPool_grow(t *testing.T) {
	pool, err := NewChanPool(DefaultOption, func(i interface{}) {

	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 896; i++ {
		pool.Send(3, true)
	}
	if pool.capacity != 1024 {
		t.Fatal("grow() error")
	}
	pool.Send(3, true)
	if pool.capacity != 2048 {
		t.Fatal("grow() error")
	}
}

func TestChanPool_shrink(t *testing.T) {
	pool, err := NewChanPool(DefaultOption, func(i interface{}) {

	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 897; i++ {
		pool.Send(3, true)
	}
	if pool.capacity != 2048 || pool.preCapacity != 1024 {
		t.Fatal("grow() error")
	}
	err = pool.StartRecv()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if pool.capacity != 1024 {
		t.Fatal("shrink() error")
	}
}
