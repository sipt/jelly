package jelly

import (
	"errors"
	"sync/atomic"
	"sync"
)

const (
	DefaultChanLen    = 1 << 9 // 默认channel长度
	DefaultChanCount  = 1 << 1 // 默认channel个数
	DefaultChanStep   = 1 << 1 // 默认channel增长步长
	DefaultLoadFactor = 3      // 负载系数

	StatusRecvStop  = 0 // 停止接收处理
	StatusRecvStart = 1 // 开始接收处理
)

var (
	ErrBlock         = errors.New("chan block")
	ErrCallbackIsNil = errors.New("callback is nil")
	ErrRecvStarted   = errors.New("receive started")
	DefaultOption    = Option{
		ChanLen:    DefaultChanLen,
		ChanCount:  DefaultChanCount,
		Step:       DefaultChanStep,
		LoadFactor: DefaultLoadFactor,
		ChanSelect: &round{count: DefaultChanCount},
	}
)

//channel 选择器函数
type ChanSelect func(interface{}) int

//接收处理函数
type Callback func(interface{})

//配置
type Option struct {
	ChanLen    int
	ChanCount  int
	Step       int
	LoadFactor uint8
	ChanSelect ISelect
}

type ISelect interface {
	Select(param interface{}) int
}

type round struct {
	sync.Mutex
	index int
	count int
}

func (r *round) Select(param interface{}) int {
	r.Lock()
	r.index ++
	if r.index == r.count {
		r.index = 0
	}
	i := r.index
	r.Unlock()
	return i
}

func NewChanPool(option Option, callback Callback) (*ChanPool, error) {
	if callback == nil {
		return nil, ErrCallbackIsNil
	}
	c := &ChanPool{
		callback: callback,
		chans:    make([]chan interface{}, option.ChanCount),
		option:   option,
	}
	for i, l := 0, len(c.chans); i < l; i++ {
		c.chans[i] = make(chan interface{}, option.ChanLen)
	}
	c.refreshCapacity()
	return c, nil
}

//channel 池
type ChanPool struct {
	sync.Mutex
	option      Option
	chans       []chan interface{}
	callback    Callback
	status      int32
	capacity    int64 //当前缓存容量
	preCapacity int64 //扩容前缓存容量
	load        int64
}

// caller must be new func or hold c.Mutex.
func (c *ChanPool) refreshCapacity() {
	c.capacity = int64(len(c.chans) * c.option.ChanLen)
}

// 发送数据
// 参数：v 值，block 是否允许阻塞
func (c *ChanPool) Send(v interface{}, block bool) error {
	i := c.option.ChanSelect.Select(v)
	//if i > len(c.chans) {
	//	i = DefaultChanSelect(v)
	//}
	if block {
		c.chans[i] <- v
	} else {
		select {
		case c.chans[i] <- v:
		default:
			c.grow()
			return ErrBlock
		}
	}
	atomic.AddInt64(&c.load, 1)
	c.grow()
	return nil
}

//开始接收
func (c *ChanPool) StartRecv() error {
	if atomic.LoadInt32(&c.status) == StatusRecvStop {
		for !atomic.CompareAndSwapInt32(&c.status, StatusRecvStop, StatusRecvStart) {
			if atomic.LoadInt32(&c.status) == StatusRecvStart {
				return ErrRecvStarted // 已经开始接收了
			}
		}
	}
	for i, l := 0, len(c.chans); i < l; i++ {
		go func(rc chan interface{}) {
			for {
				v, ok := <-rc
				if ok {
					atomic.AddInt64(&c.load, -1)
					c.callback(v)
					c.shrink() //检查是否要收缩
				} else {
					break
				}
			}
		}(c.chans[i])
	}
	return nil
}

//停止接收
func (c *ChanPool) StopRecv() error {
	//todo
	return nil
}

//扩容
func (c *ChanPool) grow() bool {
	load := atomic.LoadInt64(&c.load)
	if c.capacity>>c.option.LoadFactor > c.capacity-load {
		c.Lock()
		if load > 0 && c.capacity>>c.option.LoadFactor <= c.capacity-load {
			c.Unlock()
			return false
		}
		newChans := make([]chan interface{}, c.option.Step)
		for i := 0; i < c.option.Step; i++ {
			newChans[i] = make(chan interface{}, c.option.ChanLen)
		}
		for i, l := 0, len(newChans); i < l; i++ {
			go func(rc chan interface{}) {
				for {
					v, ok := <-rc
					if ok {
						c.callback(v)
					} else {
						break
					}
				}
			}(newChans[i])
		}
		c.chans = append(c.chans, newChans...)
		c.preCapacity = c.capacity
		c.capacity = int64(len(c.chans) * c.option.ChanLen)
		c.Unlock()
		return true
	}
	return false
}

//收缩
func (c *ChanPool) shrink() bool {
	load := atomic.LoadInt64(&c.load)
	if c.preCapacity != 0 && c.preCapacity>>c.option.LoadFactor <= c.preCapacity-load {
		//达到收缩条件
		c.Lock()
		//double check
		if c.preCapacity>>c.option.LoadFactor > c.preCapacity-load { //
			c.Unlock()
			return false
		}
		//收缩c.option.Step长度
		index := len(c.chans) - c.option.Step
		//保存要关闭的chan
		closeChans := c.chans[index:]
		//压缩
		c.chans = c.chans[:index]
		c.capacity = c.preCapacity
		c.preCapacity = int64((len(c.chans) - c.option.Step) * c.option.ChanLen)
		//close chans
		for i, l := 0, len(closeChans); i < l; i++ {
			close(closeChans[i])
			closeChans[i] = nil
		}
		c.Unlock()
		return true
	}
	return false
}
