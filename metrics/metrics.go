package metrics

import (
	"expvar"
	"strconv"
	"sync"
	"sync/atomic"
)

// A Counter is a thread-safe counter implementation
type Counter struct {
	count int64
}

func (c *Counter) Set(val int64) {
	atomic.StoreInt64(&c.count, val)
}

// Increment the counter by some value
func (c *Counter) Add(val int64) {
	atomic.AddInt64(&c.count, val)
}

// Return the counter's current value
func (c *Counter) Value() int64 {
	return atomic.LoadInt64(&c.count)
}

func (c *Counter) String() string {
	return strconv.FormatInt(c.Value(), 10)
}

var (
	counters = make(map[string]*Counter)
	cm       sync.Mutex
)

func NewCounter(key string) *Counter {
	cm.Lock()
	defer cm.Unlock()
	if c, ok := counters[key]; ok {
		return c
	}
	c := new(Counter)
	counters[key] = c
	return c
}

func Snapshot() (c map[string]int64) {
	cm.Lock()
	defer cm.Unlock()

	c = make(map[string]int64, len(counters))
	for n, v := range counters {
		c[n] = v.Value()
	}
	return
}


func init() {
	expvar.Publish("kaman.metrics", expvar.Func(func() interface{} {
		counters := Snapshot()
		return map[string]interface{}{
			"Counters": counters,
		}
	}))

}
