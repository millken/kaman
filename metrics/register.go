package metrics

import (
	"fmt"
	"sync"
)

type Registry struct {
	metrics map[string]interface{}
	mutex   sync.Mutex
}

// Create a new registry.
func NewRegistry() *Registry {
	return &Registry{metrics: make(map[string]interface{})}
}

func (r *Registry) Register(name string, i interface{}) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if _, ok := r.metrics[name]; ok {
		return fmt.Errorf("duplicate metric: %s", name)
	}
	r.metrics[name] = i
	return nil
}

func (r *Registry) GetOrRegister(name string, i interface{}) interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if metric, ok := r.metrics[name]; ok {
		return metric
	}
	r.metrics[name] = i
	return i
}

var DefaultRegistry *Registry = NewRegistry()

// Gets an existing metric or creates and registers a new one. Threadsafe
// alternative to calling Get and Register on failure.
func GetOrRegister(name string, i interface{}) interface{} {
	return DefaultRegistry.GetOrRegister(name, i)
}

// Register the given metric under the given name.  Returns a DuplicateMetric
// if a metric by the given name is already registered.
func Register(name string, i interface{}) error {
	return DefaultRegistry.Register(name, i)
}
