package reporter

import (
	"encoding/json"
	"expvar"
	"net/http"
	"sync"

	_ "github.com/millken/metrics/runtime"
)

const (
	metricsVar = "kaman.metrics"
)

// metricsHandler displays expvars.
type metricsHandler struct {
	stats     map[string]uint64
	statsLock sync.Mutex
}

func NewMetric() *metricsHandler {
	mh := new(metricsHandler)
	mh.stats = make(map[string]uint64)
	return mh
}

func writeJsonResponse(w http.ResponseWriter, obj interface{}, err error) error {
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.Encode(obj)
		return nil
	}
	return err
}
func (mh *metricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate,no-cache,no-store")

	//	_, _ = metrics.Snapshot()
	val := expvar.Get(metricsVar)
	if val == nil {
		w.WriteHeader(http.StatusNotImplemented)
		w.Write([]byte("No metrics."))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(val.String()))
}
