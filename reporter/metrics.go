package reporter

import (
	"encoding/json"
	"expvar"
	"net/http"
	"strings"
	"time"

	"github.com/millken/metrics"
	_ "github.com/millken/metrics/runtime"
)

const (
	metricsVar = "metrics"
)

// metricsHandler displays expvars.
type metricsHandler struct {
}

func rpsRepost() {
	pre_counters := make(map[string]uint64)
	//metrics.Reset()
	for _ = range time.NewTicker(1 * time.Second).C {
		counters, _ := metrics.Snapshot()
		for n, c := range counters {
			if strings.HasPrefix(n, "Tag") {
				if pc, ok := pre_counters[n]; ok {
					if !strings.HasSuffix(n, "rps") {
						metrics.Counter(n + ":rps").Set(c - pc)
					}
				}
			}
		}
		pre_counters = counters
	}
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
