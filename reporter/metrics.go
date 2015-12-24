package reporter

import (
	"encoding/json"
	"expvar"
	"net/http"

	_ "github.com/codahale/metrics/runtime"
)

const (
	metricsVar = "metrics"
)

// metricsHandler displays expvars.
type metricsHandler struct {
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
