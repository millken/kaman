package report

import (
	"runtime"
	"net/http"
	"encoding/json"
)

var memstats runtime.MemStats

type runtimeHandler int

func NewRuntime() *runtimeHandler {
	this := new(runtimeHandler)
	return this
}

func (this *runtimeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate,no-cache,no-store")
	runtime.ReadMemStats(&memstats)
	js := make(map[string]interface{})
	js["gomaxprocs"] = runtime.GOMAXPROCS(0)
	js["numcgocall"] = runtime.NumCgoCall()
	js["numcpu"] = runtime.NumCPU()
	js["numgoroutine"] = runtime.NumGoroutine()
	js["memstats"] = memstats
	js1, err := json.Marshal(js)
    if err != nil {
        panic(err)
    }
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(js1))
}
