package tracing

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/chubaofs/chubaofs/util/tracing"
)

func httpAPISetTracingHandler(w http.ResponseWriter, r *http.Request) {

	const paramKeyEnable = "enable"

	defer func() {
		httpAPIGetTracingHandler(w, r)
	}()

	_ = r.ParseForm()
	var enable bool
	enable, _ = strconv.ParseBool(r.FormValue(paramKeyEnable))
	tracing.SetEnable(enable)
	return
}

func httpAPIGetTracingHandler(w http.ResponseWriter, r *http.Request) {
	var resp = struct {
		Tracing bool
	}{
		Tracing: tracing.IsEnabled(),
	}
	var encoded []byte
	encoded, _ = json.Marshal(&resp)
	_, _ = w.Write(encoded)
	return
}

func init() {
	http.HandleFunc("/trace/set", httpAPISetTracingHandler)
	http.HandleFunc("/trace/get", httpAPIGetTracingHandler)
}
