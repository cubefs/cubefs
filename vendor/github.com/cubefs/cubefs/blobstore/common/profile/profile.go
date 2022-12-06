// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package profile

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

const (
	envMetricExporterLabels = "profile_metric_exporter_labels"

	suffixListenAddr     = "_profile_listen_addr"
	suffixMetricExporter = "_profile_metric_exporter.yaml"
	suffixDumpScript     = "_profile_dump.sh"
)

var (
	profileAddr string
	httpRouter  = rpc.New()
	startTime   = time.Now()

	uptime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "profile",
		Subsystem: "process",
		Name:      "uptime_seconds",
		Help:      "Process uptime seconds.",
	}, func() (v float64) {
		return time.Since(startTime).Seconds()
	})

	gomaxprocs = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "profile",
		Subsystem: "process",
		Name:      "maxprocs",
		Help:      "Go runtime.GOMAXPROCS.",
	}, func() (v float64) {
		return float64(runtime.GOMAXPROCS(-1))
	})

	router = new(route)
)

type route struct {
	mu sync.Mutex

	vars    []string
	pprof   []string
	metrics []string
	users   []string
}

func (r *route) AddUserPath(path string) {
	r.mu.Lock()
	r.users = append(r.users, path)
	sort.Strings(r.users)
	r.mu.Unlock()
}

func (r *route) String() string {
	r.mu.Lock()
	sb := strings.Builder{}
	sb.WriteString("usage:\n\t/\n\n")
	sb.WriteString("vars:\n")
	sb.WriteString(fmt.Sprintf("\t%s\n\n", strings.Join(r.vars, "\n\t")))
	sb.WriteString("pprof:\n")
	sb.WriteString(fmt.Sprintf("\t%s\n\n", strings.Join(r.pprof, "\n\t")))
	sb.WriteString("metrics:\n")
	sb.WriteString(fmt.Sprintf("\t%s\n\n", strings.Join(r.metrics, "\n\t")))
	if len(r.users) > 0 {
		sb.WriteString("users:\n")
		sb.WriteString(fmt.Sprintf("\t%s\n\n", strings.Join(r.users, "\n\t")))
	}
	r.mu.Unlock()
	return sb.String()
}

type profileHandler struct{}

func (ph *profileHandler) Handler(w http.ResponseWriter, r *http.Request, f func(http.ResponseWriter, *http.Request)) {
	path := r.URL.Path
	addr := r.RemoteAddr
	handle, _, _ := httpRouter.Router.Lookup(r.Method, path)
	if handle != nil {
		// to support local host, need not authentication
		if strings.HasPrefix(addr, "localhost") || strings.HasPrefix(addr, "127.0.0.1") {
			httpRouter.ServeHTTP(w, r)
			return
		}
		// todo Authentication manager authority
		httpRouter.ServeHTTP(w, r)
		return
	}
	f(w, r)
}

func init() {
	prometheus.MustRegister(uptime)
	prometheus.MustRegister(gomaxprocs)

	expvar.NewString("GoVersion").Set(runtime.Version())
	expvar.NewInt("NumCPU").Set(int64(runtime.NumCPU()))
	expvar.NewInt("Pid").Set(int64(os.Getpid()))
	expvar.NewInt("StartTime").Set(startTime.Unix())

	expvar.Publish("Uptime", expvar.Func(func() interface{} { return time.Since(startTime) / time.Second }))
	expvar.Publish("NumGoroutine", expvar.Func(func() interface{} { return runtime.NumGoroutine() }))
}

func NewProfileHandler(addr string) rpc.ProgressHandler {
	// without profile setting
	if without {
		return nil
	}

	ph := &profileHandler{}

	httpRouter.Router.HandlerFunc(http.MethodGet, "/", index)
	httpRouter.Router.HandlerFunc(http.MethodGet, "/debug/pprof/", pprof.Index)
	httpRouter.Router.HandlerFunc(http.MethodGet, "/debug/pprof/:key", secondIndex)
	httpRouter.Router.HandlerFunc(http.MethodGet, "/debug/vars", expvar.Handler().ServeHTTP)
	httpRouter.Router.HandlerFunc(http.MethodGet, "/metrics", promhttp.Handler().ServeHTTP)
	httpRouter.Router.HandlerFunc(http.MethodGet, "/debug/var/", oneExpvar)
	httpRouter.Router.HandlerFunc(http.MethodGet, "/debug/var/:key", oneExpvar)

	router.mu.Lock()
	router.vars = []string{
		"/debug/vars",
		"/debug/var/*",
	}

	router.pprof = []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
		"/debug/pprof/trace",
	}
	router.metrics = []string{
		"/metrics",
	}
	router.mu.Unlock()

	if strings.HasPrefix(addr, ":") {
		profileAddr = "http://127.0.0.1" + addr
	} else {
		profileAddr = "http://" + addr
	}

	// do not gen files in `go test`
	if !strings.HasSuffix(os.Args[0], ".test") {
		genListenAddr()
		genDumpScript()
		genMetricExporter()
	}
	return ph
}

// handle path /debug/ , show usage
func index(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, router.String())
}

// handler path /debug/var/<var>, get one expvar
func oneExpvar(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/debug/var/"):]
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	val := expvar.Get(key)
	if val == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Write([]byte(val.String()))
}

func secondIndex(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	splits := strings.Split(path, "/")
	switch splits[len(splits)-1] {
	case "cmdline":
		pprof.Cmdline(w, r)
	case "profile":
		pprof.Profile(w, r)
	case "symbol":
		pprof.Symbol(w, r)
	case "trace":
		pprof.Trace(w, r)
	default:
		pprof.Index(w, r)
	}
}

// HandleFunc register handler func to profile serve multiplexer.
func HandleFunc(method, pattern string, handler rpc.HandlerFunc, opts ...rpc.ServerOption) {
	if without {
		return
	}
	router.AddUserPath(pattern)
	httpRouter.Handle(method, pattern, handler, opts...)
}
