package profile

import (
	"expvar"
	"io/ioutil"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func TestProfileBase(t *testing.T) {
	i := expvar.NewInt("int_key")
	i.Set(100)

	HandleFunc(http.MethodGet, "/test/before", func(ctx *rpc.Context) {
		ctx.RespondStatus(800)
		ctx.Writer.Write([]byte("ok"))
	})

	var defaultHandleAddr string
	initDone := make(chan bool)
	go func() {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defaultHandleAddr = "http://" + ln.Addr().String()
		close(initDone)
		err = http.Serve(ln, nil)
		require.NoError(t, err)
	}()
	<-initDone

	// default handle 404
	resp, err := http.Get(defaultHandleAddr + "/debug/var/")
	require.NoError(t, err)
	require.Equal(t, 404, resp.StatusCode)
	resp.Body.Close()
	resp, err = http.Get(defaultHandleAddr + "/metrics")
	require.NoError(t, err)
	require.Equal(t, 404, resp.StatusCode)
	resp.Body.Close()

	defaultRouter := rpc.DefaultRouter
	ph := NewProfileHandler("127.0.0.1:8888")
	httpServer := &http.Server{
		Addr:    "127.0.0.1:8888",
		Handler: rpc.MiddlewareHandlerWith(defaultRouter, ph),
	}
	log.Info("Server is running at", "127.0.0.1:8888")
	go func() {
		err = httpServer.ListenAndServe()
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 10)
	// profile handle 200
	resp, err = http.Get(profileAddr + "/")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	buf := make([]byte, 1<<20)
	n, _ := resp.Body.Read(buf)
	t.Log(string(buf[:n]))
	resp.Body.Close()

	resp, err = http.Get(profileAddr + "/debug/vars")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
	resp, err = http.Get(profileAddr + "/debug/pprof/")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// test /debug/pprof/:key, ignore profile
	for _, m := range []string{"cmdline", "symbol", "trace", " "} {
		resp, err := http.Get(profileAddr + "/debug/pprof/" + m)
		require.NoError(t, err)
		resp.Body.Close()
	}

	// get one expvar
	resp, err = http.Get(profileAddr + "/debug/var/")
	require.NoError(t, err)
	require.Equal(t, 400, resp.StatusCode)
	resp.Body.Close()
	resp, err = http.Get(profileAddr + "/debug/var/not_exist_key")
	require.NoError(t, err)
	require.Equal(t, 404, resp.StatusCode)
	resp.Body.Close()
	resp, err = http.Get(profileAddr + "/debug/var/int_key")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	data, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "100", string(data))
	resp.Body.Close()

	resp, err = http.Get(profileAddr + "/test/before")
	require.NoError(t, err)
	require.Equal(t, 800, resp.StatusCode)
	data, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(data))
	resp.Body.Close()

	HandleFunc(http.MethodGet, "/test/handlfunc", func(ctx *rpc.Context) {
		ctx.RespondStatus(900)
		ctx.Writer.Write([]byte("ok"))
	})
	resp, err = http.Get(profileAddr + "/test/handlfunc")
	require.NoError(t, err)
	require.Equal(t, 900, resp.StatusCode)
	data, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(data))
	resp.Body.Close()

	{
		genListenAddr()
		genDumpScript()
		genMetricExporter()
	}
}
