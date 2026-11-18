package profile

import (
	"expvar"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

func TestProfileHandlerLocalOnly(t *testing.T) {
	routerOnce.Do(registerRouter)
	ph := &profileHandler{}

	cases := []struct {
		name       string
		remoteAddr string
		wantStatus int
	}{
		{"ipv4 loopback", "127.0.0.1:54321", http.StatusOK},
		{"ipv6 loopback", "[::1]:54321", http.StatusOK},
		{"remote ipv4", "203.0.113.5:54321", http.StatusForbidden},
		{"remote ipv6", "[2001:db8::1]:54321", http.StatusForbidden},
		{"no port", "203.0.113.5", http.StatusForbidden},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
			req.RemoteAddr = c.remoteAddr
			w := httptest.NewRecorder()
			ph.Handler(w, req, func(http.ResponseWriter, *http.Request) {
				t.Fatal("fallback handler should not be invoked for a registered profile route")
			})
			require.Equal(t, c.wantStatus, w.Code)
		})
	}
}

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
	_ = NewProfileHandler(":8888")
	ph := NewProfileHandler("127.0.0.1:8888")
	httpServer := &http.Server{
		Addr:    "127.0.0.1:8888",
		Handler: rpc.MiddlewareHandlerWith(defaultRouter, ph),
	}
	t.Log("Server is running at", "127.0.0.1:8888")
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
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "100", string(data))
	resp.Body.Close()

	resp, err = http.Get(profileAddr + "/test/before")
	require.NoError(t, err)
	require.Equal(t, 800, resp.StatusCode)
	data, err = io.ReadAll(resp.Body)
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
	data, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(data))
	resp.Body.Close()

	{
		genListenAddr(profileAddr)
		genMetricExporter(profileAddr)
		genDumpScript(profileAddr)
	}

	// new profile port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	pAddr := fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	profileServer := &http.Server{
		Addr:    pAddr,
		Handler: rpc.MiddlewareHandlerWith(rpc.New(), NewProfileHandler("")),
	}
	t.Log("Profile is running at", pAddr)
	go func() {
		err = profileServer.Serve(ln)
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 10)
	// test /debug/pprof/:key, ignore profile
	for _, m := range []string{"cmdline", "symbol", "trace", " "} {
		resp, err := http.Get(profileAddr + "/debug/pprof/" + m)
		require.NoError(t, err)
		resp.Body.Close()
	}
	genDumpScript("http://" + pAddr)
}
