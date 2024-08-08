package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/profile"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var (
	mode    = flag.String("mode", "rpc2", "service of rpc2, rpc, smux or tcp")
	client  = flag.Bool("client", false, "server or client")
	procs   = flag.Int("procs", 0, "maximum number of CPUs")
	addr    = flag.String("addr", "127.0.0.1:8888", "bind address")
	discard = flag.Bool("discard", false, "discard memcopy")
	v1      = flag.Bool("v1", false, "change to version 1 on rpc2 and smux")
	bench   = flag.String("bench", "", "bench config json file")
	resultf = flag.String("resultf", "result.json", "result json file")
	times   = flag.Int("times", 1, "run times")

	// server
	connbuffer = flag.Int("connbuffer", 0, "connection buffer read size")

	// client
	runsecond   = flag.Int("runsecond", 300, "seconds of one case run time")
	connection  = flag.Int("connection", 1, "num of connections")
	concurrence = flag.Int("concurrence", 1, "num of concurrence per connection")
	requestsize = flag.Int("requestsize", 128<<10, "per request size")
	writev      = flag.Bool("writev", false, "connection buffers writev")
	crc         = flag.Bool("crc", false, "transfer with crc")
)

var transportConfig = &rpc2.TransportConfig{
	Version:           2,
	KeepAliveDisabled: true,
	MaxFrameSize:      256 << 10,
	MaxReceiveBuffer:  32 * (1 << 20),
	MaxStreamBuffer:   8 << 20,
}

var config BenchConfig

type BenchConfig struct {
	Transport *rpc2.TransportConfig `json:"transport"`

	Connection  []int  `json:"connection"`
	Concurrence []int  `json:"concurrence"`
	Requestsize []int  `json:"requestsize"`
	Writev      []bool `json:"writev"`
	Crc         []bool `json:"crc"`
}

var (
	pool   sync.Pool
	readn  int64
	writen int64
)

func init() {
	pool.New = func() any { return make([]byte, 1<<25) }

	ticker := time.NewTicker(3 * time.Second)
	go func() {
		defer ticker.Stop()
		t := time.Now()
		name, ptr := "server", &readn
		if *client {
			name, ptr = "client", &writen
		}
		var oldn int64
		for range ticker.C {
			newn := atomic.LoadInt64(ptr)
			if newn-oldn > 0 {
				speed := float64(newn-oldn) / time.Since(t).Seconds() / (1 << 20)
				fmt.Printf("%s(%s) speed: %.2f MB/s\n", name, *addr, speed)
			}
			t = time.Now()
			oldn = newn
		}
	}()
}

func newCtx() context.Context {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	return ctx
}

func loadConfig() {
	if file := *bench; file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			panic(err)
		}
		if err = json.Unmarshal(data, &config); err != nil {
			panic(err)
		}
	}
	if config.Transport == nil {
		config.Transport = transportConfig
	}
	if *v1 {
		config.Transport.Version = 1
	}
	if len(config.Connection) == 0 {
		config.Connection = []int{*connection}
	}
	if len(config.Concurrence) == 0 {
		config.Concurrence = []int{*concurrence}
	}
	if len(config.Requestsize) == 0 {
		config.Requestsize = []int{*requestsize}
	}
	if len(config.Writev) == 0 {
		config.Writev = []bool{*writev}
	}
	if len(config.Crc) == 0 {
		config.Crc = []bool{*crc}
	}

	switch *mode {
	case "rpc":
		config.Writev = []bool{false}
	case "smux", "tcp":
		config.Crc = []bool{false}
	default:
	}
}

type clientCarrier struct {
	wg          sync.WaitGroup
	endtime     time.Time
	connection  int
	concurrence int
	requestsize int
	writev      bool
	crc         bool
}

func (cc *clientCarrier) startEndtime() {
	if *runsecond <= 0 {
		*runsecond = 24 * 3600
	}
	cc.endtime = time.Now().Add(time.Duration(*runsecond) * time.Second)
}

func (cc *clientCarrier) set(connection, concurrence, requestsize int, writev, crc bool) {
	cc.connection = connection
	cc.concurrence = concurrence
	cc.requestsize = requestsize
	cc.writev = writev
	cc.crc = crc
}

type clientRunner interface {
	set(connection, concurrence, requestsize int, writev, crc bool)
	run()
}

type result struct {
	Mode       string `json:"mode"`
	Procs      int    `json:"procs"`
	Discard    bool   `json:"discard"`
	V1         bool   `json:"v1"`
	Connbuffer int    `json:"connbuffer"`

	Connection  int  `json:"connection"`
	Concurrence int  `json:"concurrence"`
	Requestsize int  `json:"requestsize"`
	Writev      bool `json:"writev"`
	Crc         bool `json:"crc"`

	Times      int   `json:"times"`
	Timesecond int   `json:"timesecond"`
	Written    int64 `json:"written"`
	Speed      int64 `json:"speed"`
}

type discardReader struct{ n int }

func (d *discardReader) Read(p []byte) (int, error) {
	if d.n <= 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > d.n {
		n = d.n
	}
	d.n -= n
	return n, nil
}

func main() {
	flag.Parse()
	profile.NewProfileHandler(*addr)
	if p := *procs; p > 0 {
		runtime.GOMAXPROCS(p)
	}
	loadConfig()

	if !*client {
		switch *mode {
		case "rpc2":
			serverRpc2()
		case "rpc":
			serverRpc()
		case "smux":
			serverSmux()
		case "tcp":
			serverTcp()
		default:
			panic(*mode)
		}
	}

	progress := 0
	all := len(config.Connection) * len(config.Concurrence) *
		len(config.Requestsize) * len(config.Writev) * len(config.Crc) * *times

	var runner clientRunner
	var results []result
	for _, connection := range config.Connection {
		for _, concurrence := range config.Concurrence {
			for _, requestsize := range config.Requestsize {
				for _, writev := range config.Writev {
					for _, crc := range config.Crc {
						switch *mode {
						case "rpc2":
							runner = &clientRpc2{}
						case "rpc":
							runner = &clientRpc{}
						case "smux":
							runner = &clientSmux{}
						case "tcp":
							runner = &clientTcp{}
						default:
							panic(*mode)
						}

						for ii := 0; ii < *times; ii++ {
							progress++
							fmt.Printf("progress [%d/%d]\n", progress, all)
							runner.set(connection, concurrence, requestsize, writev, crc)
							runner.run()
							time.Sleep(time.Second * 5)
						}

						r := result{
							Mode:       *mode,
							Procs:      *procs,
							Discard:    *discard,
							V1:         *v1,
							Connbuffer: *connbuffer,

							Connection:  connection,
							Concurrence: concurrence,
							Requestsize: requestsize,
							Writev:      writev,
							Crc:         crc,

							Times:      *times,
							Timesecond: *runsecond,
							Written:    atomic.SwapInt64(&writen, 0),
						}
						r.Speed = r.Written / int64(r.Timesecond) / int64(r.Times)
						results = append(results, r)
					}
				}
			}
		}
	}

	data, _ := json.MarshalIndent(results, "  ", "  ")
	os.WriteFile(*resultf, data, 0o666)
}
