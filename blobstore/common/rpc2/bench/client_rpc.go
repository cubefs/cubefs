package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type clientRpc struct{ clientCarrier }

func (c *clientRpc) run() {
	c.startEndtime()
	if n := c.connection; n == 1 {
		c.doConnection()
	} else if n > 1 {
		for ii := 0; ii < n; ii++ {
			go c.doConnection()
		}
	}
	time.Sleep(time.Second)
	c.wg.Wait()
}

func (c *clientRpc) doConnection() {
	var conf rpc.Config
	conf.Tc.MaxConnsPerHost = 1
	client := rpc.NewClient(&conf)
	if n := c.concurrence; n == 1 {
		c.doConcurrence(client)
	} else if n > 1 {
		for ii := 0; ii < n; ii++ {
			go c.doConcurrence(client)
		}
	}
}

func (c *clientRpc) doConcurrence(client rpc.Client) {
	c.wg.Add(1)
	defer c.wg.Done()

	l := c.requestsize
	buff := make([]byte, l)

	ctx := newCtx()
	url := fmt.Sprintf("http://%s/%d", *addr, l)
	opt := func(req *http.Request) {}
	if c.crc {
		opt = rpc.WithCrcEncode()
	}
	for {
		if time.Now().After(c.endtime) {
			return
		}
		var r io.Reader = &discardReader{n: l}
		if !*discard {
			r = bytes.NewReader(buff)
		}
		req, err := http.NewRequest(http.MethodPut, url, r)
		if err != nil {
			panic(err)
		}
		req.ContentLength = int64(l)
		if err = client.DoWith(ctx, req, nil, opt); err != nil {
			panic(err)
		}
		atomic.AddInt64(&writen, int64(l))
	}
}
