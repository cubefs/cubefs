package main

import (
	"bytes"
	"io"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
)

type clientRpc2 struct{ clientCarrier }

func (c *clientRpc2) run() {
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

func (c *clientRpc2) doConnection() {
	client := &rpc2.Client{
		ConnectorConfig: rpc2.ConnectorConfig{
			Transport:            config.Transport,
			Network:              "tcp",
			MaxSessionPerAddress: 1,
			ConnectionWriteV:     c.writev,
		},
		Retry: 1000,
	}
	if n := c.concurrence; n == 1 {
		c.doConcurrence(client)
	} else if n > 1 {
		for ii := 0; ii < n; ii++ {
			go c.doConcurrence(client)
		}
	}
}

func (c *clientRpc2) doConcurrence(client *rpc2.Client) {
	c.wg.Add(1)
	defer c.wg.Done()

	l := c.requestsize
	buff := make([]byte, l)
	ctx := newCtx()
	for {
		if time.Now().After(c.endtime) {
			return
		}
		var r io.Reader = &discardReader{n: l}
		if !*discard {
			r = bytes.NewReader(buff)
		}
		req, err := rpc2.NewRequest(ctx, *addr, "/", nil, r)
		if err != nil {
			panic(err)
		}
		if !*discard {
			req.ContentLength = int64(l)
		}
		if c.crc {
			req.OptionCrcUpload()
		}
		if err = client.DoWith(req, nil); err != nil {
			panic(err)
		}
		atomic.AddInt64(&writen, int64(l))
	}
}
