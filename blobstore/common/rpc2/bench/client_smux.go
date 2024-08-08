package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

type clientSmux struct{ clientCarrier }

func (c *clientSmux) run() {
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

func (c *clientSmux) doConnection() {
	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		panic(err)
	}

	sess, err := transport.Client(transport.NetConn(conn, nil, c.writev),
		config.Transport.Transport())
	if err != nil {
		panic(err)
	}

	if n := c.concurrence; n == 1 {
		c.doConcurrence(sess)
	} else if n > 1 {
		for ii := 0; ii < n; ii++ {
			go c.doConcurrence(sess)
		}
	}
}

func (c *clientSmux) doConcurrence(sess *transport.Session) {
	c.wg.Add(1)
	defer c.wg.Done()

	stream, err := sess.OpenStream()
	if err != nil {
		panic(err)
	}

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
		_, err = stream.SizedWrite(ctx, r, l)
		if err != nil {
			panic(fmt.Sprintf("(%s %d) %v", stream.LocalAddr(), stream.ID(), err))
		}
		atomic.AddInt64(&writen, int64(l))
	}
}
