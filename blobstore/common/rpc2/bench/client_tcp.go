package main

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

type netConnv struct{ net.Conn }

func (c netConnv) WriteBuffers(buffers [][]byte) (int, error) {
	v := net.Buffers(buffers)
	nn, err := v.WriteTo(c.Conn)
	return int(nn), err
}

type clientTcp struct{ clientCarrier }

func (c *clientTcp) run() {
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

func (c *clientTcp) doConnection() {
	c.wg.Add(1)
	defer c.wg.Done()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		panic(err)
	}
	if c.writev {
		conn = netConnv{Conn: conn}
	}

	l := c.requestsize
	buff := make([]byte, l)
	for {
		if time.Now().After(c.endtime) {
			return
		}
		n, err := conn.Write(buff)
		if err != nil {
			panic(fmt.Sprintf("%s %v", conn.LocalAddr(), err))
		}
		atomic.AddInt64(&writen, int64(n))
	}
}
