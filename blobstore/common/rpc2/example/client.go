package main

import (
	"bytes"
	"context"
	"net"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type connector struct {
	stream *transport.Stream
}

func (c *connector) Get(context.Context, string) (*transport.Stream, error) { return c.stream, nil }
func (c *connector) Put(context.Context, *transport.Stream) error           { return nil }

func runClient() {
	conn, err := net.Dial("tcp", listenon[int(time.Now().UnixNano())%len(listenon)])
	if err != nil {
		panic(err)
	}
	session, err := transport.Client(conn, nil)
	if err != nil {
		panic(err)
	}
	stream, err := session.OpenStream()
	if err != nil {
		panic(err)
	}
	cli := rpc2.Client{Connector: &connector{stream: stream}}

	buff := []byte("ping")
	req := rpc2.NewRequest(context.Background(), "", "", bytes.NewReader(buff))
	resp, err := cli.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	log.Info(resp.ResponseHeader.Status)

	time.Sleep(time.Second)
	stream.Close()
}
