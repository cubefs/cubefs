package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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

type pingPara struct {
	I int
	S string
}

var _ rpc2.Codec = (*pingPara)(nil)

func (p *pingPara) Marshal() ([]byte, error) {
	return json.Marshal(p)
}

func (p *pingPara) Unmarshal(data []byte) error {
	return json.Unmarshal(data, p)
}

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

	para := pingPara{ I: 7, S: "ping string", }
	buff := []byte("ping")
	req, _ := rpc2.NewRequest(context.Background(), "", "/ping", &para, bytes.NewReader(buff))
	var ret pingPara
	resp, err := cli.Do(req, &ret)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	log.Info(resp.ResponseHeader.Status)
	got, err := io.ReadAll(resp.Body)
	log.Infof("got para: %+v | %v", ret, err)
	log.Info("got body:", string(got), err)

	stream.Close()
}
