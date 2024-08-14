package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type streamReq struct{ str string }

func (s *streamReq) Size() int                       { return len(s.str) }
func (s *streamReq) Marshal() ([]byte, error)        { return []byte(s.str), nil }
func (s *streamReq) MarshalTo(b []byte) (int, error) { return copy(b, []byte(s.str)), nil }
func (s *streamReq) Unmarshal(b []byte) error        { s.str = string(b); return nil }

type streamResp struct{ str string }

func (s *streamResp) Size() int                       { return len(s.str) }
func (s *streamResp) Marshal() ([]byte, error)        { return []byte(s.str), nil }
func (s *streamResp) MarshalTo(b []byte) (int, error) { return copy(b, []byte(s.str)), nil }
func (s *streamResp) Unmarshal(b []byte) error        { s.str = string(b); return nil }

var (
	_ rpc2.Codec = (*streamReq)(nil)
	_ rpc2.Codec = (*streamResp)(nil)
)

func runStream() {
	client := rpc2.Client{ConnectorConfig: rpc2.ConnectorConfig{Network: "tcp"}}
	streamCli := rpc2.StreamClient[streamReq, streamResp]{Client: &client}

	para := pingPara{I: 11, S: "stream string"}
	req, err := rpc2.NewStreamRequest(context.Background(),
		listenon[int(time.Now().UnixNano())%len(listenon)], "/stream", &para)
	if err != nil {
		panic(err)
	}

	var ret pingPara
	cli, err := streamCli.Streaming(req, &ret)
	if err != nil {
		panic(err)
	}
	log.Infof("recv: result  %+v", ret)
	header, _ := cli.Header()
	log.Infof("recv: header  %+v", header.M)
	log.Infof("recv: trailer %+v", cli.Trailer().M)

	waitc := make(chan struct{})
	go func() {
		for {
			resp, errx := cli.Recv()
			if errx == io.EOF {
				close(waitc)
				return
			}
			if errx != nil {
				panic(errx)
			}
			log.Info("recv:", resp.str)
		}
	}()
	for idx := range [10]struct{}{} {
		req := streamReq{str: fmt.Sprintf("request-%d", idx)}
		log.Info("send:", req.str)
		if err = cli.Send(&req); err != nil {
			panic(err)
		}
	}
	cli.CloseSend()
	<-waitc
	log.Infof("recv: trailer %+v", cli.Trailer().M)
	log.Info("done.")
}
