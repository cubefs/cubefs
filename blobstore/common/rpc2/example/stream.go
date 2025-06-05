package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type (
	streamReq  rpc2.AnyCodec[string]
	streamResp rpc2.AnyCodec[string]
)

func runStream() {
	client := rpc2.Client{ConnectorConfig: rpc2.ConnectorConfig{Network: "tcp"}}
	streamCli := rpc2.StreamClient[streamReq, streamResp]{Client: &client}

	var para paraCodec
	para.Value = message{I: 11, S: "stream string"}
	req, err := rpc2.NewStreamRequest(context.Background(),
		listenon[int(time.Now().UnixNano())%len(listenon)], "/stream", &para)
	if err != nil {
		panic(err)
	}

	var ret paraCodec
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
			log.Info("recv:", resp.Value)
		}
	}()
	for idx := range [10]struct{}{} {
		var req streamReq
		req.Value = fmt.Sprintf("request-%d", idx)
		log.Info("send:", req.Value)
		if err = cli.Send(&req); err != nil {
			panic(err)
		}
	}
	cli.CloseSend()
	<-waitc
	log.Infof("recv: trailer %+v", cli.Trailer().M)
	log.Info("done.")
}
