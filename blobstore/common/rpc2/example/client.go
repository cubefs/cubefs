package main

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type message struct {
	I int
	S string
}

type paraCodec = rpc2.AnyCodec[message]

func runClient() {
	client := rpc2.Client{
		ConnectorConfig: rpc2.ConnectorConfig{Network: "tcp"},
		Retry:           10,
		RetryOn: func(err error) bool {
			status := rpc2.DetectStatusCode(err)
			return status >= 500
		},
		Timeout: util.Duration{Duration: time.Second},
	}
	{
		var para paraCodec
		para.Value = message{I: 7, S: "ping string"}
		log.Infof("before request para  : %+v", para)
		if err := client.Request(context.Background(),
			listenon[int(time.Now().UnixNano())%len(listenon)],
			"/kick", &para, &para); err != nil {
			panic(rpc2.ErrorString(err))
		}
		log.Infof("after request result : %+v", para)
	}

	var para paraCodec
	para.Value = message{I: 7, S: "ping string"}
	buff := []byte("ping")
	req, _ := rpc2.NewRequest(context.Background(),
		listenon[int(time.Now().UnixNano())%len(listenon)],
		"/ping", &para, bytes.NewReader(buff))
	req.Trailer.SetLen("trailer-1", 1)
	req.AfterBody = func() error {
		req.Trailer.Set("trailer-1", "xX")
		return nil
	}
	req.OptionCrc()
	req.Option(func(r *rpc2.Request) {
		log.Info("run request option")
	})
	var ret paraCodec

	log.Infof("before request header : %+v", req.Header.M)
	log.Infof("before request trailer: %+v", req.Trailer.M)
	log.Infof("before request para   : %+v", para)
	log.Infof("bofore request body   : %s", string(buff))
	log.Infof("bofore request result : %+v", ret)

	resp, err := client.Do(req, &ret)
	if err != nil {
		panic(rpc2.ErrorString(err))
	}
	req.Header.Set("ignored", "x")
	log.Infof("after request header : %+v", req.Header.M)
	log.Infof("after request trailer: %+v", req.Trailer.M)
	log.Infof("after response header : %+v", resp.Header.M)
	log.Infof("after response trailer: %+v", resp.Trailer.M)

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(rpc2.ErrorString(err))
	}
	if err = resp.Body.Close(); err != nil {
		panic(rpc2.ErrorString(err))
	}
	log.Infof("after response status : %d", resp.ResponseHeader.Status)
	log.Infof("after response header : %+v", resp.Header.M)
	log.Infof("after response trailer: %+v", resp.Trailer.M)
	log.Infof("after response body   : %+v", string(got))
	log.Infof("after response result : %+v", ret)

	client.Close()
}
