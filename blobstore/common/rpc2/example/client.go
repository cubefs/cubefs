package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type pingPara struct {
	I int
	S string
}

var _ rpc2.Codec = (*pingPara)(nil)

func (p *pingPara) Size() int {
	b, _ := p.Marshal()
	return len(b)
}

func (p *pingPara) Marshal() ([]byte, error) {
	return json.Marshal(p)
}

func (p *pingPara) MarshalTo(data []byte) (int, error) {
	b, _ := p.Marshal()
	return copy(data, b), nil
}

func (p *pingPara) Unmarshal(data []byte) error {
	return json.Unmarshal(data, p)
}

func runClient() {
	client := rpc2.Client{
		ConnectorConfig: rpc2.ConnectorConfig{Network: "tcp"},
		Retry:           10,
		RetryOn: func(err error) bool {
			status := rpc2.DetectStatusCode(err)
			return status >= 500
		},
		Timeout: time.Second,
	}
	{
		para := pingPara{I: 7, S: "ping string"}
		req, _ := rpc2.NewRequest(context.Background(),
			listenon[int(time.Now().UnixNano())%len(listenon)],
			"/kick", nil, rpc2.Codec2Reader(&para))
		req.ContentLength = int64(para.Size())

		log.Infof("before request para  : %+v", para)
		if err := client.DoWith(req, &para); err != nil {
			panic(rpc2.ErrorString(err))
		}
		log.Infof("after request result : %+v", para)
	}

	para := pingPara{I: 7, S: "ping string"}
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
	var ret pingPara

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
