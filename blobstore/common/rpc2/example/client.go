package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type dialer struct{}

func (dialer) Dial(context.Context, string) (net.Conn, error) {
	return net.Dial("tcp", listenon[int(time.Now().UnixNano())%len(listenon)])
}

func makeConnector() rpc2.Connector {
	return rpc2.DefaultConnector(dialer{}, rpc2.ConnectorConfig{})
}

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
	client := rpc2.Client{Connector: makeConnector()}

	para := pingPara{I: 7, S: "ping string"}
	buff := []byte("ping")
	req, _ := rpc2.NewRequest(context.Background(), "", "/ping", &para, bytes.NewReader(buff))
	var ret pingPara
	resp, err := client.Do(req, &ret)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	log.Info(resp.ResponseHeader.Status)
	got, err := io.ReadAll(resp.Body)
	log.Infof("got para: %+v | %v", ret, err)
	log.Info("got body:", string(got), err)

	client.Connector.Close()
}
