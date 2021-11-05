package mocktest

import (
	"bytes"
	"net"
	"net/http"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

const (
	ColonSeparator = ":"
	hostAddr       = "127.0.0.1:8080"
)

func responseAckOKToMaster(conn net.Conn, p *proto.Packet, data []byte) error {
	if len(data) != 0 {
		p.PacketOkWithBody(data)
	} else {
		p.PacketOkReply()
	}
	return p.WriteToConn(conn, proto.WriteDeadlineTime)
}

func responseAckErrToMaster(conn net.Conn, p *proto.Packet, err error) error {
	status := proto.OpErr
	buf := []byte(err.Error())
	p.PacketErrorWithBody(status, buf)
	p.ResultCode = proto.TaskFailed
	return p.WriteToConn(conn, proto.WriteDeadlineTime)
}

func PostToMaster(method, url string, reqData []byte) (resp *http.Response, err error) {
	client := &http.Client{}
	reader := bytes.NewReader(reqData)
	client.Timeout = time.Second * 3
	var req *http.Request
	if req, err = http.NewRequest(method, url, reader); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	resp, err = client.Do(req)
	return
}
