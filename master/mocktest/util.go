package mocktest

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

const (
	ColonSeparator = ":"
	hostAddr       = "127.0.0.1:8080"
)

var (
	LogOn   = os.Getenv("DOCKER_TESTING_LOG_OFF") == ""
	Print   = fmt.Print
	Printf  = fmt.Printf
	Println = fmt.Println
)

func init() {
	if !LogOn {
		SetOutput(io.Discard)
	}
}

// SetOutput reset fmt output writer.
func SetOutput(w io.Writer) {
	Print = func(a ...interface{}) (int, error) { return fmt.Fprint(w, a...) }
	Printf = func(format string, a ...interface{}) (int, error) { return fmt.Fprintf(w, format, a...) }
	Println = func(a ...interface{}) (int, error) { return fmt.Fprintln(w, a...) }
}

func Log(tb testing.TB, a ...interface{}) {
	if LogOn {
		tb.Log(a...)
	}
}

func responseAckOKToMaster(conn net.Conn, p *proto.Packet, data []byte) error {
	if len(data) != 0 {
		p.PacketOkWithBody(data)
	} else {
		p.PacketOkReply()
	}
	return p.WriteToConn(conn)
}

func responseAckErrToMaster(conn net.Conn, p *proto.Packet, err error) error {
	status := proto.OpErr
	buf := []byte(err.Error())
	p.PacketErrorWithBody(status, buf)
	p.ResultCode = proto.TaskFailed
	return p.WriteToConn(conn)
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
