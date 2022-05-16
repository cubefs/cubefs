package ecnode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/log"
)

func DoRequest(request *repl.Packet, addr string, timeoutSec int, e *EcNode) (err error) {
	if _, ok := e.nodeDialFailMap.Load(addr); ok {
		err = errors.New(fmt.Sprintf("no route to host %v", addr))
		return
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		e.nodeDialFailMap.Store(addr, time.Now())
		return
	}

	defer func() {
		err = conn.Close()
		if err != nil {
			log.LogErrorf("close tcp connection fail, host(%v) error(%v)", addr, err)
		}
	}()

	err = request.WriteToConn(conn, proto.WriteDeadlineTime)
	if err != nil {
		err = errors.New(fmt.Sprintf("write to host(%v) error(%v)", addr, err))
		return
	}

	err = request.ReadFromConn(conn, timeoutSec) // read the response
	if err != nil {
		err = errors.New(fmt.Sprintf("read from host(%v) error(%v)", addr, err))
		return
	}

	return
}

func NewReadReply(ctx context.Context, packet *repl.Packet) *repl.Packet {
	p := repl.NewPacket(ctx)
	p.Opcode = packet.Opcode
	p.ReqID = packet.ReqID
	p.PartitionID = packet.PartitionID
	p.ExtentID = packet.ExtentID
	p.Size = packet.Size
	p.ExtentOffset = packet.ExtentOffset
	p.KernelOffset = packet.KernelOffset
	p.ExtentType = proto.NormalExtentType
	return p
}

func Response(w http.ResponseWriter, code int, data interface{}, msg string) (err error) {
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
		Msg  string      `json:"msg"`
	}{
		Code: code,
		Data: data,
		Msg:  msg,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return
	}

	_, err = w.Write(jsonBody)
	return
}
