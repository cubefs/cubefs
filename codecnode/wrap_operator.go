// Copyright 2018 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package codecnode

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

func (s *CodecServer) OperatePacket(p *repl.Packet, c *net.TCPConn) (err error) {
	sz := p.Size
	tpObject := exporter.NewTPCnt(p.GetOpMsg())
	start := time.Now().UnixNano()
	defer func() {
		resultSize := p.Size
		p.Size = sz
		if p.IsErrPacket() {
			err = fmt.Errorf("op(%v) error(%v)", p.GetOpMsg(), string(p.Data[:resultSize]))
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, err))
			log.LogErrorf(logContent)
		} else {
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
			switch p.Opcode {
			default:
				log.LogInfo(logContent)
			}
		}
		p.Size = resultSize
		tpObject.Set(err)
	}()

	switch p.Opcode {
	case proto.OpCodecNodeHeartbeat:
		s.handleHeartbeatPacket(p)
	default:
		p.PackErrorBody(repl.ErrorUnknownOp.Error(), repl.ErrorUnknownOp.Error()+strconv.Itoa(int(p.Opcode)))
	}

	return
}

func (s *CodecServer) handleHeartbeatPacket(p *repl.Packet) {
	var err error
	task := &proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody("ActionHeartbeat", err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}

	go func() {
		request := &proto.HeartBeatRequest{}
		response := &proto.CodecNodeHeartbeatResponse{}
		s.buildHeartBeatResponse(response)

		if task.OpCode == proto.OpCodecNodeHeartbeat {
			marshaled, _ := json.Marshal(task.Request)
			_ = json.Unmarshal(marshaled, request)
			response.Status = proto.TaskSucceeds
		} else {
			response.Status = proto.TaskFailed
			err = fmt.Errorf("illegal opcode")
			response.Result = err.Error()
		}
		task.Response = response
		if err = MasterClient.NodeAPI().ResponseCodecNodeTask(task); err != nil {
			err = errors.Trace(err, "heartbeat to master(%v) failed.", request.MasterAddr)
			log.LogErrorf(err.Error())
			return
		}
	}()

}

func (s *CodecServer) buildHeartBeatResponse(response *proto.CodecNodeHeartbeatResponse) {
	response.Status = proto.TaskSucceeds
}

