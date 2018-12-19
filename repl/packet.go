package repl

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/ump"
	"io"
	"net"
	"strings"
	"time"
)

var (
	ErrBadNodes       = errors.New("BadNodesErr")
	ErrArgLenMismatch = errors.New("ArgLenMismatchErr")
)

type Packet struct {
	proto.Packet
	followersConns []*net.TCPConn
	followersAddrs []string
	IsRelase       int32
	Object         interface{}
	TpObject       *ump.TpObject
	NeedReply      bool
}

func (p *Packet) AfterTp() (ok bool) {
	var err error
	if p.IsErrPacket() {
		err = fmt.Errorf(p.GetOpMsg()+" failed because(%v)", string(p.Data[:p.Size]))
	}
	ump.AfterTP(p.TpObject, err)

	return
}

func (p *Packet) BeforeTp(clusterId string) (ok bool) {
	umpKey := fmt.Sprintf("%s_datanode_stream%v", clusterId, p.GetOpMsg())
	p.TpObject = ump.BeforeTP(umpKey)
	return
}

func (p *Packet) resolveReplicateAddrs() (err error) {
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionPreparePkg, err.Error())
		}
	}()
	if len(p.Arg) < int(p.Arglen) {
		err = ErrArgLenMismatch
		return
	}
	str := string(p.Arg[:int(p.Arglen)])
	replicateAddrs := strings.SplitN(str, proto.AddrSplit, -1)
	followerNum := uint8(len(replicateAddrs) - 1)
	p.followersAddrs = make([]string, followerNum)
	p.followersConns = make([]*net.TCPConn, followerNum)
	if p.RemainReplicates < 0 {
		err = ErrBadNodes
		return
	}

	return
}

func (p *Packet) forceDestoryAllConnect() {
	for i := 0; i < len(p.followersConns); i++ {
		gConnPool.PutConnect(p.followersConns[i], ForceCloseConnect)
	}
}

func (p *Packet) forceDestoryCheckUsedClosedConnect(err error) {
	for i := 0; i < len(p.followersConns); i++ {
		gConnPool.CheckErrorForceClose(p.followersConns[i], p.followersAddrs[i], err)
	}
}

func (p *Packet) PutConnectsToPool() {
	for i := 0; i < len(p.followersConns); i++ {
		gConnPool.PutConnect(p.followersConns[i], NoCloseConnect)
	}
}

func NewPacket() (p *Packet) {
	p = new(Packet)
	p.Magic = proto.ProtoMagic
	p.StartT = time.Now().UnixNano()
	p.NeedReply = true
	return
}

func (p *Packet) IsMasterCommand() bool {
	switch p.Opcode {
	case
		proto.OpDataNodeHeartbeat,
		proto.OpLoadDataPartition,
		proto.OpCreateDataPartition,
		proto.OpDeleteDataPartition,
		proto.OpOfflineDataPartition:
		return true
	}
	return false
}

func (p *Packet) isForwardPacket() bool {
	r := p.RemainReplicates > 0
	return r
}

func NewGetAllWaterMarker(partitionId uint64, extentType uint8) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpGetAllWaterMark
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GeneratorRequestID()
	p.ExtentMode = extentType

	return
}

func NewExtentRepairReadPacket(partitionId uint64, extentId uint64, offset, size int) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentId
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpExtentRepairRead
	p.ExtentMode = proto.NormalExtentMode
	p.ReqID = proto.GeneratorRequestID()

	return
}

func NewStreamReadResponsePacket(requestId int64, partitionId uint64, extentId uint64) (p *Packet) {
	p = new(Packet)
	p.ExtentID = extentId
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpOk
	p.ReqID = requestId
	p.ExtentMode = proto.NormalExtentMode

	return
}

func NewNotifyExtentRepair(partitionId uint64) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpNotifyExtentRepair
	p.PartitionID = partitionId
	p.Magic = proto.ProtoMagic
	p.ExtentMode = proto.NormalExtentMode
	p.ReqID = proto.GeneratorRequestID()

	return
}

func (p *Packet) IsErrPacket() bool {
	return p.ResultCode != proto.OpOk
}

func (p *Packet) getErrMessage() (m string) {
	return fmt.Sprintf("req(%v) err(%v)", p.GetUniqueLogId(), string(p.Data[:p.Size]))
}

var (
	ErrorUnknownOp = errors.New("unknown opcode")
)

func (p *Packet) identificationErrorResultCode(errLog string, errMsg string) {
	if strings.Contains(errLog, ActionReceiveFromFollower) || strings.Contains(errLog, ActionSendToFollowers) ||
		strings.Contains(errLog, ConnIsNullErr) || strings.Contains(errLog, ActionCheckAndAddInfos) {
		p.ResultCode = proto.OpIntraGroupNetErr
		return
	}

	if strings.Contains(errMsg, storage.ErrorParamMismatch.Error()) ||
		strings.Contains(errMsg, ErrorUnknownOp.Error()) {
		p.ResultCode = proto.OpArgMismatchErr
	} else if strings.Contains(errMsg, storage.ErrorExtentNotFound.Error()) ||
		strings.Contains(errMsg, storage.ErrorExtentHasDelete.Error()) {
		p.ResultCode = proto.OpNotExistErr
	} else if strings.Contains(errMsg, storage.ErrSyscallNoSpace.Error()) {
		p.ResultCode = proto.OpDiskNoSpaceErr
	} else if strings.Contains(errMsg, storage.ErrorAgain.Error()) {
		p.ResultCode = proto.OpAgain
	} else if strings.Contains(errMsg, storage.ErrNotLeader.Error()) {
		p.ResultCode = proto.OpNotLeaderErr
	} else if strings.Contains(errMsg, storage.ErrorExtentNotFound.Error()) {
		if p.Opcode != proto.OpWrite {
			p.ResultCode = proto.OpNotExistErr
		} else {
			p.ResultCode = proto.OpIntraGroupNetErr
		}
	} else {
		p.ResultCode = proto.OpIntraGroupNetErr
	}
}

func (p *Packet) PackErrorBody(action, msg string) {
	p.identificationErrorResultCode(action, msg)
	if p.ResultCode == proto.OpDiskNoSpaceErr || p.ResultCode == proto.OpDiskErr {
		p.ResultCode = proto.OpIntraGroupNetErr
	}
	p.Size = uint32(len([]byte(action + "_" + msg)))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:int(p.Size)], []byte(action+"_"+msg))
}
func (p *Packet) ReadFull(c net.Conn, readSize int) (err error) {
	if p.Opcode == proto.OpWrite && readSize == util.BlockSize {
		p.Data, _ = proto.Buffers.Get(util.BlockSize)
	} else {
		p.Data = make([]byte, readSize)
	}
	_, err = io.ReadFull(c, p.Data[:readSize])
	return
}

func (p *Packet) isReadOperation() bool {
	return p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpRead || p.Opcode == proto.OpExtentRepairRead
}

func (p *Packet) ReadFromConnFromCli(c net.Conn, deadlineTime time.Duration) (err error) {
	if deadlineTime != proto.NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(deadlineTime * time.Second))
	}
	header, err := proto.Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer proto.Buffers.Put(header)
	if _, err = io.ReadFull(c, header); err != nil {
		return
	}
	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if p.Arglen > 0 {
		if err = proto.ReadFull(c, &p.Arg, int(p.Arglen)); err != nil {
			return
		}
	}

	if p.Size < 0 {
		return
	}
	size := p.Size
	if p.isReadOperation() && p.ResultCode == proto.OpInitResultCode {
		size = 0
	}
	return p.ReadFull(c, int(size))
}
