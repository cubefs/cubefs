package v1

import (
    jmtpClient "github.com/jmtp/jmtp-client-go"
)

var JMTPV1ProtocolDefineInstance = &JMTPV1ProtocolDefine{}

type JMTPV1ProtocolDefine struct {

}

func (j *JMTPV1ProtocolDefine) Name() string {
    return "JMTP"
}

func (j *JMTPV1ProtocolDefine) Version() int16 {
    return 1
}

func (j *JMTPV1ProtocolDefine) PacketDefine(code byte) jmtpClient.JmtpPacketDefine {
    return JMTPV1PacketDefineInstance.Get(code)
}

func (j *JMTPV1ProtocolDefine) ConnectPacket(option *jmtpClient.ConnectOption) jmtpClient.ConnectPacket {
    connect := &Connect{
        ProtocolName: j.Name(),
        ProtocolVersion: j.Version(),
        HeartbeatSec: option.HeartbeatSeconds,
        SerializeType: option.SerializeType,
        ApplicationId: option.ApplicationId,
        InstanceId: option.InstanceId,
        Tags: option.Tags,
    }
    return connect
}

func (j *JMTPV1ProtocolDefine) PingPacket() jmtpClient.PingPacket {
    return PingPacket
}

func (j *JMTPV1ProtocolDefine) PongPacket() jmtpClient.PingPacket {
    return PongPacket
}

