package v1

import (
    jmtpClient "github.com/jmtp/jmtp-client-go"
)

var JMTPV1PacketDefineInstance = newJMTPV1PacketDefine()

type jmtpV1PacketDefine struct {
    packetDefines []jmtpClient.JmtpPacketDefine
}

func newJMTPV1PacketDefine() *jmtpV1PacketDefine{
    jpd := &jmtpV1PacketDefine{}
    jpd.packetDefines = make([]jmtpClient.JmtpPacketDefine, 16)
    jpd.packetDefines[ConnectPacketDefineInstance.Code()] = ConnectPacketDefineInstance
    jpd.packetDefines[ConnectAckPacketDefineInstance.Code()] = ConnectAckPacketDefineInstance
    jpd.packetDefines[PingPacketDefineIns.Code()] = PingPacketDefineIns
    jpd.packetDefines[PongPacketDefineIns.Code()] = PongPacketDefineIns
    jpd.packetDefines[CommandPacketDefineIns.Code()] = CommandPacketDefineIns
    jpd.packetDefines[CommandPacketDefineIns.Code()] = CommandPacketDefineIns
    jpd.packetDefines[DisconnectPacketDefineIns.Code()] = DisconnectPacketDefineIns
    jpd.packetDefines[ReportPacketDefineIns.Code()] = ReportPacketDefineIns
    jpd.packetDefines[ReportAckPacketDefineIns.Code()] = ReportAckPacketDefineIns
    return jpd
}

func (j *jmtpV1PacketDefine) Get(code byte) jmtpClient.JmtpPacketDefine{
    if code < 0 || code > 15 {
        return nil
    }
    return j.packetDefines[code]
}
