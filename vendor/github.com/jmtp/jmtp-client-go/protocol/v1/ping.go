package v1

import (
    jmtpClient "github.com/jmtp/jmtp-client-go"
    "bytes"
)

var PingPacket = &Ping{}

type Ping struct {

}

func (*Ping) Define() jmtpClient.JmtpPacketDefine {
    return PingPacketDefineIns
}

func (*Ping) HasAck() bool {
    return true
}

func (*Ping) PingPacket() {
    // empty implement
}

var PingPacketDefineIns = &PingPacketDefine{}

type PingPacketDefine struct {

}

func (p *PingPacketDefine) PacketType() *jmtpClient.PacketType {
    return jmtpClient.Ping
}

func (p *PingPacketDefine) Code() byte {
    return p.PacketType().Code()
}

func (p *PingPacketDefine) CheckFlag(flagBits byte) bool {
    return flagBits == 0
}

func (p *PingPacketDefine) CreatePacket() jmtpClient.JmtpPacket {
    return &Ping{}
}

func (p *PingPacketDefine) Codec() jmtpClient.JmtpPacketCodec {
    return pingPacketCodecIns
}

func (p *PingPacketDefine) ProtocolDefine() jmtpClient.JmtpProtocolDefine {
    return JMTPV1ProtocolDefineInstance
}

var pingPacketCodecIns = &PingPacketCodec{}
var pingInstance = &Ping{}

type PingPacketCodec struct {

}

func (codec *PingPacketCodec) EncodeBody(packet jmtpClient.JmtpPacket) ([]byte, error) {
    return nil, nil
}

func (codec *PingPacketCodec) Decode(flagBits byte, input *bytes.Reader) (jmtpClient.JmtpPacket, error) {
    return pingInstance, nil
}

func (codec *PingPacketCodec) GetFixedHeader(packet jmtpClient.JmtpPacket) (byte, error) {
    return packet.Define().PacketType().BuildHeader(), nil
}




