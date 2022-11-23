package v1

import (
    jmtpClient "github.com/jmtp/jmtp-client-go"
    "bytes"
)

var PongPacket = &Pong{}

type Pong struct {

}

func (p *Pong) PingPacket() {
    // empty implement
}

func (p *Pong) Define() jmtpClient.JmtpPacketDefine {
    return PongPacketDefineIns
}

func (p *Pong) HasAck() bool {
    return false
}


var PongPacketDefineIns = &PongPacketDefine{}

type PongPacketDefine struct {

}

func (p *PongPacketDefine) PacketType() *jmtpClient.PacketType {
    return jmtpClient.Pong
}

func (p *PongPacketDefine) Code() byte {
    return p.PacketType().Code()
}

func (p *PongPacketDefine) CheckFlag(flagBits byte) bool {
    return flagBits == 0
}

func (p *PongPacketDefine) CreatePacket() jmtpClient.JmtpPacket {
    return &Pong{}
}

func (p *PongPacketDefine) Codec() jmtpClient.JmtpPacketCodec {
    return PongPacketCodecInstance
}

func (p *PongPacketDefine) ProtocolDefine() jmtpClient.JmtpProtocolDefine {
    return JMTPV1ProtocolDefineInstance
}

var pongInstance = &Pong{}
var PongPacketCodecInstance = &PongPacketCodec{}

type PongPacketCodec struct {

}

func (p *PongPacketCodec) EncodeBody(packet jmtpClient.JmtpPacket) ([]byte, error) {
    return nil, nil
}

func (p *PongPacketCodec) Decode(flagBits byte, input *bytes.Reader) (jmtpClient.JmtpPacket, error) {
    return pongInstance, nil
}

func (p *PongPacketCodec) GetFixedHeader(packet jmtpClient.JmtpPacket) (byte, error) {
    return packet.Define().PacketType().BuildHeader(), nil
}



