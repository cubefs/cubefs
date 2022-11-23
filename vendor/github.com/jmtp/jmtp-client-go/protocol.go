package jmtp_client_go

import "bytes"

type JmtpPacket interface {
    Define() JmtpPacketDefine
    HasAck() bool
}

type ConnectPacket interface {
    JmtpPacket
    GetProtocolName() string
    GetProtocolVersion() int16
    GetHeartbeatSeconds() int16
    GetSerializeType() int16
    GetApplicationId() int
    GetInstanceId() int
    GetTags()   map[string]interface{}
}

type ConnectAckPacket interface {
    JmtpPacket
    GetCode() int
    GetMessage() string
    GetRetrySeconds() int
    GetRedirectUrl() string
}

type DisconnectPacket interface {
    JmtpPacket
    GetCode() int
    GetMessage() string
    GetRedirectUrl() string
}

type CommandAckPacket interface {
    JmtpPacket
    GetPacketId() []byte
    GetCode() int
    GetMessage() string
    GetPayload() []byte
}

type ReportPacket interface {
    JmtpPacket
    GetPacketId() []byte
    GetReportType() int16
    GetSerializeType() int16
    GetPayload() []byte
    IsHighQos() bool
    IsSpecificSerialize() bool
}

type ReportAckPacket interface {
    JmtpPacket
    GetPacketId() []byte
    GetCode() int
    GetMessage() string
}

type JmtpPacketCodec interface {
    EncodeBody(packet JmtpPacket) ([]byte, error)
    Decode(flagBits byte, input *bytes.Reader) (JmtpPacket, error)
    GetFixedHeader(packet JmtpPacket) (byte, error)
}

type JmtpPacketDefine interface {
    PacketType() *PacketType
    Code() byte
    CheckFlag(flagBits byte) bool
    CreatePacket() JmtpPacket
    Codec() JmtpPacketCodec
    ProtocolDefine() JmtpProtocolDefine
}

type JmtpProtocolDefine interface {
    Name() string
    Version() int16
    PacketDefine(code byte) JmtpPacketDefine
    ConnectPacket(option *ConnectOption) ConnectPacket
    PingPacket() PingPacket
    PongPacket() PingPacket
}

var Connect = NewPacketType(byte(0x1))
var ConnectAck = NewPacketType(byte(0x2))
var Ping = NewPacketType(byte(0x3))
var Pong = NewPacketType(byte(0x4))
var Disconnect = NewPacketType(byte(0x5))
var Report = NewPacketType(byte(0x6))
var ReportAck = NewPacketType(byte(0x7))
var Command = NewPacketType(byte(0x8))
var CommandAck = NewPacketType(byte(0x9))


type PacketType struct {
    code byte
    headerBits byte
}

func (p *PacketType) Check(t PacketType) bool {
    return *p == t
}

func (p *PacketType) Code() byte {
    return p.code
}

func (p *PacketType) BuildHeader(flagBits ...byte) byte{
    header := p.headerBits
    for _, flagBit := range flagBits {
        header |= flagBit
    }
    return header
}

func NewPacketType(t byte) *PacketType {
    pt := &PacketType{
        code: t,
    }
    pt.headerBits = t << 4
    return pt
}

type ConnectOption struct {
    HeartbeatSeconds    int16
    SerializeType   int16
    ApplicationId   int
    InstanceId  int
    Tags    map[string]interface{}
}

type PingPacket interface {
    JmtpPacket
    PingPacket()
}

