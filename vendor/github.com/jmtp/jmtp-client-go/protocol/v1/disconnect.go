package v1

import (
    jmtpClient "github.com/jmtp/jmtp-client-go"
    "bytes"
    "github.com/jmtp/jmtp-client-go/util"
    "github.com/jmtp/jmtp-client-go/util/fieldcodec"
)

var (
    DisconnectPacketDefineIns = &DisconnectPacketDefine{}
    DisconnectPacketCodecIns = &DisconnectPacketCodec{}
    disconnectRedirectFlag = util.NewBooleanFlag(byte(0x01))
)

type Disconnect struct {
    Code    int
    Message string
    RedirectUrl string
}

func (d *Disconnect) Define() jmtpClient.JmtpPacketDefine {
    return DisconnectPacketDefineIns
}

func (d *Disconnect) HasAck() bool {
    return false
}

func (d *Disconnect) GetCode() int {
    return d.Code
}

func (d *Disconnect) GetMessage() string {
    return d.Message
}

func (d *Disconnect) GetRedirectUrl() string {
    return d.RedirectUrl
}

type DisconnectPacketDefine struct {

}

func (d *DisconnectPacketDefine) PacketType() *jmtpClient.PacketType {
    return jmtpClient.Disconnect
}

func (d *DisconnectPacketDefine) Code() byte {
    return d.PacketType().Code()
}

func (d *DisconnectPacketDefine) CheckFlag(flagBits byte) bool {
    return (flagBits & 0xE) == 0
}

func (d *DisconnectPacketDefine) CreatePacket() jmtpClient.JmtpPacket {
    return &Disconnect{}
}

func (d *DisconnectPacketDefine) Codec() jmtpClient.JmtpPacketCodec {
    return DisconnectPacketCodecIns
}

func (d *DisconnectPacketDefine) ProtocolDefine() jmtpClient.JmtpProtocolDefine {
    return JMTPV1ProtocolDefineInstance
}

type DisconnectPacketCodec struct {

}

func (d *DisconnectPacketCodec) EncodeBody(packet jmtpClient.JmtpPacket) ([]byte, error) {
    return encodeBody(packet)
}

func (d *DisconnectPacketCodec) Decode(flagBits byte, input *bytes.Reader) (jmtpClient.JmtpPacket, error) {
    reader := util.NewJMTPDecodingReader(input)
    disconnect := &Disconnect{}
    if code, err := reader.ReadVarUnsignedInt();err != nil {
        return nil, err
    } else {
        disconnect.Code = code
    }
    if message, err := reader.ReadVShortField(fieldcodec.StringCodec);err != nil {
        return nil, err
    } else {
        disconnect.Message = message.(string)
    }
    if disconnectRedirectFlag.Check(flagBits) {
        if redirectUrl, err := reader.ReadVShortField(fieldcodec.StringCodec);err != nil {
            return nil, err
        } else {
            disconnect.RedirectUrl = redirectUrl.(string)
        }
    }
    return disconnect, nil
}

func (d *DisconnectPacketCodec) GetFixedHeader(packet jmtpClient.JmtpPacket) (byte, error) {
    packetType := packet.Define().PacketType()
    disconnectPacket := packet.(*Disconnect)
    return packetType.BuildHeader(disconnectRedirectFlag.Get(disconnectPacket.RedirectUrl != "")), nil
}

