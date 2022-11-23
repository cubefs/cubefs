package v1

import (
    jmtpClient "github.com/jmtp/jmtp-client-go"
    "bytes"
    "github.com/jmtp/jmtp-client-go/util"
    "github.com/jmtp/jmtp-client-go/util/fieldcodec"
)

var (
    ConnectAckPacket = &ConnectAck{}
    ConnectAckPacketCodecInstance = &connectAckPacketCodec{}
    redirectFlag = util.NewBooleanFlag(byte(0x01))
    retryFlag = util.NewBooleanFlag(byte(0x02))
)

type ConnectAck struct {
    Code    int
    Message string
    RetrySeconds    int
    RedirectUrl string
}

func (c *ConnectAck) GetCode() int {
    return c.Code
}

func (c *ConnectAck) GetMessage() string {
    return c.GetMessage()
}

func (c *ConnectAck) GetRetrySeconds() int {
    return c.GetRetrySeconds()
}

func (c *ConnectAck) GetRedirectUrl() string {
    return c.GetRedirectUrl()
}

func (c *ConnectAck) Define() jmtpClient.JmtpPacketDefine {
    return ConnectAckPacketDefineInstance
}

func (c *ConnectAck) HasAck() bool {
    return false
}

var ConnectAckPacketDefineInstance = &ConnectAckPacketDefine{}

type ConnectAckPacketDefine struct {

}

func (c *ConnectAckPacketDefine) PacketType() *jmtpClient.PacketType {
    return jmtpClient.ConnectAck
}

func (c *ConnectAckPacketDefine) Code() byte {
    return c.PacketType().Code()
}

func (c *ConnectAckPacketDefine) CheckFlag(flagBits byte) bool {
    return flagBits == 0
}

func (c *ConnectAckPacketDefine) CreatePacket() jmtpClient.JmtpPacket {
    return &ConnectAck{}
}

func (c *ConnectAckPacketDefine) Codec() jmtpClient.JmtpPacketCodec {
    return ConnectAckPacketCodecInstance
}

func (c *ConnectAckPacketDefine) ProtocolDefine() jmtpClient.JmtpProtocolDefine {
    return JMTPV1ProtocolDefineInstance
}

type connectAckPacketCodec struct {

}

func (c *connectAckPacketCodec) EncodeBody(packet jmtpClient.JmtpPacket) ([]byte, error) {
    return encodeBody(packet)
}

func (c *connectAckPacketCodec) Decode(flagBits byte, input *bytes.Reader) (jmtpClient.JmtpPacket, error) {
    reader := util.NewJMTPDecodingReader(input)
    connectAck := &ConnectAck{}
    if code, err := reader.ReadVarUnsignedInt();err != nil {
        return nil, err
    } else {
        connectAck.Code = code
    }
    if connectAck.GetCode() != 0 {
        if message, err := reader.ReadVShortField(fieldcodec.StringCodec);err != nil {
            return nil, err
        } else {
            connectAck.Message = message.(string)
        }
    }
    if retryFlag.Check(flagBits) {
        if retrySec, err := reader.ReadVarUnsignedShort();err != nil {
            return nil, err
        } else {
            connectAck.RetrySeconds = int(retrySec)
        }
    }
    if redirectFlag.Check(flagBits) {
        if redirectUrl, err := reader.ReadVShortField(fieldcodec.StringCodec);err != nil {
            return nil, err
        } else {
            connectAck.RedirectUrl = redirectUrl.(string)
        }
    }
    return connectAck, nil
}

func (c *connectAckPacketCodec) GetFixedHeader(packet jmtpClient.JmtpPacket) (byte, error) {
    packetType := packet.Define().PacketType()
    connectAck := packet.(*ConnectAck)
    fixedHeader := packetType.BuildHeader(
        retryFlag.Get(connectAck.GetRetrySeconds() > 0),
        redirectFlag.Get(connectAck.RedirectUrl != ""))
    return fixedHeader, nil
}


