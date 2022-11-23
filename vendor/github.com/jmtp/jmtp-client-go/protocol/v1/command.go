package v1

import (
    jmtpClient "github.com/jmtp/jmtp-client-go"
    "bytes"
    "github.com/jmtp/jmtp-client-go/util"
    "github.com/jmtp/jmtp-client-go/util/fieldcodec"
)

var (
    CommandPacketIns = &Command{}
    CommandPacketDefineIns = &CommandPacketDefine{}
    CommandPacketCodecInst = &CommandPacketCodec{}
)

type Command struct {
    PacketId    []byte
    Command string
    Payload []byte
}

func (c *Command) Define() jmtpClient.JmtpPacketDefine {
    return CommandPacketDefineIns
}

func (c *Command) HasAck() bool {
    return false
}


type CommandPacketDefine struct {

}

func (c *CommandPacketDefine) PacketType() *jmtpClient.PacketType {
    return jmtpClient.Command
}

func (c *CommandPacketDefine) Code() byte {
    return c.PacketType().Code()
}

func (c *CommandPacketDefine) CheckFlag(flagBits byte) bool {
    return flagBits == 0
}

func (c *CommandPacketDefine) CreatePacket() jmtpClient.JmtpPacket {
    return &Command{}
}

func (c *CommandPacketDefine) Codec() jmtpClient.JmtpPacketCodec {
    return CommandPacketCodecInst
}

func (c *CommandPacketDefine) ProtocolDefine() jmtpClient.JmtpProtocolDefine {
    return JMTPV1ProtocolDefineInstance
}


type CommandPacketCodec struct {

}

func (c *CommandPacketCodec) EncodeBody(packet jmtpClient.JmtpPacket) ([]byte, error) {
    return encodeBody(packet)
}

func (c *CommandPacketCodec) Decode(flagBits byte, input *bytes.Reader) (jmtpClient.JmtpPacket, error) {
    reader := util.NewJMTPDecodingReader(input)
    command := &Command{}
    if packetId, err := reader.ReadTinyBytesField();err != nil {
        return nil, err
    } else {
        command.PacketId = packetId
    }
    if cmd, err := reader.ReadVShortField(fieldcodec.StringCodec);err != nil {
        return nil, err
    } else {
        command.Command = cmd.(string)
    }
    if payload, err := reader.ReadAllByte();err != nil {
        return nil, err
    } else {
        command.Payload = payload
    }
    return command, nil
}

func (c *CommandPacketCodec) GetFixedHeader(packet jmtpClient.JmtpPacket) (byte, error) {
    return packet.Define().PacketType().BuildHeader(), nil
}


