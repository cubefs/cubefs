package v1

import (
    jmtpClient "github.com/jmtp/jmtp-client-go"
    "github.com/jmtp/jmtp-client-go/util"
    "bytes"
    "github.com/jmtp/jmtp-client-go/util/fieldcodec"
)

type Connect struct {
    ProtocolName    string
    ProtocolVersion int16
    HeartbeatSec    int16
    SerializeType   int16
    ApplicationId   int
    InstanceId      int
    Tags    map[string]interface{}
}

func NewConnectPacket(protocolName string,
    protocolVer int16,
    heartbeatSec int16,
    serializeType int16,
    applicationId int,
    instanceId int,
    tags map[string]interface{}) *Connect {

        return &Connect {
            ProtocolName: protocolName,
            ProtocolVersion: protocolVer,
            HeartbeatSec: heartbeatSec,
            SerializeType: serializeType,
            ApplicationId: applicationId,
            InstanceId: instanceId,
            Tags: tags,
        }
}

func (c *Connect) GetProtocolName() string {
    return c.ProtocolName
}

func (c *Connect) GetProtocolVersion() int16 {
    return c.ProtocolVersion
}

func (c *Connect) GetHeartbeatSeconds() int16 {
    return c.HeartbeatSec
}

func (c *Connect) GetSerializeType() int16 {
    return c.SerializeType
}

func (c *Connect) GetApplicationId() int {
    return c.ApplicationId
}

func (c *Connect) GetInstanceId() int {
    return c.InstanceId
}

func (c *Connect) GetTags() map[string]interface{} {
    return c.Tags
}

func (c *Connect) Define() jmtpClient.JmtpPacketDefine {
    return ConnectPacketDefineInstance
}

func (c *Connect) HasAck() bool {
    return false
}

var ConnectPacketDefineInstance = &ConnectPacketDefine{}

type ConnectPacketDefine struct {

}

func (c *ConnectPacketDefine) Codec() jmtpClient.JmtpPacketCodec {
    return ConnectPacketCodecInstance
}

func (c *ConnectPacketDefine) ProtocolDefine() jmtpClient.JmtpProtocolDefine{
    return JMTPV1ProtocolDefineInstance
}

func (c *ConnectPacketDefine) PacketType() *jmtpClient.PacketType {
    return jmtpClient.Connect
}

func (c *ConnectPacketDefine) Code() byte {
    return c.PacketType().Code()
}

func (c *ConnectPacketDefine) CheckFlag(flagBits byte) bool {
    return flagBits == 0
}

func (c *ConnectPacketDefine) CreatePacket() jmtpClient.JmtpPacket {
    return &Connect{}
}

var ConnectPacketCodecInstance = &connectPacketCodec{}

type connectPacketCodec struct {

}

func (cpc *connectPacketCodec) EncodeBody(packet jmtpClient.JmtpPacket) ([]byte, error) {
    return encodeBody(packet)
}

func (cpc *connectPacketCodec) Decode(flagBits byte, input *bytes.Reader) (jmtpClient.JmtpPacket, error) {
    reader := util.NewJMTPDecodingReader(input)
    conn := &Connect{}
    if protocolName, err := reader.ReadTinyField(fieldcodec.StringCodec); err != nil {
        return nil, err
    } else {
        conn.ProtocolName = protocolName.(string)
    }
    if protocolVer, err := reader.ReadUnsignedTiny(); err != nil {
        return nil , err
    } else {
        conn.ProtocolVersion = protocolVer
    }
    if heartbeatSec, err := reader.ReadVarUnsignedShort(); err != nil {
        return nil, err
    } else {
        conn.HeartbeatSec = heartbeatSec
    }
    if serializeType, err := reader.ReadVarUnsignedShort(); err != nil {
        return nil, err
    } else {
        conn.SerializeType = serializeType
    }
    if appId, err := reader.ReadInt32(); err != nil {
        return nil, err
    } else {
        conn.ApplicationId = int(appId)
    }
    if insId, err := reader.ReadInt32(); err != nil {
        conn.InstanceId = int(insId)
    }
    if tag, err := reader.ReadTinyMap(fieldcodec.StringCodec); err != nil {
        return nil, err
    } else {
        conn.Tags = tag
    }
    return conn, nil
}

func (cpc *connectPacketCodec) GetFixedHeader(packet jmtpClient.JmtpPacket) (byte, error) {
    return packet.Define().PacketType().BuildHeader(), nil
}




