package v1

import (
    jmtpClient "github.com/jmtp/jmtp-client-go"
    "bytes"
    "github.com/jmtp/jmtp-client-go/util"
    "github.com/jmtp/jmtp-client-go/util/fieldcodec"
)

var (
    ReportAckPacketCodecIns = &ReportAckPacketCodec{}
    ReportAckPacketDefineIns = &ReportAckPacketDefine{}
)

type ReportAck struct {
    PacketId    []byte
    Code    int
    Message string
}

func (r *ReportAck) Define() jmtpClient.JmtpPacketDefine {
    return ReportAckPacketDefineIns
}

func (r *ReportAck) HasAck() bool {
    return false
}

func (r *ReportAck) GetPacketId() []byte {
    return r.PacketId
}

func (r *ReportAck) GetCode() int {
    return r.Code
}

func (r *ReportAck) GetMessage() string {
    return r.Message
}

type ReportAckPacketDefine struct {

}

func (r *ReportAckPacketDefine) PacketType() *jmtpClient.PacketType {
    return jmtpClient.ReportAck
}

func (r *ReportAckPacketDefine) Code() byte {
    return r.PacketType().Code()
}

func (r *ReportAckPacketDefine) CheckFlag(flagBits byte) bool {
    return flagBits == 0
}

func (r *ReportAckPacketDefine) CreatePacket() jmtpClient.JmtpPacket {
    return &ReportAck{}
}

func (r *ReportAckPacketDefine) Codec() jmtpClient.JmtpPacketCodec {
    return ReportAckPacketCodecIns
}

func (r *ReportAckPacketDefine) ProtocolDefine() jmtpClient.JmtpProtocolDefine {
    return JMTPV1ProtocolDefineInstance
}

type ReportAckPacketCodec struct {

}

func (r *ReportAckPacketCodec) EncodeBody(packet jmtpClient.JmtpPacket) ([]byte, error) {
    return encodeBody(packet)
}

func (r *ReportAckPacketCodec) Decode(flagBits byte, input *bytes.Reader) (jmtpClient.JmtpPacket, error) {
    reader := util.NewJMTPDecodingReader(input)
    reportAck := &ReportAck{}
    if packetId, err := reader.ReadTinyBytesField();err != nil {
        return nil, err
    } else {
        reportAck.PacketId = packetId
    }
    if code, err := reader.ReadVarUnsignedInt();err != nil {
        return nil, err
    } else {
        reportAck.Code = code
    }
    if reportAck.GetCode() != 0 {
        if message, err := reader.ReadVShortField(fieldcodec.StringCodec);err != nil {
            return nil, err
        } else {
            reportAck.Message = message.(string)
        }
    }

    return reportAck, nil
}

func (r *ReportAckPacketCodec) GetFixedHeader(packet jmtpClient.JmtpPacket) (byte, error) {
    panic("implement me")
}
