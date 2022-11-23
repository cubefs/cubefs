package v1

import (
    jmtpClient "github.com/jmtp/jmtp-client-go"
    "bytes"
    "github.com/jmtp/jmtp-client-go/util"
)

var (
    ReportPacketDefineIns = &ReportPacketDefine{}
    ReportPacketCodecIns = &ReportPacketCodec{}
    qosFlag = util.NewBooleanFlag(byte(0x01))
    ssFlag = util.NewBooleanFlag(byte(0x02))
)

type Report struct {
    PacketId    []byte
    ReportType  int16
    SerializeType int16
    Payload []byte
}

func (r *Report) Define() jmtpClient.JmtpPacketDefine {
    return ReportPacketDefineIns
}

func (r *Report) HasAck() bool {
    return false
}

func (r *Report) GetPacketId() []byte {
    return r.PacketId
}

func (r *Report) GetReportType() int16 {
    return r.ReportType
}

func (r *Report) GetSerializeType() int16 {
    return r.SerializeType
}

func (r *Report) GetPayload() []byte {
    return r.Payload
}

func (r *Report) IsHighQos() bool {
    return r.PacketId != nil && len(r.PacketId) > 0
}

func (r *Report) IsSpecificSerialize() bool {
    return r.SerializeType > 0
}

type ReportPacketDefine struct {

}

func (r *ReportPacketDefine) PacketType() *jmtpClient.PacketType {
    return jmtpClient.Report
}

func (r *ReportPacketDefine) Code() byte {
    return r.PacketType().Code()
}

func (r *ReportPacketDefine) CheckFlag(flagBits byte) bool {
    return flagBits == 0
}

func (r *ReportPacketDefine) CreatePacket() jmtpClient.JmtpPacket {
    return &Report{}
}

func (r *ReportPacketDefine) Codec() jmtpClient.JmtpPacketCodec {
    return ReportPacketCodecIns
}

func (r *ReportPacketDefine) ProtocolDefine() jmtpClient.JmtpProtocolDefine {
    return JMTPV1ProtocolDefineInstance
}

type ReportPacketCodec struct {

}

func (r *ReportPacketCodec) EncodeBody(packet jmtpClient.JmtpPacket) ([]byte, error) {
    return encodeBody(packet)
}

func (r *ReportPacketCodec) Decode(flagBits byte, input *bytes.Reader) (jmtpClient.JmtpPacket, error) {
    reader := util.NewJMTPDecodingReader(input)
    report := &Report{}
    if qosFlag.Check(flagBits) {
        if packetId, err := reader.ReadTinyBytesField();err != nil {
            return nil, err
        } else {
            report.PacketId = packetId
        }
    }
    if ssFlag.Check(flagBits) {
        if serializeType, err := reader.ReadVarUnsignedShort();err != nil {
            return nil, err
        } else {
            report.SerializeType = serializeType
        }
    }
    if reportType, err := reader.ReadVarUnsignedShort();err != nil {
        return nil, err
    } else {
        report.ReportType = reportType
    }
    if payload, err := reader.ReadAllByte();err != nil {
        return nil, err
    } else {
        report.Payload = payload
    }
    return report, nil
}

func (r *ReportPacketCodec) GetFixedHeader(packet jmtpClient.JmtpPacket) (byte, error) {
    report := packet.(*Report)
    return packet.Define().PacketType().BuildHeader(
        qosFlag.Get(report.IsHighQos()),
        ssFlag.Get(report.IsSpecificSerialize())), nil
}
