package protocol

import (
    "bufio"
    "bytes"
    "errors"
    "fmt"
    jmtpClient "github.com/jmtp/jmtp-client-go"
    "github.com/jmtp/jmtp-client-go/protocol/v1"
    "github.com/jmtp/jmtp-client-go/util"
    "io"
    "time"
)

const packetMinSize = 3

func PacketEncoder(packet jmtpClient.JmtpPacket) ([]byte, error) {
    var out bytes.Buffer
    codec := packet.Define().Codec()
    head, err := codec.GetFixedHeader(packet)
    if err != nil {
        return nil, err
    }
    out.WriteByte(head)
    out.WriteByte(head ^ 0xFF)
    byteBody, err := codec.EncodeBody(packet)
    if byteBody == nil || len(byteBody) == 0 {
        out.WriteByte(0x00)
    } else {
        if err := util.EncodeRemainingLength(len(byteBody), &out);err != nil {
            return nil, err
        }
        out.Write(byteBody)
    }
    return out.Bytes(), nil
}

func PacketDecoder(reader *bufio.Reader, packetsChain chan jmtpClient.JmtpPacket,errorChain chan error) error {
    for {
        if readPkg, err := reader.Peek(3);err == nil && len(readPkg) >= packetMinSize {
            header := readPkg[0]
            crc := readPkg[1] ^ 0xFF
            if header != crc {
               return errors.New("check header error")
            }
            idx := (header >> 4) & 0x0F
            packetDefine := v1.JMTPV1ProtocolDefineInstance.PacketDefine(idx)
            if packetDefine == nil {
                return errors.New(fmt.Sprintf("can't find packet define, index=%d", idx))
            }
            flagBits := header & 0x0F
            if !packetDefine.CheckFlag(flagBits) {
                // close & continue
                return errors.New(
                    fmt.Sprintf("invalid packet flag. channel will be close. head=%d, idx=%d", header, idx))
            }
            if discarded, err := reader.Discard(2);err != nil || discarded != 2 {
                if err != nil {
                    return err
                } else {
                    return errors.New("reader discard failed")
                }
            }
            remainingLength, err := util.DecodeRemainingLength(reader)
            if err != nil {
                return err
            }
            if remainingLength < 0 {
                continue
            }
            if remainingLength > 0 {
                retryTimes := 0
                for {
                    if retryTimes > 10 {
                        err := errors.New(
                            fmt.Sprintf(
                                "can't read read enough byte stream, expect %d", remainingLength))
                        return err
                    }
                    if payload, err := reader.Peek(remainingLength);err == nil && len(payload) == remainingLength {
                        if discarded, err := reader.Discard(len(payload));err != nil {
                           return err
                        } else if discarded != len(payload) {
                           return errors.New(
                               fmt.Sprintf("discarded length %d not equal payload length %d",
                                   len(payload),
                                   discarded))
                        }
                        packet, err:= packetDefine.Codec().Decode(flagBits, bytes.NewReader(payload))
                        if err != nil {
                            errorChain <- err
                            break
                        }
                        packetsChain <- packet
                        break
                    } else {
                        retryTimes += 1
                    }
                }
            } else {
                packet, err := packetDefine.Codec().Decode(flagBits, nil)
                if err != nil {
                    errorChain <- err
                } else {
                    packetsChain <- packet
                }
            }
        } else {
            if err != nil {
                if err == io.EOF {
                    break
                } else {
                    return err
                }
            }
            time.Sleep(time.Duration(1) * time.Millisecond)
        }
    }
    return nil
}
