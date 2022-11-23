package protocol

import (
    "testing"
    "github.com/jmtp/jmtp-client-go/util"
    "bytes"
    "bufio"
)

var EncodingMap = make(map[int][]byte)

func init() {
   if bytes, err := util.HexStrToBytes("00");err == nil {
      EncodingMap[0] = bytes
   }
   if bytes, err := util.HexStrToBytes("7F");err == nil {
       EncodingMap[127] = bytes
   }
   if bytes, err := util.HexStrToBytes("80 01");err == nil {
       EncodingMap[128] = bytes
   }
   if bytes, err := util.HexStrToBytes("FF 7F");err == nil {
       EncodingMap[16383] = bytes
   }
   if bytes, err := util.HexStrToBytes("80 80 01");err == nil {
       EncodingMap[16384] = bytes
   }
   if bytes, err := util.HexStrToBytes("FF FF 7F");err == nil {
       EncodingMap[2097151] = bytes
   }
   if bytes, err := util.HexStrToBytes("80 80 80 01");err == nil {
       EncodingMap[2097152] = bytes
   }
   if bytes, err := util.HexStrToBytes("FF FF FF 7F");err == nil {
       EncodingMap[268435455] = bytes
   }
}

func TestJmtpRemainingLengthEncode(t *testing.T) {
    for k, v := range EncodingMap {
        out := new(bytes.Buffer)
        err := util.EncodeRemainingLength(k, out)
        if err != nil {
            t.Error(err)
        }
        if len(v) != out.Len() {
            t.Errorf("encode bytes length error, expect %d but len(%d)", len(v), out.Len())
        }
        for i, b := range out.Bytes() {
            if b != v[i] {
                t.Errorf("byte %d not equals %d", b, v[i])
            }
        }
    }
}

func TestJmtpRemainingLengthDecode(t *testing.T) {
    for k, v := range EncodingMap {
        reader := bufio.NewReader(bytes.NewReader(v))
        remainLength, err := util.DecodeRemainingLength(reader)
        if err != nil {
            t.Errorf("decode %d err: %v", k, err)
        }
        if remainLength != k {
            t.Errorf("decode bytes length error, expect %d but len(%d)", len(v), remainLength)
        }
    }
}
