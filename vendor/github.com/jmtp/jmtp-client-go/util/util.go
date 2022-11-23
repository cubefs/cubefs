package util

import (
    "fmt"
    "encoding/binary"
    "bytes"
    "errors"
    "bufio"
    "strings"
    "math/big"
)

const (
    PacketMaxSize = 268435455
    PacketMinSize = 3
)

var hexChars = []byte {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'}


func Uint2Byte(x uint64) byte {
    out := make([]byte, 1)
    binary.PutUvarint(out, x)
    return out[0]
}

func Byte2Int(in byte) (int, error) {
    byteArr := make([]byte, 1)
    byteArr = append(byteArr, in)
    reader := bytes.NewReader(byteArr)
    i, err := binary.ReadVarint(reader)
    return int(i), err
}

func FloorMod(x int, y int) int {
    return x - FloorDiv(x, y) * y
}

func FloorDiv(x int, y int) int {
    r := x / y
    if (x ^ y) < 0 && (r * y != x) {
        r--
    }

    return r
}

func ReadableHexString(data []byte) string{

    var output string
    if data == nil {
        return output
    }
    if len(data) > 32 {
        output = fmt.Sprintf("(len:%d)%s...", len(data), BytesToHexString(data, 32))
    } else {
        output = fmt.Sprintf("(len:%d)%s", len(data), BytesToHexString(data, len(data)))
    }
    return output
}

func BytesToHexString(input []byte, length int) string {
    var output string
    if input != nil {
        for i := 0; i < length; i++ {
            output += string(hexChars[(input[i] >> 4) & 0x0F])
            output += string(hexChars[input[i] & 0x0F])
        }
    }
    return output
}

func HexStrToBytes(hexStr string) ([]byte, error) {
    buffer := new(bytes.Buffer)
    hexArray :=strings.Split(hexStr, " ")
    for _, hex := range hexArray {
        bigInt := new(big.Int)
        bigInt, _ = bigInt.SetString(hex, 16)
        bytes := bigInt.Bytes()
        if len(bytes) == 0 {
            buffer.Write([]byte{0})
        } else {
            buffer.Write(bigInt.Bytes())
        }
    }
    return buffer.Bytes(), nil
}

func EncodeRemainingLength(len int, out *bytes.Buffer) error {
    if len > PacketMaxSize {
        return errors.New("remaining length overflow")
    }
    x := len
    var encodeByte byte
    for {
        encodeByte = Uint2Byte(uint64(FloorMod(int(x), 128)))
        x = FloorDiv(x, 128)
        if x > 0 {
            out.WriteByte(encodeByte | 128)
        } else {
            out.WriteByte(encodeByte)
        }
        if x <= 0 {
            break
        }
    }
    return nil
}

func DecodeRemainingLength(in *bufio.Reader) (int, error) {

    multiplier := 1
    remainingLength := 0
    for {
        encodeByte, err := in.ReadByte()
        if err != nil {
            return remainingLength, err
        }
        remainingLength += (int(encodeByte) & 127) * multiplier
        //remainingLength += javaEncodeInt * multiplier
        if (int(encodeByte) & 128) != 0 {
            if multiplier == 128 * 128 * 128 {
                return remainingLength, errors.New("malformed remaining length")
            }
            if _, err := in.Peek(1);err == nil {
                multiplier *= 128
            } else {
                return -1, nil
            }
        } else {
            break
        }
    }

    return remainingLength, nil
}
