package util

import (
    "bytes"
    "errors"
    "fmt"
    "github.com/jmtp/jmtp-client-go/util/fieldcodec"
)

const (
    varUnsignedIntMaxMultiplier = 128 * 128 * 128
    varUnsigneddShortMaxMultiplier = 128
)

type JMTPDecodingReader struct {
    reader *bytes.Reader
}

func NewJMTPDecodingReader(input *bytes.Reader) *JMTPDecodingReader {
    reader := &JMTPDecodingReader{}
    reader.reader = input
    return reader
}

func (r *JMTPDecodingReader) ReadUnsignedTiny() (int16, error) {
    if !r.Readable(1) {
        return 0, errors.New("read unsigned-tiny fault. readable data too short. at least 1 byte")
    }
    b, err := r.ReadByte()
    if err != nil {
        return 0, err
    }
    return int16(0xFF & b), nil
}

func (r *JMTPDecodingReader) ReadVarUnsignedShort() (int16, error) {
    if !r.Readable(1) {
        return 0, errors.New("read var-unsigned-short fault. readable data too short. at least 1 byte")
    }
    var multiplier int16 = 1
    var result int16
    for {
        encodeByte, err := r.ReadByte()
        if err != nil {
            return 0, err
        }
        result += int16(encodeByte & 127) * multiplier
        if (encodeByte & 128) != 0 {
            if multiplier == varUnsigneddShortMaxMultiplier {
                return 0, errors.New("malformed var-unsigned-short length")
            }
            if r.Readable(1) {
                multiplier *= 128
            } else {
                return 0, errors.New("read var-unsigned-short fault. readable data too short")
            }
        } else {
            break
        }
    }
    return result, nil
}

func (r *JMTPDecodingReader) ReadVarUnsignedInt() (int, error) {
    var result int = 0
    if !r.Readable(1) {
        return result, errors.New("read var-unsigned-int fault. readable data too short. at least 1 byte")
    }
    var multiplier int = 1
    for {
        encodedByte, err := r.ReadByte()
        if err != nil {
            return result, err
        }
        result += int(encodedByte & 127) * multiplier
        if (encodedByte & 128) != 0 {
            if multiplier == varUnsignedIntMaxMultiplier {
                return result, errors.New("malformed var-unsigned-int length")
            }
            if r.Readable(1) {
                multiplier *= 128
            } else {
                return result, errors.New("read var-unsigned-int fault. readable data too short")
            }
        } else {
            break
        }
    }
    return result, nil
}

func (r *JMTPDecodingReader) ReadTinyField(codec fieldcodec.FieldCodec) (interface{}, error) {
    len, err := r.ReadUnsignedTiny()
    if err != nil {
        return nil, err
    }
    if len <= 0 {
        return nil, nil
    }
    content, err := r.ReadBytes(int(len))
    if err != nil {
        return nil, err
    }
    return codec.Decode(content)
}

func (r *JMTPDecodingReader) ReadTinyBytesField() ([]byte, error) {
    len, err := r.ReadUnsignedTiny()
    if err != nil || len <= 0{
        return nil, err
    }
    return r.ReadBytes(int(len))
}

func (r *JMTPDecodingReader) ReadVShortField(codec fieldcodec.FieldCodec) (interface{}, error) {
    content, err := r.ReadVShortFieldBytes()
    if err != nil {
        return nil, err
    }
    return codec.Decode(content)
}

func (r *JMTPDecodingReader) ReadVShortFieldBytes() ([]byte, error) {
    len, err := r.ReadVarUnsignedShort()
    if err != nil {
        return nil, err
    }
    if len <= 0 {
        return nil, nil
    }
    return r.ReadBytes(int(len))
}

func (r *JMTPDecodingReader) ReadVIntField(codec fieldcodec.FieldCodec) (interface{}, error) {
    content, err := r.ReadVIntFieldBytes()
    if err != nil {
        return nil, err
    }
    return codec.Decode(content)
}

func (r *JMTPDecodingReader) ReadVIntFieldBytes() ([]byte, error) {
    len, err := r.ReadVarUnsignedInt()
    if err != nil {
        return nil, err
    } else if len <= 0 {
        return nil, nil
    } else {
        return r.ReadBytes(int(len))
    }
}

func (r *JMTPDecodingReader) ReadInt32() (int32, error) {
    if !r.Readable(4) {
        return 0, errors.New("read int32 fault. readable data too short. at least 4 bytes")
    }
    content, err := r.ReadBytes(4)
    if err != nil {
        return 0, err
    }
    return int32(((content[0] & 0xFF) << 24) |
        ((content[1] & 0xFF) << 16) |
        ((content[2] & 0xFF) << 16) |
        ((content[3] & 0xFF) << 16)), nil
}

func (r *JMTPDecodingReader) ReadTinyMap(codec fieldcodec.FieldCodec) (map[string]interface{}, error) {
    len, err := r.ReadUnsignedTiny()
    if err != nil || len <= 0{
        return make(map[string]interface{}, 0), err
    }
    result := make(map[string]interface{}, len)
    var i int16
    for i = 0; i < len; i++ {
        key, err := r.ReadVShortField(fieldcodec.StringCodec)
        if err != nil {
            return result, err
        }
        val, err := r.ReadVShortField(codec)
        if err != nil {
            return result, err
        }
        result[key.(string)] = val
    }
    return result, nil
}

func (r *JMTPDecodingReader) ReadByte() (byte, error) {
    return r.reader.ReadByte()
}

func (r *JMTPDecodingReader) ReadAllByte() ([]byte, error) {
    bytes := make([]byte, 0, r.reader.Len())
    _, err := r.reader.Read(bytes)
    return bytes, err
}

func (r *JMTPDecodingReader) ReadBytes(size int) ([]byte, error) {
    bytes := make([]byte, size)
    if size > 0 {
        num, err := r.reader.Read(bytes)
        if err != nil {
            return nil, err
        }
        if num != size {
            return nil , errors.New(fmt.Sprintf("except to get %d bytes, actually got %d bytes", size, num))
        }
    }
    return bytes, nil
}

func (r *JMTPDecodingReader) Readable(limit int) bool {
    return r.reader.Len() >= limit
}
