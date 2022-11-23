package util

import (
    "bytes"
    "errors"
    "fmt"
    "github.com/jmtp/jmtp-client-go/util/fieldcodec"
)

const (
    varUnsignedIntMax = 268435455
    varUnsignedShortMax = 16383
    unsignedTinyMax = 255
)

type JMTPEncodingWriter struct {
    byteBuffer  bytes.Buffer
}

func NewJMTPEncodingWriter() *JMTPEncodingWriter {
    return &JMTPEncodingWriter{}
}

func (jmtp *JMTPEncodingWriter) GetBytes() []byte {
    return jmtp.byteBuffer.Bytes()
}

func (jmtp *JMTPEncodingWriter) WriteUnsignedTiny(val int) error {
    if val < 0 {
        return errors.New(fmt.Sprintf("unsigned-tiny not support negative number. value=%d", val))
    }
    if val > unsignedTinyMax {
        return errors.New(fmt.Sprintf("unsigned-tiny overflow. value=%d  max=%d", val, unsignedTinyMax))
    }
    return jmtp.byteBuffer.WriteByte(byte(val & 0xFF))
}

func (jmtp *JMTPEncodingWriter) WriteVarUnsignedShort(val int) error {
    if val < 0 {
        return errors.New(fmt.Sprintf("var-unsigned-short not support negative number. value=%d", val))
    }
    if val > varUnsignedShortMax {
        return errors.New(fmt.Sprintf("var-unsigned-short overflow. value=%d  max=%d", val, varUnsignedShortMax))
    }
    x := val
    var encodeByte byte
    for {
        encodeByte = byte(FloorMod(x, 128))
        x = FloorDiv(x, 128)
        if x <= 0 {
            jmtp.byteBuffer.WriteByte(encodeByte)
            break
        } else {
            jmtp.byteBuffer.WriteByte(encodeByte | 128)
        }
    }
    return nil
}

func (jmtp *JMTPEncodingWriter) WriteVarUnsignedInt(val int) error {
    if val < 0 {
        return errors.New(fmt.Sprintf("var-unsigned-int not support negative number. value=%d", val))
    }
    if val > varUnsignedIntMax {
        return errors.New(fmt.Sprintf("var-unsigned-int overflow. value=%d  max=%d", val, varUnsignedIntMax))
    }
    x := val
    var encodeByte byte
    for {
        encodeByte = byte(FloorMod(x, 128))
        x = FloorDiv(x, 128)
        if x <= 0 {
            jmtp.byteBuffer.WriteByte(encodeByte)
            break
        } else {
            jmtp.byteBuffer.WriteByte(encodeByte | 128)
        }
    }
    return nil
}

func (jmtp *JMTPEncodingWriter) WriteAllBytes(input []byte) error {
    if input == nil || len(input) == 0 {
        return nil
    }
    _, err := jmtp.byteBuffer.Write(input)
    return err
}

func (jmtp *JMTPEncodingWriter) WriteTinyField(val interface{}, codec fieldcodec.FieldCodec) error {
    out, err := codec.Encode(val)
    if err != nil {
        return err
    }
    return jmtp.WriteTinyByte(out)
}

func (jmtp *JMTPEncodingWriter) WriteTinyByte(input []byte) error {
    if input == nil || len(input) == 0 {
        err := jmtp.WriteUnsignedTiny(0)
        if err != nil {
            return err
        }
    } else {
        err := jmtp.WriteUnsignedTiny(len(input));
        if err != nil {
            return err
        }
        err = jmtp.WriteAllBytes(input);
        if err != nil {
            return err
        }
    }
    return nil
}

func (jmtp *JMTPEncodingWriter) WriteShortField(val interface{}, codec fieldcodec.FieldCodec) error {
    out, err := codec.Encode(val)
    if err != nil {
        return err
    }
    return jmtp.WriteShortByte(out)
}

func (jmtp *JMTPEncodingWriter) WriteShortByte(input []byte) error {
    if input == nil || len(input) == 0 {
        err := jmtp.WriteVarUnsignedShort(0)
        if err != nil {
            return err
        }
    } else {
        err := jmtp.WriteVarUnsignedShort(len(input));
        if err != nil {
            return err
        }
        err = jmtp.WriteAllBytes(input);
        if err != nil {
            return err
        }
    }
    return nil
}

func (jmtp *JMTPEncodingWriter) WriteIntField(val interface{}, codec fieldcodec.FieldCodec) error {
    out, err := codec.Encode(val)
    if err != nil {
        return err
    }
    return jmtp.WriteIntByte(out)
}

func (jmtp *JMTPEncodingWriter) WriteIntByte(input []byte) error {
    if input == nil || len(input) == 0 {
        err := jmtp.WriteVarUnsignedInt(0)
        if err != nil {
            return err
        }
    } else {
        err := jmtp.WriteVarUnsignedInt(len(input));
        if err != nil {
            return err
        }
        err = jmtp.WriteAllBytes(input);
        if err != nil {
            return err
        }
    }
    return nil
}

func (jmtp *JMTPEncodingWriter) WriteInt32(val int) error {
    x := uint(val)
    bytes := []byte {
        byte((x >> 24) & 0xFF),
        byte((x >> 16) & 0xFF),
        byte((x >> 8) & 0xFF),
        byte(x & 0xFF),
    }
    return jmtp.WriteAllBytes(bytes)
}

func (jmtp *JMTPEncodingWriter) WriteTinyMap(tags map[string]interface{}, codec fieldcodec.FieldCodec) error {

    if tags == nil || len(tags) == 0 {
        err := jmtp.WriteUnsignedTiny(0)
        if err != nil {
            return err
        }
    } else {
        err := jmtp.WriteUnsignedTiny(len(tags))
        if err != nil {
            return err
        }
        for k, v := range tags {
            jmtp.WriteShortField(k, fieldcodec.StringCodec)
            jmtp.WriteShortField(v, codec)
        }
    }

    return nil
}
