package fieldcodec

import "errors"

type FieldCodec interface {
    Encode(input interface{}) ([]byte, error)
    Decode(input []byte) (interface{}, error)
}

var StringCodec = &stringCodec{}

type stringCodec struct {

}

func (sc *stringCodec) Encode(input interface{}) ([]byte, error) {
    var output []byte
    var err error
    if str, ok := input.(string);ok {
       if str != "" {
            output = []byte(str)
       }
    } else {
        err = errors.New("incorrect data type")
    }
    return output, err
}

func (sc *stringCodec) Decode(input []byte) (interface{}, error) {

    return string(input), nil
}


