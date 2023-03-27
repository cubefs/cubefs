package decode

import (
	"github.com/cubefs/cubefs/raftstore/walreader/common"
	"github.com/cubefs/cubefs/raftstore/walreader/decode/data"
	"github.com/cubefs/cubefs/raftstore/walreader/decode/meta"
	"github.com/cubefs/cubefs/raftstore/walreader/decode/meta_dbback"
)

type LogCommandDecoder interface {
	Name() string
	Header() common.ColumnValues
	DecodeCommand(command []byte) (common.ColumnValues, error)
}

type DecoderConstructor = func() (LogCommandDecoder, error)

var (
	constructors = make(map[string]DecoderConstructor)
)

func RegisterDecoder(name string, constructor DecoderConstructor) {
	constructors[name] = constructor
}

func GetDecoder(name string) DecoderConstructor {
	if constructor, found := constructors[name]; found {
		return constructor
	}
	return nil
}

func RegisteredDecoders() []string {
	names := make([]string, 0, len(constructors))
	for name := range constructors {
		names = append(names, name)
	}
	return names
}

func init() {
	RegisterDecoder(meta.DecoderName, func() (decoder LogCommandDecoder, e error) {
		return &meta.MetadataCommandDecoder{}, nil
	})
	RegisterDecoder(data.DecoderName, func() (decoder LogCommandDecoder, e error) {
		return &data.DataCommandDecoder{}, nil
	})
	RegisterDecoder(meta_dbback.DecoderName, func() (decoder LogCommandDecoder, e error) {
		return &meta_dbback.MetadataCommandDecoder{}, nil
	})
}
