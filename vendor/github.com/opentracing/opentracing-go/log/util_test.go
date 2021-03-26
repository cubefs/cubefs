package log

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

var nilInterface io.Reader

func TestInterleavedKVToFields(t *testing.T) {

	tests := []struct {
		name      string
		keyValues []interface{}
		want      []Field
		wantErr   bool
	}{
		{
			"incorrect pair",
			[]interface{}{"test"},
			nil,
			true,
		},
		{
			"non string key",
			[]interface{}{struct{}{}, "foo"},
			nil,
			true,
		},
		{
			"happy path",
			[]interface{}{
				"bool", true,
				"string", "string",
				"int", int(1),
				"int8", int8(2),
				"int16", int16(3),
				"int64", int64(4),
				"uint", uint(5),
				"uint64", uint64(6),
				"uint8", uint8(7),
				"uint16", uint16(8),
				"uint32", uint32(9),
				"float32", float32(10),
				"float64", float64(11),
				"int32", int32(12),
				"stringer", errors.New("err"),
				"nilInterface", nilInterface,
				"nil", nil,
			},
			[]Field{
				Bool("bool", true),
				String("string", "string"),
				Int("int", int(1)),
				Int32("int8", int32(2)),
				Int32("int16", int32(3)),
				Int64("int64", int64(4)),
				Uint64("uint", uint64(5)),
				Uint64("uint64", uint64(6)),
				Uint32("uint8", uint32(7)),
				Uint32("uint16", uint32(8)),
				Uint32("uint32", uint32(9)),
				Float32("float32", float32(10)),
				Float64("float64", float64(11)),
				Int32("int32", int32(12)),
				String("stringer", errors.New("err").Error()),
				String("nilInterface", "nil"),
				String("nil", "nil"),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InterleavedKVToFields(tt.keyValues...)
			if (err != nil) != tt.wantErr {
				t.Errorf("InterleavedKVToFields() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
