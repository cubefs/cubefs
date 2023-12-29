package proto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPReplyRaw(t *testing.T) {
	reply := HTTPReply{}
	genBody := func() []byte {
		b, _ := json.Marshal(reply)
		return b
	}

	body := []byte(`[]`)
	require.Error(t, UnmarshalHTTPReply(body, nil))

	reply.Code = ErrCodeParamError
	require.Error(t, UnmarshalHTTPReply(genBody(), nil))

	reply.Code = ErrCodeSuccess
	require.NoError(t, UnmarshalHTTPReply(genBody(), nil))

	var aInt64 int64
	var aUint64 uint64
	var aString string

	reply.Data = "not-a-number"
	require.Error(t, UnmarshalHTTPReply(genBody(), &aInt64))
	require.Error(t, UnmarshalHTTPReply(genBody(), &aUint64))
	require.NoError(t, UnmarshalHTTPReply(genBody(), &aString))

	reply.Data = 177
	require.NoError(t, UnmarshalHTTPReply(genBody(), &aInt64))
	require.Equal(t, int64(177), aInt64)
	require.NoError(t, UnmarshalHTTPReply(genBody(), &aUint64))
	require.Equal(t, uint64(177), aUint64)

	var aStruct struct {
		Outter int
		inner  string
	}
	reply.Data = struct {
		Outter int
		inner  string
		Other  uint
	}{Outter: 177, inner: "177", Other: 11}
	require.NoError(t, UnmarshalHTTPReply(genBody(), &aStruct))
	require.Equal(t, int(177), aStruct.Outter)
	require.Equal(t, "", aStruct.inner)
}
