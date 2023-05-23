package objectnode

import (
	"bytes"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSignChunkedHeader(t *testing.T) {
	header := "400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"
	sign, size, err := parseSignChunkedHeader(header)
	require.NoError(t, err)
	require.Equal(t, "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497", sign)
	require.Equal(t, int64(1024), size)
}

func TestSignChunkedReader(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n")
	buf.WriteString(strings.Repeat("a", 65536))
	buf.WriteString("\r\n")
	buf.WriteString("400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n")
	buf.WriteString(strings.Repeat("a", 1024))
	buf.WriteString("\r\n")
	buf.WriteString("0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n")
	buf.WriteString("")
	buf.WriteString("\r\n")

	sk := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	key := buildSigningKey("AWS4", sk, "20130524", "us-east-1", "s3", "aws4_request")
	scope := "20130524/us-east-1/s3/aws4_request"
	datetime := "20130524T000000Z"
	seed := "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"

	reader := NewSignChunkedReader(buf, key, scope, datetime, seed)
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, 66560, len(b))
	require.Equal(t, strings.Repeat("a", 66560), string(b))
}
