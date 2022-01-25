package rpc

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/cubefs/blobstore/util/version"
)

// headers
const (
	HeaderContentType   = "Content-Type"
	HeaderContentLength = "Content-Length"
	HeaderContentRange  = "Content-Range"
	HeaderContentMD5    = "Content-MD5"
	HeaderUA            = "User-Agent"

	// trace
	HeaderTraceLog  = "Trace-Log"
	HeaderTraceTags = "Trace-Tags"

	// crc checker
	HeaderCrcEncoded    = "X-Crc-Encoded"
	HeaderAckCrcEncoded = "X-Ack-Crc-Encoded"
)

// mime
const (
	MIMEStream            = "application/octet-stream"
	MIMEJSON              = "application/json"
	MIMEXML               = "application/xml"
	MIMEPlain             = "text/plain"
	MIMEPOSTForm          = "application/x-www-form-urlencoded"
	MIMEMultipartPOSTForm = "multipart/form-data"
	MIMEYAML              = "application/x-yaml"
)

// encoding
const (
	GzipEncodingType = "gzip"
)

var (
	// UserAgent user agent
	UserAgent = "Golang oppo/rpc package"
)

type (
	// ValueGetter get value from url values or http params
	ValueGetter func(string) string
	// Parser is the interface implemented by types
	// that can parse themselves from url.Values.
	Parser interface {
		Parse(ValueGetter) error
	}
	// Marshaler is the interface implemented by types that
	// can marshal themselves into bytes, second parameter
	// is content type.
	Marshaler interface {
		Marshal() ([]byte, string, error)
	}
	// Unmarshaler is the interface implemented by types
	// that can unmarshal themselves from bytes.
	Unmarshaler interface {
		Unmarshal([]byte) error
	}

	// HTTPError interface of error with http status code
	HTTPError interface {
		// StatusCode http status code
		StatusCode() int
		// ErrorCode special defined code
		ErrorCode() string
		// Error detail message
		Error() string
	}
)

// ProgressHandler http progress handler
type ProgressHandler interface {
	Handler(http.ResponseWriter, *http.Request, func(http.ResponseWriter, *http.Request))
}

func marshalObj(obj interface{}) ([]byte, string, error) {
	var (
		body []byte
		ct   string = MIMEJSON
		err  error
	)
	if obj == nil {
		body = jsonNull[:]
	} else if o, ok := obj.(Marshaler); ok {
		body, ct, err = o.Marshal()
	} else {
		body, err = json.Marshal(obj)
	}
	return body, ct, err
}

func programVersion() string {
	sp := strings.Fields(strings.TrimSpace(version.Version()))
	if len(sp) == 0 || sp[0] == "develop" {
		data, err := ioutil.ReadFile(os.Args[0])
		if err != nil {
			return "_"
		}
		return fmt.Sprintf("%x", md5.Sum(data))[:10]
	}
	if len(sp) > 10 {
		return sp[0][:10]
	}
	return sp[0]
}

func init() {
	hostname, _ := os.Hostname()
	ua := fmt.Sprintf("%s/%s (%s/%s; %s) %s/%s",
		path.Base(os.Args[0]),
		programVersion(),
		runtime.GOOS,
		runtime.GOARCH,
		runtime.Version(),
		hostname,
		fmt.Sprint(os.Getpid()),
	)
	if UserAgent != ua {
		UserAgent = ua
	}
}
