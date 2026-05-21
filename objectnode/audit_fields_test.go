package objectnode

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

type captureAuditLogger struct {
	ch chan []byte
}

func (l *captureAuditLogger) Name() string {
	return "capture"
}

func (l *captureAuditLogger) Send(data []byte) error {
	buf := make([]byte, len(data))
	copy(buf, data)
	l.ch <- buf
	return nil
}

func (l *captureAuditLogger) Close() error {
	return nil
}

func TestAuditFieldsRoundTrip(t *testing.T) {
	req := mux.SetURLVars(httptest.NewRequest(http.MethodPut, "/bucket/object", nil), map[string]string{})
	want := AuditFields{
		Size:    123,
		ETag:    "etag-value",
		Objects: []string{"a.txt", "b.txt"},
	}

	SetAuditFields(req, want)

	require.Equal(t, want, GetAuditFields(req))
}

func TestExternalAuditLoggerIncludesAuditFields(t *testing.T) {
	logger := &captureAuditLogger{ch: make(chan []byte, 1)}
	audit := NewExternalAudit()
	audit.AddLoggers(logger)

	req := httptest.NewRequest(http.MethodPut, "http://example.com/test-bucket/test-object?uploadId=1", strings.NewReader("payload"))
	req.RemoteAddr = "127.0.0.1:12345"
	req = mux.SetURLVars(req, map[string]string{
		ContextKeyRequestID: "req-123",
		ContextKeyRequester: "requester-1",
		ContextKeyAccessKey: "ak-test",
		ContextKeyOwner:     "owner-1",
		ContextKeyBucket:    "test-bucket",
		ContextKeyObject:    "test-object",
	})
	SetRequestAction(req, proto.OSSPutObjectAction)
	SetAuditFields(req, AuditFields{
		Size:    456,
		ETag:    "etag-456",
		Objects: []string{"deleted-a", "deleted-b"},
	})

	recorder := NewResponseStater(httptest.NewRecorder())
	recorder.StartTime = time.Now().UTC().Add(-2 * time.Millisecond)
	recorder.Header().Set(ETag, wrapUnescapedQuot("etag-456"))
	recorder.WriteHeader(http.StatusCreated)
	_, err := recorder.Write([]byte("ok"))
	require.NoError(t, err)

	audit.Logger(recorder, req)

	select {
	case data := <-logger.ch:
		var entry AuditEntry
		require.NoError(t, json.Unmarshal(data, &entry))
		require.Equal(t, "req-123", entry.RequestID)
		require.Equal(t, "PutObject", entry.Request.API)
		require.Equal(t, "test-bucket", entry.Request.Bucket)
		require.Equal(t, "test-object", entry.Request.Object)
		require.Equal(t, http.StatusCreated, entry.Response.StatusCode)
		require.Equal(t, int64(456), entry.Size)
		require.Equal(t, "etag-456", entry.ETag)
		require.Equal(t, []string{"deleted-a", "deleted-b"}, entry.Objects)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for audit payload")
	}
}
