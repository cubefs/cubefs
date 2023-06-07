package objectnode

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRequestAuthInfo(t *testing.T) {
	// Create a mock HTTP request for each signature version
	v2HeaderReq := httptest.NewRequest("GET", "http://example.com/bucket", nil)
	v2HeaderReq.Header.Set(HeaderNameAuthorization, RequestHeaderV2AuthorizationScheme+" AWSAccessKeyId:Signature")

	// v4HeaderReq := httptest.NewRequest("GET", "http://example.com/bucket", nil)
	// v4HeaderReq.Header.Set(HeaderNameAuthorization, SignatureV4Algorithm+" Credential=AWSAccessKeyId/20230721/us-east-1/s3/aws4_request")

	v2QueryReq := httptest.NewRequest("GET", "http://example.com/bucket?AWSAccessKeyId=AccessKey&Expires=1573369185&Signature=Signature", nil)

	// v4QueryReq := httptest.NewRequest("GET", "http://example.com/bucket?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AWSAccessKeyId/20230721/us-east-1/s3/aws4_request", nil)

	tests := []struct {
		name     string
		request  *http.Request
		expected *RequestAuthInfo
	}{
		{
			name:    "V2 header",
			request: v2HeaderReq,
			expected: &RequestAuthInfo{
				authType:  SignatrueV2,
				accessKey: "AWSAccessKeyId",
			},
		},
		{
			name:    "V2 query",
			request: v2QueryReq,
			expected: &RequestAuthInfo{
				authType:  PresignedV2,
				accessKey: "AccessKey",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := parseRequestAuthInfo(tc.request)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
