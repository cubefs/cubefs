// Package s3 provides ...
package objectnode

import "net/http"

//https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheAuthenticationHeader

type AuthType string

const (
	SignatrueV2 AuthType = "signature_v2"
	SignatrueV4          = "signature_v4"
	PresignedV2          = "presigned_v2"
	PresignedV4          = "presigned_v4"
)

type RequestAuthInfo struct {
	authType  AuthType
	accessKey string
}

func parseRequestAuthInfo(r *http.Request) *RequestAuthInfo {
	auth := new(RequestAuthInfo)
	if isHeaderUsingSignatureAlgorithmV2(r) {
		auth.authType = SignatrueV2
		ai, _ := parseRequestAuthInfoV2(r)
		if ai != nil {
			auth.accessKey = ai.accessKeyId
		}
	} else if isHeaderUsingSignatureAlgorithmV4(r) {
		auth.authType = SignatrueV4
		ai, _ := parseRequestV4(r)
		if ai != nil {
			auth.accessKey = ai.Credential.AccessKey
		}
	} else if isUrlUsingSignatureAlgorithmV2(r) {
		auth.authType = PresignedV2
		ai, _ := parsePresignedV2AuthInfo(r)
		if ai != nil {
			auth.accessKey = ai.accessKeyId
		}
	} else if isUrlUsingSignatureAlgorithmV4(r) {
		auth.authType = PresignedV4
		ai, _ := parseRequestV4(r)
		if ai != nil {
			auth.accessKey = ai.Credential.AccessKey
		}
	}

	return auth
}
