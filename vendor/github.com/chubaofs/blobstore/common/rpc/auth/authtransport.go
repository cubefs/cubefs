package auth

import (
	"net/http"
	"time"
)

type AuthTransport struct {
	Secret []byte
	Tr     http.RoundTripper
}

func NewAuthTransport(tr http.RoundTripper, cfg *Config) http.RoundTripper {
	if cfg.EnableAuth {
		if cfg.Secret == "" {
			cfg.Secret = DefaultAuthSecret
		}
		return &AuthTransport{
			Secret: []byte(cfg.Secret),
			Tr:     tr,
		}
	}
	return nil
}

// a simple auth token
func (self *AuthTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	now := time.Now().Unix()
	if err != nil {
		return self.Tr.RoundTrip(req)
	}

	info := &authInfo{timestamp: now, others: genEncodeStr(req)}

	err = calculate(info, self.Secret)
	if err != nil {
		return self.Tr.RoundTrip(req)
	}

	token, err := encodeAuthInfo(info)
	if err != nil {
		return self.Tr.RoundTrip(req)
	}

	req.Header.Set(TokenHeaderKey, token)
	return self.Tr.RoundTrip(req)
}
