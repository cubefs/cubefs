package auth

import (
	"net/http"
)

type AuthHandler struct {
	Secret []byte
}

func NewAuthHandler(cfg *Config) *AuthHandler {
	if cfg.EnableAuth {
		if cfg.Secret == "" {
			cfg.Secret = DefaultAuthSecret
		}
		return &AuthHandler{
			Secret: []byte(cfg.Secret),
		}
	}
	return nil
}

func (self *AuthHandler) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	token := req.Header.Get(TokenHeaderKey)
	if token == "" {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	info, err := decodeAuthInfo(token)
	if err != nil {
		f(w, req)
		return
	}
	info.others = genEncodeStr(req)

	err = verify(info, self.Secret)
	if err != nil && err == errMismatchToken {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	f(w, req)
}
