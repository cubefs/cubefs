package proto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

type AuthUser struct {
	UserID    string `json:"userID"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}

type AuthSignature struct {
	UserID    string
	Signature string
	isRoot    bool
}

func (sign *AuthSignature) SetRoot() {
	sign.isRoot = true
}

func (sign *AuthSignature) IsRoot() bool {
	return sign.isRoot
}

func (verifySign *AuthSignature) Compare(sign *AuthSignature) (err error) {
	if verifySign.UserID != sign.UserID {
		return ErrInvalidUserID
	}
	if verifySign.Signature != sign.Signature {
		return ErrInvalidSignature
	}
	return
}

func (user *AuthUser) GenerateSignature(path string) (*AuthSignature, error) {
	var (
		sign *AuthSignature
		err  error
	)

	sign = &AuthSignature{UserID: user.UserID}

	dataHMAC := hmac.New(sha256.New, []byte(user.AccessKey))
	if _, err = dataHMAC.Write([]byte(path)); err != nil {
		return nil, err
	}

	keyHMAC := hmac.New(sha256.New, []byte(user.SecretKey))
	if _, err = keyHMAC.Write(dataHMAC.Sum(nil)); err != nil {
		return nil, err
	}

	sign.Signature = hex.EncodeToString(keyHMAC.Sum(nil))

	return sign, nil
}
