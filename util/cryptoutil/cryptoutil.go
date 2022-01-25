// Copyright 2018 The Cubefs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cryptoutil

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	rand2 "math/rand"
	"net/http"
	"strconv"
	"time"
	"unsafe"
)

func pad(src []byte) []byte {
	padding := aes.BlockSize - len(src)%aes.BlockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(src, padtext...)
}

func unpad(src []byte) []byte {
	length := len(src)
	unpadding := int(src[length-1])
	return src[:(length - unpadding)]
}

// AesEncryptCBC defines aes encryption with CBC
func AesEncryptCBC(key, plaintext []byte) (ciphertext []byte, err error) {
	var (
		block cipher.Block
	)

	if plaintext == nil || len(plaintext) == 0 {
		err = fmt.Errorf("input for encryption is invalid")
		return
	}

	paddedText := pad(plaintext)

	if len(paddedText)%aes.BlockSize != 0 {
		err = fmt.Errorf("paddedText [len=%d] is not a multiple of the block size", len(paddedText))
		return
	}

	block, err = aes.NewCipher(key)
	if err != nil {
		return
	}

	ciphertext = make([]byte, aes.BlockSize+len(paddedText))
	iv := ciphertext[:aes.BlockSize]
	if _, err = io.ReadFull(rand.Reader, iv); err != nil {
		return
	}

	cbc := cipher.NewCBCEncrypter(block, iv)
	cbc.CryptBlocks(ciphertext[aes.BlockSize:], paddedText)

	return
}

// AesDecryptCBC defines aes decryption with CBC
func AesDecryptCBC(key, ciphertext []byte) (plaintext []byte, err error) {
	var block cipher.Block

	if block, err = aes.NewCipher(key); err != nil {
		return
	}

	if len(ciphertext) < aes.BlockSize {
		err = fmt.Errorf("ciphertext [len=%d] too short; should greater than blocksize", len(ciphertext))
		return
	}

	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	cbc := cipher.NewCBCDecrypter(block, iv)
	cbc.CryptBlocks(ciphertext, ciphertext)

	plaintext = unpad(ciphertext)

	return
}

// GenSecretKey generate a secret key according to pair {ts, id}
func GenSecretKey(key []byte, ts int64, id string) (secretKey []byte) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(ts))
	data := append(b, []byte(id)...)
	secretKey = genKey(key, data)
	return
}

func genKey(key []byte, data []byte) (sessionKey []byte) {
	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte(data))
	sessionKey = h.Sum(nil)
	return
}

// AuthGenSessionKeyTS authnode generates a session key according to its master key and current timestamp
func AuthGenSessionKeyTS(key []byte) (sessionKey []byte) {
	data := []byte(strconv.FormatInt(int64(time.Now().Unix()), 10))
	sessionKey = genKey(key, data)
	return
}

// Base64Encode encoding using base64
func Base64Encode(text []byte) (encodedText string) {
	encodedText = base64.StdEncoding.EncodeToString(text)
	return
}

// Base64Decode Decoding using base64
func Base64Decode(encodedText string) (text []byte, err error) {
	text, err = base64.StdEncoding.DecodeString(encodedText)
	return
}

// EncodeMessage encode a message with aes encrption, md5 signature
func EncodeMessage(plaintext []byte, key []byte) (message string, err error) {
	var cipher []byte

	// 8 for random number; 16 for md5 hash
	buffer := make([]byte, RandomNumberSize+CheckSumSize+len(plaintext))

	// add random
	random := rand2.Uint64()
	binary.LittleEndian.PutUint64(buffer[RandomNumberOffset:], random)

	// add request body
	copy(buffer[MessageOffset:], plaintext)

	// calculate and add checksum
	checksum := md5.Sum(buffer)
	copy(buffer[CheckSumOffset:], checksum[:])

	// encryption with aes CBC with keysize of 256-bit
	if cipher, err = AesEncryptCBC(key, buffer); err != nil {
		return
	}
	// base64 encoding
	message = base64.StdEncoding.EncodeToString(cipher)

	return

}

// DecodeMessage decode a message and verify its validity
func DecodeMessage(message string, key []byte) (plaintext []byte, err error) {
	var (
		cipher      []byte
		decodedText []byte
	)

	if cipher, err = base64.StdEncoding.DecodeString(message); err != nil {
		return
	}

	if decodedText, err = AesDecryptCBC(key, cipher); err != nil {
		return
	}

	if len(decodedText) <= MessageMetaDataSize {
		err = fmt.Errorf("invalid json format with size [%d] less than message meta data size", len(decodedText))
		return
	}

	msgChecksum := make([]byte, CheckSumSize)
	copy(msgChecksum, decodedText[CheckSumOffset:CheckSumOffset+CheckSumSize])

	// calculate checksum
	filltext := bytes.Repeat([]byte{byte(0)}, CheckSumSize)
	copy(decodedText[CheckSumOffset:], filltext[:])
	newChecksum := md5.Sum(decodedText)

	// verify checksum
	if bytes.Compare(msgChecksum, newChecksum[:]) != 0 {
		err = fmt.Errorf("checksum not match")
	}

	plaintext = decodedText[MessageOffset:]

	//fmt.Printf("DecodeMessage CBC: %s\n", plaintext)
	return
}

// GenVerifier generate a verifier for replay mitigation in http
func GenVerifier(key []byte) (v string, ts int64, err error) {
	ts = time.Now().Unix()
	tsbuf := make([]byte, unsafe.Sizeof(ts))
	binary.LittleEndian.PutUint64(tsbuf, uint64(ts))
	if v, err = EncodeMessage(tsbuf, key); err != nil {
		panic(err)
	}
	return
}

// CreateClientX creates a https client
func CreateClientX(cert *[]byte) (client *http.Client, err error) {
	caCertPool := x509.NewCertPool()
	ok := caCertPool.AppendCertsFromPEM(*cert)

	if !ok {
		err = fmt.Errorf("CreateClientX AppendCertsFromPEM fails")
		return
	}

	// We don't use PKI to verify client since we have secret key for authentication
	client = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:            caCertPool,
				InsecureSkipVerify: true,
			},
		},
	}
	return
}
