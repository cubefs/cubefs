package crypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

func Md5Encryption(s string) string {
	data := []byte(s)
	sum := md5.Sum(data)
	md5String := fmt.Sprintf("%x", sum)
	return md5String
}

func PKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func Encrypt(origData []byte) (string, error) {
	block, err := aes.NewCipher(GetDefalutKey())
	if err != nil {
		return "", err
	}
	blockSize := block.BlockSize()
	origData = PKCS7Padding(origData, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, GetDefalutKey()[:blockSize])
	crypted := make([]byte, len(origData))
	blockMode.CryptBlocks(crypted, origData)

	result := base64.StdEncoding.EncodeToString(crypted)
	return result, nil
}

func Decrypt(result string) (string, error) {
	if len(result) == 0 {
		return "", nil
	}

	crypted, err := base64.StdEncoding.DecodeString(result)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(GetDefalutKey())
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, GetDefalutKey()[:blockSize])
	origData := make([]byte, len(crypted))
	blockMode.CryptBlocks(origData, crypted)
	origData = PKCS7UnPadding(origData)
	return string(origData), nil
}

func EncryptByKey(origData []byte, key []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	blockSize := block.BlockSize()
	origData = PKCS7Padding(origData, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, key[:blockSize])
	crypted := make([]byte, len(origData))
	blockMode.CryptBlocks(crypted, origData)

	result := base64.StdEncoding.EncodeToString(crypted)
	return result, nil
}

func DecryptByKey(result string, key []byte) (string, error) {
	if len(result) == 0 {
		return "", nil
	}

	crypted, err := base64.StdEncoding.DecodeString(result)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	origData := make([]byte, len(crypted))
	blockMode.CryptBlocks(origData, crypted)
	origData = PKCS7UnPadding(origData)
	return string(origData), nil
}

func GetDefalutKey() []byte {
	return []byte{
		54, 38, 98, 35, 89, 102, 99, 38, 57, 52, 106, 71, 76, 112, 99, 52, 40, 66, 68,
		113, 76, 120, 50, 93, 56, 46, 79, 52, 80, 76, 101, 46,
	}
}

func GetKey(key string) []byte {
	return []byte(key)
}

func GenKey(n int) []byte {
	if n < 1 {
		return nil
	}
	b := make([]byte, n)
	io.ReadFull(rand.Reader, b)
	return b
}
