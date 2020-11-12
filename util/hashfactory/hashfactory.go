package hashfactory

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"strings"
)

type Hash = hash.Hash

type FactoryFunc func() Hash

var factoryMap = make(map[string]FactoryFunc)

func AddFactoryFunc(name string, f FactoryFunc) {
	factoryMap[name] = f
}

func GetFactoryFunc(name string) FactoryFunc {
	name = strings.ToLower(name)
	return factoryMap[name]
}

func Exist(name string) bool {
	return GetFactoryFunc(name) != nil
}

func New(name string) Hash {
	f := GetFactoryFunc(name)
	if f != nil {
		return f()
	} else {
		return nil
	}
}

func init() {
	AddFactoryFunc("crc32", func() Hash {
		return crc32.NewIEEE()
	})
	AddFactoryFunc("crc64", func() Hash {
		return crc64.New(crc64.MakeTable(crc64.ISO))
	})
	AddFactoryFunc("md5", md5.New)
	AddFactoryFunc("sha1", sha1.New)
	AddFactoryFunc("sha256", sha256.New)
	AddFactoryFunc("sha512", sha512.New)
}
