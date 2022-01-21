package metanode

import (
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/chubaofs/chubaofs/util/errors"
)

func checksumToStr(sum []byte) string {
	return base64.StdEncoding.EncodeToString(sum)
}

func strToChecksum(str string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(str)
}

func writeChecksumToFile(filename string, sum []byte) error {
	raw := []byte(fmt.Sprintf("crc32|%s", checksumToStr(sum)))
	fp, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	n, err := fp.Write(raw)
	if err == nil && n < len(raw) {
		err = io.ErrShortWrite
	}
	if err1 := fp.Sync(); err == nil {
		err = err1
	}
	if err1 := fp.Close(); err == nil {
		err = err1
	}
	return err
}

func readChecksumFromFile(file string) (sum []byte, err error) {
	var raw []byte
	raw, err = ioutil.ReadFile(file)
	if err != nil {
		return
	}
	str := string(raw)
	parts := strings.Split(str, "|")
	if len(parts) != 2 {
		err = errors.NewErrorf("invalid checksum format, should be [type]|[checksum], but got %s", str)
		return
	}
	typePart, sumPart := parts[0], parts[1]
	if strings.ToLower(typePart) != "crc32" {
		err = errors.NewErrorf("unknown checksum type %s", typePart)
		return
	}
	sum, err = strToChecksum(sumPart)
	return
}
