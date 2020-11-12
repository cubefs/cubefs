package metanode

import (
	"encoding/base64"
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/hashfactory"
	"io/ioutil"
	"strings"
)

func checksumToStr(sum []byte) string {
	return base64.StdEncoding.EncodeToString(sum)
}

func strToChecksum(str string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(str)
}

func writeChecksumToFile(file string, sum []byte, typ string) error {
	raw := fmt.Sprintf("%s|%s", typ, checksumToStr(sum))
	return ioutil.WriteFile(file, []byte(raw), 0644)
}

func readChecksumFromFile(file string) (sum []byte, typ string, err error) {
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
	if !hashfactory.Exist(typePart) {
		err = errors.NewErrorf("unknown checksum type %s", typePart)
		return
	}
	if sum, err = strToChecksum(sumPart); err != nil {
		return
	}
	typ = typePart
	return
}
