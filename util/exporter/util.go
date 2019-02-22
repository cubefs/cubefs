package exporter

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"strconv"
)

func stringMD5(str string) string {
	h := md5.New()
	_, err := io.WriteString(h, str)
	if err != nil {
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func stringMapToString(m map[string]string) string {
	b := bytes.NewBuffer(make([]byte, 0, 4096))
	_, err := fmt.Fprintf(b, "{")
	if err != nil {
	}
	firstValue := true
	for k, v := range m {
		if firstValue {
			firstValue = false
		} else {
			_, err = fmt.Fprintf(b, ", ")
			if err != nil {
			}
		}
		_, err = fmt.Fprintf(b, "\"%v\": %v", k, strconv.Quote(v))
		if err != nil {
		}
	}
	_, err = fmt.Fprintf(b, "}")
	return b.String()
}
