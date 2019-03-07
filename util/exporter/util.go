// Copyright 2018 The Chubao Authors.
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
