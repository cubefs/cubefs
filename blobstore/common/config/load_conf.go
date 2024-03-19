// Copyright 2022 The CubeFS Authors.
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

package config

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"gopkg.in/go-playground/validator.v9"
)

var confName *string

const (
	_ESCAPE   = 92 /* \ */
	_QUOTE    = 34 /* " */
	_HASH     = 35 /* # */
	_SPACE    = 32 /* space  */
	_TAB      = 9  /* tab */
	_LF       = 10 /* \n */
	_ASTERISK = 42 /* * */
	_SLASH    = 47 /* / */
)

func Init(cflag, app, default_conf string) {
	confName = flag.String(cflag, default_conf, "the config file")
}

func Load(conf interface{}) (err error) {
	if !flag.Parsed() {
		flag.Parse()
	}

	log.Info("Use the config file of ", *confName)
	return LoadFile(conf, *confName)
}

func LoadFile(conf interface{}, confName string) (err error) {
	data, err := os.ReadFile(confName)
	if err != nil {
		log.Error("LoadFile conf failed:", err)
		return
	}

	log.Infof("LoadFile config file %s:\n%s", confName, data)
	return LoadData(conf, data)
}

func LoadData(conf interface{}, data []byte) (err error) {
	data = trimComments(data)

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	err = dec.Decode(conf)
	if err != nil {
		log.Error("Parse conf failed:", err)
		return
	}

	err = validate.Struct(conf)
	if err != nil {
		if _, ok := err.(*validator.InvalidValidationError); ok {
			return nil
		}
		err = fmt.Errorf("validate failed: %v", err)
		log.Error("LoadData failed:", err)
	}
	return
}

func ConfName() string {
	if confName != nil {
		return *confName
	}
	return ""
}

func SafeLoadData(conf interface{}, data []byte) (err error) {
	data = trimComments(data)

	err = json.Unmarshal(data, conf)
	if err != nil {
		log.Error("SafeParse conf failed:", err)
	}
	return
}

// support json5 comment
func trimComments(s []byte) []byte {
	var (
		quote             bool
		escaped           bool
		commentStart      bool
		multiCommentLine  bool
		singleCommentLine bool
	)

	start := func(c byte) {
		commentStart = true
		multiCommentLine = c == _ASTERISK
		singleCommentLine = (c == _SLASH || c == _HASH)
	}
	stop := func() {
		commentStart = false
		multiCommentLine = false
		singleCommentLine = false
	}

	n := len(s)
	if n == 0 {
		return s
	}
	str := make([]byte, 0, n)

	for i := 0; i < n; i++ {
		if s[i] == _ESCAPE || escaped {
			if !commentStart {
				str = append(str, s[i])
			}
			escaped = !escaped
			continue
		}
		if s[i] == _QUOTE {
			quote = !quote
		}
		if (s[i] == _SPACE || s[i] == _TAB) && !quote {
			continue
		}
		if s[i] == _LF {
			if singleCommentLine {
				stop()
			}
			continue
		}
		if quote && !commentStart {
			str = append(str, s[i])
			continue
		}
		if commentStart {
			if multiCommentLine && s[i] == _ASTERISK && i+1 < n && s[i+1] == _SLASH {
				stop()
				i++
			}
			continue
		}

		if s[i] == _HASH {
			start(_HASH)
			continue
		}

		if s[i] == _SLASH && i+1 < n {
			if s[i+1] == _ASTERISK {
				start(_ASTERISK)
				i++
				continue
			}

			if s[i+1] == _SLASH {
				start(_SLASH)
				i++
				continue
			}
		}
		str = append(str, s[i])
	}
	return str
}
