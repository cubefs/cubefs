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
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/blobstore/cli/common"
)

// Config config in file
type Config struct {
	Verbose  bool `json:"verbose" cache:"Flag-Verbose" help:"enable verbose mode"`
	Vverbose bool `json:"vverbose" cache:"Flag-Vverbose" help:"enable verbose verbose mode"`

	RedisAddrs []string `json:"redis_addrs" cache:"Key-RedisAddrs" help:"redis addrs"`
	RedisUser  string   `json:"redis_user" cache:"Key-RedisUser" help:"redis username"`
	RedisPass  string   `json:"redis_pass" cache:"Key-RedisPass" help:"redis password"`

	ClusterMgrAddrs  []string `json:"cm_addrs" cache:"Key-ClusterMgrAddrs" help:"cluster manager addrs"`
	ClusterMgrSecret string   `json:"cm_secret" cache:"Key-ClusterMgrSecret" help:"cluster manager secret"`

	Access struct { // see more in api/access/client.go
		ConnMode          uint8    `json:"conn_mode" cache:"Key-Access-ConnMode" help:"connection mode, 4 means no timeout"`
		ConsulAddr        string   `json:"consul_addr" cache:"Key-Access-ConsulAddr" help:"consul address"`
		ServiceIntervalMs int64    `json:"service_interval_ms" cache:"Key-Access-ServiceIntervalMs" help:"service interval ms"`
		PriorityAddrs     []string `json:"priority_addrs" cache:"Key-Access-PriorityAddrs" help:"priority addresses to try"`
		MaxSizePutOnce    int64    `json:"max_size_put_once" cache:"Key-Access-MaxSizePutOnce" help:"max size put once"`
		MaxPartRetry      int      `json:"max_part_retry" cache:"Key-Access-MaxPartRetry" help:"max times to retry part"`
		MaxHostRetry      int      `json:"max_host_retry" cache:"Key-Access-MaxHostRetry" help:"max times to retry host"`
	} `json:"access"`
}

func load(conf *Config) {
	cacheSetter := func(elemT reflect.Type, elemV reflect.Value) {
		for idx := 0; idx < elemT.NumField(); idx++ {
			field := elemT.Field(idx)
			value := elemV.Field(idx)
			if key := field.Tag.Get("cache"); key != "" {
				keyValuer[key] = typeValuer{Type: field.Type.Kind().String(), Valuer: valuer(field.Type.Kind())}
				Set(key, value.Interface())
			}
		}
	}

	cacheSetter(reflect.TypeOf(conf).Elem(), reflect.ValueOf(conf).Elem())

	confAccess := &conf.Access
	cacheSetter(reflect.TypeOf(confAccess).Elem(), reflect.ValueOf(confAccess).Elem())
}

// LoadConfig load config from path
func LoadConfig(path string) {
	var conf *Config
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	if err := common.NewDecoder(f).Decode(&conf); err != nil {
		panic(err)
	}
	load(conf)
}

type typeValuer struct {
	Type   string
	Valuer func(string) interface{}
}

var (
	keyValuer = make(map[string]typeValuer)

	valuer = func(t reflect.Kind) func(string) interface{} {
		switch t {
		case reflect.Bool:
			return func(val string) interface{} { v, _ := strconv.ParseBool(val); return v }

		case reflect.Int:
			return func(val string) interface{} { v, _ := strconv.Atoi(val); return int(v) }
		case reflect.Int8:
			return func(val string) interface{} { v, _ := strconv.Atoi(val); return int8(v) }
		case reflect.Int16:
			return func(val string) interface{} { v, _ := strconv.Atoi(val); return int16(v) }
		case reflect.Int32:
			return func(val string) interface{} { v, _ := strconv.Atoi(val); return int32(v) }
		case reflect.Int64:
			return func(val string) interface{} { v, _ := strconv.Atoi(val); return int64(v) }

		case reflect.Uint:
			return func(val string) interface{} { v, _ := strconv.Atoi(val); return uint(v) }
		case reflect.Uint8:
			return func(val string) interface{} { v, _ := strconv.Atoi(val); return uint8(v) }
		case reflect.Uint16:
			return func(val string) interface{} { v, _ := strconv.Atoi(val); return uint16(v) }
		case reflect.Uint32:
			return func(val string) interface{} { v, _ := strconv.Atoi(val); return uint32(v) }
		case reflect.Uint64:
			return func(val string) interface{} { v, _ := strconv.Atoi(val); return uint64(v) }

		case reflect.Float32:
			return func(val string) interface{} { v, _ := strconv.ParseFloat(val, 32); return float32(v) }
		case reflect.Float64:
			return func(val string) interface{} { v, _ := strconv.ParseFloat(val, 64); return float64(v) }

		case reflect.String:
			return func(val string) interface{} { return val }
		case reflect.Slice: // []string
			return func(val string) interface{} {
				if val == "" {
					return []string{}
				}
				return strings.Split(val, ",")
			}

		case reflect.Map: // map[string]string
			return func(val string) interface{} {
				m := make(map[string]string)
				if val == "" {
					return m
				}
				kvs := strings.Split(val, ",")
				for _, kvStr := range kvs {
					kv := strings.SplitN(kvStr, ":", 2)
					k, v := strings.TrimSpace(kv[0]), ""
					if len(kv) > 1 {
						v = strings.TrimSpace(kv[1])
					}
					m[k] = v
				}
				return m
			}
		default:
			panic(fmt.Sprintf("unknown type %v", t))
		}
	}
)

// PrintType print type of keyValuer
func PrintType() {
	keys := make([]string, 0, len(keyValuer))
	for key := range keyValuer {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fmt.Println("Init Cache Keys:")
	for _, key := range keys {
		fmt.Printf("\t| %-30s | %-10s |\n", key, keyValuer[key].Type)
	}
}

func init() {
	conf := &Config{}
	load(conf)
}
