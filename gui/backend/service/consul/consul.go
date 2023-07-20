package consul

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func Sort(kvs []*KV) []*KV {
	sort.SliceStable(kvs,
		func(i, j int) bool {
			keyI := kvs[i].Key
			keyJ := kvs[j].Key
			return keyI < keyJ
		})
	return kvs
}

func Decode(kvs []*KV) (destKvs []*KV, err error) {
	for _, kv := range kvs {
		val, err := base64.StdEncoding.DecodeString(kv.Value)
		if err != nil {
			fmt.Printf("decode fail, kv %+v\n", kv)
			return nil, err
		}

		kv.Value = string(val)
		destKvs = append(destKvs, kv)
	}

	return destKvs, nil
}

func Unmarshal(data []byte) ([]*KV, error) {
	if len(data) == 0 {
		fmt.Printf("there is no target value\n")
		return nil, nil
	}
	var kvs []*KV
	err := json.Unmarshal(data, &kvs)
	if err != nil {
		fmt.Printf("unmarshal data fail : %s, err : %v \n", string(data), err)
		return nil, err
	}

	kvs, err = Decode(kvs)
	if err != nil {
		fmt.Printf("decode data fail : %s, err : %v \n", string(data), err)
		return nil, err
	}

	return kvs, nil
}

func ToString(data interface{}) (string, error) {
	content, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func AddDc(key, dc string) string {
	return fmt.Sprintf("%s?dc=%s", key, dc)
}

func AddDcRecurse(key, dc string) string {
	return fmt.Sprintf("%s?recurse=true&dc=%s", key, dc)
}

func AddAlbPrefix(key string) string {
	return fmt.Sprintf("v1/kv/nginx/bucketLimit/%s", key)
}

func AddAlbQueryAll() string {
	return "v1/kv/nginx/bucketLimit/?recurse"
}

func DelAlbPrefix(key string) string {
	return fmt.Sprintf(strings.Replace(key,"nginx/bucketLimit/","",1))
}
