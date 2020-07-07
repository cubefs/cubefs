package internal

import "encoding/json"

func MarshalJSON(v interface{}) string {
	bytes, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func ParseJSON(s string) interface{} {
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		panic(err)
	}
	return v
}

func AsJSON(v interface{}) interface{} {
	return ParseJSON(MarshalJSON(v))
}
