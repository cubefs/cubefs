package config

import (
	"reflect"
	"testing"
)

const configTxt = `
{
  "GetInt": 1234, # English Comment test
  "GetInt64": 5678, # 中文注释测试
  "GetFloat": 1234.5678, # Prueba de comentarios en español
  "GetString": "test string", # Deutscher Kommentartest
  "GetBool": true, 
  "GetBoolStr": "true",
  "GetStringSlice": [
    "test string in slice 1",
    "test string in slice 2" # in [ comment test
  ],
  "Quote": "in quote # test"
}
`

func TestLoadConfigString(t *testing.T) {
	conf := LoadConfigString(configTxt)
	if conf.GetInt("GetInt") != 1234 {
		t.Fatal()
	}
	if conf.GetInt64("GetInt64") != 5678 {
		t.Fatal()
	}
	if conf.GetFloat("GetFloat") != 1234.5678 {
		t.Fatal()
	}
	if conf.GetString("GetString") != "test string" {
		t.Fatal()
	}
	if conf.GetBool("GetBool") != true {
		t.Fatal()
	}
	if conf.GetBool("GetBoolStr") != true {
		t.Fatal()
	}
	if !reflect.DeepEqual(conf.GetStringSlice("GetStringSlice"),
		[]string{
			"test string in slice 1",
			"test string in slice 2"}) {
		t.Fatal()
	}
	if conf.GetString("Quote") != "in quote # test" {
		t.Fatal()
	}
	t.Log("conf check success")
}
