package types

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
)

type MapStr map[string]string

func (m MapStr) Value() (driver.Value, error) {
	if len(m) == 0 {
		m = map[string]string{}
	}
	b, e := json.Marshal(m)
	return string(b), e
}

func (m *MapStr) Scan(v interface{}) error {
	b, ok := v.([]byte)
	if !ok {
		return errors.New(fmt.Sprintf("invalid type:%T", v))
	}
	return json.Unmarshal(b, m)
}

type Map map[string]interface{}

func (m Map) Value() (driver.Value, error)  {
	if len(m) == 0 {
		m = map[string]interface{}{}
	}
	b, e := json.Marshal(m)
	return string(b), e
}

func (m *Map) Scan(v interface{}) error  {
	b, ok := v.([]byte)
	if !ok {
		return errors.New(fmt.Sprintf("invalid type:%T", v))
	}
	return json.Unmarshal(b, m)
}

type Values map[string][]string

func (val Values) Value() (driver.Value, error)  {
	if len(val) == 0 {
		val = map[string][]string{}
	}
	b, e := json.Marshal(val)
	return string(b), e
}

func (val *Values) Scan(v interface{}) error  {
	b, ok := v.([]byte)
	if !ok {
		return errors.New(fmt.Sprintf("invalid type:%T", v))
	}
	return json.Unmarshal(b, val)
}