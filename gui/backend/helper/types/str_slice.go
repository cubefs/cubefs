package types

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
)

type StrSlice []string

func (s StrSlice) Value() (driver.Value, error) {
	if s == nil {
		s = []string{}
	}
	b, err := json.Marshal(s)
	return string(b), err
}

func (s *StrSlice) Scan(v interface{}) error {
	if b, ok := v.([]byte); ok {
		return json.Unmarshal(b, s)
	}
	if str, ok := v.(string); ok {
		return json.Unmarshal([]byte(str), s)
	}
	return errors.New(fmt.Sprintf("invalid data type:%T", v))
}

func (s StrSlice) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}
