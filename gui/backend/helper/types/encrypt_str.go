package types

import (
	"errors"
	"fmt"

	"database/sql/driver"

	"github.com/cubefs/cubefs/console/backend/helper/crypt"
)

type EncryptStr string

func (e EncryptStr) Value() (driver.Value, error)  {
	if e == "" {
		return nil, nil
	}
	return crypt.Encrypt([]byte(e))
}

func (e *EncryptStr) Scan(v interface{}) error {
	b, ok := v.([]byte)
	if !ok {
		return errors.New(fmt.Sprintf("invalid type:%T", v))
	}
	s, err := crypt.Decrypt(string(b))
	if err != nil {
		return err
	}
	*e = EncryptStr(s)
	return nil
}
