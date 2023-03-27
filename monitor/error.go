package monitor

import "github.com/cubefs/cubefs/util/errors"

var (
	TableExistError    = errors.New("TableExistsException")
	TableNotFoundError = errors.New("TableNotFoundException")
)
