package convertnode

import "github.com/chubaofs/chubaofs/util/errors"

var (
	TableExistError    = errors.New("TableExistsException")
	TableNotFoundError = errors.New("TableNotFoundException")
)
