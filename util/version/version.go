package version

import (
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/util/unit"
)

type VersionID string

// LessThan 只按照最短的版本位数比较，比如 'xx.xx.xx' 与 'yy.yy' 比较，只比较前 'xx.xx' 位
func (v1 VersionID) LessThan(v2 VersionID) (less bool, err error) {
	v1NumArr := strings.Split(string(v1), ".")
	v2NumArr := strings.Split(string(v2), ".")
	numLen := unit.Min(len(v1NumArr), len(v2NumArr))
	for i := 0; i < numLen; i++ {
		var (
			v1Num, v2Num 	int64
			err				error
		)
		if v1Num, err = strconv.ParseInt(v1NumArr[i], 10, 64); err != nil {
			return false, err
		}
		if v2Num, err = strconv.ParseInt(v2NumArr[i], 10, 64); err != nil {
			return false, err
		}
		if v1Num < v2Num {
			return true, nil
		}
		if v1Num > v2Num {
			return false, nil
		}
	}
	return false, nil
}