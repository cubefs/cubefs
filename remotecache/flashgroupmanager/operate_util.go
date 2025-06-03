package flashgroupmanager

import (
	"fmt"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

func Warn(clusterID, msg string) {
	key := fmt.Sprintf("%s_%s", clusterID, ModuleName)
	WarnBySpecialKey(key, msg)
}

// WarnBySpecialKey provides warnings when exits
func WarnBySpecialKey(key, msg string) {
	log.LogWarn(msg)
	exporter.Warning(msg)
}

func notFoundMsg(name string) (err error) {
	return errors.NewErrorf("%v not found", name)
}

func unmatchedKey(name string) (err error) {
	return errors.NewErrorf("parameter %v not match", name)
}
