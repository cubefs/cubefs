// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package exporter

import (
	"fmt"

	"github.com/cubefs/cubefs/util/exporter/backend/ump"
	"github.com/cubefs/cubefs/util/log"
)

var (
	warningKey string
)

func Warning(detail string) {
	if warningKey == "" {
		warningKey = fmt.Sprintf("%v_%v_warning", clusterName, moduleName)
	}
	ump.Alarm(warningKey, detail)
	log.LogCritical(warningKey, detail)
	return
}

func WarningBySpecialUMPKey(key, detail string) {
	if key == "" {
		key = warningKey
	}
	ump.Alarm(key, detail)
	log.LogCritical(key, detail)
	return
}

func WarningCritical(detail string) {
	if warningKey == "" {
		warningKey = fmt.Sprintf("%v_%v_critical", clusterName, moduleName)
	}
	ump.Alarm(warningKey, detail)
	log.LogCritical(warningKey, detail)
	return
}

func WarningPanic(detail string) {
	if warningKey == "" {
		warningKey = fmt.Sprintf("%v_%v_panic", clusterName, moduleName)
	}
	ump.Alarm(warningKey, detail)
	log.LogCritical(warningKey, detail)
	return
}

func WarningRocksdbError(detail string) {
	key := fmt.Sprintf("%v_metanode_rocksdb_error_warning", clusterName)
	ump.Alarm(key, detail)
	log.LogCritical(key, detail)
	return
}

func FlushWarning() {
	ump.FlushAlarm()
}
