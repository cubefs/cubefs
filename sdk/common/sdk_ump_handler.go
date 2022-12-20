package common

import (
	"fmt"
	"github.com/cubefs/cubefs/util/exporter"
)

func HandleUmpAlarm(cluster, vol, act, msg string) {
	umpKeyCluster := fmt.Sprintf("%s_client_warning", cluster)
	umpMsgCluster := fmt.Sprintf("volume(%s) %s", vol, msg)
	exporter.WarningBySpecialUMPKey(umpKeyCluster, umpMsgCluster)

	umpKeyVol := fmt.Sprintf("%s_%s_warning", cluster, vol)
	umpMsgVol := fmt.Sprintf("act(%s) - %s", act, msg)
	exporter.WarningBySpecialUMPKey(umpKeyVol, umpMsgVol)
}
