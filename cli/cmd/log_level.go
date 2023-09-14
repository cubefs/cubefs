package cmd

import (
	"fmt"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"strings"
)

func resetDataNodeLogLevel(client *master.MasterClient) {
	c, err := client.AdminAPI().GetCluster()
	if err != nil {
		return
	}
	for _, d := range c.DataNodes {
		rangeResetLogLevel(client.DataNodeProfPort, d.Addr)
	}
}

func rangeResetLogLevel(profPort uint16, host string) {
	clientHttp := http_client.NewDataClient(fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], profPort), false)
	clientHttp.SetLoglevel("error")
}
