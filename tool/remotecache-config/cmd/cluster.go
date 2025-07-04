package cmd

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdClusterUse   = "cluster [COMMAND]"
	cmdClusterShort = "Manage cluster components"

	cmdClusterInfoShort = "Show cluster summary information"
)

func newClusterCmd(client *master.MasterClient) *cobra.Command {
	clusterCmd := &cobra.Command{
		Use:   cmdClusterUse,
		Short: cmdClusterShort,
	}
	clusterCmd.AddCommand(
		newClusterInfoCmd(client),
		//TODO
		//newClusterStatCmd(client),
		//newClusterFreezeCmd(client),
		//newClusterSetThresholdCmd(client),
		//newClusterSetParasCmd(client),
	)
	return clusterCmd
}

func newClusterInfoCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdClusterInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var cv *proto.ClusterView
			if cv, err = client.AdminAPI().GetCluster(false); err != nil {
				errout(err)
			}
			stdout("[Cluster]\n")
			stdout("%v", formatClusterView(cv))
			stdout("\n")
		},
	}
	return cmd
}
