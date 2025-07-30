package cmd

import (
	"fmt"
	"strconv"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdClusterUse   = "cluster [COMMAND]"
	cmdClusterShort = "Manage cluster components"

	cmdClusterInfoShort           = "Show cluster summary information"
	cmdClusterSetClusterInfoShort = "Set cluster parameters"

	cmdVolMinRemoteCacheTTL = 10 * 60
)

func newClusterCmd(client *master.MasterClient) *cobra.Command {
	clusterCmd := &cobra.Command{
		Use:   cmdClusterUse,
		Short: cmdClusterShort,
	}
	clusterCmd.AddCommand(
		newClusterInfoCmd(client),
		newClusterSetParasCmd(client),
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
			if cv, err = client.AdminAPI().GetClusterView(); err != nil {
				errout(err)
			}
			stdout("[Cluster]\n")
			stdout("%v", formatClusterView(cv))
			stdout("\n")
		},
	}
	return cmd
}

func newClusterSetParasCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	var tmp int64
	handleTimeout := ""
	readDataNodeTimeout := ""
	optRcTTL := ""
	optRcReadTimeout := ""
	optRemoteCacheMultiRead := ""
	optFlashNodeTimeoutCount := ""
	optRemoteCacheSameZoneTimeout := ""
	optRemoteCacheSameRegionTimeout := ""
	optFlashHotKeyMissCount := ""
	cmd := &cobra.Command{
		Use:   CliOpSetCluster,
		Short: cmdClusterSetClusterInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			if handleTimeout != "" {
				if tmp, err = strconv.ParseInt(handleTimeout, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", handleTimeout)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("handleTimeout (%v) should grater than 0", handleTimeout)
					return
				}
			}
			if readDataNodeTimeout != "" {
				if tmp, err = strconv.ParseInt(readDataNodeTimeout, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", readDataNodeTimeout)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("readDataNodeTimeout (%v) should grater than 0", readDataNodeTimeout)
					return
				}
			}

			if optRcTTL != "" {
				if tmp, err = strconv.ParseInt(optRcTTL, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optRcTTL)
					return
				}
				if tmp <= cmdVolMinRemoteCacheTTL {
					err = fmt.Errorf("param remoteCacheTTL(%v) must greater than or equal to %v", optRcTTL, cmdVolMinRemoteCacheTTL)
					return
				}
			}
			if optRcReadTimeout != "" {
				if tmp, err = strconv.ParseInt(optRcReadTimeout, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optRcReadTimeout)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("param remoteCacheReadTimeout(%v) must greater than 0", optRcReadTimeout)
					return
				}
			}
			if optRemoteCacheMultiRead != "" {
				if _, err = strconv.ParseBool(optRemoteCacheMultiRead); err != nil {
					err = fmt.Errorf("param remoteCacheMultiRead(%v) should be true or false", optRemoteCacheMultiRead)
					return
				}
			}
			if optFlashNodeTimeoutCount != "" {
				if tmp, err = strconv.ParseInt(optFlashNodeTimeoutCount, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optFlashNodeTimeoutCount)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("param flashNodeTimeoutCount(%v) must greater than 0", optFlashNodeTimeoutCount)
					return
				}
			}
			if optRemoteCacheSameZoneTimeout != "" {
				if tmp, err = strconv.ParseInt(optRemoteCacheSameZoneTimeout, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optRemoteCacheSameZoneTimeout)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("param remoteCacheSameZoneTimeout(%v) must greater than 0", optRemoteCacheSameZoneTimeout)
					return
				}
			}
			if optRemoteCacheSameRegionTimeout != "" {
				if tmp, err = strconv.ParseInt(optRemoteCacheSameRegionTimeout, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optRemoteCacheSameRegionTimeout)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("param remoteCacheSameRegionTimeout(%v) must greater than 0", optRemoteCacheSameRegionTimeout)
					return
				}
			}

			if optFlashHotKeyMissCount != "" {
				if tmp, err = strconv.ParseInt(optFlashHotKeyMissCount, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optFlashHotKeyMissCount)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("param flashHotKeyMissCount(%v) must greater than 0", optFlashHotKeyMissCount)
					return
				}
			}

			if err = client.AdminAPI().SetClusterParas("", "", "",
				"", "", "", "", clientIDKey,
				"", "",
				"", "",
				"", "", "", "", "", "",
				"", "", handleTimeout, readDataNodeTimeout,
				optRcTTL, optRcReadTimeout, optRemoteCacheMultiRead, optFlashNodeTimeoutCount,
				optRemoteCacheSameZoneTimeout, optRemoteCacheSameRegionTimeout, optFlashHotKeyMissCount); err != nil {
				return
			}
			stdout("Cluster parameters has been set successfully. \n")
		},
	}
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	cmd.Flags().StringVar(&handleTimeout, "flashNodeHandleReadTimeout", "", "Specify flash node handle read timeout (example:1000ms)")
	cmd.Flags().StringVar(&readDataNodeTimeout, "flashNodeReadDataNodeTimeout", "", "Specify flash node read data node timeout (example:3000ms)")
	cmd.Flags().StringVar(&optRcTTL, CliFlagRemoteCacheTTL, "", "Remote cache ttl[Unit: s](must >= 10min, default 5day)")
	cmd.Flags().StringVar(&optRcReadTimeout, CliFlagRemoteCacheReadTimeout, "", "Remote cache read timeout millisecond(must > 0)")
	cmd.Flags().StringVar(&optRemoteCacheMultiRead, CliFlagRemoteCacheMultiRead, "", "Remote cache follower read(true|false)")
	cmd.Flags().StringVar(&optFlashNodeTimeoutCount, CliFlagFlashNodeTimeoutCount, "", "FlashNode timeout count, flashNode will be removed by client if it's timeout count exceeds this value")
	cmd.Flags().StringVar(&optRemoteCacheSameZoneTimeout, CliFlagRemoteCacheSameZoneTimeout, "", "Remote cache same zone timeout microsecond(must > 0)")
	cmd.Flags().StringVar(&optRemoteCacheSameRegionTimeout, CliFlagRemoteCacheSameRegionTimeout, "", "Remote cache same region timeout millisecond(must > 0)")
	cmd.Flags().StringVar(&optFlashHotKeyMissCount, CliFlagFlashHotKeyMissCount, "", "Flash hot key miss count(must > 0)")
	return cmd
}
