package cmd

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	sdk "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"encoding/json"

)

const (
	cmdInodeUse   = "inode  [COMMAND]"
	cmdInodeInfoUse   = "info  volumeName inodeId"
	cmdInodeShort = "Show inode information"
)

func newInodeCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdInodeUse,
		Short:   cmdInodeShort,
		Args:    cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(
		newInodeInfoCmd(client),
	)
	return cmd
}


func newInodeInfoCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdInodeInfoUse,
		Short: cmdInodeShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var inodeNumber = args[1]
			intNum, _ := strconv.Atoi(inodeNumber)
			inodeUnint64Number := uint64(intNum)
			fmt.Printf("inode to be queried is %s, volume_name is  %s: \n", inodeNumber, volumeName)
			var svv *proto.SimpleVolView
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if svv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				err = fmt.Errorf("Get volume info failed:\n%v\n", err)
				return
			}
			// print summary info
			stdout("Summary of Volume [%s] :\n%s\n",volumeName, formatSimpleVolView(svv))
			// debug
			//fmt.Printf("svv: %+v\n", svv)
			metaPartitionCount := uint64(svv.MpCnt)
			maxMetaPartitionID := svv.MaxMetaPartitionID
			volumeOwer := svv.Owner
			for i:= (maxMetaPartitionID - metaPartitionCount +1); i <= metaPartitionCount; i++ {
				fmt.Printf("metaPartitionNumber is %d\n", i)
			}

			// print metadata detail
			var views []*proto.MetaPartitionView
			if views, err = client.ClientAPI().GetMetaPartitions(volumeName); err != nil {
				err = fmt.Errorf("Get volume metadata detail information failed:\n%v\n", err)
				return
			}
			for _, view := range views {
				stdout("%v\n", formatMetaPartitionTableRow(view))
				inodeCount := view.InodeCount
				inodeStartNumber := view.Start
				inodeEndNumber  := view.End
				metaPartitionNumber := view.PartitionID

				fmt.Printf("inodeStartNumber is %d, count is %d, end is %d \n",inodeStartNumber, inodeCount, view.End)
				//inodeEndNumber := inodeStartNumber + inodeCount
				if inodeStartNumber < inodeUnint64Number  && inodeUnint64Number <  inodeEndNumber{
					// get inode information
					fmt.Printf("inode to Requied is %d, partitionNumber is %d \n", inodeUnint64Number, metaPartitionNumber)
					//var inodeTest  = sdk.newAPIRequest("GET", "getInode?pid=1&ino=33")

					//if inodeTest == nil {
					//
					//}
					var partition *proto.MetaPartitionInfo
					if partition, err = client.ClientAPI().GetMetaPartition(metaPartitionNumber); err != nil {
						return
					}
					fmt.Println("------------")
					stdout(formatMetaPartitionInfo(partition))
					fmt.Println("------------ leader",client.Leader())
					//if partition, err = client.ClientAPI().GetInodeInfo(metaPartitionNumber, 22); err != nil {
					//	return
					//}
					for _, addr := range view.Members{
						addr := strings.Split(addr, ":")[0]
						resp, err := http.Get(fmt.Sprintf("http://%s:%d/getInode?pid=%d&ino=%d", addr, client.MetaNodeProfPort, metaPartitionNumber, 12))
						if err != nil {
							errout("get partition list failed:\n%v\n", err)
							return
						}
						all, err := ioutil.ReadAll(resp.Body)
						if err != nil {
							errout("get partition list failed:\n%v\n", err)
							return
						}

						value := make(map[string]interface{})
						err = json.Unmarshal(all, &value)
						if err != nil {
							errout("get partition info failed:\n%v\n", err)
							return
						}
						fmt.Printf("valule: %+v\n", value)

					}
					//volume, err := client.ClientAPI().GetVolume(volumeName,calcAuthKey(volumeOwer))
					if err != nil {
						errout("get volume list failed:\n%v\n", err)
						return
					}
					fmt.Println("*********************" )

					//fmt.Printf("%+v\n", metanode.OpInode.InodeGet())

				}
			}
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

