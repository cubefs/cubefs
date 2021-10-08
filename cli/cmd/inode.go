package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	sdk "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdInodeUse     = "inode [COMMAND]"
	cmdInodeInfoUse = "info volumeName inodeId"
	cmdInodeShort   = "Show inode information"
)

func newInodeCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdInodeUse,
		Short: cmdInodeShort,
		Args:  cobra.MinimumNArgs(1),
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
			//var svv *proto.SimpleVolView
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var views []*proto.MetaPartitionView
			if views, err = client.ClientAPI().GetMetaPartitions(volumeName); err != nil {
				err = fmt.Errorf("Get volume metadata detail information failed:\n%v\n", err)
				return
			}
			for _, view := range views {
				inodeStartNumber := view.Start
				inodeEndNumber := view.End
				metaPartitionNumber := view.PartitionID
				if inodeStartNumber < inodeUnint64Number && inodeUnint64Number < inodeEndNumber {
					for _, addr := range view.Members {
						addr := strings.Split(addr, ":")[0]
						metaNodeProfPort := client.MetaNodeProfPort
						if metaNodeProfPort == 0 {
							metaNodeProfPort = 17220
						}
						resp, err := http.Get(fmt.Sprintf("http://%s:%d/getInode?pid=%d&ino=%d", addr, metaNodeProfPort, metaPartitionNumber, inodeUnint64Number))
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
						if value["msg"] != "Ok" {
							errout("get inode information failed: %v\n", value["msg"])
							return
						}
						data := value["data"].(map[string]interface{})
						dataInfo := data["info"].(map[string]interface{})
						var inodeInfoView *proto.InodeInfoView
						inodeInfoView = &proto.InodeInfoView{
							Ino:         uint64(dataInfo["ino"].(float64)),
							PartitionID: metaPartitionNumber,
							At:          dataInfo["at"].(string),
							Ct:          dataInfo["ct"].(string),
							Mt:          dataInfo["mt"].(string),
							Nlink:       uint64(dataInfo["nlink"].(float64)),
							Gen:         uint64(dataInfo["gen"].(float64)),
							Gid:         uint64(dataInfo["gid"].(float64)),
							Uid:         uint64(dataInfo["uid"].(float64)),
							Mode:        uint64(dataInfo["mode"].(float64)),
						}
						stdout("Summary of inode  :\n%s\n", formatInodeInfoView(inodeInfoView))

						// getExtentsByInode
						resp, err = http.Get(fmt.Sprintf("http://%s:%d/getExtentsByInode?pid=%d&ino=%d", addr, metaNodeProfPort, metaPartitionNumber, inodeUnint64Number))
						if err != nil {
							errout("get partition list failed:\n%v\n", err)
							return
						}
						all, err = ioutil.ReadAll(resp.Body)
						if err != nil {
							errout("get partition list failed:\n%v\n", err)
							return
						}

						value = make(map[string]interface{})
						err = json.Unmarshal(all, &value)
						if err != nil {
							errout("get partition info failed:\n%v\n", err)
							return
						}
						if value["msg"] != "Ok" {
							errout("get inode information failed: %v\n", value["msg"])
							return
						}
						data = value["data"].(map[string]interface{})
						if data["eks"] != nil {
							dataEks := data["eks"].([]interface{})
							stdout("Summary of inodeExtentInfo  :\n%s\n", inodeExtentInfoTableHeader)
							for _, ek := range dataEks {
								inodeExtentInfo := ek.(map[string]interface{})
								var inodeExtentInfoView *proto.InodeExtentInfoView
								inodeExtentInfoView = &proto.InodeExtentInfoView{
									FileOffset:   uint64(inodeExtentInfo["FileOffset"].(float64)),
									PartitionId:  uint64(inodeExtentInfo["PartitionId"].(float64)),
									ExtentId:     uint64(inodeExtentInfo["ExtentId"].(float64)),
									ExtentOffset: uint64(inodeExtentInfo["ExtentOffset"].(float64)),
									Size:         uint64(inodeExtentInfo["Size"].(float64)),
									CRC:          uint64(inodeExtentInfo["CRC"].(float64)),
								}
								stdout("%v\n", formatInodeExtentInfoTableRow(inodeExtentInfoView))
							}
						}
						return
					}
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
