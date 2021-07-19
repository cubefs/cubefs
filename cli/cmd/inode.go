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
	cmdInodeUse          = "inode [COMMAND]"
	cmdInodeInfoUse      = "info volumeName inodeId"
	cmdInodeShort        = "Show inode information"
	cmdCheckReplicaUse   = "check-replica volumeName inodeId(comma separated list)"
	cmdCheckReplicaShort = "Check inode replica consistency"
)

func newInodeCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdInodeUse,
		Short: cmdInodeShort,
		Args:  cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(
		newInodeInfoCmd(client),
		newCheckReplicaCmd(client),
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

type ExtentMd5 struct {
	PartitionID uint64 `json:"PartitionID"`
	ExtentID    uint64 `json:"ExtentID"`
	Md5         string `json:"md5"`
}

func newCheckReplicaCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdCheckReplicaUse,
		Short: cmdCheckReplicaShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var volumeName = args[0]
			var inodeStr = args[1]
			inodeSlice := strings.Split(inodeStr, ",")
			var inodes []uint64
			for _, inode := range inodeSlice {
				ino, err := strconv.Atoi(inode)
				if err != nil {
					continue
				}
				inodes = append(inodes, uint64(ino))
			}
			for _, inode := range inodes {
				checkInode(client, volumeName, inode)
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

func checkInode(client *sdk.MasterClient, vol string, inode uint64) {
	var err error
	defer func() {
		if err != nil {
			errout("checkInode failed: %v", err)
		}
	}()
	var (
		extentsResp *proto.GetExtentsResponse
		errCount    int = 0
		partition   *proto.DataPartitionInfo
		extentMd5   *ExtentMd5
	)
	extentsResp, err = getExtentsByInode(client, vol, inode)
	if err != nil {
		return
	}
	stdout("begin check, inode: %d, extent count: %d\n", inode, len(extentsResp.Extents))
	for _, ek := range extentsResp.Extents {
		partition, err = client.AdminAPI().GetDataPartition("", ek.PartitionId)
		if err != nil {
			return
		}
		var preDatanode string
		var preMd5 string
		for idx, replica := range partition.Replicas {
			datanode := strings.ReplaceAll(replica.Addr, "17030", "17031")
			extentMd5, err = getExtentMd5(datanode, ek.PartitionId, ek.ExtentId)
			if err != nil {
				return
			}
			msg := fmt.Sprintf("dp: %d, extent: %d, datanode: %s, md5: %s\n", ek.PartitionId, ek.ExtentId, datanode, extentMd5.Md5)
			if idx == 0 || extentMd5.Md5 == preMd5 {
				stdout(msg)
			} else {
				errout("ERROR %s, preDatanode: %s, preMd5: %s\n", msg, preDatanode, preMd5)
				errCount++
			}
			preDatanode = datanode
			preMd5 = extentMd5.Md5
		}
	}
	stdout("finish check, inode: %d, err count: %d\n", inode, errCount)
}

func getExtentsByInode(client *sdk.MasterClient, vol string, inode uint64) (re *proto.GetExtentsResponse, err error) {
	mps, err := client.ClientAPI().GetMetaPartitions(vol)
	if err != nil {
		return
	}
	var metanode string
	var mpId uint64
	for _, mp := range mps {
		if inode >= mp.Start && inode < mp.End {
			metanode = mp.LeaderAddr
			mpId = mp.PartitionID
			break
		}
	}
	addressInfo := strings.Split(metanode, ":")
	metanode = fmt.Sprintf("%s:%d", addressInfo[0], client.MetaNodeProfPort)
	resp, err := http.Get(fmt.Sprintf("http://%s/getExtentsByInode?pid=%d&ino=%d", metanode, mpId, inode))
	if err != nil {
		return
	}
	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var data []byte
	if data, err = parseResp(respData); err != nil {
		return
	}
	re = &proto.GetExtentsResponse{}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	return
}

func getExtentMd5(datanode string, dpId uint64, extentId uint64) (re *ExtentMd5, err error) {
	resp, _ := http.Get(fmt.Sprintf("http://%s/computeExtentMd5?id=%d&extent=%d", datanode, dpId, extentId))
	respData, _ := ioutil.ReadAll(resp.Body)
	var data []byte
	if data, err = parseResp(respData); err != nil {
		return
	}
	re = &ExtentMd5{}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	return
}

func parseResp(resp []byte) (data []byte, err error) {
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(resp, &body); err != nil {
		return
	}
	data = body.Data
	return
}
