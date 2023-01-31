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
	var addr string
	var cmd = &cobra.Command{
		Use:   cmdInodeInfoUse,
		Short: cmdInodeShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err        error
				volumeName = args[0]
				inodeStr   = args[1]
				inode, _   = strconv.Atoi(inodeStr)
				ino        = uint64(inode)
				leader     string
				mpId       uint64
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			mps, err := client.ClientAPI().GetMetaPartitions(volumeName)
			if err != nil {
				errout("get metapartitions failed:\n%v\n", err)
				return
			}
			for _, mp := range mps {
				if ino >= mp.Start && ino < mp.End {
					leader = mp.LeaderAddr
					mpId = mp.PartitionID
					break
				}
			}
			if leader == "" {
				errout("mp[%v] no leader:\n", mpId)
				return
			}
			if addr == "" {
				addr = strings.Split(leader, ":")[0]
			}
			metaNodeProfPort := client.MetaNodeProfPort

			if !proto.IsDbBack {
				resp, err := http.Get(fmt.Sprintf("http://%s:%d/getInode?pid=%d&ino=%d", addr, metaNodeProfPort, mpId, ino))
				if err != nil {
					errout("get inode info failed:\n%v\n", err)
					return
				}
				all, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					errout("read inode info failed:\n%v\n", err)
					return
				}
				value := make(map[string]interface{})
				err = json.Unmarshal(all, &value)
				if err != nil {
					errout("unmarshal inode info failed:\n%v\n", err)
					return
				}
				if value["msg"] != "Ok" {
					errout("get inode info failed:\n%v\n", value["msg"])
					return
				}
				data := value["data"].(map[string]interface{})
				dataInfo := data["info"].(map[string]interface{})
				inodeInfoView := &proto.InodeInfoView{
					Ino:         uint64(dataInfo["ino"].(float64)),
					PartitionID: mpId,
					At:          dataInfo["at"].(string),
					Ct:          dataInfo["ct"].(string),
					Mt:          dataInfo["mt"].(string),
					Nlink:       uint64(dataInfo["nlink"].(float64)),
					Size:        uint64(dataInfo["sz"].(float64)),
					Gen:         uint64(dataInfo["gen"].(float64)),
					Gid:         uint64(dataInfo["gid"].(float64)),
					Uid:         uint64(dataInfo["uid"].(float64)),
					Mode:        uint64(dataInfo["mode"].(float64)),
				}
				stdout("Summary of inode  :\n%s\n", formatInodeInfoView(inodeInfoView))
			}

			// getExtentsByInode
			path := "getExtentsByInode"
			if proto.IsDbBack {
				path = "getExtents"
			}
			resp, err := http.Get(fmt.Sprintf("http://%s:%d/%s?pid=%d&ino=%d", addr, metaNodeProfPort, path, mpId, ino))
			if err != nil {
				errout("get inode extents failed:\n%v\n", err)
				return
			}
			all, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				errout("read inode extents failed:\n%v\n", err)
				return
			}
			value := make(map[string]interface{})
			err = json.Unmarshal(all, &value)
			if err != nil {
				errout("unmarshal inode extents failed:\n%s\n%v\n", string(all), err)
				return
			}
			var eks []interface{}
			if proto.IsDbBack {
				if value["Extents"] == nil {
					errout("no extents\n")
					return
				}
				eks = value["Extents"].([]interface{})
			} else {
				if value["msg"] != "Ok" {
					errout("get inode extents failed: %v\n", value["msg"])
					return
				}
				data := value["data"].(map[string]interface{})
				if data["eks"] == nil {
					errout("no extents\n")
					return
				}
				eks = data["eks"].([]interface{})
			}
			stdout("Summary of inodeExtentInfo  :\nEks length: %v\n%s\n", len(eks), inodeExtentInfoTableHeader)
			var (
				fileOffset float64
				crc        float64
				total      uint64
			)
			for _, ek := range eks {
				inodeExtentInfo := ek.(map[string]interface{})
				var inodeExtentInfoView *proto.InodeExtentInfoView
				// to be compatible with dbback cluster
				if proto.IsDbBack {
					fileOffset = float64(total)
				} else {
					fileOffset = inodeExtentInfo["FileOffset"].(float64)
					crc = inodeExtentInfo["CRC"].(float64)
				}
				inodeExtentInfoView = &proto.InodeExtentInfoView{
					FileOffset:   uint64(fileOffset),
					PartitionId:  uint64(inodeExtentInfo["PartitionId"].(float64)),
					ExtentId:     uint64(inodeExtentInfo["ExtentId"].(float64)),
					ExtentOffset: uint64(inodeExtentInfo["ExtentOffset"].(float64)),
					Size:         uint64(inodeExtentInfo["Size"].(float64)),
					CRC:          uint64(crc),
				}
				total += inodeExtentInfoView.Size
				stdout("%v\n", formatInodeExtentInfoTableRow(inodeExtentInfoView))
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
	cmd.Flags().StringVar(&addr, "addr", "", "address of metanode")
	return cmd
}
