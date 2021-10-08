package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/chubaofs/chubaofs/proto"
	sdk "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/spf13/cobra"
)

const (
	cmdExtentUse         = "extent [command]"
	cmdExtentShort       = "Check extent consistency"
	cmdCheckReplicaUse   = "check-replica volumeName"
	cmdCheckReplicaShort = "Check replica consistency"
	cmdCheckLengthUse    = "check-length volumeName"
	cmdCheckLengthShort  = "Check extent length"
)

const (
	extentReplica = 0
	extentLength  = 1
)

type ExtentMd5 struct {
	PartitionID uint64 `json:"PartitionID"`
	ExtentID    uint64 `json:"ExtentID"`
	Md5         string `json:"md5"`
}

func newExtentCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdExtentUse,
		Short: cmdExtentShort,
		Args:  cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(
		newCheckCmd(client, extentReplica),
		newCheckCmd(client, extentLength),
	)
	return cmd
}

func newCheckCmd(client *sdk.MasterClient, checkType int) *cobra.Command {
	var (
		use         string
		short       string
		concurrency uint64
		path        string
		inodeStr    string
	)
	if checkType == extentReplica {
		use = cmdCheckReplicaUse
		short = cmdCheckReplicaShort
	} else if checkType == extentLength {
		use = cmdCheckLengthUse
		short = cmdCheckLengthShort
	}
	var cmd = &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				vol    = args[0]
				inodes []uint64
				names  []string
			)
			if len(inodeStr) > 0 {
				inodeSlice := strings.Split(inodeStr, ",")
				for _, inode := range inodeSlice {
					ino, err := strconv.Atoi(inode)
					if err != nil {
						continue
					}
					inodes = append(inodes, uint64(ino))
				}
				names = make([]string, len(inodes))
			}
			if vol == "all" {
				vols, err := client.AdminAPI().ListVols("")
				if err != nil {
					stdout("ListVols fails, err:%v\n", err)
					return
				}
				var sortVol []string
				for _, vol := range vols {
					sortVol = append(sortVol, vol.Name)
				}
				sort.Strings(sortVol)
				for _, vol := range sortVol {
					checkVol(client, vol, path, inodes, names, concurrency, checkType)
				}
			} else {
				checkVol(client, vol, path, inodes, names, concurrency, checkType)
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
	cmd.Flags().StringVar(&path, "path", "/", "path")
	cmd.Flags().StringVar(&inodeStr, "inode", "", "comma separated inodes")
	cmd.Flags().Uint64Var(&concurrency, "concurrency", 1, "max concurrent checking extent")
	return cmd
}

func checkVol(client *sdk.MasterClient, vol string, path string, inodes []uint64, names []string, concurrency uint64, checkType int) {
	defer func() {
		msg := fmt.Sprintf("checkVol, vol:%s, path%s", vol, path)
		if r := recover(); r != nil {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			stdout("%s%s", msg, stack)
		}
	}()
	var checkedExtent sync.Map
	stdout("begin check, vol:%s\n", vol)
	if len(inodes) == 0 {
		inodes, names = lookupPath(client, vol, path)
	}
	for idx, inode := range inodes {
		checkInode(client, vol, inode, names[idx], checkedExtent, concurrency, checkType)
	}
	stdout("finish check, vol:%s\n", vol)
}

func lookupPath(client *sdk.MasterClient, vol string, path string) (inodes []uint64, names []string) {
	ctx := context.Background()
	mw, err := meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        vol,
		Masters:       client.Nodes(),
		ValidateOwner: false,
		InfiniteRetry: true,
	})
	if err != nil {
		stdout("NewMetaWrapper fails, err:%v\n", err)
	}
	ino, err := mw.LookupPath(ctx, path)
	if err != nil {
		stdout("LookupPath fails, err:%v\n", err)
	}
	return lookupParentInode(mw, vol, ino)
}

func lookupParentInode(mw *meta.MetaWrapper, vol string, parent uint64) (inodes []uint64, names []string) {
	ctx := context.Background()
	dentries, err := mw.ReadDir_ll(ctx, parent)
	if err != nil {
		stdout("ReadDir_ll fails, err:%v\n", err)
		return inodes, names
	}
	for _, dentry := range dentries {
		if proto.IsRegular(dentry.Type) {
			inodes = append(inodes, dentry.Inode)
			names = append(names, dentry.Name)
		} else if proto.IsDir(dentry.Type) {
			newInodes, newNames := lookupParentInode(mw, vol, dentry.Inode)
			inodes = append(inodes, newInodes...)
			names = append(names, newNames...)
		}
	}
	return
}

func checkInode(client *sdk.MasterClient, vol string, inode uint64, name string, checkedExtent sync.Map, concurrency uint64, checkType int) {
	var err error
	var (
		extentsResp *proto.GetExtentsResponse
		errCount    int = 0
		wg          sync.WaitGroup
	)
	extentsResp, err = getExtentsFromMeta(client, vol, inode)
	if err != nil {
		return
	}

	stdout("begin check, vol:%s, inode: %d, name: %s, extent count: %d\n", vol, inode, name, len(extentsResp.Extents))
	ekCh := make(chan proto.ExtentKey)
	wg.Add(len(extentsResp.Extents))
	go func() {
		for _, ek := range extentsResp.Extents {
			ekCh <- ek
		}
		close(ekCh)
	}()
	var idx int32
	for i := 0; i < int(concurrency); i++ {
		go func(client *sdk.MasterClient, checkedExtent sync.Map) {
			for ek := range ekCh {
				if checkType == extentReplica {
					checkExtentReplica(client, &ek, checkedExtent)
				} else if checkType == extentLength {
					checkExtentLength(client, &ek, checkedExtent)
				}
				atomic.AddInt32(&idx, 1)
				if idx%100 == 0 {
					stdout("%d extents checked\n", idx)
				}
				wg.Done()
			}
		}(client, checkedExtent)
	}
	wg.Wait()
	stdout("finish check, vol:%s, inode: %d, name: %s, err count: %d\n", vol, inode, name, errCount)
}

func checkExtentReplica(client *sdk.MasterClient, ek *proto.ExtentKey, checkedExtent sync.Map) bool {
	var (
		ok    bool
		ekStr string = fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId)
	)
	if _, ok = checkedExtent.LoadOrStore(ekStr, true); ok {
		return true
	}
	partition, err := client.AdminAPI().GetDataPartition("", ek.PartitionId)
	if err != nil {
		stdout("GetDataPartition PartitionId(%v) err(%v)\n", ek.PartitionId, err)
		return false
	}

	var (
		replicas = make([]struct {
			partitionId uint64
			extentId    uint64
			datanode    string
			md5         string
		}, len(partition.Replicas))
		md5Map = make(map[string]int)
	)
	for idx, replica := range partition.Replicas {
		datanode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], client.DataNodeProfPort)
		extentMd5, err := getExtentMd5(datanode, ek.PartitionId, ek.ExtentId)
		if err != nil {
			stdout("getExtentMd5 datanode(%v) PartitionId(%v) ExtentId(%v) err(%v)\n", datanode, ek.PartitionId, ek.ExtentId, err)
			return false
		}
		replicas[idx].partitionId = ek.PartitionId
		replicas[idx].extentId = ek.ExtentId
		replicas[idx].datanode = datanode
		replicas[idx].md5 = extentMd5.Md5
		if _, ok = md5Map[replicas[idx].md5]; ok {
			md5Map[replicas[idx].md5]++
		} else {
			md5Map[replicas[idx].md5] = 1
		}
	}
	if len(md5Map) == 1 {
		return true
	}
	for _, r := range replicas {
		msg := fmt.Sprintf("dp: %d, extent: %d, datanode: %s, md5: %s\n", r.partitionId, r.extentId, r.datanode, r.md5)
		if _, ok = md5Map[r.md5]; ok && md5Map[r.md5] > len(partition.Replicas)/2 {
			stdout(msg)
		} else {
			stdout("ERROR %s", msg)
		}
	}
	return false
}

func checkExtentLength(client *sdk.MasterClient, ek *proto.ExtentKey, checkedExtent sync.Map) bool {
	var (
		ok    bool
		ekStr string = fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId)
	)
	if _, ok = checkedExtent.LoadOrStore(ekStr, true); ok {
		return true
	}
	partition, err := client.AdminAPI().GetDataPartition("", ek.PartitionId)
	if err != nil {
		stdout("GetDataPartition PartitionId(%v) err(%v)\n", ek.PartitionId, err)
		return false
	}

	datanode := fmt.Sprintf("%s:%d", strings.Split(partition.Replicas[0].Addr, ":")[0], client.DataNodeProfPort)
	extent, err := getExtentFromData(client, ek.PartitionId, ek.ExtentId)
	if err != nil {
		stdout("getExtentFromData datanode(%v) PartitionId(%v) ExtentId(%v) err(%v)\n", datanode, ek.PartitionId, ek.ExtentId, err)
		return false
	}
	if ek.ExtentOffset+uint64(ek.Size) > extent.Size {
		stdout("ERROR ek:%v, extent:%v\n", ek, extent)
		return false
	}
	return true
}

func getExtentsFromMeta(client *sdk.MasterClient, vol string, inode uint64) (re *proto.GetExtentsResponse, err error) {
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
	url := fmt.Sprintf("http://%s/getExtentsByInode?pid=%d&ino=%d", metanode, mpId, inode)
	resp, err := http.Get(url)
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
	if re == nil {
		err = fmt.Errorf("Get %s fails, data: %s", url, string(data))
		return
	}
	return
}

func getExtentFromData(client *sdk.MasterClient, partitionId uint64, extentId uint64) (re *storage.ExtentInfo, err error) {
	partition, err := client.AdminAPI().GetDataPartition("", partitionId)
	datanode := partition.Hosts[0]
	addressInfo := strings.Split(datanode, ":")
	datanode = fmt.Sprintf("%s:%d", addressInfo[0], client.DataNodeProfPort)
	url := fmt.Sprintf("http://%s/extent?partitionID=%d&extentID=%d", datanode, partitionId, extentId)
	resp, err := http.Get(url)
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
	re = &storage.ExtentInfo{}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	if re == nil {
		err = fmt.Errorf("Get %s fails, data: %s", url, string(data))
		return
	}
	return
}

func getExtentMd5(datanode string, dpId uint64, extentId uint64) (re *ExtentMd5, err error) {
	var (
		resp *http.Response
		data []byte
		url  string = fmt.Sprintf("http://%s/computeExtentMd5?id=%d&extent=%d", datanode, dpId, extentId)
	)
	if resp, err = http.Get(url); err != nil {
		return
	}
	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	if data, err = parseResp(data); err != nil {
		return
	}
	re = &ExtentMd5{}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	if re == nil {
		err = fmt.Errorf("Get %s fails, data: %s", url, string(data))
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
