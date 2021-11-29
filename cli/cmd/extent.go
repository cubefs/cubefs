package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/metanode"
	"github.com/chubaofs/chubaofs/proto"
	sdk "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util"
	"github.com/spf13/cobra"
	"github.com/tiglabs/raft"
)

const (
	cmdExtentUse         = "extent [command]"
	cmdExtentShort       = "Check extent consistency"
	cmdCheckReplicaUse   = "check-replica volumeName"
	cmdCheckReplicaShort = "Check replica consistency"
	cmdCheckLengthUse    = "check-length volumeName"
	cmdCheckLengthShort  = "Check extent length"
	cmdSearchExtentUse   = "search volumeName"
	cmdSearchExtentShort = "Search extent key"
	cmdCheckGarbageUse   = "check-garbage volumeName"
	cmdCheckGarbageShort = "Check garbage extents"
)

const (
	extentReplica = 0
	extentLength  = 1
)

var client *sdk.MasterClient

type ExtentMd5 struct {
	PartitionID uint64 `json:"PartitionID"`
	ExtentID    uint64 `json:"ExtentID"`
	Md5         string `json:"md5"`
}

type DataPartition struct {
	VolName              string                `json:"volName"`
	ID                   uint64                `json:"id"`
	Size                 int                   `json:"size"`
	Used                 int                   `json:"used"`
	Status               int                   `json:"status"`
	Path                 string                `json:"path"`
	Files                []*storage.ExtentInfo `json:"extents"`
	FileCount            int                   `json:"fileCount"`
	Replicas             []string              `json:"replicas"`
	Peers                []proto.Peer          `json:"peers"`
	TinyDeleteRecordSize int64                 `json:"tinyDeleteRecordSize"`
	RaftStatus           *raft.Status          `json:"raftStatus"`
}

type Inode struct {
	Inode      uint64
	Type       uint32
	Size       uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	NLink      uint32

	Dens  []*Dentry
	Valid bool
}

type Dentry struct {
	ParentId uint64
	Name     string
	Inode    uint64
	Type     uint32

	Valid bool
}

func newExtentCmd(mc *sdk.MasterClient) *cobra.Command {
	client = mc
	var cmd = &cobra.Command{
		Use:   cmdExtentUse,
		Short: cmdExtentShort,
		Args:  cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(
		newExtentCheckCmd(extentReplica),
		newExtentCheckCmd(extentLength),
		newExtentSearchCmd(),
		newExtentGarbageCheckCmd(),
	)
	return cmd
}

func newExtentCheckCmd(checkType int) *cobra.Command {
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
					checkVol(vol, path, inodes, names, concurrency, checkType)
				}
			} else {
				checkVol(vol, path, inodes, names, concurrency, checkType)
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
	cmd.Flags().Uint64Var(&concurrency, "concurrency", 1, "max concurrent checking extents")
	return cmd
}

func newExtentSearchCmd() *cobra.Command {
	var (
		use         = cmdSearchExtentUse
		short       = cmdSearchExtentShort
		concurrency uint64
		dpStr       string
		extentStr   string
	)
	var cmd = &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				vol     = args[0]
				dps     []uint64
				extents []uint64
			)
			if len(dpStr) > 0 {
				for _, v := range strings.Split(dpStr, ",") {
					dp, err := strconv.Atoi(v)
					if err != nil {
						continue
					}
					dps = append(dps, uint64(dp))
				}
			}
			if len(extentStr) > 0 {
				for _, v := range strings.Split(extentStr, ",") {
					extentRange := strings.Split(v, "-")
					if len(extentRange) == 2 {
						begin, err := strconv.Atoi(extentRange[0])
						if err != nil {
							continue
						}
						end, err := strconv.Atoi(extentRange[1])
						if err != nil {
							continue
						}
						for i := begin; i <= end; i++ {
							extents = append(extents, uint64(i))
						}
						continue
					}
					extent, err := strconv.Atoi(v)
					if err != nil {
						continue
					}
					extents = append(extents, uint64(extent))
				}
			}
			if len(dps) == 0 || (len(dps) > 1 && len(dps) != len(extents)) {
				stdout("invalid parameters.\n")
				return
			}
			searchExtent(vol, dps, extents, concurrency)
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&dpStr, "dps", "", "comma separated data partitions")
	cmd.Flags().StringVar(&extentStr, "extents", "", "comma separated extents")
	cmd.Flags().Uint64Var(&concurrency, "concurrency", 1, "max concurrent searching inodes")
	return cmd
}

func searchExtent(vol string, dps []uint64, extents []uint64, concurrency uint64) {
	inodes, _ := getAllInodesByVol(vol)
	extentMap := make(map[string]bool)
	var dp uint64
	for i := 0; i < len(extents); i++ {
		if len(dps) == 1 {
			dp = dps[0]
		} else {
			dp = dps[i]
		}
		extentMap[fmt.Sprintf("%d-%d", dp, extents[i])] = true
	}

	var wg sync.WaitGroup
	wg.Add(len(inodes))
	for i := 0; i < int(concurrency); i++ {
		go func(i int) {
			idx := 0
			for {
				if idx*int(concurrency)+i >= len(inodes) {
					break
				}
				inode := inodes[idx*int(concurrency)+i]
				extentsResp, err := getExtentsByInode(vol, inode)
				if err != nil {
					stdout("get extents error: %v, inode: %d\n", err, inode)
					wg.Done()
					continue
				}
				for _, ek := range extentsResp.Extents {
					_, ok := extentMap[fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId)]
					if ok {
						stdout("inode: %d, dp: %d, extent: %d\n", inode, ek.PartitionId, ek.ExtentId)
					}
				}
				wg.Done()
				idx++
			}
		}(i)
	}
	wg.Wait()
}

func newExtentGarbageCheckCmd() *cobra.Command {
	var (
		use         = cmdCheckGarbageUse
		short       = cmdCheckGarbageShort
		all         bool
		active      bool
		dir         string
		clean       bool
		concurrency uint64
	)
	var cmd = &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				vol = args[0]
			)
			garbageCheck(vol, all, active, dir, clean, concurrency)
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "Check all garbage extents (only for extents modified in last 7 days by default)")
	cmd.Flags().BoolVar(&active, "active", false, "Check garbage extents using active inodes of user file system (all inodes of metanode by default)")
	cmd.Flags().StringVar(&dir, "dir", ".", "Output file dir")
	cmd.Flags().BoolVar(&clean, "clean", false, "Clean garbage extents")
	cmd.Flags().Uint64Var(&concurrency, "concurrency", 1, "max concurrent checking extents")
	return cmd
}

func garbageCheck(vol string, all bool, active bool, dir string, clean bool, concurrency uint64) {
	// get all extents from datanode, MUST get extents from datanode first in case of newly added extents being deleted
	dataExtentMap := make(map[uint64]map[uint64]*storage.ExtentInfo)
	var (
		view   *proto.DataPartitionsView
		dpInfo *DataPartition
		err    error
	)
	view, err = client.ClientAPI().GetDataPartitions(vol)
	if err != nil {
		stdout("get data partitions error: %v\n", err)
		return
	}
	for _, dp := range view.DataPartitions {
		dpInfo, err = getExtentsByDp(dp.PartitionID)
		if err != nil {
			stdout("get extents error: %v, dp: %d\n", err, dp.PartitionID)
			return
		}
		_, ok := dataExtentMap[dp.PartitionID]
		if !ok {
			dataExtentMap[dp.PartitionID] = make(map[uint64]*storage.ExtentInfo)
		}
		for _, extent := range dpInfo.Files {
			dataExtentMap[dp.PartitionID][extent.FileID] = extent
		}
	}

	// get all extents from metanode
	var inodes []uint64
	if active {
		inodes, _, err = getAllInodesByPath(vol, "")
	} else {
		inodes, err = getAllInodesByVol(vol)
	}
	if err != nil {
		stdout("get all inodes error: %v\n", err)
		return
	}
	metaExtentMap := make(map[uint64]map[uint64]bool)
	extents, err := getExtentsByInodes(vol, inodes, concurrency)
	if err != nil {
		stdout("get extents error: %v\n", err)
		return
	}
	for _, ek := range extents {
		_, ok := metaExtentMap[ek.PartitionId]
		if !ok {
			metaExtentMap[ek.PartitionId] = make(map[uint64]bool)
		}
		metaExtentMap[ek.PartitionId][ek.ExtentId] = true
	}

	garbage := make(map[uint64][]uint64)
	year, month, day := time.Now().Date()
	today := time.Date(year, month, day, 0, 0, 0, 0, time.Local)
	for dp := range dataExtentMap {
		for extent, extentInfo := range dataExtentMap[dp] {
			_, ok := metaExtentMap[dp]
			if ok {
				_, ok = metaExtentMap[dp][extent]
			}
			if !ok && (all || today.Unix()-extentInfo.ModifyTime >= 604800) && extentInfo.FileID > storage.MinExtentID {
				garbage[dp] = append(garbage[dp], extentInfo.FileID)
			}
		}
	}

	os.Mkdir(fmt.Sprintf("%s/%s", dir, vol), os.ModePerm)
	for dp := range garbage {
		sort.Slice(garbage[dp], func(i, j int) bool { return garbage[dp][i] < garbage[dp][j] })
		strSlice := make([]string, len(garbage[dp]))
		for i, extent := range garbage[dp] {
			strSlice[i] = fmt.Sprintf("%d", extent)
		}
		ioutil.WriteFile(fmt.Sprintf("%s/%s/%d", dir, vol, dp), []byte(strings.Join(strSlice, "\n")), 0666)
		if clean {
			batchDeleteExtent(dp, garbage[dp])
		}
	}
}

func batchDeleteExtent(partitionId uint64, extents []uint64) (err error) {
	if len(extents) == 0 {
		return
	}
	stdout("start delete extent, partitionId: %d, extents len: %d\n", partitionId, len(extents))
	partition, err := client.AdminAPI().GetDataPartition("", partitionId)
	if err != nil {
		stdout("GetDataPartition error: %v, PartitionId: %v\n", err, partitionId)
		return
	}
	var gConnPool = util.NewConnectPool()
	conn, err := gConnPool.GetConnect(partition.Hosts[0])
	defer func() {
		if err != nil {
			gConnPool.PutConnect(conn, true)
		} else {
			gConnPool.PutConnect(conn, false)
		}
	}()

	if err != nil {
		stdout("get conn from pool error: %v, partitionId: %d\n", err, partitionId)
		return
	}
	dp := &metanode.DataPartition{
		PartitionID: partitionId,
		Hosts:       partition.Hosts,
	}
	eks := make([]*proto.ExtentKey, len(extents))
	for i := 0; i < len(extents); i++ {
		eks[i] = &proto.ExtentKey{
			PartitionId: partitionId,
			ExtentId:    extents[i],
		}
	}
	packet := metanode.NewPacketToBatchDeleteExtent(context.Background(), dp, eks)
	if err = packet.WriteToConn(conn); err != nil {
		stdout("write to dataNode error: %v, logId: %s\n", err, packet.GetUniqueLogId())
		return
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime*10); err != nil {
		stdout("read response from dataNode error: %s, logId: %s\n", err, packet.GetUniqueLogId())
		return
	}
	if packet.ResultCode != proto.OpOk {
		stdout("batch delete extent response: %s, logId: %s\n", packet.GetResultMsg(), packet.GetUniqueLogId())
	}
	stdout("finish delete extent, partitionId: %d, extents len: %v\n", partitionId, len(extents))
	return
}

func checkVol(vol string, path string, inodes []uint64, names []string, concurrency uint64, checkType int) {
	defer func() {
		msg := fmt.Sprintf("checkVol, vol:%s, path%s", vol, path)
		if r := recover(); r != nil {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			stdout("%s%s\n", msg, stack)
		}
	}()
	var checkedExtent sync.Map
	stdout("begin check, vol:%s\n", vol)
	if len(inodes) == 0 {
		inodes, names, _ = getAllInodesByPath(vol, path)
	}
	for idx, inode := range inodes {
		checkInode(vol, inode, names[idx], checkedExtent, concurrency, checkType)
	}
	stdout("finish check, vol:%s\n", vol)
}

func getAllInodesByPath(vol string, path string) (inodes []uint64, names []string, err error) {
	ctx := context.Background()
	var mw *meta.MetaWrapper
	mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        vol,
		Masters:       client.Nodes(),
		ValidateOwner: false,
		InfiniteRetry: true,
	})
	if err != nil {
		stdout("NewMetaWrapper fails, err:%v\n", err)
		return
	}
	var ino uint64
	ino, err = mw.LookupPath(ctx, path)
	if err != nil {
		stdout("LookupPath fails, err:%v\n", err)
		return
	}
	return getChildInodesByParent(mw, vol, ino)
}

func getChildInodesByParent(mw *meta.MetaWrapper, vol string, parent uint64) (inodes []uint64, names []string, err error) {
	ctx := context.Background()
	var dentries []proto.Dentry
	dentries, err = mw.ReadDir_ll(ctx, parent)
	if err != nil {
		stdout("ReadDir_ll fails, err:%v\n", err)
		return
	}
	var newInodes []uint64
	var newNames []string
	for _, dentry := range dentries {
		if proto.IsRegular(dentry.Type) {
			inodes = append(inodes, dentry.Inode)
			names = append(names, dentry.Name)
		} else if proto.IsDir(dentry.Type) {
			newInodes, newNames, err = getChildInodesByParent(mw, vol, dentry.Inode)
			if err != nil {
				return
			}
			inodes = append(inodes, newInodes...)
			names = append(names, newNames...)
		}
	}
	return
}

func getAllInodesByVol(vol string) (inodes []uint64, err error) {
	mps, err := client.ClientAPI().GetMetaPartitions(vol)
	if err != nil {
		return
	}
	var inos []*Inode
	for _, mp := range mps {
		inos, err = getInodesByMp(mp.PartitionID, mp.LeaderAddr)
		if err != nil {
			return
		}
		for _, ino := range inos {
			inodes = append(inodes, ino.Inode)
		}
	}
	return
}

func checkInode(vol string, inode uint64, name string, checkedExtent sync.Map, concurrency uint64, checkType int) {
	var err error
	var (
		extentsResp *proto.GetExtentsResponse
		errCount    int = 0
		wg          sync.WaitGroup
	)
	extentsResp, err = getExtentsByInode(vol, inode)
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
					checkExtentReplica(&ek, checkedExtent)
				} else if checkType == extentLength {
					checkExtentLength(&ek, checkedExtent)
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

func checkExtentReplica(ek *proto.ExtentKey, checkedExtent sync.Map) bool {
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

func checkExtentLength(ek *proto.ExtentKey, checkedExtent sync.Map) bool {
	var (
		ok    bool
		ekStr string = fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId)
	)
	if _, ok = checkedExtent.LoadOrStore(ekStr, true); ok {
		return true
	}
	partition, err := client.AdminAPI().GetDataPartition("", ek.PartitionId)
	if err != nil {
		stdout("GetDataPartition ERROR: %v, PartitionId: %d\n", err, ek.PartitionId)
		return false
	}

	datanode := fmt.Sprintf("%s:%d", strings.Split(partition.Replicas[0].Addr, ":")[0], client.DataNodeProfPort)
	extent, err := getExtent(ek.PartitionId, ek.ExtentId)
	if err != nil {
		stdout("getExtentFromData ERROR: %v, datanode: %v, PartitionId: %v, ExtentId: %v\n", err, datanode, ek.PartitionId, ek.ExtentId)
		return false
	}
	if ek.ExtentOffset+uint64(ek.Size) > extent.Size {
		stdout("ERROR ek:%v, extent:%v\n", ek, extent)
		return false
	}
	return true
}

func getInodesByMp(metaPartitionId uint64, leaderAddr string) (Inodes []*Inode, err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d/getAllInodes?pid=%d", strings.Split(leaderAddr, ":")[0], client.MetaNodeProfPort, metaPartitionId))
	if err != nil {
		return nil, fmt.Errorf("Get all inode info failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Get all inode info read all body failed: %v", err)
	}

	body := &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(data, body); err != nil {
		return nil, fmt.Errorf("Unmarshal all inode info body failed: %v", err)
	}

	var Result []*Inode
	if err = json.Unmarshal(body.Data, &Result); err != nil {
		return nil, fmt.Errorf("Unmarshal all inode info failed: %v", err)
	}

	return Result, err
}

func getExtentsByInodes(vol string, inodes []uint64, concurrency uint64) (extents []proto.ExtentKey, err error) {
	var wg sync.WaitGroup
	inoCh := make(chan uint64, 1024)
	wg.Add(len(inodes))
	go func() {
		for _, ino := range inodes {
			inoCh <- ino
		}
		close(inoCh)
	}()

	resultCh := make(chan *proto.GetExtentsResponse, 1024)
	for i := 0; i < int(concurrency); i++ {
		go func() {
			for ino := range inoCh {
				re, tmpErr := getExtentsByInode(vol, ino)
				if tmpErr != nil {
					err = fmt.Errorf("get extents from inode err: %v, inode: %d", tmpErr, ino)
					resultCh <- nil
				} else {
					resultCh <- re
				}
				wg.Done()
			}
		}()
	}

	var wgResult sync.WaitGroup
	wgResult.Add(len(inodes))
	go func() {
		for re := range resultCh {
			if re == nil {
				wgResult.Done()
				continue
			}
			extents = append(extents, re.Extents...)
			wgResult.Done()
		}
	}()
	wg.Wait()
	close(resultCh)
	wgResult.Wait()
	if err != nil {
		extents = extents[:0]
	}
	return
}

func getExtentsByInode(vol string, inode uint64) (re *proto.GetExtentsResponse, err error) {
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

func getExtentsByDp(partitionId uint64) (re *DataPartition, err error) {
	partition, err := client.AdminAPI().GetDataPartition("", partitionId)
	datanode := partition.Hosts[0]
	addressInfo := strings.Split(datanode, ":")
	datanode = fmt.Sprintf("%s:%d", addressInfo[0], client.DataNodeProfPort)
	url := fmt.Sprintf("http://%s/partition?id=%d", datanode, partitionId)
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
	re = &DataPartition{}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	if re == nil {
		err = fmt.Errorf("Get %s fails, data: %s", url, string(data))
		return
	}
	return
}

func getExtent(partitionId uint64, extentId uint64) (re *storage.ExtentInfo, err error) {
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
