package data_check

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"
)

type ErrorEntry struct {
	BlockOffset uint64
	BlockSize   uint64
	ErrMessage  string
	BadHosts    []string
}

func (entry *ErrorEntry) String() string {
	return fmt.Sprintf("BlockOffset:%v, BlockSize:%v, ErrorMessage:\n%v", entry.BlockOffset, entry.BlockSize, entry.ErrMessage)
}

func CheckFixedOffsetSize(cluster string, dnProf uint16, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, volume string, quickCheck bool) (badExtent bool, badExtentInfo BadExtentInfo, err error) {
	log.LogDebugf("action[CheckFixedOffsetSize] cluster:%v, volume:%v, ek:%v", cluster, volume, ek)
	badExtent, badExtentInfo, err = slowCheckExtent(cluster, dnProf, dataReplicas, ek, 0, volume)
	return
}

func CheckFullExtent(cluster string, dnProf uint16, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, volume string, quickCheck bool) (badExtent bool, badExtentInfo BadExtentInfo, err error) {
	log.LogDebugf("action[CheckFullExtent] cluster:%v, volume:%v, ek:%v", cluster, volume, ek)
	if ek.Size == 0 {
		return
	}
	if proto.IsTinyExtent(ek.ExtentId) {
		badExtent, badExtentInfo, err = samplingCheckTinyExtent(cluster, dnProf, dataReplicas, ek, 0, volume)
		return
	}
	if quickCheck {
		badExtent, badExtentInfo, err = quickCheckExtent(cluster, dnProf, dataReplicas, ek, 0, volume)
		return
	}
	badExtent, badExtentInfo, err = slowCheckExtent(cluster, dnProf, dataReplicas, ek, 0, volume)
	return
}

func checkInodeExtentKey(cluster string, dnProf uint16, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, ino uint64, volume string, quickCheck bool) (badExtent bool, badExtentInfo BadExtentInfo, err error) {
	log.LogDebugf("action[checkInodeExtentKey] cluster:%v, volume:%v, ino:%v, ek:%v", cluster, volume, ino, ek)
	if ek.Size > unit.MB*128 {
		log.LogErrorf("invalid size extent:%v", ek)
		return
	}
	if proto.IsTinyExtent(ek.ExtentId) {
		badExtent, badExtentInfo, err = slowCheckExtent(cluster, dnProf, dataReplicas, ek, ino, volume)
		return
	}
	if quickCheck {
		badExtent, badExtentInfo, err = quickCheckExtent(cluster, dnProf, dataReplicas, ek, ino, volume)
		return
	}
	badExtent, badExtentInfo, err = slowCheckExtent(cluster, dnProf, dataReplicas, ek, ino, volume)
	return
}

func quickCheckExtent(cluster string, dnProf uint16, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, ino uint64, volume string) (badExtent bool, badExtentInfo BadExtentInfo, err error) {
	if proto.IsTinyExtent(ek.ExtentId) {
		return
	}
	var same bool
	var wrongCrcBlocks []int
	blocksLengh := ek.Size / uint32(DefaultCheckBlockKB*unit.KB)
	if ek.Size-blocksLengh*uint32(DefaultCheckBlockKB*unit.KB) > 0 {
		blocksLengh += 1
	}
	errEntryMap := make(map[int]*ErrorEntry, 0)
	//先查128K块的CRC是否有不一致的，如果没有直接返回
	if wrongCrcBlocks, err = CheckBlockCrc(dataReplicas, dnProf, ek.PartitionId, ek.ExtentId); err != nil {
		log.LogErrorf("quickCheckExtent failed, cluster:%s, partition:%v, extent:%v, err:%v", cluster, ek.PartitionId, ek.ExtentId, err)
		return
	}
	if len(wrongCrcBlocks) == 0 {
		log.LogInfof("action[quickCheckExtent] cluster:%s partition:%v, extent:%v, inode:%v, check same at block crc check", cluster, ek.PartitionId, ek.ExtentId, ino)
		return
	}
	//如果错误CRC块过多，直接查询整个extent,如果整个extent md5相同，直接返回即可
	if len(wrongCrcBlocks) > 100 {
		for i := 0; i < 5; i++ {
			if _, _, same, err = checkExtentMd5(dnProf, dataReplicas, ek); err != nil {
				log.LogErrorf("quickCheckExtent failed, cluster:%s, partition:%v, extent:%v, err:%v", cluster, ek.PartitionId, ek.ExtentId, err)
				return
			}
			if same {
				log.LogInfof("action[quickCheckExtent] cluster:%s partition:%v, extent:%v, inode:%v, check same at md5 check", cluster, ek.PartitionId, ek.ExtentId, ino)
				return
			}
			time.Sleep(time.Second)
		}
	}
	var wrongMd5Blocks []int
	//继续查询每个CRC块的Md5
	if wrongMd5Blocks, errEntryMap, err = retryCheckBlockMd5(dataReplicas, cluster, dnProf, ek, ino, 10, DefaultCheckBlockKB, wrongCrcBlocks); err != nil {
		return
	}
	if len(wrongMd5Blocks) == 0 {
		return
	}

	badExtent = true
	repairHost := make([]string, 0)
	repairHostMap := make(map[string]bool, 0)
	for _, entry := range errEntryMap {
		for _, h := range entry.BadHosts {
			repairHostMap[h] = true
		}
	}
	for h := range repairHostMap {
		repairHost = append(repairHost, h)
	}
	log.LogWarnf("cluster:%s found bad extent: pid(%v) eid(%v) eOff(%v) fOff(%v) size(%v) host(%v) ino(%v) vol(%v)",
		cluster, ek.PartitionId, ek.ExtentId, ek.ExtentOffset, ek.FileOffset, ek.Size, repairHost, ino, volume)
	badExtentInfo = BadExtentInfo{
		ExtentID:     ek.ExtentId,
		PartitionID:  ek.PartitionId,
		Hosts:        repairHost,
		Inode:        ino,
		Volume:       volume,
		ExtentOffset: ek.ExtentOffset,
		FileOffset:   ek.FileOffset,
		Size:         uint64(ek.Size),
	}
	return
}

func slowCheckExtent(cluster string, dnProf uint16, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, ino uint64, volume string) (badExtent bool, badExtentInfo BadExtentInfo, err error) {
	var badAddrs []string
	var output string
	var same bool
	for i := 0; i < 10; i++ {
		if badAddrs, output, same, err = checkExtentMd5(dnProf, dataReplicas, ek); err != nil {
			log.LogErrorf("slowCheckExtent failed, cluster:%v, partition:%v, extent:%v, err:%v\n", cluster, ek.PartitionId, ek.ExtentId, err)
			return
		}
		if same {
			log.LogInfof("action[slowCheckExtent], cluster:%v, partition:%v, extent:%v, inode:%v, check same at md5 check", cluster, ek.PartitionId, ek.ExtentId, ino)
			return
		}
		time.Sleep(time.Second)
	}
	if len(badAddrs) == 0 {
		return
	}
	badExtent = true
	log.LogWarnf("action[slowCheckExtent] found bad extent, output: \n%v", output)
	badExtentInfo = BadExtentInfo{
		ExtentID:     ek.ExtentId,
		PartitionID:  ek.PartitionId,
		Hosts:        badAddrs,
		Inode:        ino,
		Volume:       volume,
		ExtentOffset: ek.ExtentOffset,
		FileOffset:   ek.FileOffset,
		Size:         uint64(ek.Size),
	}
	return
}

func samplingCheckTinyExtent(cluster string, dnProf uint16, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, ino uint64, volume string) (badExtent bool, badExtentInfo BadExtentInfo, err error) {
	sample := uint32(4) * unit.KB
	sampleN := ek.Size / sample
	if sampleN > 128 {
		sampleN = 128
	}
	if ek.Size <= sample*2 {
		return
	}
	if ek.Size == sample*2 {
		//if size=0, the full extent will be checked
		newEk := &proto.ExtentKey{
			Size:         sample,
			ExtentOffset: 0,
			PartitionId:  ek.PartitionId,
			ExtentId:     ek.ExtentId,
		}
		badExtent, badExtentInfo, err = slowCheckExtent(cluster, dnProf, dataReplicas, newEk, ino, volume)
		return
	}
	var allHoles [][]uint64
	for _, replica := range dataReplicas {
		datanode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], dnProf)
		dataClient := http_client.NewDataClient(datanode, false)
		tinyExtentInfo, e := dataClient.GetExtentHoles(ek.PartitionId, ek.ExtentId)
		if e != nil {
			err = e
			return
		}
		for _, hole := range tinyExtentInfo.Holes {
			allHoles = append(allHoles, []uint64{hole.Offset, hole.Offset + hole.Size})
		}
	}
	if len(allHoles) > 0 {
		allHoles = merge(allHoles)
	}
	log.LogInfof("samplingCheckTinyExtent, cluster:%v, volume:%v, ino:%v, ek:%v, holesLen:%v", cluster, volume, ino, ek, len(allHoles))
	//ek is aligned to 4K, skip the last 4KB to avoid overflow
	maxOffset := int64(ek.Size - sample*2)
	retry := 200
	for i := 0; i < int(sampleN) && retry > 0; {
		rand.Seed(time.Now().UnixNano())
		offset := uint64(rand.Int63n(maxOffset))
		if isHole(offset, offset+uint64(sample), allHoles) {
			retry--
			continue
		}
		i++
		newEk := &proto.ExtentKey{
			Size:         sample,
			ExtentOffset: offset,
			PartitionId:  ek.PartitionId,
			ExtentId:     ek.ExtentId,
		}
		log.LogDebugf("samplingCheckTinyExtent, cluster:%v, volume:%v, ino:%v, ek:%v", cluster, volume, ino, newEk)
		badExtent, badExtentInfo, err = slowCheckExtent(cluster, dnProf, dataReplicas, newEk, ino, volume)
		if err != nil {
			return
		}
		if badExtent {
			return
		}
	}
	return
}

func merge(intervals [][]uint64) [][]uint64 {
	sort.Slice(intervals, func(i, j int) bool {
		v1, v2 := intervals[i], intervals[j]
		return v1[0] < v2[0] || v1[0] == v2[0] && v1[1] < v2[1]
	})
	stack := [][]uint64{intervals[0]}
	for i := 1; i < len(intervals); i++ {
		cnt1 := stack[len(stack)-1]
		cnt2 := intervals[i]
		if cnt1[0] <= cnt2[0] && cnt1[1] >= cnt2[0] {
			cnt1[1] = max(cnt1[1], cnt2[1])
			stack[len(stack)-1] = cnt1
		} else {
			stack = append(stack, cnt2)
		}
	}
	return stack
}
func max(i, j uint64) uint64 {
	if i > j {
		return i
	}
	return j
}

func isHole(start, end uint64, allHoles [][]uint64) bool {
	if start >= end {
		return false
	}
	for _, hole := range allHoles {
		if !(end <= hole[0] || start >= hole[1]) {
			return true
		}
	}
	return false
}

func retryCheckBlockMd5(dataReplicas []*proto.DataReplica, cluster string, dnProf uint16, ek *proto.ExtentKey, ino uint64, retry int, blockSize uint64, wrongBlocks []int) (newBlocks []int, blockError map[int]*ErrorEntry, err error) {
	for j := 0; j < retry; j++ {
		blockError = make(map[int]*ErrorEntry)
		newBlk := make([]int, 0)
		for _, b := range wrongBlocks {
			var errorEntry *ErrorEntry
			if errorEntry, err = checkBlockMd5(dataReplicas, cluster, dnProf, ek, uint32(b), ino, blockSize); err != nil {
				return
			}
			if errorEntry == nil {
				continue
			}
			blockError[b] = errorEntry
			log.LogWarnf("cluster:" + cluster + " " + errorEntry.ErrMessage)
			newBlk = append(newBlk, b)
		}
		if len(newBlk) == 0 {
			return nil, nil, nil
		}
		wrongBlocks = newBlk
		time.Sleep(time.Second * 1)
	}
	return wrongBlocks, blockError, nil
}

func CheckBlockCrc(dataReplicas []*proto.DataReplica, dnProf uint16, partitionId, extentId uint64) (wrongBlocks []int, err error) {
	var replicas = make([]struct {
		partitionId uint64
		extentId    uint64
		datanode    string
		blockCrc    []*proto.BlockCrc
	}, len(dataReplicas))
	var minBlockNum int
	var minBlockNumIdx int
	minBlockNum = math.MaxInt32
	wrongBlocks = make([]int, 0)
	for idx, replica := range dataReplicas {
		datanode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], dnProf)
		dataClient := http_client.NewDataClient(datanode, false)

		var extentBlocks []*proto.BlockCrc
		extentBlocks, err = dataClient.GetExtentBlockCrc(partitionId, extentId)
		if err != nil {
			return
		}
		replicas[idx].partitionId = partitionId
		replicas[idx].extentId = extentId
		replicas[idx].datanode = datanode
		replicas[idx].blockCrc = extentBlocks
		if minBlockNum > len(extentBlocks) {
			minBlockNum = len(extentBlocks)
			minBlockNumIdx = idx
		}
	}
	for blkIdx, blk := range replicas[minBlockNumIdx].blockCrc {
		for idx, rp := range replicas {
			if minBlockNumIdx == idx {
				continue
			}
			if blk.Crc == 0 || rp.blockCrc[blkIdx].Crc != blk.Crc {
				wrongBlocks = append(wrongBlocks, blk.BlockNo)
				break
			}
		}
	}
	return
}

func checkExtentMd5(dnProf uint16, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey) (badAddrs []string, output string, same bool, err error) {
	var (
		ok       bool
		replicas = make([]struct {
			partitionId uint64
			extentId    uint64
			datanode    string
			md5OrCrc    string
		}, len(dataReplicas))
		md5Map         = make(map[string]int)
		extentMd5orCrc string
	)
	badAddrs = make([]string, 0)
	for idx, replica := range dataReplicas {
		datanode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], dnProf)
		dataClient := http_client.NewDataClient(datanode, false)
		extentMd5orCrc, err = dataClient.ComputeExtentMd5(ek.PartitionId, ek.ExtentId, ek.ExtentOffset, uint64(ek.Size))
		if err != nil {
			log.LogErrorf("action[checkExtentMd5]: datanode(%v) PartitionId(%v) ExtentId(%v) err(%v)\n", datanode, ek.PartitionId, ek.ExtentId, err)
			return
		}
		replicas[idx].partitionId = ek.PartitionId
		replicas[idx].extentId = ek.ExtentId
		replicas[idx].datanode = datanode
		replicas[idx].md5OrCrc = extentMd5orCrc
		if _, ok = md5Map[extentMd5orCrc]; ok {
			md5Map[extentMd5orCrc]++
		} else {
			md5Map[extentMd5orCrc] = 1
		}
	}

	if len(md5Map) == 1 {
		return badAddrs, "", true, nil
	}
	for _, r := range replicas {
		addr := strings.Split(r.datanode, ":")[0] + ":6000"
		if _, ok = md5Map[r.md5OrCrc]; ok && md5Map[r.md5OrCrc] > len(dataReplicas)/2 {
			output += fmt.Sprintf("dp: %d, extent: %d, datanode: %s, MD5: %s\n", r.partitionId, r.extentId, addr, r.md5OrCrc)
		} else {
			output += fmt.Sprintf("dp: %d, extent: %d, datanode: %s, MD5: %s, Error Replica\n", r.partitionId, r.extentId, addr, r.md5OrCrc)
			badAddrs = append(badAddrs, addr)
		}
	}
	return
}

func checkBlockMd5(dataReplicas []*proto.DataReplica, cluster string, dnProf uint16, ek *proto.ExtentKey, blockOffset uint32, inode, blockSize uint64) (errorEntry *ErrorEntry, err error) {
	var (
		ok       bool
		replicas = make([]struct {
			partitionId  uint64
			extentId     uint64
			datanode     string
			md5          string
			extentOffset uint64
			size         uint64
		}, len(dataReplicas))
		md5Map    = make(map[string]int)
		extentMd5 string
	)
	size := uint32(blockSize * unit.KB)
	offset := blockOffset * uint32(blockSize*unit.KB)
	if size > ek.Size-offset {
		size = ek.Size - offset
	}
	for idx, replica := range dataReplicas {
		datanode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], dnProf)
		dataClient := http_client.NewDataClient(datanode, false)
		for j := 0; j == 0 || j < 3 && err != nil; j++ {
			extentMd5, err = dataClient.ComputeExtentMd5(ek.PartitionId, ek.ExtentId, uint64(offset), uint64(size))
		}
		if err != nil {
			log.LogErrorf("checkBlockMd5, cluster:%s, getExtentMd5 datanode(%v) PartitionId(%v) ExtentId(%v) err(%v)\n", cluster, datanode, ek.PartitionId, ek.ExtentId, err)
			return
		}
		replicas[idx].partitionId = ek.PartitionId
		replicas[idx].extentId = ek.ExtentId
		replicas[idx].extentOffset = ek.ExtentOffset
		replicas[idx].size = uint64(ek.Size)
		replicas[idx].datanode = datanode
		replicas[idx].md5 = extentMd5
		if _, ok = md5Map[replicas[idx].md5]; ok {
			md5Map[replicas[idx].md5]++
		} else {
			md5Map[replicas[idx].md5] = 1
		}
	}
	if len(md5Map) == 1 {
		return nil, nil
	}

	errorEntry = &ErrorEntry{
		BlockOffset: uint64(blockOffset),
		BlockSize:   blockSize,
		ErrMessage:  "",
		BadHosts:    make([]string, 0),
	}
	for _, r := range replicas {
		msg := fmt.Sprintf("dp: %d, extent: %d, datanode: %s, inode:%v, offset:%v, size:%v, md5: %s\n", r.partitionId, r.extentId, r.datanode, inode, offset, size, r.md5)
		if _, ok = md5Map[r.md5]; ok && md5Map[r.md5] > len(dataReplicas)/2 {
			errorEntry.ErrMessage += msg
		} else {
			errorEntry.ErrMessage += fmt.Sprintf("ERROR ExtentBlock %s", msg)
			errorEntry.BadHosts = append(errorEntry.BadHosts, strings.Split(r.datanode, ":")[0]+":6000")
		}
	}
	return
}
