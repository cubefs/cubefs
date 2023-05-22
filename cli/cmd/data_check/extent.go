package data_check

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"math"
	"strings"
	"time"
)

func CheckExtentReplicaInfo(cluster string, dnProf uint16, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, ino uint64, volume string, rCh chan RepairExtentInfo, checkTiny bool) (err error) {
	if checkTiny {
		if proto.IsTinyExtent(ek.ExtentId) {
			err = checkTinyExtentReplicaInfo(dnProf, dataReplicas, ek, ino, volume, rCh)
		}
		return
	}
	err = checkNormalExtentReplicaInfo(cluster, dnProf, dataReplicas, ek, ino, volume, rCh)
	return
}

func checkNormalExtentReplicaInfo(cluster string, dnProf uint16, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, ino uint64, volume string, rCh chan RepairExtentInfo) (err error) {
	blockSize := uint64(128)
	blocksLengh := ek.Size / uint32(blockSize*unit.KB)
	if ek.Size-blocksLengh*uint32(blockSize*unit.KB) > 0 {
		blocksLengh += 1
	}
	var output string
	var same bool
	var wrongBlocks []int
	if wrongBlocks, err = CheckExtentBlockCrc(dataReplicas, dnProf, ek.PartitionId, ek.ExtentId); err != nil {
		log.LogErrorf("checkNormalExtentReplicaInfo failed, cluster:%s, partition:%v, extent:%v, err:%v", cluster, ek.PartitionId, ek.ExtentId, err)
		return
	}
	if len(wrongBlocks) == 0 {
		log.LogInfof("action[checkNormalExtentReplicaInfo] cluster:%s partition:%v, extent:%v, inode:%v, check same at block crc check", cluster, ek.PartitionId, ek.ExtentId, ino)
		return
	}
	if len(wrongBlocks) > 100 {
		for i := 0; i < 5; i++ {
			if _, output, same, err = checkExtentReplica(dnProf, dataReplicas, ek, "md5"); err != nil {
				log.LogErrorf("checkNormalExtentReplicaInfo failed, cluster:%s, partition:%v, extent:%v, err:%v", cluster, ek.PartitionId, ek.ExtentId, err)
				return
			}
			if same {
				log.LogInfof("action[checkNormalExtentReplicaInfo] cluster:%s partition:%v, extent:%v, inode:%v, check same at md5 check", cluster, ek.PartitionId, ek.ExtentId, ino)
				return
			}
			time.Sleep(time.Second)
		}
	}
	if wrongBlocks, err = RetryCheckBlockMd5(dataReplicas, cluster, dnProf, ek, ino, 10, blockSize, wrongBlocks); err != nil {
		return
	}
	//print bad blocks
	if len(wrongBlocks) != 0 {
		addrMap := make(map[string]int, 0)
		for _, b := range wrongBlocks {
			var badAddrs []string
			badAddrs, output, same, err = CheckExtentReplicaByBlock(dataReplicas, cluster, dnProf, ek, uint32(b), ino, blockSize)
			if err != nil {
				return
			}
			if same {
				continue
			}
			for _, addr := range badAddrs {
				if _, ok := addrMap[addr]; !ok {
					addrMap[addr] = 0
				}
				addrMap[addr] += 1
			}
			log.LogWarnf("cluster:" + cluster + " " + output)
		}
		if len(addrMap) > 0 {
			repairHost := make([]string, 0)
			for k := range addrMap {
				repairHost = append(repairHost, k)
			}
			if rCh != nil {
				rCh <- RepairExtentInfo{
					ExtentID:    ek.ExtentId,
					PartitionID: ek.PartitionId,
					Hosts:       repairHost,
					Inode:       ino,
					Volume:      volume,
				}
			} else {
				if len(repairHost) == 1 {
					log.LogWarnf("cluster:%s autoRepairExtent: %v %v %v %v %v", cluster, ek.PartitionId, ek.ExtentId, repairHost[0], ino, volume)
				} else {
					log.LogWarnf("cluster:%s canNotAutoRepairExtent: %v %v %v %v %v", cluster, ek.PartitionId, ek.ExtentId, repairHost, ino, volume)
				}
			}
		}
	}
	return
}

func checkTinyExtentReplicaInfo(dnProf uint16, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, ino uint64, volume string, rCh chan RepairExtentInfo) (err error) {
	var badAddrs []string
	var output string
	var same bool
	for i := 0; i < 10; i++ {
		if badAddrs, output, same, err = checkExtentReplica(dnProf, dataReplicas, ek, "md5"); err != nil {
			log.LogErrorf("checkTinyExtentReplicaInfo failed, partition:%v, extent:%v, err:%v\n", ek.PartitionId, ek.ExtentId, err)
			return
		}
		if same {
			log.LogInfof("action[checkTinyExtentReplicaInfo] partition:%v, extent:%v, inode:%v, check same at md5 check", ek.PartitionId, ek.ExtentId, ino)
			return
		}
		time.Sleep(time.Second)
	}
	log.LogWarnf(output)
	if len(badAddrs) > 0 {
		repairHost := make([]string, 0)
		for _, k := range badAddrs {
			repairHost = append(repairHost, k)
		}
		if rCh != nil {
			rCh <- RepairExtentInfo{
				ExtentID:    ek.ExtentId,
				PartitionID: ek.PartitionId,
				Hosts:       repairHost,
				Inode:       ino,
				Volume:      volume,
			}
		} else {
			if len(repairHost) == 1 {
				log.LogWarnf("autoRepairExtent: %v %v %v %v %v\n", ek.PartitionId, ek.ExtentId, repairHost[0], ino, volume)
			} else {
				log.LogWarnf("canNotAutoRepairExtent: %v %v %v %v %v\n", ek.PartitionId, ek.ExtentId, repairHost, ino, volume)
			}
		}
	}

	return
}

func RetryCheckBlockMd5(dataReplicas []*proto.DataReplica, cluster string, dnProf uint16, ek *proto.ExtentKey, ino uint64, retry int, blockSize uint64, wrongBlocks []int) (resultBlocks []int, err error) {
	var (
		same bool
	)
	for j := 0; j < retry; j++ {
		newBlk := make([]int, 0)
		for _, b := range wrongBlocks {
			if _, _, same, err = CheckExtentReplicaByBlock(dataReplicas, cluster, dnProf, ek, uint32(b), ino, blockSize); err != nil {
				return
			} else if same {
				continue
			}
			newBlk = append(newBlk, b)
		}
		wrongBlocks = newBlk
		if len(wrongBlocks) == 0 {
			break
		}
		time.Sleep(time.Second * 1)
	}
	return wrongBlocks, nil
}

func CheckExtentBlockCrc(dataReplicas []*proto.DataReplica, dnProf uint16, partitionId, extentId uint64) (wrongBlocks []int, err error) {
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
		dataClient := data.NewDataHttpClient(datanode, false)

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

func checkExtentReplica(dnProf uint16, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, mod string) (badAddrs []string, output string, same bool, err error) {
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
		dataClient := data.NewDataHttpClient(datanode, false)
		switch mod {
		case "crc":
			var extentInfo *proto.ExtentInfoBlock
			//new version
			extentInfo, err = dataClient.GetExtentInfo(ek.PartitionId, ek.ExtentId)
			if err != nil {
				log.LogErrorf("GetExtentInfo datanode(%v) PartitionId(%v) ExtentId(%v) err(%v)\n", datanode, ek.PartitionId, ek.ExtentId, err)
				return
			}
			extentMd5orCrc = fmt.Sprintf("%v", extentInfo[proto.ExtentInfoCrc])
		case "md5":
			var (
				size   uint32
				offset uint64
			)
			if proto.IsTinyExtent(ek.ExtentId) {
				offset = ek.ExtentOffset
				size = ek.Size
			}
			extentMd5orCrc, err = dataClient.ComputeExtentMd5(ek.PartitionId, ek.ExtentId, offset, uint64(size))
			if err != nil {
				log.LogErrorf("getExtentMd5 datanode(%v) PartitionId(%v) ExtentId(%v) err(%v)\n", datanode, ek.PartitionId, ek.ExtentId, err)
				return
			}
		default:
			err = fmt.Errorf("wrong mod")
			log.LogErrorf(err.Error())
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
		msg := fmt.Sprintf("dp: %d, extent: %d, datanode: %s, %v: %s\n", r.partitionId, r.extentId, addr, mod, r.md5OrCrc)
		if _, ok = md5Map[r.md5OrCrc]; ok && md5Map[r.md5OrCrc] > len(dataReplicas)/2 {
			output += msg
		} else {
			output += fmt.Sprintf("ERROR Extent %s", msg)
			badAddrs = append(badAddrs, addr)
		}
	}
	return
}

func CheckExtentReplicaByBlock(dataReplicas []*proto.DataReplica, cluster string, dnProf uint16, ek *proto.ExtentKey, blockOffset uint32, inode, blockSize uint64) (badAddrs []string, output string, same bool, err error) {
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
	badAddrs = make([]string, 0)
	size := uint32(blockSize * unit.KB)
	offset := blockOffset * uint32(blockSize*unit.KB)
	if size > ek.Size-offset {
		size = ek.Size - offset
	}
	for idx, replica := range dataReplicas {
		datanode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], dnProf)
		dataClient := data.NewDataHttpClient(datanode, false)
		for j := 0; j == 0 || j < 3 && err != nil; j++ {
			extentMd5, err = dataClient.ComputeExtentMd5(ek.PartitionId, ek.ExtentId, uint64(offset), uint64(size))
		}
		if err != nil {
			log.LogErrorf("CheckExtentReplicaByBlock, cluster:%s, getExtentMd5 datanode(%v) PartitionId(%v) ExtentId(%v) err(%v)\n", cluster, datanode, ek.PartitionId, ek.ExtentId, err)
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
		return nil, "", true, nil
	}

	for _, r := range replicas {
		msg := fmt.Sprintf("dp: %d, extent: %d, datanode: %s, inode:%v, offset:%v, size:%v, md5: %s\n", r.partitionId, r.extentId, r.datanode, inode, offset, size, r.md5)
		if _, ok = md5Map[r.md5]; ok && md5Map[r.md5] > len(dataReplicas)/2 {
			output += msg
		} else {
			output += fmt.Sprintf("ERROR ExtentBlock %s", msg)
			badAddrs = append(badAddrs, strings.Split(r.datanode, ":")[0]+":6000")
		}
	}
	return
}
