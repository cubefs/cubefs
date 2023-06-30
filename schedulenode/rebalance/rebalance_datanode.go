package rebalance

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
)

// DNReBalanceController 用于控制每台机器(DataNode)上数据的迁移
type DNReBalanceController struct {
	proto.DataNodeInfo
	*master.MasterClient
	dataClient *data.DataHttpClient

	disks        map[string]*Disk
	migratedDp   map[uint64]*proto.DataPartitionInfo // 已执行迁移程序的dp
	migratedSize uint64                              // 迁移dp的总大小
	masterAddr   string

	minWritableDPNum     int // 最小可写dp限制
	clusterMaxBatchCount int
	migrateLimitPerDisk  int
}

func NewDNReBalanceController(info proto.DataNodeInfo, masterClient *master.MasterClient, masterAddr string,
	minWritableDPNum, clusterMaxBatchCount, migrateLimitPerDisk int) (*DNReBalanceController, error) {
	dataClient := getDataHttpClient(info.Addr, masterAddr)
	dnCtrl := &DNReBalanceController{
		DataNodeInfo:         info,
		MasterClient:         masterClient,
		dataClient:           dataClient,
		masterAddr:           masterAddr,
		minWritableDPNum:     minWritableDPNum,
		clusterMaxBatchCount: clusterMaxBatchCount,
		migrateLimitPerDisk:  migrateLimitPerDisk,
	}
	//err := dnCtrl.updateDataNode()
	//if err != nil {
	//	return nil, err
	//}
	err := dnCtrl.updateDisks()
	if err != nil {
		return nil, err
	}

	err = dnCtrl.updateDps()
	if err != nil {
		return nil, err
	}
	dnCtrl.migratedDp = make(map[uint64]*proto.DataPartitionInfo)
	return dnCtrl, nil
}

// NeedReBalance 判断当前DataNode是否需要迁移
func (dnCtrl *DNReBalanceController) NeedReBalance(goalRatio float64) bool {
	if dnCtrl.Usage() < goalRatio {
		return false
	}
	return true
}

func (dnCtrl *DNReBalanceController) SelectDP(goalRatio float64) (*Disk, *proto.DataPartitionInfo, error) {
	for _, disk := range dnCtrl.disks {
		if disk.total == 0 {
			continue
		}
		if disk.Usage() < goalRatio {
			continue
		}
		dp, err := disk.SelectDP()
		if err != nil {
			log.LogWarnf("disk select dp error dataNode: %v disk: %v %v", dnCtrl.Addr, disk.path, err.Error())
			continue
		}
		return disk, dp, nil
	}
	return nil, nil, ErrNoSuitableDP
}

func (dnCtrl *DNReBalanceController) UpdateMigratedDP(disk *Disk, dpInfo *proto.DataPartitionInfo) {
	disk.UpdateMigratedDPSize(dpInfo)
	dnCtrl.migratedDp[dpInfo.PartitionID] = dpInfo
	for _, replica := range dpInfo.Replicas {
		if replica.Addr == dnCtrl.Addr {
			dnCtrl.migratedSize += replica.Used
			break
		}
	}
}

// Usage 当前DataNode出去已迁移dp后的实际使用率
func (dnCtrl *DNReBalanceController) Usage() float64 {
	return float64(dnCtrl.Used-dnCtrl.migratedSize) / float64(dnCtrl.Total)
}

func (dnCtrl *DNReBalanceController) SetMigrateLimitPerDisk(limit int) {
	dnCtrl.migrateLimitPerDisk = limit
	for _, disk := range dnCtrl.disks {
		disk.SetMigrateLimit(limit)
	}
}

// 更新当前dataNode的信息
func (dnCtrl *DNReBalanceController) updateDataNode() error {
	node, err := dnCtrl.MasterClient.NodeAPI().GetDataNode(dnCtrl.Addr)
	if err != nil {
		return err
	}
	dnCtrl.DataNodeInfo = *node

	err = dnCtrl.updateDisks()
	if err != nil {
		return err
	}

	err = dnCtrl.updateDps()
	if err != nil {
		return err
	}
	return nil
}

// 更新dataNode的disk列表
func (dnCtrl *DNReBalanceController) updateDisks() error {
	dnCtrl.disks = make(map[string]*Disk)
	diskReport, err := dnCtrl.dataClient.GetDisks()
	if err != nil {
		return fmt.Errorf("get disks error zoneName: %v dataNode: %v", dnCtrl.ZoneName, dnCtrl.Addr)
	}
	for _, diskInfo := range diskReport.Disks {
		if diskInfo.Total == 0 {
			continue
		}
		disk := NewDiskReBalanceController(diskInfo, dnCtrl.masterAddr, dnCtrl.Addr, dnCtrl.minWritableDPNum, dnCtrl.migrateLimitPerDisk, dnCtrl.MasterClient)
		dnCtrl.disks[diskInfo.Path] = disk
	}
	return nil
}

// 更新dataNode的data Partition
func (dnCtrl *DNReBalanceController) updateDps() error {
	dnCtrl.migratedSize = 0

	persistenceDataPartitionsMap := make(map[uint64]struct{})
	for _, dpID := range dnCtrl.PersistenceDataPartitions {
		persistenceDataPartitionsMap[dpID] = struct{}{}
	}

	// 将dp绑定对应disk
	dpReportsMap := make(map[uint64]*proto.PartitionReport)
	for _, dpReport := range dnCtrl.DataPartitionReports {
		if _, ok := persistenceDataPartitionsMap[dpReport.PartitionID]; !ok {
			continue
		}
		if disk, ok := dnCtrl.disks[dpReport.DiskPath]; ok {
			disk.AddDP(dpReport)
		}
		dpReportsMap[dpReport.PartitionID] = dpReport
	}

	// 更新上轮次迁移的dp列表
	for id, dpInfo := range dnCtrl.migratedDp {
		var dpReport *proto.PartitionReport
		var disk *Disk
		var ok bool

		// 上轮次迁移节点已下线，删除节点
		if dpReport, ok = dpReportsMap[id]; !ok {
			delete(dnCtrl.migratedDp, id)
			continue
		}
		if disk, ok = dnCtrl.disks[dpReport.DiskPath]; !ok {
			delete(dnCtrl.migratedDp, id)
			continue
		}

		for _, replica := range dpInfo.Replicas {
			if replica.Addr == dnCtrl.Addr {
				dnCtrl.migratedSize += replica.Used
				break
			}
		}
		disk.UpdateMigratedDPSize(dpInfo)
	}
	return nil
}
