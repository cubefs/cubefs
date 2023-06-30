package rebalance

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"gorm.io/gorm/utils"
	"sync"
	"time"
)

type Status int

// ZoneReBalanceController 用于控制每个Zone内数据的迁移
type ZoneReBalanceController struct {
	*master.MasterClient

	cluster   string
	zoneName  string
	srcNodes  map[string]*DNReBalanceController // 待迁移的源机器
	dstNodes  []*proto.DataNodeInfo             // 目标机器
	highRatio float64                           // 源机器的阈值
	lowRatio  float64                           // 目标机器的阈值
	goalRatio float64                           // 迁移需达到的阈值
	ctx       context.Context                   // context控制ReBalance程序终止
	cancel    context.CancelFunc
	status    Status
	mutex     sync.Mutex

	saveToDB func(info *MigrateRecordTable) error

	minWritableDPNum     int // 最小可写DP限制
	clusterMaxBatchCount int // 最大并发量
	dstIndex             int
	migrateLimitPerDisk  int // 每块磁盘每轮迁移dp数量上限，默认-1无上限
}

func NewZoneReBalanceController(cluster, zoneName string, highRatio, lowRatio, goalRatio float64, saveToDB func(info *MigrateRecordTable) error) (*ZoneReBalanceController, error) {
	if err := checkRatio(highRatio, lowRatio, goalRatio); err != nil {
		return nil, err
	}
	zoneCtrl := &ZoneReBalanceController{
		cluster:              cluster,
		zoneName:             zoneName,
		highRatio:            highRatio,
		lowRatio:             lowRatio,
		goalRatio:            goalRatio,
		status:               StatusStop,
		minWritableDPNum:     defaultMinWritableDPNum,
		clusterMaxBatchCount: defaultClusterMaxBatchCount,
		migrateLimitPerDisk:  -1,
		saveToDB:             saveToDB,
	}
	zoneCtrl.MasterClient = master.NewMasterClient([]string{zoneCtrl.cluster}, false)

	if err := zoneCtrl.updateDataNodes(); err != nil {
		return zoneCtrl, err
	}
	return zoneCtrl, nil
}

func (zoneCtrl *ZoneReBalanceController) UpdateRatio(highRatio, lowRatio, goalRatio float64) error {
	if err := checkRatio(highRatio, lowRatio, goalRatio); err != nil {
		return err
	}
	zoneCtrl.highRatio = highRatio
	zoneCtrl.lowRatio = lowRatio
	zoneCtrl.goalRatio = goalRatio
	return nil
}

// ReBalanceStart 开启ReBalance程序
func (zoneCtrl *ZoneReBalanceController) ReBalanceStart() error {
	zoneCtrl.mutex.Lock()
	defer zoneCtrl.mutex.Unlock()

	// 判断当前迁移状态
	if zoneCtrl.status != StatusStop {
		return fmt.Errorf("%v: expected Stop but found %v", ErrWrongStatus, getStatusStr(zoneCtrl.status))
	}

	// 更新源节点列表和目标节点列表
	err := zoneCtrl.updateDataNodes()
	if err != nil {
		return err
	}

	zoneCtrl.status = StatusRunning
	zoneCtrl.ctx, zoneCtrl.cancel = context.WithCancel(context.Background())

	log.LogInfof("start rebalance zone: %v", zoneCtrl.zoneName)
	go zoneCtrl.doReBalance()

	return nil
}

// ReBalanceStop 关闭ReBalance程序
func (zoneCtrl *ZoneReBalanceController) ReBalanceStop() error {
	zoneCtrl.mutex.Lock()
	defer zoneCtrl.mutex.Unlock()
	switch zoneCtrl.status {
	case StatusRunning:
		zoneCtrl.status = StatusTerminating
		zoneCtrl.cancel()
	default:
		return fmt.Errorf("status error while stop rebalance in %v status code: %v", zoneCtrl.zoneName, zoneCtrl.status)
	}
	return nil
}

// Status 返回ReBalance状态
func (zoneCtrl *ZoneReBalanceController) Status() Status {
	zoneCtrl.mutex.Lock()
	defer zoneCtrl.mutex.Unlock()
	return zoneCtrl.status
}

// SetMinWritableDPNum 设置最小可写DP限制
func (zoneCtrl *ZoneReBalanceController) SetMinWritableDPNum(minWritableDPNum int) {
	if minWritableDPNum <= 0 {
		return
	}
	zoneCtrl.minWritableDPNum = minWritableDPNum
}

// SetClusterMaxBatchCount 设置最大并发量
func (zoneCtrl *ZoneReBalanceController) SetClusterMaxBatchCount(clusterMaxBatchCount int) {
	zoneCtrl.clusterMaxBatchCount = clusterMaxBatchCount
}

func (zoneCtrl *ZoneReBalanceController) SetMigrateLimitPerDisk(limit int) {
	zoneCtrl.migrateLimitPerDisk = limit
	for _, node := range zoneCtrl.srcNodes {
		node.SetMigrateLimitPerDisk(limit)
	}
}

// 读取当前zone中的dataNode列表，根据使用率划分为srcNode和dstNode
// srcNode: 使用率高的源机器
// dstNode: 使用率低可作为迁移目标的目标机器
func (zoneCtrl *ZoneReBalanceController) updateDataNodes() error {
	zoneCtrl.srcNodes = make(map[string]*DNReBalanceController)
	zoneCtrl.dstNodes = make([]*proto.DataNodeInfo, 0)
	dataNodes := make(map[string]*proto.DataNodeInfo)
	lock := sync.Mutex{}

	dataNodeAddrs, err := getZoneDataNodesByClient(zoneCtrl.MasterClient, zoneCtrl.zoneName)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(len(dataNodeAddrs))

	for _, addr := range dataNodeAddrs {
		go func(addr string) {
			node, err := zoneCtrl.MasterClient.NodeAPI().GetDataNode(addr)
			if err != nil {
				log.LogErrorf("updateDataNodes error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, node.Addr, err.Error())
				return
			}
			lock.Lock()
			dataNodes[addr] = node
			lock.Unlock()
			wg.Done()
		}(addr)
	}

	wg.Wait()

	zoneCtrl.updateDstNodes(dataNodes)
	err = zoneCtrl.updateSrcNodes(dataNodes)
	if err != nil {
		return err
	}
	return nil
}

// 更新源节点
func (zoneCtrl *ZoneReBalanceController) updateSrcNodes(dataNodes map[string]*proto.DataNodeInfo) error {
	for addr, dataNode := range dataNodes {
		if dataNode.UsageRatio < zoneCtrl.highRatio {
			continue
		}
		srcNode, err := NewDNReBalanceController(*dataNode, zoneCtrl.MasterClient, zoneCtrl.cluster, zoneCtrl.minWritableDPNum, zoneCtrl.clusterMaxBatchCount, zoneCtrl.migrateLimitPerDisk)
		if err != nil {
			log.LogErrorf("newDataNode error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, addr, err.Error())
			continue
		}
		zoneCtrl.srcNodes[addr] = srcNode
	}
	return nil
}

func (zoneCtrl *ZoneReBalanceController) updateDstNodes(dataNodes map[string]*proto.DataNodeInfo) {
	for _, dataNode := range dataNodes {
		if dataNode.UsageRatio < zoneCtrl.lowRatio {
			zoneCtrl.dstNodes = append(zoneCtrl.dstNodes, dataNode)
		}
	}
	return
}

// 1. 从当前index开始往后遍历， 找到第一个符合条件的dstNode
// 2. 将index+1，避免重复选择一个dstNode
func (zoneCtrl *ZoneReBalanceController) selectDstNode(dpInfo *proto.DataPartitionInfo) (*proto.DataNodeInfo, error) {
	var node *proto.DataNodeInfo
	offset := 0
	for offset < len(zoneCtrl.dstNodes) {
		index := (zoneCtrl.dstIndex + offset) % len(zoneCtrl.dstNodes)
		node = zoneCtrl.dstNodes[index]
		if !utils.Contains(dpInfo.Hosts, node.Addr) {
			break
		}
		offset++
	}
	if offset >= len(zoneCtrl.dstNodes) {
		return nil, ErrNoSuitableDstNode
	}
	zoneCtrl.dstIndex++
	zoneCtrl.dstIndex = zoneCtrl.dstIndex % len(zoneCtrl.dstNodes)
	return node, nil
}

func (zoneCtrl *ZoneReBalanceController) isInRecoveringDPsMoreThanMaxBatchCount(maxBatchCount int) (inRecoveringDPMap map[uint64]int, err error) {
	inRecoveringDPMap, err = zoneCtrl.getInRecoveringDPMapIgnoreMig()
	if err != nil {
		return
	}
	if len(inRecoveringDPMap) > maxBatchCount {
		err = fmt.Errorf("inRecoveringDPCount:%v more than maxBatchCount:%v", len(inRecoveringDPMap), maxBatchCount)
	}
	return
}

func (zoneCtrl *ZoneReBalanceController) getInRecoveringDPMapIgnoreMig() (inRecoveringDPMap map[uint64]int, err error) {
	clusterView, err := zoneCtrl.AdminAPI().GetCluster()
	if err != nil {
		return
	}
	inRecoveringDPMap = make(map[uint64]int)
	for _, badPartitionViews := range clusterView.BadPartitionIDs {
		inRecoveringDPMap[badPartitionViews.PartitionID]++
	}
	delete(inRecoveringDPMap, 0)
	return
}

// 1. 判断迁移过程是否终止
// 2. 判断是否超过最大并发量
// 3. 选择一个可迁移的DP
// 4. 选择一个用于迁入的DataNode
// 5. 执行迁移
func (zoneCtrl *ZoneReBalanceController) doReBalance() {
	defer func() {
		zoneCtrl.mutex.Lock()
		zoneCtrl.status = StatusStop
		zoneCtrl.mutex.Unlock()
	}()

	for _, srcNode := range zoneCtrl.srcNodes {
		err := srcNode.updateDataNode()
		if err != nil {
			log.LogErrorf("update dataNode error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, srcNode.Addr, err.Error())
		}
		for srcNode.NeedReBalance(zoneCtrl.goalRatio) {
			select {
			case <-zoneCtrl.ctx.Done():
				return
			default:
			}

			// 检查最大并发量限制
			_, err := zoneCtrl.isInRecoveringDPsMoreThanMaxBatchCount(zoneCtrl.clusterMaxBatchCount)
			if err != nil {
				log.LogWarnf("ScheduleToMigHighRatioDiskDataPartition err:%v", err.Error())
				time.Sleep(defaultInterval)
				continue
			}

			// 选择srcNode中的一个可用dp
			disk, dpInfo, err := srcNode.SelectDP(zoneCtrl.goalRatio)
			if err != nil {
				// 当前srcNode已经没有可选的dp，break
				log.LogWarnf("dataNode select dp error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, srcNode.Addr, err.Error())
				break
			}

			// 根据选择的dp指定一个目标机器
			dstNode, err := zoneCtrl.selectDstNode(dpInfo)
			if err != nil {
				// 根据当前选定的dp找不到合适的dstNode，继续下一轮循环，选择新的dp
				log.LogWarnf("select dstNode error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, srcNode.Addr, err.Error())
				continue
			}

			// 执行迁移
			err = zoneCtrl.doMigrate(disk, dpInfo, srcNode, dstNode)
			if err != nil {
				log.LogErrorf("migrate dp %v error Zone: %v from %v to %v %v", dpInfo.PartitionID, zoneCtrl.zoneName, srcNode.Addr, dstNode.Addr, err.Error())
				continue
			}
		}
	}
}

// 执行迁移
func (zoneCtrl *ZoneReBalanceController) doMigrate(disk *Disk, dpInfo *proto.DataPartitionInfo, srcNode *DNReBalanceController, dstNode *proto.DataNodeInfo) error {
	err := zoneCtrl.AdminAPI().DecommissionDataPartition(dpInfo.PartitionID, srcNode.Addr, dstNode.Addr)
	if err != nil {
		return err
	}
	oldUsage, oldDiskUsage := srcNode.Usage(), disk.Usage()
	srcNode.UpdateMigratedDP(disk, dpInfo)
	newUsage, newDiskUsage := srcNode.Usage(), disk.Usage()
	err = zoneCtrl.saveToDB(&MigrateRecordTable{
		ClusterName:  zoneCtrl.cluster,
		ZoneName:     zoneCtrl.zoneName,
		VolName:      dpInfo.VolName,
		PartitionID:  dpInfo.PartitionID,
		SrcAddr:      srcNode.Addr,
		SrcDisk:      disk.path,
		DstAddr:      dstNode.Addr,
		OldUsage:     oldUsage,
		OldDiskUsage: oldDiskUsage,
		NewUsage:     newUsage,
		NewDiskUsage: newDiskUsage,
	})
	if err != nil {
		return err
	}
	return nil
}
