package cmd

import (
	"fmt"
	"os"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	atomic2 "go.uber.org/atomic"
)

type RepairPersist struct {
	MasterAddr              string
	failedDpFd              *os.File
	failedExtentsFd         *os.File
	singleBadNormalExtentFd *os.File
	multiBadNormalExtentsFd *os.File
	singleBadTinyExtentFd   *os.File
	multiBadTinyExtentsFd   *os.File
	lock                    sync.RWMutex
	dpCounter               atomic2.Int64
	rCh                     chan RepairExtentInfo
}

func (rp *RepairPersist) persistFailedDp(dp uint64) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	rp.failedDpFd.WriteString(fmt.Sprintf("%v\n", dp))
	rp.failedDpFd.Sync()
}

func (rp *RepairPersist) persistFailedExtents(dp uint64, failedExtents []uint64) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	for _, e := range failedExtents {
		rp.failedExtentsFd.WriteString(fmt.Sprintf("%v %v\n", dp, e))
	}
	rp.failedExtentsFd.Sync()
}

func (rp *RepairPersist) persistOneBadHostNormal(rExtent RepairExtentInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	msg := fmt.Sprintf("Domain[%s] found bad crc extent, it will be automatically repaired later: %v %v %v\n", rp.MasterAddr, rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts[0])
	exporter.WarningBySpecialUMPKey("check_crc_server", msg)
	rp.singleBadNormalExtentFd.WriteString(fmt.Sprintf("%v %v %v\n", rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts[0]))
	rp.singleBadNormalExtentFd.Sync()
}

func (rp *RepairPersist) persistOneBadHostTiny(rExtent RepairExtentInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	msg := fmt.Sprintf("Domain[%s] found bad crc extent, it will be automatically repaired later: %v %v %v\n", rp.MasterAddr, rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts[0])
	exporter.WarningBySpecialUMPKey("check_crc_server", msg)
	rp.singleBadTinyExtentFd.WriteString(fmt.Sprintf("%v %v %v %v %v %v %v\n", rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts[0], rExtent.Inode, rExtent.Offset, rExtent.Size, rExtent.Volume))
	rp.singleBadTinyExtentFd.Sync()
}

func (rp *RepairPersist) persistMultiBadHostsNormal(rExtent RepairExtentInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	msg := fmt.Sprintf("Domain[%s] found bad crc more than 1, please check again: %v %v %v\n", rp.MasterAddr, rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts)
	exporter.WarningBySpecialUMPKey("check_crc_server", msg)
	rp.multiBadNormalExtentsFd.WriteString(fmt.Sprintf("%v %v %v\n", rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts))
	rp.multiBadNormalExtentsFd.Sync()
}

func (rp *RepairPersist) persistMultiBadHostsTiny(rExtent RepairExtentInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	msg := fmt.Sprintf("Domain[%s] found bad crc more than 1, please check again: %v %v %v\n", rp.MasterAddr, rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts)
	exporter.WarningBySpecialUMPKey("check_crc_server", msg)
	rp.multiBadTinyExtentsFd.WriteString(fmt.Sprintf("%v %v %v %v %v %v %v\n", rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts, rExtent.Inode, rExtent.Offset, rExtent.Size, rExtent.Volume))
	rp.multiBadTinyExtentsFd.Sync()
}
func (rp *RepairPersist) persistResult() {
	for {
		select {
		case rExtent := <-rp.rCh:
			if rExtent.PartitionID == 0 && rExtent.ExtentID == 0 {
				return
			}
			if proto.IsTinyExtent(rExtent.ExtentID) {
				if len(rExtent.Hosts) == 1 {
					rp.persistOneBadHostTiny(rExtent)
				} else {
					rp.persistMultiBadHostsTiny(rExtent)
				}
			} else {
				if len(rExtent.Hosts) == 1 {
					rp.persistOneBadHostNormal(rExtent)
				} else {
					rp.persistMultiBadHostsNormal(rExtent)
				}
			}

		}
	}
}

func NewRepairPersist(master string) (rp *RepairPersist) {
	rp = new(RepairPersist)
	rp.MasterAddr = master
	rp.rCh = make(chan RepairExtentInfo, 1024)
	rp.failedDpFd, _ = os.OpenFile(fmt.Sprintf("checkFailedDp_%v.csv", master), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	rp.failedExtentsFd, _ = os.OpenFile(fmt.Sprintf("checkFailedExtents_%v.csv", master), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	rp.singleBadNormalExtentFd, _ = os.OpenFile(fmt.Sprintf("single_bad_normal_extents_%v", master), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	rp.multiBadNormalExtentsFd, _ = os.OpenFile(fmt.Sprintf("multi_bad_tiny_extents_%v", master), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	rp.singleBadTinyExtentFd, _ = os.OpenFile(fmt.Sprintf("single_bad_tiny_extents_%v", master), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	rp.multiBadTinyExtentsFd, _ = os.OpenFile(fmt.Sprintf("multi_bad_tiny_extents_%v", master), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	rp.dpCounter = atomic2.Int64{}
	return
}

func (rp *RepairPersist) close() {
	rp.rCh <- RepairExtentInfo{
		PartitionID: 0,
		ExtentID:    0,
	}
	rp.failedDpFd.Sync()
	rp.failedExtentsFd.Sync()
	rp.singleBadNormalExtentFd.Sync()
	rp.multiBadNormalExtentsFd.Sync()
	rp.singleBadTinyExtentFd.Sync()
	rp.multiBadTinyExtentsFd.Sync()
	rp.failedDpFd.Close()
	rp.failedExtentsFd.Close()
	rp.singleBadNormalExtentFd.Close()
	rp.multiBadNormalExtentsFd.Close()
	rp.singleBadTinyExtentFd.Close()
	rp.multiBadTinyExtentsFd.Close()
}
