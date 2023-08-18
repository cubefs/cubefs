package data_check

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
)

type RepairExtentInfo struct {
	PartitionID uint64
	ExtentID    uint64
	Offset      uint64
	Size        uint64
	Hosts       []string
	Inode       uint64
	Volume      string
}

type RepairPersist struct {
	MasterAddr              string
	checkFailedFd           *os.File
	singleBadNormalExtentFd *os.File
	multiBadNormalExtentsFd *os.File
	singleBadTinyExtentFd   *os.File
	multiBadTinyExtentsFd   *os.File
	lock                    sync.RWMutex
	RepairPersistCh         chan RepairExtentInfo
}

const (
	CheckCrcFailDp uint32 = iota
	CheckCrcFailMp
	CheckCrcFailVol
	CheckCrcFailExtent
	CheckCrcFailInode
)

var CheckFailKey = map[uint32]string{
	CheckCrcFailDp:     "dp",
	CheckCrcFailMp:     "mp",
	CheckCrcFailVol:    "vol",
	CheckCrcFailExtent: "extent",
	CheckCrcFailInode:  "inode",
}

func (rp *RepairPersist) persistFailed(pType uint32, info string) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	rp.checkFailedFd.WriteString(fmt.Sprintf("%s %v\n", CheckFailKey[pType], info))
	rp.checkFailedFd.Sync()
}

func (rp *RepairPersist) refreshFailedFD() {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	rp.checkFailedFd.Sync()
	rp.checkFailedFd.Close()
	os.Rename(fmt.Sprintf("checkFailed_%v.csv", strings.Split(rp.MasterAddr, ":")[0]), fmt.Sprintf("checkFailed_archive_%v.csv", strings.Split(rp.MasterAddr, ":")[0]))
	rp.checkFailedFd, _ = os.OpenFile(fmt.Sprintf("checkFailed_%v.csv", strings.Split(rp.MasterAddr, ":")[0]), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
}

func (rp *RepairPersist) loadFailedVols() (vols []string, err error) {
	r := bufio.NewReader(rp.checkFailedFd)
	vols = make([]string, 0)
	buf := make([]byte, 2048)
	vMp := make(map[string]bool, 0)
	for {
		buf, _, err = r.ReadLine()
		if err == io.EOF {
			err = nil
			break
		}
		vMp[strings.Split(string(buf), " ")[0]] = true
	}
	for v := range vMp {
		vols = append(vols, v)
	}
	return
}

func (rp *RepairPersist) persistOneBadHostNormal(rExtent RepairExtentInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	msg := fmt.Sprintf("Domain[%s] found bad crc extent, it will be automatically repaired later: %v %v %v %v\n", rp.MasterAddr, rExtent.Inode, rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts[0])
	exporter.WarningBySpecialUMPKey(UmpWarnKey, msg)
	rp.singleBadNormalExtentFd.WriteString(fmt.Sprintf("%v %v %v %v\n", rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts[0], time.Now()))
	rp.singleBadNormalExtentFd.Sync()
}

func (rp *RepairPersist) persistOneBadHostTiny(rExtent RepairExtentInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	msg := fmt.Sprintf("Domain[%s] found bad crc extent, it will be automatically repaired later: %v %v %v %v\n", rp.MasterAddr, rExtent.Inode, rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts[0])
	exporter.WarningBySpecialUMPKey(UmpWarnKey, msg)
	rp.singleBadTinyExtentFd.WriteString(fmt.Sprintf("%v %v %v %v %v %v %v\n", rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts[0], rExtent.Inode, rExtent.Offset, rExtent.Size, rExtent.Volume))
	rp.singleBadTinyExtentFd.Sync()
}

func (rp *RepairPersist) persistMultiBadHostsNormal(rExtent RepairExtentInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	msg := fmt.Sprintf("Domain[%s] found bad crc more than 1, please check again: %v %v %v\n", rp.MasterAddr, rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts)
	exporter.WarningBySpecialUMPKey(UmpWarnKey, msg)
	rp.multiBadNormalExtentsFd.WriteString(fmt.Sprintf("%v %v %v %v\n", rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts, time.Now()))
	rp.multiBadNormalExtentsFd.Sync()
}

func (rp *RepairPersist) persistMultiBadHostsTiny(rExtent RepairExtentInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	msg := fmt.Sprintf("Domain[%s] found bad crc more than 1, please check again: %v %v %v\n", rp.MasterAddr, rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts)
	exporter.WarningBySpecialUMPKey(UmpWarnKey, msg)
	rp.multiBadTinyExtentsFd.WriteString(fmt.Sprintf("%v %v %v %v %v %v %v\n", rExtent.PartitionID, rExtent.ExtentID, rExtent.Hosts, rExtent.Inode, rExtent.Offset, rExtent.Size, rExtent.Volume))
	rp.multiBadTinyExtentsFd.Sync()
}
func (rp *RepairPersist) PersistResult() {
	for {
		select {
		case rExtent := <-rp.RepairPersistCh:
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

func NewRepairPersist(dir, master string) (rp *RepairPersist) {
	rp = new(RepairPersist)
	rp.MasterAddr = master
	rp.RepairPersistCh = make(chan RepairExtentInfo, 1024)
	rp.checkFailedFd, _ = os.OpenFile(fmt.Sprintf("%v/checkFailed_%v.csv", dir, strings.Split(master, ":")[0]), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	rp.singleBadNormalExtentFd, _ = os.OpenFile(fmt.Sprintf("%v/single_bad_normal_extents_%v", dir, strings.Split(master, ":")[0]), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	rp.multiBadNormalExtentsFd, _ = os.OpenFile(fmt.Sprintf("%v/multi_bad_tiny_extents_%v", dir, strings.Split(master, ":")[0]), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	rp.singleBadTinyExtentFd, _ = os.OpenFile(fmt.Sprintf("%v/single_bad_tiny_extents_%v", dir, strings.Split(master, ":")[0]), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	rp.multiBadTinyExtentsFd, _ = os.OpenFile(fmt.Sprintf("%v/multi_bad_tiny_extents_%v", dir, strings.Split(master, ":")[0]), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	return
}

func (rp *RepairPersist) Close() {
	rp.RepairPersistCh <- RepairExtentInfo{
		PartitionID: 0,
		ExtentID:    0,
	}
	rp.checkFailedFd.Sync()
	rp.singleBadNormalExtentFd.Sync()
	rp.multiBadNormalExtentsFd.Sync()
	rp.singleBadTinyExtentFd.Sync()
	rp.multiBadTinyExtentsFd.Sync()
	rp.checkFailedFd.Close()
	rp.singleBadNormalExtentFd.Close()
	rp.multiBadNormalExtentsFd.Close()
	rp.singleBadTinyExtentFd.Close()
	rp.multiBadTinyExtentsFd.Close()
}
