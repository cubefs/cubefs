package data_check

import (
	"bufio"
	"fmt"
	util_sdk "github.com/cubefs/cubefs/cli/cmd/util/sdk"
	"github.com/cubefs/cubefs/sdk/master"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
)

type BadExtentPersist struct {
	MasterAddr       string
	path             string
	autoFix          bool
	mc               *master.MasterClient
	checkFailedFd    *os.File
	fixedBadExtentFd *os.File
	badExtentFd      *os.File
	lock             sync.RWMutex
	BadExtentCh      chan BadExtentInfo
}

const (
	CheckFailDp uint32 = iota
	CheckFailMp
	CheckFailVol
	CheckFailExtent
	CheckFailInode
)

var CheckFailKey = map[uint32]string{
	CheckFailDp:     "dp",
	CheckFailMp:     "mp",
	CheckFailVol:    "vol",
	CheckFailExtent: "extent",
	CheckFailInode:  "inode",
}

func (rp *BadExtentPersist) persistFailed(pType uint32, info string) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	rp.checkFailedFd.WriteString(fmt.Sprintf("%s %v\n", CheckFailKey[pType], info))
	rp.checkFailedFd.Sync()
}

func (rp *BadExtentPersist) refreshFailedFD() {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	rp.checkFailedFd.Sync()
	rp.checkFailedFd.Close()
	os.Rename(fmt.Sprintf("%v/.checkFailed_%v.csv", rp.path, strings.Split(rp.MasterAddr, ":")[0]), fmt.Sprintf("%v/.checkFailed_archieve_%v.csv", rp.path, strings.Split(rp.MasterAddr, ":")[0]))
	rp.checkFailedFd, _ = os.OpenFile(fmt.Sprintf("%v/.checkFailed_%v.csv", rp.path, strings.Split(rp.MasterAddr, ":")[0]), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
}

func (rp *BadExtentPersist) loadFailedVols() (vols []string, err error) {
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

func (rp *BadExtentPersist) persistBadExtent(e BadExtentInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	msg := fmt.Sprintf("pid(%v) eid(%v) tiny(%v) badhostLen(%v) badhost(%v) vol(%v) ino(%v) eOff(%v) fOff(%v) size(%v) time(%v)",
		e.PartitionID, e.ExtentID, proto.IsTinyExtent(e.ExtentID), len(e.Hosts), e.Hosts, e.Volume, e.Inode, e.ExtentOffset,
		e.FileOffset, e.Size, time.Now().Format("2006-01-02 15:04:05"))
	exporter.WarningBySpecialUMPKey(UmpWarnKey, fmt.Sprintf("Domain[%s] found bad crc extent: %v", rp.MasterAddr, msg))
	rp.badExtentFd.WriteString(msg + "\n")
	rp.badExtentFd.Sync()
}

func (rp *BadExtentPersist) persistFixedBadExtent(e BadExtentInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	msg := fmt.Sprintf("pid(%v) eid(%v) tiny(%v) badhostLen(%v) badhost(%v) vol(%v) ino(%v) eOff(%v) fOff(%v) size(%v) time(%v)",
		e.PartitionID, e.ExtentID, proto.IsTinyExtent(e.ExtentID), len(e.Hosts), e.Hosts, e.Volume, e.Inode, e.ExtentOffset,
		e.FileOffset, e.Size, time.Now().Format("2006-01-02 15:04:05"))
	exporter.WarningBySpecialUMPKey(UmpWarnKey, fmt.Sprintf("Domain[%s] found bad crc extent: %v", rp.MasterAddr, msg))
	rp.fixedBadExtentFd.WriteString(msg + "\n")
	rp.fixedBadExtentFd.Sync()
}

func (rp *BadExtentPersist) PersistResult() {
	for {
		select {
		case rExtent := <-rp.BadExtentCh:
			if rExtent.PartitionID == 0 && rExtent.ExtentID == 0 {
				return
			}
			if rp.autoFix && !proto.IsTinyExtent(rExtent.ExtentID) && len(rExtent.Hosts) == 1 {
				err := util_sdk.RepairExtents(rp.mc, rExtent.Hosts[0], rExtent.PartitionID, []uint64{rExtent.ExtentID})
				if err != nil {
					rp.persistBadExtent(rExtent)
				} else {
					rp.persistFixedBadExtent(rExtent)
				}
				time.Sleep(time.Minute)
			} else {
				rp.persistBadExtent(rExtent)
			}
		}
	}
}

func NewRepairPersist(dir, master string, mc *master.MasterClient, autoFix bool) (rp *BadExtentPersist, err error) {
	rp = new(BadExtentPersist)
	rp.MasterAddr = master
	rp.mc = mc
	rp.autoFix = autoFix
	rp.BadExtentCh = make(chan BadExtentInfo, 1024)
	rp.path = dir
	rp.checkFailedFd, err = os.OpenFile(fmt.Sprintf("%v/.checkFailed_%v.csv", rp.path, strings.Split(master, ":")[0]), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return
	}
	rp.fixedBadExtentFd, err = os.OpenFile(fmt.Sprintf("%v/fixed_bad_extents_%v", rp.path, strings.Split(master, ":")[0]), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return
	}
	rp.badExtentFd, err = os.OpenFile(fmt.Sprintf("%v/bad_extents_%v_%v", rp.path, strings.Split(master, ":")[0], time.Now().Format("20060102")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	return
}

func (rp *BadExtentPersist) Close() {
	rp.BadExtentCh <- BadExtentInfo{
		PartitionID: 0,
		ExtentID:    0,
	}
	if rp.checkFailedFd != nil {
		rp.checkFailedFd.Sync()
		rp.checkFailedFd.Close()
	}
	if rp.badExtentFd != nil {
		rp.badExtentFd.Sync()
		rp.badExtentFd.Close()
	}
	if rp.fixedBadExtentFd != nil {
		rp.fixedBadExtentFd.Sync()
		rp.fixedBadExtentFd.Close()
	}
}
