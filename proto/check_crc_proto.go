package proto

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

type RepairType uint8

type CheckTaskInfo struct {
	Concurrency         uint32   `json:"limit_level"`
	Filter              Filter   `json:"filter"`
	ExtentModifyTimeMin string   `json:"extent_modify_time_min"`
	ExtentModifyTimeMax string   `json:"extent_modify_time_max"`
	InodeModifyTimeMin  string   `json:"inode_modify_time_min"`
	InodeModifyTimeMax  string   `json:"inode_modify_time_max"`
	CheckMod            CheckMod `json:"check_mod"`
	CheckTiny           bool     `json:"check_tiny"`
	QuickCheck          bool     `json:"quick_check"`
}

type CheckMod int

const (
	NodeExtent  CheckMod = iota
	VolumeInode          = 1
)

func (t CheckTaskInfo) String() string {
	return fmt.Sprintf("dimension(%v) quick(%v) checkTiny(%v) extentMinT(%v) extentMaxT(%v) inodeMinT(%v) inodeMaxT(%v) "+
		"volFilter(%v) volExclude(%v) zoneFilter(%v) zoneExclude(%v) nodeFilter(%v) nodeExcludeFilter(%v)",
		t.CheckMod, t.QuickCheck, t.CheckTiny, t.ExtentModifyTimeMin, t.ExtentModifyTimeMax, t.InodeModifyTimeMin, t.InodeModifyTimeMax,
		t.Filter.VolFilter, t.Filter.VolExcludeFilter, t.Filter.ZoneFilter, t.Filter.ZoneExcludeFilter, t.Filter.NodeFilter, t.Filter.NodeExcludeFilter)
}

type Filter struct {
	VolFilter         []string `json:"vol_filter"`
	VolExcludeFilter  []string `json:"vol_exclude_filter"`
	ZoneFilter        []string `json:"zone_filter"`
	ZoneExcludeFilter []string `json:"zone_exclude_filter"`
	NodeFilter        []string `json:"node_filter"`
	NodeExcludeFilter []string `json:"node_exclude_filter"`
	InodeFilter       []uint64 `json:"inode_filter"`
	DpFilter          []uint64 `json:"dp_filter"`
}

type Frequency struct {
	Interval     uint32 `json:"interval"`
	ExecuteCount uint32 `json:"execute_count"`
}

func (t CheckTaskInfo) IsValid() (err error) {
	ipReg := regexp.MustCompile(`^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)+([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$`)
	if t.CheckMod != VolumeInode && t.CheckMod != NodeExtent {
		err = fmt.Errorf("check dimension illegal, specify 0-inode or 1-extent")
		return
	}
	if t.ExtentModifyTimeMin != "" {
		if _, err = time.Parse("2006-01-02 15:04:05", t.ExtentModifyTimeMin); err != nil {
			err = fmt.Errorf("ExtentModifyTimeMin illegal, err:%v", err)
			return
		}
	}
	if t.ExtentModifyTimeMax != "" {
		if _, err = time.Parse("2006-01-02 15:04:05", t.ExtentModifyTimeMax); err != nil {
			err = fmt.Errorf("ExtentModifyTimeMax illegal, err:%v", err)
			return
		}
	}
	if t.InodeModifyTimeMin != "" {
		if _, err = time.Parse("2006-01-02 15:04:05", t.InodeModifyTimeMin); err != nil {
			err = fmt.Errorf("InodeModifyTimeMin illegal, err:%v", err)
			return
		}
	}
	if t.InodeModifyTimeMax != "" {
		if _, err = time.Parse("2006-01-02 15:04:05", t.InodeModifyTimeMax); err != nil {
			err = fmt.Errorf("InodeModifyTimeMax illegal, err:%v", err)
			return
		}
	}
	if t.CheckMod == NodeExtent && len(t.Filter.NodeFilter) == 0 {
		err = fmt.Errorf("nodeAddress can not be empty when repair datanode")
		return
	}
	for _, n := range t.Filter.NodeFilter {
		if !ipReg.MatchString(strings.Split(n, ":")[0]) {
			err = fmt.Errorf("nodeAddress illegal")
			return
		}
	}
	return
}

func IncludeUint64(target uint64, filter []uint64) bool {
	for _, item := range filter {
		if item == target {
			return true
		}
	}
	return false
}

func IncludeString(target string, filter []string) bool {
	for _, item := range filter {
		if item == target {
			return true
		}
	}
	return false
}
