package normalextentcheck

import (
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"strings"
)

var (
	mistakeDelEKsInfoFormatPattern = "    %-15v    %-15v    %-15v    %-15v    %-15v    %-15v"
	mistakeDelEKsInfoFormatHeader  = fmt.Sprintf(mistakeDelEKsInfoFormatPattern,
		"InodeID", "FileOffset", "DPID", "ExtentID", "ExtentOffset", "Size")
	extentsSearchFailedFormatPattern = "    %-15v    %-15v"
	extentsSearchFailedFormatHeader = fmt.Sprintf(extentsSearchFailedFormatPattern, "DPID", "ExtentID")
)

func (t *NormalExtentCheckTask) formatMistakeDelEKSearchEmailContent() (emailContent string) {
	emailContent = fmt.Sprintf("cluster:%s volume:%s 误删除Extent检查结果:</br>", t.Cluster, t.VolName)
	emailContent += "误删除Extents:</br>"
	for dpID, blockInfo := range t.mistakeDelEK {
		var extentsStr = make([]string, 0, blockInfo.Cap())
		maxNum := blockInfo.MaxNum()
		for index := 0; index <= maxNum; index++ {
			if blockInfo.Get(index) {
				extentsStr = append(extentsStr, fmt.Sprintf("%v", index))
			}
		}
		if len(extentsStr) == 0 {
			log.LogInfof("cluster(%s) volume(%s) dp id:%v not exist extents deleted by mistake", t.Cluster, t.VolName, dpID)
			continue
		}
		emailContent += fmt.Sprintf("dpID:%v, extentsID[%s]</br>", dpID, strings.Join(extentsStr, ","))
	}
	emailContent += "</br>"

	emailContent += "Inode 查找结果:</br>"
	emailContent += mistakeDelEKsInfoFormatHeader
	emailContent += "</br>"
	for _, r := range t.searchResult {
		emailContent += fmt.Sprintf(mistakeDelEKsInfoFormatPattern,
			r.Inode, r.EK.FileOffset, r.EK.PartitionId, r.EK.ExtentId, r.EK.ExtentOffset, r.EK.Size)
		emailContent += "</br>"
	}
	emailContent += "</br>"

	emailContent += "Inode 查找失败结果:</br>"
	emailContent += extentsSearchFailedFormatHeader
	emailContent += "</br>"
	for _, extentInfo := range t.searchFailed {
		emailContent += fmt.Sprintf(extentsSearchFailedFormatPattern, extentInfo.DataPartitionID, extentInfo.ExtentID)
		emailContent += "</br>"
	}
	return
}

func (t *NormalExtentCheckTask) formatExtentConflictEmailContent() (emailContent string) {
	extentConflictInodesInfo := make([]string, 0, len(t.extentConflict))
	emailContent = fmt.Sprintf("cluster:%s volume:%s 冲突Extent查找结果:</br>", t.Cluster, t.VolName)
	for extentInfo, inodes := range t.extentConflict {
		if len(inodes) <= 1 {
			continue
		}
		msg := fmt.Sprintf("EK: %v_%v, Owner Inodes: %v", extentInfo.DataPartitionID, extentInfo.ExtentID, inodes)
		emailContent += msg
		emailContent += "</br>"
		extentConflictInodesInfo = append(extentConflictInodesInfo, emailContent)
	}
	log.LogInfof("formatExtentConflictInfo %s %v Extent Conflict Info: %v", t.Cluster, t.VolName, extentConflictInodesInfo)
	return
}