// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cfmt

import (
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/fatih/color"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
)

var chunkStatus2Str = map[blobnode.ChunkStatus]string{
	blobnode.ChunkStatusNormal:   "normal",
	blobnode.ChunkStatusReadOnly: "readonly",
	blobnode.ChunkStatusRelease:  "release",
}

// ChunkidF chunk id
func ChunkidF(id blobnode.ChunkId) string {
	t := id.UnixTime()
	c := color.New(color.Faint, color.Italic)
	return fmt.Sprintf("%s (V:%20d T:%20d %s)", id.String(), id.VolumeUnitId(),
		t, c.Sprint(time.Unix(0, int64(t)).Format("2006-01-02 15:04:05.000")),
	)
}

// ChunkInfoJoin chunk info
func ChunkInfoJoin(info *blobnode.ChunkInfo, prefix string) string {
	return joinWithPrefix(prefix, ChunkInfoF(info))
}

// ChunkInfoF chunk info
func ChunkInfoF(info *blobnode.ChunkInfo) []string {
	if info == nil {
		return []string{" <nil> "}
	}
	return []string{
		fmt.Sprintf("ID     : %s", ChunkidF(info.Id)),
		fmt.Sprintf("Vuid   : %s", VuidCF(info.Vuid)),
		fmt.Sprintf("DiskID : %-10d", info.DiskID),
		fmt.Sprintf("Status : %d           (%s)", info.Status, chunkStatus2Str[info.Status]),
		fmt.Sprintf("Compact: %v", info.Compacting),
		fmt.Sprintf("Size   : %-24s", humanIBytes(info.Size)),
		fmt.Sprintf("Total  : %-24s Used: %-24s Free: %-24s",
			humanIBytes(info.Total), humanIBytes(info.Used), humanIBytes(info.Free)),
	}
}

// DiskHeartBeatInfoJoin disk heartbeat info
func DiskHeartBeatInfoJoin(info *blobnode.DiskHeartBeatInfo, prefix string) string {
	return joinWithPrefix(prefix, DiskHeartBeatInfoF(info))
}

// DiskHeartBeatInfoF disk heartbeat info
func DiskHeartBeatInfoF(info *blobnode.DiskHeartBeatInfo) []string {
	if info == nil {
		return []string{" <nil> "}
	}
	return []string{
		fmt.Sprintf("DiskID: %-12d | MaxN: %-8d | UsedN: %-8d | FreeN: %-8d",
			info.DiskID, info.MaxChunkCnt, info.UsedChunkCnt, info.FreeChunkCnt),
		fmt.Sprintf("Size  : %-24s | Used: %-24s | Free: %-24s",
			humanIBytes(info.Size), humanIBytes(info.Used), humanIBytes(info.Free)),
	}
}

// DiskInfoJoin disk info
func DiskInfoJoin(info *blobnode.DiskInfo, prefix string) string {
	return joinWithPrefix(prefix, DiskInfoF(info))
}

// DiskInfoF disk info
func DiskInfoF(info *blobnode.DiskInfo) []string {
	if info == nil {
		return []string{" <nil> "}
	}
	return append(DiskHeartBeatInfoF(&info.DiskHeartBeatInfo),
		[]string{
			fmt.Sprintf("ClusterID : %-4d | Readonly: %-6v | IDC: %-12s | Rack: %s",
				info.ClusterID, info.Readonly, info.Idc, info.Rack),
			fmt.Sprintf("Status  : %-10s(%d) | Host: %-30s | Path: %s",
				info.Status, info.Status, info.Host, info.Path),
			fmt.Sprintf("CreateAt: %s (%s) | LastUpdateAt: %s (%s)",
				info.CreateAt.Format(time.RFC822), humanize.Time(info.CreateAt),
				info.LastUpdateAt.Format(time.RFC822), humanize.Time(info.LastUpdateAt)),
		}...)
}

// DiskInfoJoinV disk info verbose
func DiskInfoJoinV(info *blobnode.DiskInfo, prefix string) string {
	return joinWithPrefix(prefix, DiskInfoFV(info))
}

// DiskInfoFV disk info
func DiskInfoFV(info *blobnode.DiskInfo) []string {
	if info == nil {
		return []string{" <nil> "}
	}
	return []string{
		fmt.Sprint("DiskID   : ", info.DiskID),
		fmt.Sprint("Readonly : ", info.Readonly),
		fmt.Sprintf("Status   : %s(%d) ", info.Status, info.Status),
		fmt.Sprintf("Chunk    : MaxN: %-18d | UsedN: %-23d | FreeN: %-24d",
			info.MaxChunkCnt, info.UsedChunkCnt, info.FreeChunkCnt),
		fmt.Sprintf("Size     : %-24s | Used: %-24s | Free: %-24s",
			humanIBytes(info.Size), humanIBytes(info.Used), humanIBytes(info.Free)),
		fmt.Sprint("ClusterID: ", info.ClusterID),
		fmt.Sprint("IDC      : ", info.Idc),
		fmt.Sprint("Rack     : ", info.Rack),
		fmt.Sprint("Host     : ", info.Host),
		fmt.Sprint("Path     : ", info.Path),
		fmt.Sprintf("CreateAt : %s (%s)",
			info.CreateAt.Format(time.RFC822), humanize.Time(info.CreateAt)),
		fmt.Sprintf("UpdateAt : %s (%s)",
			info.LastUpdateAt.Format(time.RFC822), humanize.Time(info.LastUpdateAt)),
	}
}
