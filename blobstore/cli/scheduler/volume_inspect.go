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

package scheduler

import (
	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func addCmdVolumeInspectCheckpointTask(cmd *grumble.Command) {
	inspectCommand := &grumble.Command{
		Name:     "checkpoint",
		Help:     "inspect checkpoint tools",
		LongHelp: "inspect checkpoint tools for scheduler",
	}
	cmd.AddCommand(inspectCommand)

	inspectCommand.AddCommand(&grumble.Command{
		Name:  "get",
		Help:  "get inspect checkpoint",
		Run:   cmdGetInspectCheckpoint,
		Flags: clusterFlags,
	})
	inspectCommand.AddCommand(&grumble.Command{
		Name:  "set",
		Help:  "set inspect checkpoint",
		Run:   cmdSetInspectCheckpoint,
		Flags: clusterFlags,
		Args: func(a *grumble.Args) {
			a.Uint64("volume_id", "set the start volume id")
		},
	})
}

func cmdGetInspectCheckpoint(c *grumble.Context) error {
	clusterMgrCli := newClusterMgrTaskClient(getClusterID(c.Flags))
	ck, err := clusterMgrCli.GetVolumeInspectCheckPoint(common.CmdContext())
	if err != nil {
		return err
	}
	fmt.Println(common.RawString(ck))
	return nil
}

func cmdSetInspectCheckpoint(c *grumble.Context) error {
	clusterMgrCli := newClusterMgrTaskClient(getClusterID(c.Flags))
	vid := proto.Vid(c.Args.Uint64("volume_id"))
	if !common.Confirm(fmt.Sprintf("set volume inspect checkpoint: %d ?", vid)) {
		return nil
	}
	err := clusterMgrCli.SetVolumeInspectCheckPoint(common.CmdContext(), vid)
	if err != nil {
		return err
	}
	fmt.Println("set volume inspect checkpoint successfully")
	return nil
}

func addCmdVolumeDegradeStats(cmd *grumble.Command) {
	degradeStatsCommand := &grumble.Command{
		Name:     "degrade",
		Help:     "degrade stats tools",
		LongHelp: "degrade stats tools for scheduler",
	}
	cmd.AddCommand(degradeStatsCommand)

	degradeStatsCommand.AddCommand(&grumble.Command{
		Name:  "all",
		Help:  "get blob and volume degrade stats",
		Run:   cmdGetDegradeStatsAll,
		Flags: clusterFlags,
	})
	degradeStatsCommand.AddCommand(&grumble.Command{
		Name:  "blob",
		Help:  "get blob degrade stats",
		Run:   cmdGetDegradeStatsBlob,
		Flags: clusterFlags,
	})
	degradeStatsCommand.AddCommand(&grumble.Command{
		Name:  "volume",
		Help:  "get volume degrade stats",
		Run:   cmdGetDegradeStatsVolume,
		Flags: clusterFlags,
	})
}

type aggregatedLevel struct {
	Level int    `json:"level"`
	Count uint32 `json:"count"`
}

type aggregatedCodeMode struct {
	CodeMode     string            `json:"codemode"`
	DegradeStats []aggregatedLevel `json:"degrade"`
}

type aggregatedDegradeStat struct {
	BlobStats   []aggregatedCodeMode `json:"blob"`
	VolumeStats []aggregatedCodeMode `json:"volume"`
}

func getDegradeStatsFromKV(c *grumble.Context) (*aggregatedDegradeStat, error) {
	clusterMgrCli := newClusterMgrTaskClient(getClusterID(c.Flags))
	stats, err := clusterMgrCli.GetVolumeDegradeStats(common.CmdContext())
	if err != nil {
		return nil, err
	}

	aggregate := func(batches []proto.VolumeDegradeBatch) []aggregatedCodeMode {
		agg := make(map[string]map[int]uint32)

		for _, batch := range batches {
			for _, cm := range batch.CodeModeStats {
				levelMap, ok := agg[cm.Mode.String()]
				if !ok {
					levelMap = make(map[int]uint32)
					agg[cm.Mode.String()] = levelMap
				}
				for _, level := range cm.DegradeStats {
					levelMap[level.Level] += level.Count
				}
			}
		}

		result := make([]aggregatedCodeMode, 0, len(agg))
		for name, levelMap := range agg {
			degradeStats := make([]aggregatedLevel, 0, len(levelMap))
			for level, count := range levelMap {
				degradeStats = append(degradeStats, aggregatedLevel{
					Level: level,
					Count: count,
				})
			}
			result = append(result, aggregatedCodeMode{
				CodeMode:     name,
				DegradeStats: degradeStats,
			})
		}
		return result
	}

	return &aggregatedDegradeStat{
		BlobStats:   aggregate(stats.BlobStats),
		VolumeStats: aggregate(stats.VolStats),
	}, nil
}

func cmdGetDegradeStatsAll(c *grumble.Context) error {
	stats, err := getDegradeStatsFromKV(c)
	if err != nil {
		return err
	}
	fmt.Println(common.Readable(stats))
	return nil
}

func cmdGetDegradeStatsBlob(c *grumble.Context) error {
	stats, err := getDegradeStatsFromKV(c)
	if err != nil {
		return err
	}
	output := struct {
		BlobStats []aggregatedCodeMode `json:"blob"`
	}{BlobStats: stats.BlobStats}
	fmt.Println(common.Readable(output))
	return nil
}

func cmdGetDegradeStatsVolume(c *grumble.Context) error {
	stats, err := getDegradeStatsFromKV(c)
	if err != nil {
		return err
	}
	output := struct {
		VolumeStats []aggregatedCodeMode `json:"volume"`
	}{VolumeStats: stats.VolumeStats}
	fmt.Println(common.Readable(output))
	return nil
}
