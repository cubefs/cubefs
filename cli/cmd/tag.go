// Copyright 2025 The CubeFS Authors.
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

package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdSelectTagUse   = "tag [COMMAND]"
	cmdSelectTagShort = "tag management"
)

func newSelectTagCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdSelectTagUse,
		Short: cmdSelectTagShort,
	}
	cmd.AddCommand(
		newShowSelectTagSummaryCmd(client),
		newShowSelectTagVolSummaryCmd(client),
		newClearSelectTagFailedKeysCmd(client),
	)
	return cmd
}

const (
	cmdShowSelectTagSummary          = "summary"
	cmdShowSelectTagSummaryShort     = "Show select tag summary"
	cmdShowSelectTagVolSummary       = "vol-summary"
	cmdShowSelectTagVolSummaryShort  = "Show select tag summary for a volume"
	cmdClearSelectTagFailedKeys      = "clear-failed-keys"
	cmdClearSelectTagFailedKeysShort = "Clear select tag failed keys"
)

func newShowSelectTagSummaryCmd(client *master.MasterClient) *cobra.Command {
	var optDetail bool
	var optMeta bool
	var optData bool
	cmd := &cobra.Command{
		Use:   cmdShowSelectTagSummary,
		Short: cmdShowSelectTagSummaryShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
				out []byte
			)
			defer func() {
				errout(err)
			}()
			if optMeta && optData {
				err = fmt.Errorf("flags meta and data are mutually exclusive")
				return
			}
			var task *proto.TagSummary
			if task, err = client.AdminAPI().GetSelectTagSummary(optDetail); err != nil {
				return
			}
			output := make(map[string]any)
			if optMeta {
				output["autoFixTag"] = task.AutoFixTag
				output["volumeNum"] = task.VolumeNum
				output["volumeWithTagNum"] = task.VolWithTagNum
				output["clusterMpTag"] = task.ClusterMpTag
				output["totalMpNum"] = task.TotalMpNum
				output["unmatchMpNum"] = task.UnmatchMpNum
				output["mpDecommissionNum"] = task.MpDecommissionNum
				output["metaNodeTagCount"] = task.MetaNodeTagCount
				output["metaNodeSpace"] = task.MetaNodeSpace
				if optDetail {
					output["unmatchMpSamples"] = task.UnmatchMpSamples
					output["mpPlanStatus"] = task.MpPlanStatus
					output["failedMpKeys"] = task.FailedMpKeys
					output["mpCheckThreadStatus"] = task.MpCheckThreadStatus
					output["MpThreadLastQuitReason"] = task.LastMpQuitReason
					output["MpThreadLastQuitTime"] = task.LastMpThreadTime
				}
			} else if optData {
				output["autoFixTag"] = task.AutoFixTag
				output["volumeNum"] = task.VolumeNum
				output["volumeWithTagNum"] = task.VolWithTagNum
				output["clusterDpTag"] = task.ClusterDpTag
				output["totalDpNum"] = task.TotalDpNum
				output["unmatchDpNum"] = task.UnmatchDpNum
				output["decommissionDpNum"] = task.DecommissionDpNum
				output["dataNodeTagCount"] = task.DataNodeTagCount
				output["dataNodeSpace"] = task.DataNodeSpace
				if optDetail {
					output["unmatchDpSamples"] = task.UnmatchDpSamples
					output["dpCheckThreadStatus"] = task.DpCheckThreadStatus
					output["DpThreadLastQuitReason"] = task.LastDpQuitReason
					output["DpThreadLastQuitTime"] = task.LastDpThreadTime
				}
			}

			if optMeta || optData {
				out, err = marshalIndentNoEscape(output)
			} else {
				out, err = marshalIndentNoEscape(task)
			}
			if err != nil {
				stdout("marshal task failed: %s", err.Error())
				return
			}
			stdout("%s", string(out))
		},
	}
	cmd.Flags().BoolVarP(&optDetail, "all", "a", false, "Show all details information")
	cmd.Flags().BoolVarP(&optMeta, "meta", "m", false, "Show metaNode/metaPartition related information")
	cmd.Flags().BoolVarP(&optData, "data", "d", false, "Show dataNode/dataPartition related information")
	return cmd
}

func newShowSelectTagVolSummaryCmd(client *master.MasterClient) *cobra.Command {
	var optDetail bool
	var optMeta bool
	var optData bool
	cmd := &cobra.Command{
		Use:   cmdShowSelectTagVolSummary,
		Short: cmdShowSelectTagVolSummaryShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
				out []byte
			)
			defer func() {
				errout(err)
			}()
			if len(args) < 1 {
				err = fmt.Errorf("volume name is required")
				return
			}
			if optMeta && optData {
				err = fmt.Errorf("flags meta and data are mutually exclusive")
				return
			}
			volName := args[0]
			var task *proto.VolTagSummary
			if task, err = client.AdminAPI().GetVolTagSummary(volName); err != nil {
				return
			}
			output := make(map[string]any)
			if optMeta {
				output["volume"] = task.Vol
				output["volStatus"] = task.VolStatus
				output["mpTag"] = task.MpTag
				output["effectiveMpTags"] = task.EffectiveMpTags
				output["totalMpNum"] = task.TotalMpNum
				output["unmatchMpNum"] = task.UnmatchMpNum
				if optDetail {
					output["unmatchMps"] = task.UnmatchMps
					output["unmatchMpSamples"] = task.UnmatchMpSamples
				}
				output["failedMpKeys"] = task.FailedMpKeys
			} else if optData {
				output["volume"] = task.Vol
				output["volStatus"] = task.VolStatus
				output["dpTag"] = task.DpTag
				output["effectiveDpTags"] = task.EffectiveDpTags
				output["totalDpNum"] = task.TotalDpNum
				output["unmatchDpNum"] = task.UnmatchDpNum
				if optDetail {
					output["unmatchDps"] = task.UnmatchDps
					output["unmatchDpSamples"] = task.UnmatchDpSamples
				}
			}

			if optMeta || optData {
				out, err = marshalIndentNoEscape(output)
			} else {
				out, err = marshalIndentNoEscape(task)
			}
			if err != nil {
				stdout("marshal task failed: %s", err.Error())
				return
			}
			stdout("%s", string(out))
		},
	}
	cmd.Flags().BoolVarP(&optDetail, "all", "a", false, "Show all details information")
	cmd.Flags().BoolVarP(&optMeta, "meta", "m", false, "Show metaNode/metaPartition related information")
	cmd.Flags().BoolVarP(&optData, "data", "d", false, "Show dataNode/dataPartition related information")
	return cmd
}

func newClearSelectTagFailedKeysCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdClearSelectTagFailedKeys,
		Short: cmdClearSelectTagFailedKeysShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			if err = client.AdminAPI().ClearSelectTagFailedKeys(); err != nil {
				return
			}
			stdout("Clear select tag failed keys successfully.")
		},
	}
	return cmd
}

func marshalIndentNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "    ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	out := buf.Bytes()
	if len(out) > 0 && out[len(out)-1] == '\n' {
		out = out[:len(out)-1]
	}
	return out, nil
}
