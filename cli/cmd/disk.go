package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/qos"
	"github.com/spf13/cobra"
)

const (
	cmdDiskUse   = "disk [COMMAND]"
	cmdDiskShort = "Manage cluster disks"
)

func newDiskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdDiskUse,
		Short:   cmdDiskShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"disk"},
	}
	cmd.AddCommand(
		newListDisksCmd(client),
		newDiskDetailCmd(client),
		newListBadDiskCmd(client),
		newDecommissionDiskCmd(client),
		newRecommissionDiskCmd(client),
		newQueryDecommissionDiskCmd(client),
		newCancelDecommissionDiskCmd(client),
		newDiskIoStatCmd(),
	)
	return cmd
}

const (
	cmdDiskDetailShort = "show disk detail"
)

func newDiskDetailCmd(client *master.MasterClient) *cobra.Command {
	var optDpDetail bool
	cmd := &cobra.Command{
		Use:   CliOpInfo + " [DATANODE_IP:PORT] [DISK_PATH]",
		Short: cmdDiskDetailShort,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				detail *proto.DiskInfo
				err    error
			)
			defer func() {
				errout(err)
			}()
			if detail, err = client.AdminAPI().DiskDetail(args[0], args[1]); err != nil {
				return
			}
			stdout("Summary:\n%s\n", formatDiskDetailSummary(detail))

			// print data partition detail
			if optDpDetail {
				var view *proto.DiskDataPartitionsView
				if view, err = client.ClientAPI().GetDiskDataPartitions(args[0], args[1]); err != nil {
					err = fmt.Errorf("Get disk data detail information failed:\n%v\n", err)
					return
				}
				stdout("Data partitions:\n")
				stdout("%v\n", diskDataPartitionTableHeader)
				sort.SliceStable(view.DataPartitions, func(i, j int) bool {
					return view.DataPartitions[i].PartitionID < view.DataPartitions[j].PartitionID
				})
				for _, dp := range view.DataPartitions {
					stdout("%v\n", formatDiskDataPartitionTableRow(dp))
				}
			}
		},
	}
	cmd.Flags().BoolVarP(&optDpDetail, "data-partition", "d", false, "Display data partitions on the disk")
	return cmd
}

const (
	cmdListDisksShort = "list disks"
)

func newListDisksCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpList + " [DATANODE_IP:PORT]",
		Short: cmdListDisksShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				infos *proto.DiskInfos
				err   error
			)
			defer func() {
				errout(err)
			}()
			addr := ""
			if len(args) > 0 {
				addr = args[0]
			}
			if infos, err = client.AdminAPI().QueryDisks(addr); err != nil {
				return
			}
			sort.SliceStable(infos.Disks, func(i, j int) bool {
				return infos.Disks[i].Address < infos.Disks[j].Address
			})
			stdout("%v\n", formatDiskList(infos.Disks))
		},
	}
	return cmd
}

const (
	cmdCheckBadDisksShort = "Check and list unhealthy disks"
)

func newListBadDiskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckBadDisksShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				infos *proto.BadDiskInfos
				err   error
			)
			defer func() {
				errout(err)
			}()
			if infos, err = client.AdminAPI().QueryBadDisks(); err != nil {
				return
			}
			stdout("(partitionID=0 means detected by datanode disk checking, not associated with any partition)\n\n[Unavaliable disks]:\n")
			stdout("%v\n", formatBadDiskTableHeader())

			sort.SliceStable(infos.BadDisks, func(i, j int) bool {
				return infos.BadDisks[i].Address < infos.BadDisks[j].Address
			})
			for _, disk := range infos.BadDisks {
				stdout("%v\n", formatBadDiskInfoRow(disk))
			}
		},
	}

	return cmd
}

const (
	cmdDecommissionDisksShort = "Decommission disk on datanode"
)

func newDecommissionDiskCmd(client *master.MasterClient) *cobra.Command {
	var (
		weight       int
		raftForceDel bool
	)
	cmd := &cobra.Command{
		Use:   CliOpDecommission + " [DATA NODE ADDR] [DISK]",
		Short: cmdDecommissionDisksShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			if err = client.AdminAPI().DecommissionDisk(args[0], args[1], weight, raftForceDel); err != nil {
				return
			}
			stdout("Mark disk %v:%v to be decommissioned\n", args[0], args[1])
		},
	}
	cmd.Flags().IntVar(&weight, CliFLagDecommissionWeight, lowPriorityDecommissionWeight, "decommission weight")
	cmd.Flags().BoolVarP(&raftForceDel, CliFlagDecommissionRaftForce, "r", false, "true for raftForceDel")
	return cmd
}

const (
	cmdRecommissionDisksShort = "Recommission disk on datanode"
)

func newRecommissionDiskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpRecommission + " [DATA NODE ADDR] [DISK]",
		Short: cmdRecommissionDisksShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			if err = client.AdminAPI().RecommissionDisk(args[0], args[1]); err != nil {
				return
			}
			stdout("Mark disk %v:%v to be recommissioned\n", args[0], args[1])
		},
	}
	return cmd
}

const (
	cmdQueryDecommissionDiskProgressShort = "Query decommmission progress on datanode"
)

func newQueryDecommissionDiskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpQueryProgress + " [DATA NODE ADDR] [DISK]",
		Short: cmdQueryDecommissionDiskProgressShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			progress, err := client.AdminAPI().QueryDecommissionDiskProgress(args[0], args[1])
			if err != nil {
				return
			}
			stdout("%v", formatDecommissionProgress(progress))
		},
	}
	return cmd
}

const (
	cmdCancelDecommissionDiskShort = "cancel disk decommission"
)

func newCancelDecommissionDiskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpCancelDecommission + " [DATA NODE ADDR] [DISK]",
		Short: cmdCancelDecommissionDiskShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()

			err = client.AdminAPI().AbortDiskDecommission(args[0], args[1])
			if err != nil {
				err = fmt.Errorf("%v, please exec curl -v http://masterAddr:17010/disk/queryDecommissionProgress?addr=dataAddr:17310&disk=dataPath to check if the disk has been canceled", err)
				return
			}
			stdout("%v\n", "cancel decommission successfully")
		},
	}
	return cmd
}

const (
	cmdDiskIoStatShort = "continuously print disk QoS stats"
)

func newDiskIoStatCmd() *cobra.Command {
	var (
		intervalSec int
		count       int
	)
	cmd := &cobra.Command{
		Use:   "iostat [DATA NODE ADDR:PROF PORT] [DISK]",
		Short: cmdDiskIoStatShort,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()

			err = runDiskIostat(args[0], args[1], intervalSec, count)
		},
	}
	cmd.Flags().IntVarP(&intervalSec, "interval", "i", 1, "interval seconds between samples")
	cmd.Flags().IntVarP(&count, "count", "c", 0, "number of samples to print (0 for infinite)")
	return cmd
}

type diskIoStatReply struct {
	Code int32           `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type diskIoStatData struct {
	Path   string         `json:"path"`
	IoType string         `json:"ioType"`
	Enable bool           `json:"enable"`
	Limit  int            `json:"limit"`
	Stat   qos.WindowStat `json:"stat"`
}

var diskIoStatHTTPClient = &http.Client{
	Timeout: 5 * time.Second,
}

func runDiskIostat(addr, disk string, intervalSec, count int) error {
	if intervalSec <= 0 {
		intervalSec = 1
	}
	interval := time.Duration(intervalSec) * time.Second
	samples := 0
	for {
		stats, err := collectDiskIoStat(addr, disk)
		if err != nil {
			return err
		}
		printDiskIoStat(addr, disk, stats, samples)
		samples++
		if count > 0 && samples >= count {
			break
		}
		time.Sleep(interval)
	}
	return nil
}

func collectDiskIoStat(addr, disk string) ([]*diskIoStatData, error) {
	stats := make([]*diskIoStatData, 0, len(qos.IoTypes))
	for _, ioType := range qos.IoTypes {
		stat, err := fetchDiskStat(addr, disk, int(ioType))
		if err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	return stats, nil
}

func fetchDiskStat(addr, disk string, ioType int) (*diskIoStatData, error) {
	endpoint := fmt.Sprintf("http://%s/getDiskStat", addr)
	query := url.Values{}
	query.Set("disk", disk)
	query.Set("ioType", strconv.Itoa(ioType))
	req, err := http.NewRequest(http.MethodGet, endpoint+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := diskIoStatHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var reply diskIoStatReply
	if err = json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}
	if reply.Code != http.StatusOK {
		if reply.Msg == "" {
			reply.Msg = http.StatusText(int(reply.Code))
		}
		return nil, fmt.Errorf("get disk stat failed, ioType=%d: %s", ioType, reply.Msg)
	}
	var stat diskIoStatData
	if err = json.Unmarshal(reply.Data, &stat); err != nil {
		return nil, err
	}
	return &stat, nil
}

func printDiskIoStat(addr, disk string, stats []*diskIoStatData, sample int) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	stdout("[%s] addr=%s disk=%s sample=%d\n", timestamp, addr, disk, sample+1)
	rows := make([][]interface{}, 0, len(stats)+1)
	header := []interface{}{"IOType", "Enable", "Limit", "IOPS", "BPS", "AvgReq", "AvgQue", "Await(ms)", "RunAvg", "RunMax", "Success%", "Error%", "Reject%"}
	rows = append(rows, header)
	for _, stat := range stats {
		rows = append(rows, []interface{}{
			stat.IoType,
			boolToYN(stat.Enable),
			limitToString(stat.Enable, stat.Limit),
			stat.Stat.Iops,
			stat.Stat.Bps,
			stat.Stat.Avgrq,
			stat.Stat.Avgqu,
			awaitMillis(stat.Stat.Await),
			stat.Stat.RunAvg,
			stat.Stat.RunMax,
			percent(stat.Stat.SuccessRate),
			percent(stat.Stat.ErrorRate),
			percent(stat.Stat.RejectRate),
		})
	}
	stdout("%s\n\n", alignTable(rows...))
}

func boolToYN(v bool) string {
	if v {
		return "Y"
	}
	return "N"
}

func limitToString(enable bool, limit int) string {
	if !enable {
		return "-"
	}
	return strconv.Itoa(limit)
}

func awaitMillis(awaitNs int64) string {
	ms := float64(awaitNs) / 1e6
	return fmt.Sprintf("%.2f", ms)
}

func percent(value float64) string {
	return fmt.Sprintf("%.2f", value*100)
}
