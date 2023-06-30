package rebalance

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/master"
	"strings"
	"time"
)

func getDataHttpClient(nodeAddr, masterAddr string) *data.DataHttpClient {
	strs := strings.Split(nodeAddr, ":")
	host := strs[0]
	port := getDataNodePProfPort(masterAddr)
	return data.NewDataHttpClient(fmt.Sprintf("%s:%s", host, port), false)
}

func getDataNodePProfPort(host string) (port string) {
	switch host {
	case "cn.chubaofs.jd.local", "cn.elasticdb.jd.local", "cn.chubaofs-seqwrite.jd.local", "nl.chubaofs.jd.local", "nl.chubaofs.ochama.com":
		port = "6001"
	case "192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010":
		port = "17320"
	default:
		port = "6001"
	}
	return
}

func checkRatio(highRatio, lowRatio, goalRatio float64) error {
	if highRatio < lowRatio {
		return ErrWrongRatio
	}
	if goalRatio > highRatio {
		return ErrWrongRatio
	}
	return nil
}

func getLiveReplicas(partition *proto.DataPartitionInfo, timeOutSec int64) (replicas []*proto.DataReplica) {
	replicas = make([]*proto.DataReplica, 0)
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if isReplicaAlive(replica, timeOutSec) && hasHost(partition, replica.Addr) {
			replicas = append(replicas, replica)
		}
	}
	return
}

func isReplicaAlive(replica *proto.DataReplica, timeOutSec int64) (isAvailable bool) {
	if replica.Status != Unavailable && (time.Now().Unix()-replica.ReportTime <= timeOutSec) {
		isAvailable = true
	}
	return
}

func hasHost(partition *proto.DataPartitionInfo, addr string) (ok bool) {
	for _, host := range partition.Hosts {
		if host == addr {
			ok = true
			break
		}
	}
	return
}

func getZoneDataNodesByClusterName(cluster, zoneName string) (zoneDataNodes []string, err error) {
	client := master.NewMasterClient([]string{cluster}, false)
	return getZoneDataNodesByClient(client, zoneName)
}

func getZoneDataNodesByClient(client *master.MasterClient, zoneName string) (zoneDataNodes []string, err error) {
	topologyView, err := client.AdminAPI().GetTopology()
	if err != nil {
		return
	}
	zoneDataNodes = make([]string, 0)
	for _, zone := range topologyView.Zones {
		if zone.Name == zoneName {
			for _, nodeSetView := range zone.NodeSet {
				for _, dataNode := range nodeSetView.DataNodes {
					zoneDataNodes = append(zoneDataNodes, dataNode.Addr)
				}
			}
		}
	}
	return
}

func getStatusStr(status Status) string {
	switch status {
	case StatusStop:
		return "Stop"
	case StatusRunning:
		return "Running"
	case StatusTerminating:
		return "Terminating"
	default:
		return "None"
	}
}
