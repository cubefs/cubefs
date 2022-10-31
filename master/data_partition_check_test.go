package master

import (
	"testing"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/unit"
)

const (
	ReadOnly    = 1
	ReadWrite   = 2
	Unavailable = -1
)

func TestCheckStatusOfCrossRegionQuorumVol(t *testing.T) {
	rwDataReplica1 := &DataReplica{DataReplica: proto.DataReplica{Addr: "192.168.0.1", Status: ReadWrite}}
	rwDataReplica2 := &DataReplica{DataReplica: proto.DataReplica{Addr: "192.168.0.2", Status: ReadWrite}}
	rwDataReplica3 := &DataReplica{DataReplica: proto.DataReplica{Addr: "192.168.0.3", Status: ReadWrite}}
	roDataReplica := &DataReplica{DataReplica: proto.DataReplica{Addr: "192.168.0.4", Status: ReadOnly}}
	unavailableDataReplica := &DataReplica{DataReplica: proto.DataReplica{Status: Unavailable}}
	availDP := &DataPartition{Hosts: []string{"192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.4", "192.168.0.5"}, PartitionID: 1, ReplicaNum: 5, total: 120 * unit.GB}
	notAvailDP := &DataPartition{Hosts: []string{"192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.4", "192.168.0.5"}, PartitionID: 1, ReplicaNum: 5, total: 120 * unit.GB, used: 115 * unit.GB}
	primaryReadOnlyDP := &DataPartition{Hosts: []string{"192.168.0.4", "192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.5"}, PartitionID: 1, ReplicaNum: 5, total: 120 * unit.GB, used: 115 * unit.GB}
	caseList := []struct {
		dp                   *DataPartition
		liveReplicas         []*DataReplica
		dpWriteableThreshold float64
		res                  int8
	}{
		{availDP, []*DataReplica{rwDataReplica1}, 0, ReadOnly},
		{availDP, []*DataReplica{rwDataReplica1, rwDataReplica2}, 0, ReadOnly},
		{availDP, []*DataReplica{rwDataReplica1, rwDataReplica2, unavailableDataReplica}, 0, ReadOnly},
		{availDP, []*DataReplica{rwDataReplica1, rwDataReplica2, roDataReplica}, 0, ReadOnly},
		{availDP, []*DataReplica{rwDataReplica1, rwDataReplica2, roDataReplica, roDataReplica}, 0, ReadOnly},
		{availDP, []*DataReplica{rwDataReplica1, rwDataReplica2, roDataReplica, roDataReplica, roDataReplica}, 0, ReadOnly},
		{notAvailDP, []*DataReplica{rwDataReplica1, rwDataReplica2, rwDataReplica3}, 0, ReadOnly},
		{primaryReadOnlyDP, []*DataReplica{rwDataReplica1, rwDataReplica2, rwDataReplica3, roDataReplica}, 0, ReadOnly},

		{availDP, []*DataReplica{rwDataReplica1, rwDataReplica2, rwDataReplica3}, 0, ReadWrite},
		{availDP, []*DataReplica{rwDataReplica1, rwDataReplica2, rwDataReplica3, unavailableDataReplica}, 0, ReadWrite},
		{availDP, []*DataReplica{rwDataReplica1, rwDataReplica2, rwDataReplica3, roDataReplica}, 0, ReadWrite},
		{availDP, []*DataReplica{rwDataReplica1, rwDataReplica2, rwDataReplica3, roDataReplica, roDataReplica}, 0, ReadWrite},
	}

	for no, case_ := range caseList {
		case_.dp.checkStatusOfCrossRegionQuorumVol(case_.liveReplicas, case_.dpWriteableThreshold, nil, 3)
		res := case_.dp.Status
		if res != case_.res {
			t.Errorf("checkStatusOfCrossRegionQuorumVol failed, no=[%d] res=[%v] expectRes=[%v]", no, res, case_.res)
		}
	}
}
