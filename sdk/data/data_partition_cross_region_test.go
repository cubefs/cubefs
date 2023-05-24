package data

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

var (
	ip1 = "192.168.0.31:17310"
	ip2 = "192.168.0.32:17310"
	ip3 = "192.168.0.33:17310"
	ip4 = "192.168.0.34:17310"
	ip5 = "192.168.0.35:17310"
)

func TestCrossRegionHosts(t *testing.T) {
	dataWrapper, err := NewDataPartitionWrapper(ltptestVolume, strings.Split(ltptestMaster, ","))
	if err != nil {
		t.Fatalf("TestCrossRegionHosts: NewDataPartitionWrapper failed, err %v", err)
	}
	dataWrapper.crossRegionHostLatency.Store(ip1, 1*time.Millisecond)   // same-region
	dataWrapper.crossRegionHostLatency.Store(ip2, 400*time.Microsecond) // same-zone
	dataWrapper.crossRegionHostLatency.Store(ip3, 10*time.Microsecond)  // same-zone
	dataWrapper.crossRegionHostLatency.Store(ip4, time.Duration(0))     // unknown
	dataWrapper.crossRegionHostLatency.Store(ip5, 20*time.Millisecond)  // cross-region
	dp := &DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{Hosts: []string{ip1, ip2, ip3, ip4, ip5},LeaderAddr: proto.NewAtomicString(ip3)},
		CrossRegionMetrics:    NewCrossRegionMetrics(),
		ClientWrapper:         dataWrapper,
	}
	dp.CrossRegionMetrics.CrossRegionHosts = dataWrapper.classifyCrossRegionHosts(dp.Hosts)
	// check classify
	if !contains(dp.CrossRegionMetrics.CrossRegionHosts[SameZoneRank], ip3) || !contains(dp.CrossRegionMetrics.CrossRegionHosts[SameZoneRank], ip2) {
		t.Fatalf("TestCrossRegionHosts failed: expect rank(%v:%v/%v) but(%v)", SameZoneRank, ip2, ip3, dp.CrossRegionMetrics.CrossRegionHosts[SameZoneRank])
	}
	if !contains(dp.CrossRegionMetrics.CrossRegionHosts[SameRegionRank], ip1) {
		t.Fatalf("TestCrossRegionHosts failed: expect rank(%v:%v) but(%v)", SameRegionRank, ip1, dp.CrossRegionMetrics.CrossRegionHosts[SameRegionRank])
	}
	if !contains(dp.CrossRegionMetrics.CrossRegionHosts[CrossRegionRank], ip5) {
		t.Fatalf("TestCrossRegionHosts failed: expect rank(%v:%v) but(%v)", CrossRegionRank, ip5, dp.CrossRegionMetrics.CrossRegionHosts[CrossRegionRank])
	}
	if !contains(dp.CrossRegionMetrics.CrossRegionHosts[UnknownRegionRank], ip4) {
		t.Fatalf("TestCrossRegionHosts failed: expect rank(%v:%v) but(%v)", UnknownRegionRank, ip4, dp.CrossRegionMetrics.CrossRegionHosts[UnknownRegionRank])
	}
	// check get
	targetHost := dp.getNearestCrossRegionHost()
	if targetHost != ip3 && targetHost != ip2 {
		t.Fatalf("TestCrossRegionHosts failed: expect target host(%v/%v) but(%v) hostStatus(%v)", ip2, ip3, targetHost, dataWrapper.HostsStatus)
	}
	// check sort
	sortedHosts := dp.getSortedCrossRegionHosts()
	expectedSortedHosts := []string{ip2, ip3, ip1, ip5, ip4}
	if strings.Join(sortedHosts, ",") != strings.Join(expectedSortedHosts, ",") {
		t.Fatalf("TestCrossRegionHosts failed: expect sorted cross region hosts(%v) but(%v)", expectedSortedHosts, sortedHosts)
	}
	// check occur error
	for i := 0; i < 10; i++ {
		dp.updateCrossRegionMetrics(ip2, true)
	}
	if contains(dp.CrossRegionMetrics.CrossRegionHosts[SameZoneRank], ip2) || !contains(dp.CrossRegionMetrics.CrossRegionHosts[UnknownRegionRank], ip2) {
		t.Fatalf("TestCrossRegionHosts failed: expect target host(%v) in rank(%v)", ip2, UnknownRegionRank)
	}
	ip2Time, ok := dataWrapper.crossRegionHostLatency.Load(ip2)
	if !ok || ip2Time.(time.Duration).Nanoseconds() > 0 {
		t.Fatalf("TestCrossRegionHosts failed: expect ping time of ip2 is(0) but(%v)", ip2Time)
	}
	// check recover after failed
	dataWrapper.crossRegionHostLatency.Store(ip2, 500*time.Microsecond) // same-zone
	dp.CrossRegionMetrics.CrossRegionHosts = dataWrapper.classifyCrossRegionHosts(dp.Hosts)
	sortedHosts = dp.getSortedCrossRegionHosts()
	expectedSortedHosts = []string{ip3, ip1, ip2, ip5, ip4}
	if strings.Join(sortedHosts, ",") != strings.Join(expectedSortedHosts, ",") {
		t.Fatalf("TestCrossRegionHosts failed: expect sorted cross region hosts(%v) but(%v)", expectedSortedHosts, sortedHosts)
	}
	dp.updateCrossRegionMetrics(ip2, false)
	if len(dp.CrossRegionMetrics.HostErrCounter) > 0 {
		t.Fatalf("TestCrossRegionHosts failed: expect no host in HostErrCounter but(%v)", dp.CrossRegionMetrics.HostErrCounter)
	}
}

func TestPingCrossRegionHosts(t *testing.T) {
	dataWrapper, err := NewDataPartitionWrapper(ltptestVolume, strings.Split(ltptestMaster, ","))
	if err != nil {
		t.Fatalf("TestPingCrossRegionHosts: NewDataPartitionWrapper failed, err %v", err)
	}
	dataWrapper.crossRegionHAType = proto.CrossRegionHATypeQuorum

	for i:=0;i< 6;i++ {
		err = dataWrapper.updateDataPartition(false)
		if err==nil {
			break
		}
		time.Sleep(time.Second*10)
	}
	if err != nil {
		assert.FailNow(t,"TestPingCrossRegionHosts: updateDataPartition failed, err %v", err)
	}

	// test getUnknownCrossRegionHostStatus
	dataWrapper.getUnknownCrossRegionHostStatus()
	count1 := 0
	dataWrapper.crossRegionHostLatency.Range(func(key, value interface{}) bool {
		pingTime := value.(time.Duration)
		if pingTime.Nanoseconds() <= 0 {
			t.Errorf("TestPingCrossRegionHosts: ping failed ip(%v) time(%v)", key, pingTime)
		}
		count1++
		return true
	})
	if count1 <= 0 {
		t.Errorf("TestPingCrossRegionHosts: no host in ping map, count(%v)", count1)
	}
	// test getAllCrossRegionHostStatus
	dataWrapper.getAllCrossRegionHostStatus()
	count2 := 0
	dataWrapper.crossRegionHostLatency.Range(func(key, value interface{}) bool {
		pingTime := value.(time.Duration)
		if pingTime.Nanoseconds() <= 0 {
			t.Errorf("TestPingCrossRegionHosts: ping failed ip(%v) time(%v)", key, pingTime)
		}
		count2++
		return true
	})
	if count2 <= 0 || count1 != count2 {
		t.Errorf("TestPingCrossRegionHosts: the number of hosts in ping map expect(%v) but(%v)", count1, count2)
	}
}

func TestMiddleStatCrossRegionHosts(t *testing.T) {
	dataWrapper, err := NewDataPartitionWrapper(ltptestVolume, strings.Split(ltptestMaster, ","))
	if err != nil {
		t.Fatalf("TestCrossRegionHosts: NewDataPartitionWrapper failed, err %v", err)
	}
	dataWrapper.crossRegionHostLatency.Store(ip1, 1*time.Millisecond)   // same-region
	dataWrapper.crossRegionHostLatency.Store(ip2, 400*time.Microsecond) // same-zone
	dataWrapper.crossRegionHostLatency.Store(ip3, 10*time.Microsecond)  // same-zone
	dataWrapper.crossRegionHostLatency.Store(ip4, time.Duration(0))     // unknown
	dataWrapper.crossRegionHostLatency.Store(ip5, 20*time.Millisecond)  // cross-region
	// check before classify
	dp := &DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{Hosts: []string{ip1, ip2, ip3, ip4, ip5},LeaderAddr: proto.NewAtomicString(ip3)},
		CrossRegionMetrics:    NewCrossRegionMetrics(),
		ClientWrapper:         dataWrapper,
	}
	// check get
	targetHost := dp.getNearestCrossRegionHost()
	if targetHost != dp.GetLeaderAddr() {
		t.Fatalf("TestCrossRegionHosts failed: expect target host(%v) but(%v) hostStatus(%v)", dp.GetLeaderAddr(), targetHost, dataWrapper.HostsStatus)
	}
	// check sort
	sortedHosts := dp.getSortedCrossRegionHosts()
	if len(sortedHosts) != 0 {
		t.Fatalf("TestCrossRegionHosts failed: expect sorted cross region hosts([]) but(%v)", sortedHosts)
	}
	sortedByStatusHosts := sortByStatus(dp, ip1)
	expectedSortedHosts := []string{ip2, ip3, ip4, ip5, ip1}
	if strings.Join(sortedByStatusHosts, ",") != strings.Join(expectedSortedHosts, ",") {
		t.Fatalf("TestCrossRegionHosts failed: expect sorted cross region hosts(%v) but(%v)", expectedSortedHosts, sortedByStatusHosts)
	}
	// check occur error
	for i := 0; i < 10; i++ {
		dp.updateCrossRegionMetrics(ip2, true)
	}
	if len(dp.CrossRegionMetrics.CrossRegionHosts[SameZoneRank]) != 0 || len(dp.CrossRegionMetrics.CrossRegionHosts[UnknownRegionRank]) != 0 {
		t.Fatalf("TestCrossRegionHosts failed: expect nil CrossRegionHosts but(%v)", dp.CrossRegionMetrics)
	}

}

func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}
	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}
