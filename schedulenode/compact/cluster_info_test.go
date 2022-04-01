package compact

import (
	"github.com/chubaofs/chubaofs/sdk/master"
	"strings"
	"testing"
)

var cInfo = NewClusterInfo(clusterName, master.NewMasterClient(strings.Split(ltptestMaster, ","), false))

func TestCompactVol(t *testing.T) {
	mcc := &metaNodeControlConfig{}
	if err := cInfo.CheckAndAddCompactVol(ltptestVolume, mcc); err != nil {
		t.Fail()
	}
	count := cInfo.GetVolumeCount()
	if count != 1 {
		t.Fail()
	}
	if volInfo := cInfo.GetCompactVolByName(ltptestVolume); volInfo == nil {
		t.Fail()
	}
	if volInfo := cInfo.GetCompactVolByName("noexistvol"); volInfo != nil {
		t.Fail()
	}
}

func TestNodes(t *testing.T) {
	nodes := cInfo.Nodes()
	for _, node := range nodes {
		if !strings.Contains(ltptestMaster, node) {
			t.Fail()
		}
	}
	cInfo.AddNode("127.0.0.1:12345")
	cInfo.UpdateNodes(strings.Split(ltptestMaster, ","))
}

func TestReleaseUnusedCompVolume(t *testing.T) {
	mcc := &metaNodeControlConfig{}
	if err := cInfo.CheckAndAddCompactVol(ltptestVolume, mcc); err != nil {
		t.Fail()
	}
	volInfo := cInfo.GetCompactVolByName(ltptestVolume)
	if volInfo == nil {
		t.Fail()
		return
	}
	volInfo.LastUpdate = 16019171
	cInfo.releaseUnusedCompVolume()
	volInfo = cInfo.GetCompactVolByName(ltptestVolume)
	if volInfo != nil {
		t.Fail()
	}
}
