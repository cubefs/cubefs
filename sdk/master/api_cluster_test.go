package master

import "testing"

var (
	testIsFreeze = false
)

func TestClusterAPI(t *testing.T) {
	cInfo, err := testMc.AdminAPI().GetClusterInfo()
	if err != nil {
		t.Errorf("Get cluster info failed: err(%v), info(%v)", err, cInfo)
	}
	csInfo, err := testMc.AdminAPI().GetClusterStat()
	if err != nil {
		t.Errorf("Get cluster stat failed: err(%v), stat info(%v)", err, csInfo)
	}

	err = testMc.AdminAPI().IsFreezeCluster(testIsFreeze)
	if err != nil {
		t.Errorf("unFreeze cluster failed: err(%v)", err)
	}

	cv, err := testMc.AdminAPI().GetCluster()
	if err != nil {
		t.Errorf("Get cluster failed: err(%v), cluster(%v)", err, cv)
	}
	if cv.DisableAutoAlloc {
		t.Errorf("unFreeze cluster failed: err(%v)", err)
	}
}
