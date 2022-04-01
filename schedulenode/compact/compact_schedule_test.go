package compact

import (
	"github.com/chubaofs/chubaofs/sdk/master"
	"strings"
	"testing"
)

func TestGetMpView(t *testing.T) {
	cw := NewCompactWorker()
	if _, err := cw.GetMpView(clusterName, ltptestVolume); err == nil {
		t.Fatalf("GetMpView should hava err")
	}
	cw.mcw = make(map[string]*master.MasterClient)
	cw.mcw[clusterName] = master.NewMasterClient(strings.Split(ltptestMaster, ","), false)
	if _, err := cw.GetMpView(clusterName, ltptestVolume); err != nil {
		t.Fatalf("GetMpView should not hava err, but err:%v", err)
	}
}

func TestGetCompactVolumes(t *testing.T) {
	mc := master.NewMasterClient(strings.Split(ltptestMaster, ","), false)
	if _, err := GetCompactVolumes(clusterName, mc); err != nil {
		t.Fatalf("GetCompactVolumes should not hava err, but err:%v", err)
	}
}
