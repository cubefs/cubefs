package img

import (
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/config"
	"testing"
)

func TestCheckNeedCreateVol(t *testing.T) {
	fmt.Println(UMPImageRwVolsKey)
	cfg, _ := config.LoadConfigFile(checktool.ReDirPath("cfg.json"))
	s := NewVolStoreMonitor()
	err := s.parseConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	needCreateVol, rwVolCount, availTB := s.checkNeedCreateVol("imgmaster.jd.local", "ds_image")
	msg := fmt.Sprintf("needCreateVol:%v rwVolCount:%v,availTB:%v, minCount:%v, minAvail:%v",
		needCreateVol, rwVolCount, availTB, s.minRWVolCount, s.minAvailOfAllVolsTB)
	t.Log(msg)
	s.minRWVolCount = rwVolCount + 1
	s.minAvailOfAllVolsTB = availTB + 1
	needCreateVol, rwVolCount, availTB = s.checkNeedCreateVol("imgmaster.jd.local", "ds_image")
	msg = fmt.Sprintf("needCreateVol:%v rwVolCount:%v,availTB:%v, minCount:%v, minAvail:%v",
		needCreateVol, rwVolCount, availTB, s.minRWVolCount, s.minAvailOfAllVolsTB)
	t.Log(msg)
	if needCreateVol == false {
		t.Errorf("needCreateVol should be true")
	}
}
