package rebalance

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	data2 "github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/util/log"
	"path"
	"time"
)

func (rw *ReBalanceWorker) ReBalanceStart(cluster, zoneName string, highRatio, lowRatio, goalRatio float64,
	maxBatchCount int, MigrateLimitPerDisk int) error {
	ctrl, err := rw.newZoneCtrl(cluster, zoneName, highRatio, lowRatio, goalRatio)
	if err != nil {
		return err
	}
	if maxBatchCount > 0 {
		ctrl.SetClusterMaxBatchCount(maxBatchCount)
	}
	ctrl.SetMigrateLimitPerDisk(MigrateLimitPerDisk)
	err = ctrl.ReBalanceStart()
	if err != nil {
		return err
	}
	return nil
}

func (rw *ReBalanceWorker) ReBalanceStop(cluster, zoneName string) error {
	ctrl, err := rw.getZoneCtrl(cluster, zoneName)
	if err != nil {
		return err
	}
	err = ctrl.ReBalanceStop()
	if err != nil {
		return err
	}
	return nil
}

func (rw *ReBalanceWorker) ReBalanceStatus(cluster, zoneName string) (Status, error) {
	ctrl, err := rw.getZoneCtrl(cluster, zoneName)
	if err != nil {
		return -1, err
	}
	status := ctrl.Status()
	return status, nil
}

func (rw *ReBalanceWorker) ReBalanceSet(cluster, zoneName string, highRatio, lowRatio, goalRatio float64,
	maxBatchCount int, MigrateLimitPerDisk int) error {
	ctrl, err := rw.getZoneCtrl(cluster, zoneName)
	if err != nil {
		return err
	}
	err = ctrl.UpdateRatio(highRatio, lowRatio, goalRatio)
	if err != nil {
		return err
	}
	if maxBatchCount > 0 {
		ctrl.SetClusterMaxBatchCount(maxBatchCount)
	}
	ctrl.SetMigrateLimitPerDisk(MigrateLimitPerDisk)
	return nil
}

func (rw *ReBalanceWorker) ResetZoneMap() {
	rw.reBalanceCtrlMap.Range(func(key, value interface{}) bool {
		ctrl := value.(*ZoneReBalanceController)
		if ctrl.Status() == StatusStop {
			rw.reBalanceCtrlMap.Delete(key)
		}
		return true
	})
}

func (rw *ReBalanceWorker) newZoneCtrl(cluster, zoneName string, highRatio, lowRatio, goalRatio float64) (*ZoneReBalanceController, error) {
	ctrl, err := rw.getZoneCtrl(cluster, zoneName)
	if err == nil {
		err := ctrl.UpdateRatio(highRatio, lowRatio, goalRatio)
		if err != nil {
			return nil, err
		}
		return ctrl, nil
	}
	ctrl, err = NewZoneReBalanceController(cluster, zoneName, highRatio, lowRatio, goalRatio, rw.PutMigrateInfoToDB)
	if err != nil {
		return nil, err
	}
	rw.reBalanceCtrlMap.Store(path.Join(cluster, zoneName), ctrl)
	return ctrl, nil
}

func (rw *ReBalanceWorker) getZoneCtrl(cluster, zoneName string) (*ZoneReBalanceController, error) {
	if res, ok := rw.reBalanceCtrlMap.Load(path.Join(cluster, zoneName)); !ok {
		return nil, fmt.Errorf("get zone rebalance controller error with cluster:%v zoneName:%v", cluster, zoneName)
	} else {
		ctrl := res.(*ZoneReBalanceController)
		return ctrl, nil
	}
}

func (rw *ReBalanceWorker) doReleaseZone(cluster, zoneName string) error {
	dataNodes, err := getZoneDataNodesByClusterName(cluster, zoneName)
	if err != nil {
		return err
	}
	for _, node := range dataNodes {
		if err = rw.doReleaseDataNodePartitions(node, ""); err != nil {
			log.LogErrorf("release dataNode error cluster: %v zone: %v dataNode %v %v", cluster, zoneName, node, err)
		}
	}
	return nil
}

func (rw *ReBalanceWorker) doReleaseDataNodePartitions(dataNodeHttpAddr, timeLocation string) (err error) {
	var (
		data   []byte
		reqURL string
		key    string
	)
	if timeLocation == "" {
		key = generateAuthKey()
	} else {
		key = generateAuthKeyWithTimeZone(timeLocation)
	}
	dataHttpClient := data2.NewDataHttpClient(dataNodeHttpAddr, false)
	_, err = dataHttpClient.ReleasePartitions(key)
	if err != nil {
		return fmt.Errorf("url[%v],err %v resp[%v]", reqURL, err, string(data))
	}
	log.LogInfof("action[doReleaseDataNodePartitions] url[%v] resp[%v]", reqURL, string(data))
	return
}

func generateAuthKey() string {
	date := time.Now().Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

func generateAuthKeyWithTimeZone(timeLocation string) string {
	var t time.Time
	if timeLocation == "" {
		t = time.Now()
	} else {
		l, _ := time.LoadLocation(timeLocation)
		t = time.Now().In(l)
	}
	date := t.Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}
