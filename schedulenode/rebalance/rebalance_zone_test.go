package rebalance

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

var (
	cluster   = "sparkchubaofs.jd.local"
	zoneName  = "rh_hbase_ssd"
	highRatio = 0.7
	lowRatio  = 0.5
	goalRatio = 0.6
)

func printMigrate(info *MigrateRecordTable) error {
	fmt.Println(fmt.Sprintf("zone : %v migrate dp: %v from node: %v disk: %v to node: %v", info.ZoneName, info.PartitionID, info.SrcAddr, info.SrcDisk, info.DstAddr))
	fmt.Println(fmt.Sprintf("node usage: %v -> %v", info.OldUsage, info.NewUsage))
	fmt.Println(fmt.Sprintf("disk usage %v -> %v", info.OldDiskUsage, info.NewDiskUsage))
	return nil
}

func TestReBalanceZone(t *testing.T) {
	ctrl, err := NewZoneReBalanceController(cluster, zoneName, highRatio, lowRatio, goalRatio, printMigrate)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("test reBalance", func(t *testing.T) {
		err = ctrl.ReBalanceStart()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestReBalanceStart(t *testing.T) {
	rw := ReBalanceWorker{}
	err := rw.ReBalanceStart(cluster, zoneName, highRatio, lowRatio, goalRatio, 50, 10)
	if err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Second * 15)
		status, err := rw.ReBalanceStatus(cluster, zoneName)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
		fmt.Println(status)
		if status == 1 {
			break
		}

	}
}

func TestReBalanceStop(t *testing.T) {
	rw := ReBalanceWorker{}
	err := rw.ReBalanceStart(cluster, zoneName, highRatio, lowRatio, goalRatio, 50, 10)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 30)
	status, err := rw.ReBalanceStatus(cluster, zoneName)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
	fmt.Println(status)
	err = rw.ReBalanceStop(cluster, zoneName)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 5)
	status, err = rw.ReBalanceStatus(cluster, zoneName)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
	fmt.Println(status)
}

func TestReBalanceReStart(t *testing.T) {
	rw := ReBalanceWorker{}
	err := rw.ReBalanceStart(cluster, zoneName, highRatio, lowRatio, goalRatio, 50, 10)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 30)
	status, err := rw.ReBalanceStatus(cluster, zoneName)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
	fmt.Println(status)
	err = rw.ReBalanceStop(cluster, zoneName)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 5)
	status, err = rw.ReBalanceStatus(cluster, zoneName)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
	fmt.Println(status)
	err = rw.ReBalanceStart(cluster, zoneName, highRatio, lowRatio, goalRatio, 50, 10)
	if err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Second * 15)
		status, err := rw.ReBalanceStatus(cluster, zoneName)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
		fmt.Println(status)
		if status == 1 {
			break
		}

	}
}

func TestReBalanceDupStart(t *testing.T) {
	rw := ReBalanceWorker{}
	err := rw.ReBalanceStart(cluster, zoneName, highRatio, lowRatio, goalRatio, 50, 10)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second)
	err = rw.ReBalanceStart(cluster, zoneName, highRatio, lowRatio, goalRatio, 50, 10)
	if err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Second * 15)
		status, err := rw.ReBalanceStatus(cluster, zoneName)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
		fmt.Println(status)
		if status == 1 {
			break
		}

	}
}

func TestServer(t *testing.T) {
	rw := ReBalanceWorker{}
	rw.registerHandler()
	fmt.Println("listen at 18080")
	err := http.ListenAndServe(":18080", nil)
	if err != nil {
		t.Fatal(err)
	}
}
