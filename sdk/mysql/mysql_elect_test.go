package mysql

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	mc := &config.MysqlConfig{
		Url:      host,
		Username: username,
		Password: password,
		Database: database,
		Port:     port,
	}
	if err := InitMysqlClient(mc); err != nil {
		fmt.Println(fmt.Sprintf("init mysql client failed, err(%v)", err))
		return
	}
}

func TestElect(t *testing.T) {
	// get current leader
	var (
		err     error
		term    uint64
		le      *proto.LeaderElect
		expire  = 20 // unit: second
		localIp = "10.18.109.101"
	)
	le, err = GetLeader(expire)
	if err != nil {
		t.Fatalf(fmt.Sprintf("get leader failed: %v", err.Error()))
	}
	if le != nil {
		fmt.Println(fmt.Sprintf("current leader exist, leaderIp(%v), term(%v), updateTime(%v): ", le.LeaderAddr, le.Term, le.UpdateTime))
	}
	// get max term
	term, err = GetMaxTerm()
	if err != nil {
		t.Fatalf(fmt.Sprintf("get max term failed: %v", err.Error()))
	}
	// compute new leader
	termNew := atomic.AddUint64(&term, 1)

	// add new leader
	elect := &proto.LeaderElect{
		Term:       termNew,
		LeaderAddr: localIp,
	}
	isLeader, err := AddElectTerm(elect)
	if err != nil {
		t.Fatalf(fmt.Sprintf("add new leader failed: %v", err.Error()))
	}
	if !isLeader {
		fmt.Println(fmt.Sprintf("current node campaign leader failed. isLeader: %v", isLeader))
		return
	}
	// send heartbeat
	fmt.Println(fmt.Sprintf("current node campaign leader success. isLeader: %v", isLeader))
	time.Sleep(5 * time.Second)
	fmt.Println("finished sleep")
	if err := UpdateLeaderHeartbeat(term, localIp); err != nil {
		t.Fatalf(fmt.Sprintf("update leader heartbeat failed: %v", err.Error()))
	}
	le, err = GetLeader(expire)
	if err != nil {
		t.Fatalf(fmt.Sprintf("get leader failed: %v", err.Error()))
	}
	if le == nil || le.Term != termNew {
		t.Fatalf(fmt.Sprintf("leader term is invalid"))
	}
	if time.Now().Unix()-le.UpdateTime.Unix() >= 5 {
		t.Fatalf("heartbeat update failed")
	}
}
