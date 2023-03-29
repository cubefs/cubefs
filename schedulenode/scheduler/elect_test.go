package scheduler

import (
	"fmt"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"testing"
	"time"
)

const (
	username = ""
	password = ""
	host     = ""
	database = ""
	port     = 3306
)

func init() {
	mc := &config.MysqlConfig{
		Url:      host,
		Username: username,
		Password: password,
		Database: database,
		Port:     port,
	}
	if err := mysql.InitMysqlClient(mc); err != nil {
		fmt.Println(fmt.Sprintf("init mysql client failed, err(%v)", err))
		return
	}
}

func TestCandidate(t *testing.T) {
	ec := config.NewElectConfig(DefaultHeartbeat, DefaultLeaderPeriod, DefaultFollowerPeriod)
	candidateServerIP1 := "192.168.0.101"
	candidateServerIP2 := "192.168.0.102"
	candidateServer1 := NewCandidate(candidateServerIP1, ec)
	candidateServer2 := NewCandidate(candidateServerIP2, ec)
	defer func() {
		candidateServer1.Stop()
		candidateServer2.Stop()
	}()

	var err error
	err = candidateServer1.StartCampaign()
	if err != nil {
		t.Errorf(fmt.Sprintf("start compain server1(%s) failed cause: %s\n", candidateServerIP1, err.Error()))
	}
	err = candidateServer2.StartCampaign()
	if err != nil {
		t.Errorf(fmt.Sprintf("start compain server2(%s) failed cause: %s\n", candidateServerIP2, err.Error()))
	}

	if candidateServer1.IsLeader {
		fmt.Printf("candidate server1(%s) been elcted leader, term: %v\n", candidateServerIP1, candidateServer1.Term)
	} else {
		fmt.Printf("candidate server1(%s) is follower, term: %v\n", candidateServerIP1, candidateServer1.Term)
	}
	if candidateServer2.IsLeader {
		fmt.Printf("candidate server2(%s) been elcted leader, term: %v\n", candidateServerIP2, candidateServer2.Term)
	} else {
		fmt.Printf("candidate server2(%s) is follower, term: %v\n", candidateServerIP2, candidateServer2.Term)
	}

	le, err := mysql.GetLeader(ec.Heartbeat * ec.LeaderPeriod)
	if err != nil {
		t.Errorf(fmt.Sprintf("select leader failed cause: %s", err.Error()))
	}
	if le == nil {
		t.Fatalf(fmt.Sprintf("select leader is empty"))
	}
	fmt.Printf("leader: %v, term: %v\n", le.LeaderAddr, le.Term)

	var leader, follower *Candidate
	if le.LeaderAddr == candidateServerIP1 {
		leader = candidateServer1
		follower = candidateServer2
	}
	if le.LeaderAddr == candidateServerIP2 {
		leader = candidateServer2
		follower = candidateServer1
	}
	if leader == nil || follower == nil {
		fmt.Printf("current candidate all have been elected to be leader\n")
		return
	}
	leader.Stop()

	waitTimes := ec.Heartbeat * ec.FollowerPeriod * 3
	fmt.Printf("sleep for %v seconds to wait follower compain\n", waitTimes)
	time.Sleep(time.Second * time.Duration(waitTimes))
	fmt.Println("start to check is whether origin follower been elected leader")
	if !follower.IsLeader {
		fmt.Printf("origin follower have not been elected to be leader\n")
	}
	fmt.Printf("origin follower has been elected to be leader, new leader ip : %s\n", follower.LocalAddr)
	le, err = mysql.GetLeader(ec.Heartbeat * ec.LeaderPeriod)
	if err != nil {
		t.Errorf(fmt.Sprintf("select leader failed cause: %s", err.Error()))
	}
	if le == nil {
		t.Fatalf(fmt.Sprintf("select leader is empty"))
	}
	fmt.Printf("selected leader from database, leader ip : %s\n", le.LeaderAddr)
}
