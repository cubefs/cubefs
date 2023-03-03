package scheduler

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

type Candidate struct {
	LocalAddr       string
	LeaderAddr      string
	IsLeader        bool
	Term            uint64 // 如果当前节点是leader，term 表示当前leader真实的term值，如果是Follower term值无效，不具备参考意义
	HeartBeat       int    // 单次心跳时长, unit: second
	LeaderPeriod    int    // Leader降级心跳周期，心跳的倍数，如果在该周期内leader连续更新心跳失败，降级为Follower
	FollowerPeriod  int    // Follower抢占心跳周期，心跳的倍数，如果在该周期内Follower没有检测到Leader，开始竞选Leader
	IdentityChanged bool
	StopC           chan struct{}
	HeartbeatC      chan struct{}
}

func NewCandidate(localIp string, ec *config.ElectConfig) (v *Candidate) {
	return &Candidate{
		LocalAddr:       localIp,
		HeartBeat:       ec.Heartbeat,
		LeaderPeriod:    ec.LeaderPeriod,
		FollowerPeriod:  ec.FollowerPeriod,
		IdentityChanged: false,
		StopC:           make(chan struct{}, 1),
		HeartbeatC:      make(chan struct{}, 1),
	}
}

func (c *Candidate) StartCampaign() (err error) {
	var (
		term     uint64
		le       *proto.LeaderElect
		isLeader bool
	)
	if le, err = mysql.GetLeader(c.HeartBeat * c.LeaderPeriod); err != nil {
		log.LogErrorf("[StartCampaign] get leader info from database failed, err(%v)", err)
		return
	}

	// leader exist, and leader is efficient
	// if local ip is leader ip, current node start to be leader, otherwise start to be follower
	if le != nil {
		log.LogInfof("[StartCampaign] leader exist and leader is efficient, term(%v), leaderIp(%v), updateTime(%v), localIp(%v)",
			le.Term, le.LeaderAddr, le.UpdateTime, c.LocalAddr)

		if le.LeaderAddr == c.LocalAddr {
			if err = c.StartLeader(le.Term); err != nil {
				log.LogErrorf("[StartCampaign] leader is valid, start current node as leader failed, term(%v), leaderIp(%v), localIp(%v) error(%v)",
					le.Term, le.LeaderAddr, c.LocalAddr, err)
				return
			}
			log.LogInfof("[StartCampaign] start current node as leader success, term(%v) localIp(%v)", le.Term, c.LocalAddr)
		} else {
			if err = c.StartFollower(true); err != nil {
				log.LogErrorf("[StartCampaign] start current node as follower failed, term(%v), leaderIp(%v), localIp(%v), error(%v)",
					le.Term, le.LeaderAddr, c.LocalAddr, err)
			}
			log.LogInfof("[StartCampaign] start current node as follower success, term(%v) localIp(%v)", le.Term, c.LocalAddr)
		}
		return
	}

	// got no leader or leader has expired, start to campaign leader
	term, isLeader, err = c.CampaignLeader()
	log.LogInfof("[StartCampaign] campaign leader result. term(%v), isLeader(%v), localIp(%v), err(%v)", term, isLeader, c.LocalAddr, err)
	if err == nil && isLeader {
		if err = c.StartLeader(term); err != nil {
			log.LogErrorf("[StartCampaign] start current node as leader failed, term(%v) err(%v)", term, err)
			return
		}
		log.LogInfof("[StartCampaign] current node campaign to be leader. term(%v)", term)
		return
	}

	// start as follower
	if err != nil {
		log.LogErrorf("[StartCampaign] campaign leader failed, term(%v), isLeader(%v), localIp(%v), err(%v)", term, isLeader, c.LocalAddr, err)
	}
	if err = c.StartFollower(true); err != nil {
		log.LogErrorf("[StartCampaign] start current node as follower failed, err(%v)", err)
		return
	}
	log.LogInfof("[StartCampaign] other node campaign to be leader.")
	return
}

func (c *Candidate) CampaignLeader() (tn uint64, isLeader bool, err error) {
	var th uint64

	if th, err = mysql.GetMaxTerm(); err != nil {
		log.LogErrorf("[CampaignLeader] get current max term failed, localIp(%v), err(%v)", c.LocalAddr, err)
		return 0, false, err
	}
	tn = atomic.AddUint64(&th, 1)
	eln := &proto.LeaderElect{
		Term:       tn,
		LeaderAddr: c.LocalAddr,
	}
	if isLeader, err = mysql.AddElectTerm(eln); err != nil {
		log.LogErrorf("[CampaignLeader] campaign to be leader failed, isLeader(%v) err(%v)", isLeader, err)
		return 0, false, err
	}
	if isLeader {
		return tn, true, nil
	}
	return
}

func (c *Candidate) StartLeader(term uint64) (err error) {
	c.IsLeader = true
	c.Term = term
	c.LeaderAddr = c.LocalAddr
	c.changeIdentity()
	go c.sendLeaderHeartBeat()
	return
}

// 在Follower启动时不能将 IdentityChanged 置为true，这样scheduler会调用stopScheduler方法，然后close identityChannel，
// 在下次成为leader时，会监听identityStopC，导致成为leader之后将相应的任务都终止掉，所以修改为Follower启动时不再将IdentityChanged置为true
func (c *Candidate) StartFollower(init bool) (err error) {
	c.IsLeader = false
	if !init {
		c.changeIdentity()
	}
	go c.sendFollowerHeartBeat()
	return
}

func (c *Candidate) sendLeaderHeartBeat() {
	var tq uint64
	var counter int

	heartBeatDuration := time.Second * time.Duration(c.HeartBeat)
	timer := time.NewTimer(heartBeatDuration)
	for {
		log.LogDebugf("[sendLeaderHeartBeat] leader heartbeat is running")
		select {
		case <-timer.C:

			heartbeat := func() (err error) {
				metrics := exporter.NewModuleTP(proto.MonitorSchedulerLeaderHeartbeat)
				defer metrics.Set(err)

				// if current node is not leader or term is less then zero, exit to wait next period
				if !c.IsLeader || c.Term <= 0 {
					log.LogErrorf("[sendLeaderHeartBeat] current node is not leader or term is invalid, localIp(%v), isLeader(%v), term(%v)",
						c.LocalAddr, c.IsLeader, c.Term)
					return errors.New("current node is not leader")
				}

				// get max term from elect table
				if tq, err = mysql.GetMaxTerm(); err != nil {
					log.LogWarnf("[sendLeaderHeartBeat] get max term from database failed, localIp(%v), term(%v), counter(%v), err(%v)",
						c.LocalAddr, c.Term, counter, err)
					counter++
					goto updateFailed
				} else {
					counter = 0
				}

				// get max term is more then current node leader term, it indicates that have new leader, current node be demoted to follower
				if tq > c.Term {
					if err = c.StartFollower(false); err != nil {
						log.LogErrorf("[sendLeaderHeartBeat] demote to follower failed, currentTerm(%v), getMaxTerm(%v), localIp(%v), err(%v)",
							c.Term, tq, c.LocalAddr, err)
						return nil
					}
					c.HeartbeatC <- struct{}{}
					log.LogInfof("[sendLeaderHeartBeat] get max term from database greater then current node term, be demoted to follower, currentTerm(%v), getMaxTerm(%v), localIp(%v)",
						c.Term, tq, c.LocalAddr)
					return errors.New("other node term more then current node")
				}

				// update leader heartbeat via localIp and current term, if update failed, counter plus one,
				// if counter more then N(specified from config), current node was demoted to follower
				if err = mysql.UpdateLeaderHeartbeat(c.Term, c.LocalAddr); err != nil {
					log.LogWarnf("[sendLeaderHeartBeat] update leader heartbeat failed, term(%v) localIp(%v), err(%v)", c.Term, c.LocalAddr, err)
					counter++
				} else {
					counter = 0
				}

			updateFailed:
				if counter > c.LeaderPeriod {
					if err = c.StartFollower(false); err != nil {
						log.LogErrorf("[sendLeaderHeartBeat] demote to follower failed cause leader heartbeat failed too many times, term(%v), localIp(%v), counter(%v), leaderPeriod(%v), err(%v)",
							c.Term, c.LocalAddr, counter, c.LeaderPeriod, err)
						return nil
					}
					c.HeartbeatC <- struct{}{}
					log.LogWarnf("[sendLeaderHeartBeat] leader heartbeat failed too many times, demoted to follower, term(%v), localIp(%v), counter(%v), leaderPeriod(%v)",
						c.Term, c.LocalAddr, counter, c.LeaderPeriod)
					return errors.New("demoted to follower")
				}
				return
			}

			if err := heartbeat(); err != nil {
				log.LogErrorf("[sendLeaderHeartBeat] leader heartbeat has exception, error(%v)", err)
			}
			timer.Reset(heartBeatDuration)
		case <-c.HeartbeatC:
			timer.Stop()
			log.LogInfof("stop leader heartbeat via heartbeat channel")
			return
		case <-c.StopC:
			timer.Stop()
			log.LogInfof("stop leader heartbeat")
			return
		}
	}
}

func (c *Candidate) sendFollowerHeartBeat() {
	heartBeatDuration := time.Second * time.Duration(c.HeartBeat*c.LeaderPeriod)
	timer := time.NewTimer(heartBeatDuration)
	for {
		log.LogDebugf("[sendFollowerHeartBeat] follower heartbeat...")
		select {
		case <-timer.C:
			heartbeat := func() (err error) {
				metrics := exporter.NewModuleTP(proto.MonitorSchedulerFollowerHeartbeat)
				defer metrics.Set(err)
				le, err := mysql.GetLeader(c.HeartBeat * c.LeaderPeriod)
				if err != nil {
					log.LogErrorf("[sendFollowerHeartBeat] get leader from database failed or no leader, localIp(%v), err(%v)", c.LocalAddr, err)
					return
				}
				if le == nil {
					term, isLeader, err := c.CampaignLeader()
					log.LogInfof("[sendFollowerHeartBeat] campaign leader result, term(%v), isLeader(%v), err(%v), localIp(%v)",
						term, isLeader, err, c.LocalAddr)
					if err == nil && isLeader {
						if err = c.StartLeader(term); err != nil {
							log.LogErrorf("[sendFollowerHeartBeat] start current node as leader failed, term(%v), err(%v), localIp(%v), err(%v)",
								term, err, c.LocalAddr, err)
						} else {
							c.HeartbeatC <- struct{}{}
							log.LogInfof("[sendFollowerHeartBeat] current node campaign to be leader, term(%v), isLeader(%v), localIp(%v)",
								term, isLeader, c.LocalAddr)
							return errors.New("current node campaign to be leader")
						}
					}
				}
				c.LeaderAddr = le.LeaderAddr
				return
			}

			if err := heartbeat(); err != nil {
				log.LogErrorf("[sendFollowerHeartBeat] follower heartbeat has exception, err(%v)", err)
			}
			timer.Reset(heartBeatDuration)
		case <-c.HeartbeatC:
			timer.Stop()
			log.LogInfof("stop follower heartbeat via heartbeat channel")
			return
		case <-c.StopC:
			timer.Stop()
			log.LogInfof("stop follower heartbeat")
			return
		}
	}
}

func (c *Candidate) Stop() {
	c.StopC <- struct{}{}
}

func (c *Candidate) changeIdentity() {
	c.IdentityChanged = true
}

func (c *Candidate) ResetIdentityChange() {
	c.IdentityChanged = false
}
