package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

func (s *ChubaoFSMonitor) scheduleToCheckMasterNodesAlive() {
	s.checkMasterNodesAlive()
	for {
		t := time.NewTimer(time.Duration(s.scheduleInterval) * time.Second)
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.checkMasterNodesAlive()
		}
	}
}

func (s *ChubaoFSMonitor) checkMasterNodesAlive() {
	faultMasterNodesMap := make(map[string][]string)
	cControl := common.NewConcurrencyControl(10)
	for domain, masterNodes := range s.chubaoFSMasterNodes {
		for _, addr := range masterNodes {
			cControl.Add()
			checkMasterNodesWg.Add(1)
			go func(domain string, addr string) {
				defer func() {
					cControl.Realse()
					checkMasterNodesWg.Done()
				}()
				checknodeAlive(domain, addr, faultMasterNodesMap)
			}(domain, addr)
		}
	}
	checkMasterNodesWg.Wait()
	umpWarn(faultMasterNodesMap)
}

func checknodeAlive(domain string, addr string, faultMap map[string][]string) {
	if isPhysicalMachineFailure(addr) {
		masterNodesMutex.Lock()
		if faultNodes, ok := faultMap[domain]; ok {
			faultMap[domain] = append(faultNodes, addr)
		} else {
			faultMap[domain] = []string{addr}
		}
		masterNodesMutex.Unlock()
	}
}

func umpWarn(faultMap map[string][]string) {
	faultInfo := &FaultMasterNodesInfo{}
	for domain, faultNodes := range faultMap {
		faultInfo.FaultCount += len(faultNodes)
		faultInfo.FaultMsg += fmt.Sprintf(" %s:%v ", domain, faultNodes)
	}
	if faultInfo.FaultMsg != "" {
		msg := fmt.Sprintf("action[checkCFSMasterNodesAlive] connection failed, faultCount[%d], faultNodes[%s]", faultInfo.FaultCount, faultInfo.FaultMsg)
		fmt.Println(msg)
		checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	} else {
		log.LogInfof("action[checkCFSMasterNodesAlive] complete, the results were normal.")
	}
}
