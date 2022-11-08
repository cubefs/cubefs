package master

import (
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

type ServerFactorLimit struct {
	Name           string
	Type           uint32
	Total          uint64
	Buffer         uint64 // flowbuffer add with preallocate buffer equal with flowtotal
	CliUsed        uint64
	CliNeed        uint64
	Allocated      uint64
	NeedAfterAlloc uint64
	magnify        uint32 // for client allocation need magnify
	LimitRate      float32
	LastMagnify    uint64
	requestCh      chan interface{}
	done           chan interface{}
	qosManager     *QosCtrlManager
}

type ClientReportOutput struct {
	ID        uint64
	FactorMap map[uint32]*proto.ClientLimitInfo
	Host      string
	Status    uint8
}

type LimitOutput struct {
	ID            uint64
	Enable        bool
	ReqPeriod     uint32
	HitTriggerCnt uint8
	FactorMap     map[uint32]*proto.ClientLimitInfo
}

type ClientInfoOutput struct {
	Cli    *ClientReportOutput
	Assign *LimitOutput
	Time   time.Time
	ID     uint64
	Host   string
}

type ClientInfoMgr struct {
	Cli    *proto.ClientReportLimitInfo
	Assign *proto.LimitRsp2Client
	Time   time.Time
	ID     uint64
	Host   string
}

type qosRequestArgs struct {
	clientID       uint64
	factorType     uint32
	clientReq      *proto.ClientLimitInfo
	lastClientInfo *proto.ClientLimitInfo
	assignInfo     *proto.ClientLimitInfo
	rsp2Client     *proto.ClientLimitInfo
	wg             *sync.WaitGroup
}

type QosCtrlManager struct {
	cliInfoMgrMap        map[uint64]*ClientInfoMgr     // cientid->client_reportinfo&&assign_limitinfo
	serverFactorLimitMap map[uint32]*ServerFactorLimit // vol qos data for iops w/r and flow w/r
	defaultClientCnt     uint32
	qosEnable            bool
	ClientReqPeriod      uint32
	ClientHitTriggerCnt  uint32
	vol                  *Vol
	sync.RWMutex
}

func (qosManager *QosCtrlManager) volUpdateMagnify(magnifyArgs *qosArgs) {
	defer qosManager.Unlock()
	qosManager.Lock()

	log.LogWarnf("action[volUpdateMagnify] vol %v try set magnify iopsRVal[%v],iopsWVal[%v],flowRVal[%v],flowWVal[%v]",
		qosManager.vol.Name, magnifyArgs.iopsRVal, magnifyArgs.iopsWVal, magnifyArgs.flowRVal, magnifyArgs.flowWVal)

	arrMagnify := [4]uint64{magnifyArgs.iopsRVal, magnifyArgs.iopsWVal, magnifyArgs.flowRVal, magnifyArgs.flowWVal}
	for i := proto.IopsReadType; i <= proto.FlowWriteType; i++ {
		magnify := qosManager.serverFactorLimitMap[i].magnify
		if uint64(magnify) != arrMagnify[i-1] && arrMagnify[i-1] > 0 {
			qosManager.serverFactorLimitMap[i].magnify = uint32(arrMagnify[i-1])
			log.LogWarnf("action[volUpdateMagnify] vol %v  after update type [%v] magnify [%v] to [%v]",
				qosManager.vol.Name, proto.QosTypeString(i), magnify, arrMagnify[i-1])
		}
	}
}

func (qosManager *QosCtrlManager) volUpdateLimit(limitArgs *qosArgs) {
	defer qosManager.Unlock()
	qosManager.Lock()

	log.LogWarnf("action[volUpdateLimit] vol %v try set limit iopsrlimit[%v],iopswlimit[%v],flowrlimit[%v],flowwlimit[%v]",
		qosManager.vol.Name, limitArgs.iopsRVal, limitArgs.iopsWVal, limitArgs.flowRVal, limitArgs.flowWVal)

	//if limitArgs.iopsWVal != 0 {
	//	qosManager.serverFactorLimitMap[proto.IopsWriteType].Total = limitArgs.iopsWVal
	//	qosManager.serverFactorLimitMap[proto.IopsWriteType].LastMagnify = 0
	//}
	//if limitArgs.iopsRVal != 0 {
	//	qosManager.serverFactorLimitMap[proto.IopsReadType].Total = limitArgs.iopsRVal
	//	qosManager.serverFactorLimitMap[proto.IopsWriteType].LastMagnify = 0
	//}
	if limitArgs.flowWVal != 0 {
		qosManager.serverFactorLimitMap[proto.FlowWriteType].Total = limitArgs.flowWVal
		qosManager.serverFactorLimitMap[proto.IopsWriteType].LastMagnify = 0
		qosManager.serverFactorLimitMap[proto.FlowWriteType].Buffer = limitArgs.flowWVal
	}
	if limitArgs.flowRVal != 0 {
		qosManager.serverFactorLimitMap[proto.FlowReadType].Total = limitArgs.flowRVal
		qosManager.serverFactorLimitMap[proto.FlowReadType].LastMagnify = 0
		qosManager.serverFactorLimitMap[proto.FlowReadType].Buffer = limitArgs.flowRVal
	}

	for i := proto.IopsReadType; i <= proto.FlowWriteType; i++ {
		limitf := qosManager.serverFactorLimitMap[i]
		log.LogWarnf("action[volUpdateLimit] vol [%v] after set type [%v] [%v,%v,%v,%v]",
			qosManager.vol.Name, proto.QosTypeString(i), limitf.Allocated, limitf.NeedAfterAlloc, limitf.Total, limitf.Buffer)
	}
}

func (qosManager *QosCtrlManager) getQosMagnify(factorTYpe uint32) uint32 {
	return qosManager.serverFactorLimitMap[factorTYpe].magnify
}

func (qosManager *QosCtrlManager) getQosLimit(factorTYpe uint32) uint64 {
	return qosManager.serverFactorLimitMap[factorTYpe].Total
}

func (qosManager *QosCtrlManager) initClientQosInfo(clientID uint64, host string) (limitRsp2Client *proto.LimitRsp2Client, err error) {

	log.QosWriteDebugf("action[initClientQosInfo] vol %v clientID %v Host %v", qosManager.vol.Name, clientID, host)
	clientInitInfo := proto.NewClientReportLimitInfo()
	cliCnt := qosManager.defaultClientCnt
	if cliCnt <= proto.QosDefaultClientCnt {
		cliCnt = proto.QosDefaultClientCnt
	}
	if len(qosManager.cliInfoMgrMap) > int(cliCnt) {
		cliCnt = uint32(len(qosManager.cliInfoMgrMap))
	}

	limitRsp2Client = proto.NewLimitRsp2Client()
	limitRsp2Client.ID = clientID
	limitRsp2Client.Enable = qosManager.qosEnable

	factorType := proto.IopsReadType

	defer qosManager.Unlock()
	qosManager.Lock()

	for factorType <= proto.FlowWriteType {
		var initLimit uint64
		serverLimit := qosManager.serverFactorLimitMap[factorType]

		if qosManager.qosEnable {
			initLimit = serverLimit.Total / uint64(cliCnt)

			if serverLimit.Buffer > initLimit {
				serverLimit.Buffer -= initLimit
				serverLimit.Allocated += initLimit
			} else {
				initLimit = serverLimit.Buffer
				serverLimit.Allocated += initLimit
				serverLimit.Buffer = 0
			}
			if factorType == proto.FlowWriteType || factorType == proto.FlowReadType {
				if initLimit > 1*util.GB/8 {
					initLimit = 1 * util.GB / 8
				}
			} else {
				if initLimit > 200 {
					initLimit = 200
				}
			}
		}

		clientInitInfo.FactorMap[factorType] = &proto.ClientLimitInfo{
			UsedLimit:  initLimit,
			UsedBuffer: 0,
			Used:       0,
			Need:       0,
		}

		limitRsp2Client.Magnify[factorType] = serverLimit.magnify
		limitRsp2Client.FactorMap[factorType] = clientInitInfo.FactorMap[factorType]

		log.QosWriteDebugf("action[initClientQosInfo] vol [%v] clientID [%v] factorType [%v] init client info and set limitRsp2Client [%v]"+
			"server total[%v] used [%v] buffer [%v]",
			qosManager.vol.Name, clientID, proto.QosTypeString(factorType),
			initLimit, serverLimit.Total, serverLimit.Allocated, serverLimit.Buffer)
		factorType++
	}

	qosManager.cliInfoMgrMap[clientID] = &ClientInfoMgr{
		Cli:    clientInitInfo,
		Assign: limitRsp2Client,
		Time:   time.Now(),
		ID:     clientID,
		Host:   host,
	}
	log.QosWriteDebugf("action[initClientQosInfo] vol [%v] clientID [%v] Assign [%v]", qosManager.vol.Name, clientID, limitRsp2Client)
	return
}

func (serverLimit *ServerFactorLimit) String() string {
	return fmt.Sprintf("serverLimit {total:[%v],alloc:(allocated:[%v],need:[%v],buffer:[%v]),limit:(limitrate:[%v], magnify:[%v]),client sum {used:[%v], need:[%v]}}",
		serverLimit.Total, serverLimit.Allocated, serverLimit.NeedAfterAlloc, serverLimit.Buffer,
		serverLimit.LimitRate, serverLimit.LastMagnify,
		serverLimit.CliUsed, serverLimit.CliNeed)
}

func (serverLimit *ServerFactorLimit) getDstLimit(factorType uint32, used, need uint64) (dstLimit uint64) {
	if factorType == proto.FlowWriteType || factorType == proto.FlowReadType {
		if need > used {
			need = used
		}
		if (need + used) < 10*util.MB/8 {
			dstLimit = uint64(float64(need+used) * 2)
		} else if (need + used) < 50*util.MB/8 {
			dstLimit = uint64(float64(need+used) * 1.5)
		} else if (need + used) < 100*util.MB/8 {
			dstLimit = uint64(float64(need+used) * 1.2)
		} else if (need + used) < 1*util.GB/8 {
			dstLimit = uint64(float64(need+used) * 1.1)
		} else {
			dstLimit = uint64(float64(need+used) + 1*util.GB/8)
		}
	} else {
		if (need + used) < 100 {
			dstLimit = uint64(float64(need+used) * 2)
		} else if (need + used) < 500 {
			dstLimit = uint64(float64(need+used) * 1.5)
		} else if (need + used) < 1000 {
			dstLimit = uint64(float64(need+used) * 1.2)
		} else if (need + used) < 5000 {
			dstLimit = uint64(float64(need+used) * 1.2)
		} else {
			dstLimit = uint64(float64(need+used) + 1000)
		}
	}
	return
}

func (serverLimit *ServerFactorLimit) dispatch() {
	for {
		select {
		case request := <-serverLimit.requestCh:
			serverLimit.updateLimitFactor(request)
		case <-serverLimit.done:
			log.LogErrorf("done ServerFactorLimit type (%v)", serverLimit.Type)
			return
		}
	}
}

// handle client request and rsp with much more if buffer is enough according rules of allocate
func (serverLimit *ServerFactorLimit) updateLimitFactor(req interface{}) {

	request := req.(*qosRequestArgs)
	clientID := request.clientID
	factorType := request.factorType
	clientReq := request.clientReq
	assignInfo := request.assignInfo
	rsp2Client := request.rsp2Client
	lastClientInfo := request.lastClientInfo

	log.QosWriteDebugf("action[updateLimitFactor] vol [%v] clientID [%v] type [%v],client report [%v,%v,%v,%v] last client report [%v,%v,%v,%v] periodically cal Assign [%v,%v]",
		serverLimit.qosManager.vol.Name, clientID, proto.QosTypeString(factorType),
		clientReq.Used, clientReq.Need, clientReq.UsedLimit, clientReq.UsedBuffer,
		lastClientInfo.Used, lastClientInfo.Need, lastClientInfo.UsedLimit, lastClientInfo.UsedBuffer,
		assignInfo.UsedLimit, assignInfo.UsedBuffer)

	rsp2Client.UsedLimit = assignInfo.UsedLimit
	rsp2Client.UsedBuffer = assignInfo.UsedBuffer

	// flow limit and buffer not enough,client need more
	if (clientReq.Need + clientReq.Used) > (assignInfo.UsedLimit + assignInfo.UsedBuffer) {
		log.QosWriteDebugf("action[updateLimitFactor] vol [%v] clientID [%v] type [%v], need [%v] used [%v], used limit [%v]",
			serverLimit.qosManager.vol.Name, clientID, proto.QosTypeString(factorType), clientReq.Need, clientReq.Used, clientReq.UsedLimit)

		dstLimit := serverLimit.getDstLimit(factorType, clientReq.Used, clientReq.Need)

		// Assign already  allocated the buffer for client
		if dstLimit > assignInfo.UsedLimit+assignInfo.UsedBuffer {
			additionBuffer := dstLimit - assignInfo.UsedLimit - assignInfo.UsedBuffer
			// if buffer is available then balance must not effect, try use buffer as possible as can
			if serverLimit.Buffer > 0 {
				log.QosWriteDebugf("action[updateLimitFactor] vol [%v] clientID [%v] type [%v] client need more buffer [%v] serverlimit buffer [%v] used [%v]",
					serverLimit.qosManager.vol.Name, clientID, proto.QosTypeString(factorType),
					additionBuffer, serverLimit.Buffer, serverLimit.Allocated)

				// calc dst buffer for client to expand
				// ignore the case of s.used be zero.  used should large then 0 because dstLimit isn't zero and be part of s.used
				var dstUsedBuffer uint64
				if serverLimit.Allocated != 0 {
					dstUsedBuffer = uint64(float64(dstLimit) * (float64(serverLimit.Buffer) / float64(serverLimit.Allocated)) * 0.5)
					if dstUsedBuffer > dstLimit {
						dstUsedBuffer = dstLimit
					}
				} else {
					dstUsedBuffer = dstLimit
				}

				if assignInfo.UsedBuffer < dstUsedBuffer {
					additionBuffer = dstUsedBuffer - assignInfo.UsedBuffer
					if additionBuffer > serverLimit.Buffer {
						rsp2Client.UsedBuffer += serverLimit.Buffer
						assignInfo.UsedBuffer = rsp2Client.UsedBuffer
						serverLimit.Allocated += serverLimit.Buffer
						serverLimit.Buffer = 0
					} else {
						rsp2Client.UsedBuffer = dstUsedBuffer
						assignInfo.UsedBuffer = dstUsedBuffer
						serverLimit.Buffer -= additionBuffer
						serverLimit.Allocated += additionBuffer
					}
				}
			}
		}
	}
	log.QosWriteDebugf("action[updateLimitFactor] vol [%v] [clientID [%v] type [%v] rsp2Client.UsedLimit [%v], UsedBuffer [%v]",
		serverLimit.qosManager.vol.Name, clientID, proto.QosTypeString(factorType), rsp2Client.UsedLimit, rsp2Client.UsedBuffer)
	request.wg.Done()
}

func (qosManager *QosCtrlManager) init(cluster *Cluster, host string) (limit *proto.LimitRsp2Client, err error) {
	log.QosWriteDebugf("action[qosManage.init] vol [%v] Host %v", qosManager.vol.Name, host)
	var id uint64
	if id, err = cluster.idAlloc.allocateClientID(); err == nil {
		return qosManager.initClientQosInfo(id, host)
	}
	return
}

func (qosManager *QosCtrlManager) HandleClientQosReq(reqClientInfo *proto.ClientReportLimitInfo, clientID uint64) (limitRsp *proto.LimitRsp2Client, err error) {
	log.QosWriteDebugf("action[HandleClientQosReq] vol [%v] reqClientInfo from [%v], enable [%v]",
		qosManager.vol.Name, clientID, qosManager.qosEnable)

	qosManager.RLock()
	clientInfo, lastExist := qosManager.cliInfoMgrMap[clientID]
	if !lastExist || reqClientInfo == nil {
		qosManager.RUnlock()
		log.LogWarnf("action[HandleClientQosReq] vol [%v] id [%v] addr [%v] not exist", qosManager.vol.Name, clientID, reqClientInfo.Host)
		return qosManager.initClientQosInfo(clientID, reqClientInfo.Host)
	}
	qosManager.RUnlock()

	limitRsp = proto.NewLimitRsp2Client()
	limitRsp.Enable = qosManager.qosEnable
	limitRsp.ID = reqClientInfo.ID
	limitRsp.ReqPeriod = qosManager.ClientReqPeriod
	limitRsp.HitTriggerCnt = uint8(qosManager.ClientHitTriggerCnt)

	if !qosManager.qosEnable {
		clientInfo.Cli = reqClientInfo
		limitRsp.FactorMap = reqClientInfo.FactorMap
		clientInfo.Assign = limitRsp
		clientInfo.Time = time.Now()
		for i := proto.IopsReadType; i <= proto.FlowWriteType; i++ {
			reqClientInfo.FactorMap[i].UsedLimit = reqClientInfo.FactorMap[i].Used
			reqClientInfo.FactorMap[i].UsedBuffer = reqClientInfo.FactorMap[i].Need

			log.QosWriteDebugf("action[HandleClientQosReq] vol [%v] [%v,%v,%v,%v]", qosManager.vol.Name,
				reqClientInfo.FactorMap[i].Used,
				reqClientInfo.FactorMap[i].Need,
				reqClientInfo.FactorMap[i].UsedLimit,
				reqClientInfo.FactorMap[i].UsedBuffer)
		}
		return
	}
	index := 0
	wg := &sync.WaitGroup{}
	wg.Add(len(reqClientInfo.FactorMap))
	for factorType, clientFactor := range reqClientInfo.FactorMap {
		limitRsp.FactorMap[factorType] = &proto.ClientLimitInfo{}
		serverLimit := qosManager.serverFactorLimitMap[factorType]
		limitRsp.Magnify[factorType] = serverLimit.magnify

		request := &qosRequestArgs{
			clientID:       clientID,
			factorType:     factorType,
			clientReq:      clientFactor,
			lastClientInfo: clientInfo.Cli.FactorMap[factorType],
			assignInfo:     clientInfo.Assign.FactorMap[factorType],
			rsp2Client:     limitRsp.FactorMap[factorType],
			wg:             wg,
		}
		serverLimit.requestCh <- request
		index++
	}
	wg.Wait()

	clientInfo.Cli = reqClientInfo
	clientInfo.Assign = limitRsp
	clientInfo.Time = time.Now()

	return
}

func (qosManager *QosCtrlManager) updateServerLimitByClientsInfo(factorType uint32) {
	var (
		cliSum                      proto.ClientLimitInfo
		nextStageNeed, nextStageUse uint64
	)
	qosManager.RLock()
	serverLimit := qosManager.serverFactorLimitMap[factorType]

	log.QosWriteDebugf("action[updateServerLimitByClientsInfo] vol [%v] type [%v] last limitInfo(%v)",
		qosManager.vol.Name, proto.QosTypeString(factorType), serverLimit)

	// get sum of data from all clients reports
	for host, cliInfo := range qosManager.cliInfoMgrMap {
		cliFactor := cliInfo.Cli.FactorMap[factorType]
		cliSum.Used += cliFactor.Used
		cliSum.Need += cliFactor.Need
		cliSum.UsedLimit += cliFactor.UsedLimit
		cliSum.UsedBuffer += cliFactor.UsedBuffer
		log.QosWriteDebugf("action[updateServerLimitByClientsInfo] vol [%v] Host [%v] type [%v] used [%v] need [%v] limit [%v] buffer [%v]",
			qosManager.vol.Name, host, proto.QosTypeString(factorType),
			cliFactor.Used, cliFactor.Need, cliFactor.UsedLimit, cliFactor.UsedBuffer)
	}

	serverLimit.CliUsed = cliSum.Used
	serverLimit.CliNeed = cliSum.Need
	qosManager.RUnlock()

	if !qosManager.qosEnable {
		return
	}

	serverLimit.Buffer = 0
	nextStageUse = cliSum.Used
	nextStageNeed = cliSum.Need
	if serverLimit.Total >= nextStageUse {
		serverLimit.Buffer = serverLimit.Total - nextStageUse
		log.QosWriteDebugf("action[updateServerLimitByClientsInfo] vol [%v] reset server buffer [%v] all clients nextStageUse [%v]",
			qosManager.vol.Name, serverLimit.Buffer, nextStageUse)
		if nextStageNeed > serverLimit.Buffer {
			nextStageNeed -= serverLimit.Buffer
			nextStageUse += serverLimit.Buffer
			serverLimit.Buffer = 0
			log.QosWriteDebugf("action[updateServerLimitByClientsInfo] vol [%v] reset server buffer [%v] all clients nextStageNeed [%v] too nuch",
				qosManager.vol.Name, serverLimit.Buffer, nextStageNeed)
		} else {
			serverLimit.Buffer -= nextStageNeed
			log.QosWriteDebugf("action[updateServerLimitByClientsInfo] vol [%v] reset server buffer [%v] all clients nextStageNeed [%v]",
				qosManager.vol.Name, serverLimit.Buffer, nextStageNeed)
			nextStageUse += nextStageNeed
			nextStageNeed = 0
		}
	} else { // usage large than limitation
		log.QosWriteDebugf("action[updateServerLimitByClientsInfo] vol[%v] type [%v] clients needs [%v] plus overuse [%v],get nextStageNeed [%v]",
			qosManager.vol.Name, proto.QosTypeString(factorType), nextStageNeed, nextStageUse-serverLimit.Total,
			nextStageNeed+nextStageUse-serverLimit.Total)
		nextStageNeed += nextStageUse - serverLimit.Total
		nextStageUse = serverLimit.Total
	}

	serverLimit.Allocated = nextStageUse
	serverLimit.NeedAfterAlloc = nextStageNeed

	// get the limitRate,additionFlowNeed should be zero if total used can increase
	serverLimit.LimitRate = 0
	if serverLimit.NeedAfterAlloc > 0 {
		serverLimit.LimitRate = float32(float64(serverLimit.NeedAfterAlloc) / float64(serverLimit.Allocated+serverLimit.NeedAfterAlloc))

		log.QosWriteDebugf("action[updateServerLimitByClientsInfo] vol [%v] type [%v] alloc not enough need limitRatio serverLimit:(%v)",
			qosManager.vol.Name, proto.QosTypeString(factorType), serverLimit)

		lastMagnify := serverLimit.LastMagnify
		lastLimitRitio := serverLimit.LimitRate
		// master assigned limit and buffer not be used as expected,we need adjust the gap
		if serverLimit.CliUsed < serverLimit.Total {
			if serverLimit.LimitRate > -10.0 && serverLimit.LastMagnify < serverLimit.Total*10 {
				serverLimit.LastMagnify += uint64(float64(serverLimit.Total-serverLimit.CliUsed) * 0.1)
			}
		} else {
			if serverLimit.LastMagnify > 0 {
				var magnify uint64
				if serverLimit.LastMagnify > (serverLimit.CliUsed - serverLimit.Total) {
					magnify = serverLimit.CliUsed - serverLimit.Total
				} else {
					magnify = serverLimit.LastMagnify
				}
				serverLimit.LastMagnify -= uint64(float32(magnify) * 0.1)
			}
		}
		serverLimit.LimitRate = serverLimit.LimitRate * float32(1-float64(serverLimit.LastMagnify)/float64(serverLimit.Allocated+serverLimit.NeedAfterAlloc))
		log.QosWriteDebugf("action[updateServerLimitByClientsInfo] vol [%v] type [%v] limitRatio [%v] updated to limitRatio [%v] by magnify [%v] lastMagnify [%v]",
			qosManager.vol.Name, proto.QosTypeString(factorType),
			lastLimitRitio, serverLimit.LimitRate, serverLimit.LastMagnify, lastMagnify)
	} else {
		serverLimit.LastMagnify = 0
	}
	log.QosWriteDebugf("action[updateServerLimitByClientsInfo] vol [%v] type [%v] after adjust limitRatio serverLimit:(%v)",
		qosManager.vol.Name, proto.QosTypeString(factorType), serverLimit)
	return
}

func (qosManager *QosCtrlManager) assignClientsNewQos(factorType uint32) {
	qosManager.RLock()
	if !qosManager.qosEnable {
		return
	}
	serverLimit := qosManager.serverFactorLimitMap[factorType]
	var bufferAllocated uint64

	// recalculate client Assign limit and buffer
	for _, cliInfoMgr := range qosManager.cliInfoMgrMap {
		cliInfo := cliInfoMgr.Cli.FactorMap[factorType]
		assignInfo := cliInfoMgr.Assign.FactorMap[factorType]

		if cliInfo.Used+cliInfoMgr.Cli.FactorMap[factorType].Need == 0 {
			assignInfo.UsedLimit = 0
			assignInfo.UsedBuffer = 0
		} else {
			assignInfo.UsedLimit = uint64(float64(cliInfo.Used+cliInfo.Need) * float64(1-serverLimit.LimitRate))
			if serverLimit.Allocated != 0 {
				assignInfo.UsedBuffer = uint64(float64(serverLimit.Buffer) * (float64(assignInfo.UsedLimit) / float64(serverLimit.Allocated)) * 0.5)
			}

			// buffer left may be quit large and we should not used up and doen't mean if buffer large than used limit line
			if assignInfo.UsedBuffer > assignInfo.UsedLimit {
				assignInfo.UsedBuffer = assignInfo.UsedLimit
			}
		}

		bufferAllocated += assignInfo.UsedBuffer
	}

	qosManager.RUnlock()

	if serverLimit.Buffer > bufferAllocated {
		serverLimit.Buffer -= bufferAllocated
	} else {
		serverLimit.Buffer = 0
		log.LogWarnf("action[assignClientsNewQos] vol [%v] type [%v] clients buffer [%v] and server buffer used up trigger flow limit overall",
			qosManager.vol.Name, proto.QosTypeString(factorType), bufferAllocated)
	}

	log.QosWriteDebugf("action[assignClientsNewQos] vol [%v]  type [%v] serverLimit buffer:[%v] used:[%v] need:[%v] total:[%v]",
		qosManager.vol.Name, proto.QosTypeString(factorType),
		serverLimit.Buffer, serverLimit.Allocated, serverLimit.NeedAfterAlloc, serverLimit.Total)
}

func (vol *Vol) checkQos() {
	vol.qosManager.Lock()
	// check expire client and delete from map
	tTime := time.Now()
	for id, cli := range vol.qosManager.cliInfoMgrMap {
		if cli.Time.Add(20 * time.Second).Before(tTime) {
			log.LogWarnf("action[checkQos] vol [%v] Id [%v] addr [%v] be delete in case of long time no request",
				vol.Name, id, cli.Host)
			delete(vol.qosManager.cliInfoMgrMap, id)
		}
	}

	vol.qosManager.Unlock()

	// periodically updateServerLimitByClientsInfo and get assigned limit info for all clients
	// with last report info from client and qos control info
	for factorType := proto.IopsReadType; factorType <= proto.FlowWriteType; factorType++ {
		// calc all clients and get real used and need value , used value should less then total
		vol.qosManager.updateServerLimitByClientsInfo(factorType)
		// update client assign info by result above
		if !vol.qosManager.qosEnable {
			continue
		}

		vol.qosManager.assignClientsNewQos(factorType)

		serverLimit := vol.qosManager.serverFactorLimitMap[factorType]
		log.QosWriteDebugf("action[UpdateAllQosInfo] vol name [%v] type [%v] after updateServerLimitByClientsInfo get limitRate:[%v] "+
			"server total [%v] beAllocated [%v] NeedAfterAlloc [%v] buffer [%v]",
			vol.Name, proto.QosTypeString(factorType), serverLimit.LimitRate,
			serverLimit.Total, serverLimit.Allocated, serverLimit.NeedAfterAlloc, serverLimit.Buffer)
	}
}

func (vol *Vol) getQosStatus(cluster *Cluster) interface{} {

	type qosStatus struct {
		ServerFactorLimitMap map[uint32]*ServerFactorLimit // vol qos data for iops w/r and flow w/r
		QosEnable            bool
		ClientReqPeriod      uint32
		ClientHitTriggerCnt  uint32
		ClusterMaxUploadCnt  uint32
		ClientALiveCnt       int
	}
	vol.qosManager.RLock()
	defer vol.qosManager.RUnlock()

	return &qosStatus{
		ServerFactorLimitMap: map[uint32]*ServerFactorLimit{
			proto.FlowReadType:  vol.qosManager.serverFactorLimitMap[proto.FlowReadType],
			proto.FlowWriteType: vol.qosManager.serverFactorLimitMap[proto.FlowWriteType],
		},
		QosEnable:           vol.qosManager.qosEnable,
		ClientReqPeriod:     vol.qosManager.ClientReqPeriod,
		ClientHitTriggerCnt: vol.qosManager.ClientHitTriggerCnt,
		ClusterMaxUploadCnt: uint32(cluster.QosAcceptLimit.Limit()),
		ClientALiveCnt:      len(vol.qosManager.cliInfoMgrMap),
	}
}

func (vol *Vol) getClientLimitInfo(id uint64, ip string) (interface{}, error) {
	log.QosWriteDebugf("action[getClientLimitInfo] vol [%v] id [%v] ip [%v]", vol.Name, id, ip)
	vol.qosManager.RLock()
	defer vol.qosManager.RUnlock()

	assignFuc := func(info *ClientInfoMgr) (rspInfo *ClientInfoOutput) {
		rspInfo = &ClientInfoOutput{
			Cli: &ClientReportOutput{
				ID:        info.Cli.ID,
				Status:    info.Cli.Status,
				FactorMap: make(map[uint32]*proto.ClientLimitInfo, 0),
			},
			Assign: &LimitOutput{
				ID:            info.Assign.ID,
				Enable:        info.Assign.Enable,
				ReqPeriod:     info.Assign.ReqPeriod,
				HitTriggerCnt: info.Assign.HitTriggerCnt,
				FactorMap:     make(map[uint32]*proto.ClientLimitInfo, 0),
			},
			Time: info.Time,
			Host: info.Host,
			ID:   info.ID,
		}

		rspInfo.Cli.FactorMap[proto.FlowReadType] = info.Cli.FactorMap[proto.FlowReadType]
		rspInfo.Cli.FactorMap[proto.FlowWriteType] = info.Cli.FactorMap[proto.FlowWriteType]

		rspInfo.Assign.FactorMap[proto.FlowReadType] = info.Assign.FactorMap[proto.FlowReadType]
		rspInfo.Assign.FactorMap[proto.FlowWriteType] = info.Assign.FactorMap[proto.FlowWriteType]

		return
	}

	if id > 0 {
		if info, ok := vol.qosManager.cliInfoMgrMap[id]; ok {
			if len(ip) > 0 && info.Host != ip {
				return nil, fmt.Errorf("ip info [%v] not equal with request [%v]", info.Host, ip)
			}
			return assignFuc(info), nil
		}
	} else {
		var resp []*ClientInfoOutput
		for _, info := range vol.qosManager.cliInfoMgrMap {
			// http connection port  from client will change time by time,so ignore port here
			rspInfo := assignFuc(info)
			if len(ip) != 0 {
				if info.Host == ip {
					resp = append(resp, rspInfo)
				}
			} else {
				resp = append(resp, rspInfo)
			}
		}
		if len(resp) > 0 {
			return resp, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (vol *Vol) volQosEnable(c *Cluster, enable bool) error {
	log.LogWarnf("action[qosEnable] vol %v, set qos enable [%v], qosmgr[%v]", vol.Name, enable, vol.qosManager)
	vol.qosManager.qosEnable = enable
	vol.qosManager.Lock()
	defer vol.qosManager.Unlock()

	if !enable {
		for _, limit := range vol.qosManager.cliInfoMgrMap {
			for factorType := proto.IopsReadType; factorType <= proto.FlowWriteType; factorType++ {
				limit.Assign.FactorMap[factorType] = &proto.ClientLimitInfo{}
			}
		}
	}
	return c.syncUpdateVol(vol)
}

func (vol *Vol) updateClientParam(c *Cluster, period, triggerCnt uint64) error {
	vol.qosManager.ClientHitTriggerCnt = uint32(triggerCnt)
	vol.qosManager.ClientReqPeriod = uint32(period)
	return c.syncUpdateVol(vol)
}

func (vol *Vol) volQosUpdateMagnify(c *Cluster, magnifyArgs *qosArgs) error {
	vol.qosManager.volUpdateMagnify(magnifyArgs)
	return c.syncUpdateVol(vol)
}

func (vol *Vol) volQosUpdateLimit(c *Cluster, limitArgs *qosArgs) error {
	vol.qosManager.volUpdateLimit(limitArgs)
	return c.syncUpdateVol(vol)
}
