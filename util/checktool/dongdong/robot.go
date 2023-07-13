package dongdong

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

const (
	DDDomain   = "open.timline.jd.com"
	DDKey      = "00_312f283d5f0f4dde"
	DDSecret   = "466801e6a4a64351b1586e0549005f11"
	MsgExpires = 60 * 5 // 5 min
)

var (
	msgPool = &sync.Pool{New: func() interface{} {
		return new(Msg)
	}}
)

type DDRobot struct {
	app     string
	api     *DDAPI
	msgChan chan *Msg
	storage *FileStorage
}

func NewChinaDDRobot(app string) (robot *DDRobot, err error) {
	HostName, err = GetLocalIpAddr()
	if err != nil {
		log.LogError(err.Error())
		return
	}

	robot = new(DDRobot)
	robot.app = app
	robot.msgChan = make(chan *Msg, 1024*32)

	robot.api = NewDDAPI(DDDomain, DDKey, DDSecret, "ee")
	robot.storage, err = newStorage(app, robot.api)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	go robot.asyncSendMsg()
	return
}

func (r *DDRobot) SendGroupMsg(gid int, msg string) (err error) {
	m := msgPool.Get().(*Msg)
	m.Gid = gid
	m.MType = MsgTypeGroup
	m.Data = msg
	m.TS = time.Now().Unix()
	select {
	case r.msgChan <- m:
	default:
		err = r.storage.store(m)
		if err != nil {
			log.LogWarnf("[SendGroupMsg], failed to store msg: %v", m)
		}
		msgPool.Put(m)
	}
	return
}

func (r *DDRobot) SendERPMsg(erp, msg string) (err error) {
	m := msgPool.Get().(*Msg)
	m.ERP = erp
	m.MType = MsgTypeERP
	m.Data = msg
	m.TS = time.Now().Unix()
	select {
	case r.msgChan <- m:
	default:
		err = r.storage.store(m)
		if err != nil {
			log.LogWarnf("[SendERPMsg], failed to store msg: %v", m)
		}
		msgPool.Put(m)
	}
	return
}

func (r *DDRobot) asyncSendMsg() {
	var err error
	for {
		select {
		case msg := <-r.msgChan:
			if msg == nil {
				continue
			}
			switch msg.MType {
			case MsgTypeGroup:
				err = r.api.SendMsgToGroup(msg.Gid, msg.Data)
			case MsgTypeERP:
				err = r.api.SendMsgToERP(msg.ERP, msg.Data)
			default:
				log.LogErrorf("msg: %v,  type is invalid", msg)
			}
			if err != nil {
				log.LogWarnf("failed to send msg to ERP, err: %v, msg: %v", err.Error(), msg.Data)
				err = r.storage.store(msg)
				if err != nil {
					log.LogErrorf("failed to store msg: %v, err: %v", msg.Data, err.Error())
				}
			}
			msgPool.Put(msg)
		}
	}
}

type MsgType uint8

const (
	MsgTypeERP   = 0x01
	MsgTypeGroup = 0x02
)

type Msg struct {
	Data  string  `json:"data"`
	MType MsgType `json:"type"`
	Gid   int     `json:"gid"`
	ERP   string  `json:"erp"`
	TS    int64   `json:"ts"`
}

func (m *Msg) String() string {
	return fmt.Sprintf("type:%v, gid: %v, erp: %v, ts: %v, data: %v", m.MType, m.Gid, m.ERP, m.TS, m.Data)
}

func (m *Msg) marshal() (data []byte, err error) {
	data, err = json.Marshal(m)
	return
}

func (m *Msg) isExpires() bool {
	return (time.Now().Unix() - m.TS) > MsgExpires
}

func unmarshalMsg(data []byte) (msg *Msg, err error) {
	msg = new(Msg)
	err = json.Unmarshal(data, msg)
	return
}
