// Copyright 2018 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package meta

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/auth"
	"github.com/chubaofs/chubaofs/util/btree"
	"github.com/chubaofs/chubaofs/util/cryptoutil"
	"github.com/chubaofs/chubaofs/util/errors"
	cfslog "github.com/chubaofs/chubaofs/util/log"
)

const (
	HostsSeparator                = ","
	RefreshMetaPartitionsInterval = time.Minute * 5
	GetTicketMaxRetry             = 5
	GetTicketSleepInterval        = 100 * time.Millisecond
)

const (
	statusUnknown int = iota
	statusOK
	statusExist
	statusNoent
	statusFull
	statusAgain
	statusError
	statusInval
	statusNotPerm
)

const (
	MaxMountRetryLimit = 5
	MountRetryInterval = time.Second * 5
)

type MetaWrapper struct {
	sync.RWMutex
	cluster string
	localIP string
	volname string
	owner   string
	master  util.MasterHelper
	conns   *util.ConnectPool

	// Partitions and ranges should be modified together. So do not
	// use partitions and ranges directly. Use the helper functions instead.

	// Partition map indexed by ID
	partitions map[uint64]*MetaPartition

	// Partition tree indexed by Start, in order to find a partition in which
	// a specific inode locate.
	ranges *btree.BTree

	rwPartitions []*MetaPartition
	epoch        uint64

	totalSize uint64
	usedSize  uint64

	authenticate bool
	Ticket       Ticket
	accessToken  proto.APIAccessReq
	sessionKey   string
	ticketMess   auth.TicketMess
}

//the ticket from authnode
type Ticket struct {
	ID         string `json:"client_id"`
	SessionKey string `json:"session_key"`
	ServiceID  string `json:"service_id"`
	Ticket     string `json:"ticket"`
}

func NewMetaWrapper(volname, owner, masterHosts string, authenticate bool, ticketMess auth.TicketMess) (*MetaWrapper, error) {
	mw := new(MetaWrapper)
	if authenticate {
		ticket, err := getTicketFromAuthnode(owner, ticketMess)
		if err != nil {
			return nil, errors.Trace(err, "Get ticket from authnode failed!")
		}
		mw.authenticate = authenticate
		mw.accessToken.Ticket = ticket.Ticket
		mw.accessToken.ClientID = owner
		mw.accessToken.ServiceID = proto.MasterServiceID
		mw.sessionKey = ticket.SessionKey
		mw.ticketMess = ticketMess
	}
	mw.volname = volname
	mw.owner = owner
	master := strings.Split(masterHosts, HostsSeparator)
	mw.master = util.NewMasterHelper()
	for _, ip := range master {
		mw.master.AddNode(ip)
	}
	mw.conns = util.NewConnectPool()
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	mw.rwPartitions = make([]*MetaPartition, 0)
	mw.updateClusterInfo()
	mw.updateVolStatInfo()

	limit := MaxMountRetryLimit
retry:
	if err := mw.updateMetaPartitions(); err != nil {
		if limit <= 0 {
			return nil, errors.Trace(err, "Init meta wrapper failed!")
		} else {
			limit--
			time.Sleep(MountRetryInterval)
			goto retry
		}

	}

	go mw.refresh()
	return mw, nil
}

func (mw *MetaWrapper) Cluster() string {
	return mw.cluster
}

func (mw *MetaWrapper) LocalIP() string {
	return mw.localIP
}

func (mw *MetaWrapper) exporterKey(act string) string {
	return fmt.Sprintf("%s_sdk_meta_%s", mw.cluster, act)
}

// Proto ResultCode to status
func parseStatus(result uint8) (status int) {
	switch result {
	case proto.OpOk:
		status = statusOK
	case proto.OpExistErr:
		status = statusExist
	case proto.OpNotExistErr:
		status = statusNoent
	case proto.OpInodeFullErr:
		status = statusFull
	case proto.OpAgain:
		status = statusAgain
	case proto.OpArgMismatchErr:
		status = statusInval
	case proto.OpNotPerm:
		status = statusNotPerm
	default:
		status = statusError
	}
	return
}

func statusToErrno(status int) error {
	switch status {
	case statusOK:
		// return error anyway
		return syscall.EAGAIN
	case statusExist:
		return syscall.EEXIST
	case statusNoent:
		return syscall.ENOENT
	case statusFull:
		return syscall.ENOMEM
	case statusAgain:
		return syscall.EAGAIN
	case statusInval:
		return syscall.EINVAL
	case statusNotPerm:
		return syscall.EPERM
	case statusError:
		return syscall.EPERM
	default:
	}
	return syscall.EIO
}

func getTicketFromAuthnode(owner string, ticketMess auth.TicketMess) (ticket Ticket, err error) {
	var (
		key      []byte
		ts       int64
		msgResp  proto.AuthGetTicketResp
		body     []byte
		urlProto string
		url      string
		client   *http.Client
	)

	key, err = cryptoutil.Base64Decode(ticketMess.ClientKey)
	if err != nil {
		return
	}
	// construct request body
	message := proto.AuthGetTicketReq{
		Type:      proto.MsgAuthTicketReq,
		ClientID:  owner,
		ServiceID: proto.MasterServiceID,
	}

	if message.Verifier, ts, err = cryptoutil.GenVerifier(key); err != nil {
		return
	}

	if ticketMess.EnableHTTPS {
		urlProto = "https://"
		certFile := loadCertfile(ticketMess.CertFile)
		client, err = cryptoutil.CreateClientX(&certFile)
		if err != nil {
			return
		}
	} else {
		urlProto = "http://"
		client = &http.Client{}
	}

	authnode := strings.Split(ticketMess.TicketHost, HostsSeparator)
	//TODO don't retry if the param is wrong
	for i := 0; i < GetTicketMaxRetry; i++ {
		for _, ip := range authnode {
			url = urlProto + ip + proto.ClientGetTicket
			body, err = proto.SendData(client, url, message)

			if err != nil {
				continue
			}

			if msgResp, err = proto.ParseAuthGetTicketResp(body, key); err != nil {
				continue
			}

			if err = proto.VerifyTicketRespComm(&msgResp, proto.MsgAuthTicketReq, owner, "MasterService", ts); err != nil {
				continue
			}

			ticket.Ticket = msgResp.Ticket
			ticket.ServiceID = msgResp.ServiceID
			ticket.SessionKey = cryptoutil.Base64Encode(msgResp.SessionKey.Key)
			ticket.ID = owner
			cfslog.LogInfof("GetTicket: ok!")
			return
		}
		cfslog.LogWarnf("GetTicket: getReply error and will RETRY, url(%v) err(%v)", url, err)
		time.Sleep(GetTicketSleepInterval)
	}
	cfslog.LogWarnf("GetTicket exit: send to addr(%v) err(%v)", url, err)
	return
}

func loadCertfile(path string) (caCert []byte) {
	var err error
	caCert, err = ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	return
}
