// Copyright 2018 The CubeFS Authors.
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

package proto

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/cubefs/cubefs/util/caps"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/keystore"
)

// ServiceID defines the type of tickets
type ServiceID uint32

// MsgType defines the type of req/resp for message
type MsgType uint32

// Nonce defines the nonce to mitigate the replay attack
type Nonce uint64

const (
	APIRsc          = "API"
	APIAccess       = "access"
	capSeparator    = ":"
	reqLiveLength   = 10
	ClientMessage   = "Token"
	OwnerVOLRsc     = "OwnerVOL"
	NoneOwnerVOLRsc = "NoneOwnerVOL"
	VOLAccess       = "*"
)

// api
const (
	// Client APIs
	ClientGetTicket = "/client/getticket"

	// Admin APIs
	AdminCreateKey  = "/admin/createkey"
	AdminDeleteKey  = "/admin/deletekey"
	AdminGetKey     = "/admin/getkey"
	AdminAddCaps    = "/admin/addcaps"
	AdminDeleteCaps = "/admin/deletecaps"
	AdminGetCaps    = "/admin/getcaps"

	//raft node APIs
	AdminAddRaftNode    = "/admin/addraftnode"
	AdminRemoveRaftNode = "/admin/removeraftnode"

	// Object node APIs
	OSAddCaps    = "/os/addcaps"
	OSDeleteCaps = "/os/deletecaps"
	OSGetCaps    = "/os/getcaps"
)

const (
	// AuthServiceID defines ticket for authnode access (not supported)
	AuthServiceID = "AuthService"

	// MasterServiceID defines ticket for master access
	MasterServiceID = "MasterService"

	// MetaServiceID defines ticket for metanode access (not supported)
	MetaServiceID = "MetanodeService"

	// DataServiceID defines ticket for datanode access (not supported)
	DataServiceID = "DatanodeService"

	//ObjectServiceID defines ticket for objectnode access
	ObjectServiceID = "ObjectService"
)

const (
	MasterNode = "master"
	MetaNode   = "metanode"
	DataNode   = "datanode"
)

const (
	// MsgAuthBase define the starting value for auth message
	MsgAuthBase MsgType = 0x100000

	// MsgAuthTicketReq request type for an auth ticket
	MsgAuthTicketReq MsgType = MsgAuthBase + 0x10000

	// MsgAuthTicketResp respose type for an auth ticket
	MsgAuthTicketResp MsgType = MsgAuthBase + 0x10001

	// MsgMasterTicketReq request type for a master ticket
	MsgMasterTicketReq MsgType = MsgAuthBase + 0x20000

	// MsgMasterTicketResp response type for a master ticket
	MsgMasterTicketResp MsgType = MsgAuthBase + 0x20001

	// MsgMetaTicketReq request type for a metanode ticket
	MsgMetaTicketReq MsgType = MsgAuthBase + 0x30000

	// MsgMetaTicketResp response type for a metanode ticket
	MsgMetaTicketResp MsgType = MsgAuthBase + 0x30001

	// MsgDataTicketReq request type for a datanode ticket
	MsgDataTicketReq MsgType = MsgAuthBase + 0x40000

	// MsgDataTicketResp response type for a datanode ticket
	MsgDataTicketResp MsgType = MsgAuthBase + 0x40001

	// MsgAuthCreateKeyReq request type for authnode add key
	MsgAuthCreateKeyReq MsgType = MsgAuthBase + 0x51000

	// MsgAuthCreateKeyResp response type for authnode add key
	MsgAuthCreateKeyResp MsgType = MsgAuthBase + 0x51001

	// MsgAuthDeleteKeyReq request type for authnode delete key
	MsgAuthDeleteKeyReq MsgType = MsgAuthBase + 0x52000

	// MsgAuthDeleteKeyResp response type for authnode delete key
	MsgAuthDeleteKeyResp MsgType = MsgAuthBase + 0x52001

	// MsgAuthGetKeyReq request type for authnode get key info
	MsgAuthGetKeyReq MsgType = MsgAuthBase + 0x53000

	// MsgAuthGetKeyResp response type for authnode get key info
	MsgAuthGetKeyResp MsgType = MsgAuthBase + 0x53001

	// MsgAuthAddCapsReq request type for authnode add caps
	MsgAuthAddCapsReq MsgType = MsgAuthBase + 0x54000

	// MsgAuthAddCapsResp response type for authnode add caps
	MsgAuthAddCapsResp MsgType = MsgAuthBase + 0x54001

	// MsgAuthDeleteCapsReq request type for authnode add caps
	MsgAuthDeleteCapsReq MsgType = MsgAuthBase + 0x55000

	// MsgAuthDeleteCapsResp response type for authnode add caps
	MsgAuthDeleteCapsResp MsgType = MsgAuthBase + 0x55001

	// MsgAuthGetCapsReq request type for authnode add caps
	MsgAuthGetCapsReq MsgType = MsgAuthBase + 0x56000

	// MsgAuthGetCapsResp response type for authnode add caps
	MsgAuthGetCapsResp MsgType = MsgAuthBase + 0x56001

	// MsgAuthAddRaftNodeReq request type for authnode add node
	MsgAuthAddRaftNodeReq MsgType = MsgAuthBase + 0x57000

	// MsgAuthAddRaftNodeResp response type for authnode remove node
	MsgAuthAddRaftNodeResp MsgType = MsgAuthBase + 0x57001

	// MsgAuthRemoveRaftNodeReq request type for authnode remove node
	MsgAuthRemoveRaftNodeReq MsgType = MsgAuthBase + 0x58000

	// MsgAuthRemoveRaftNodeResp response type for authnode remove node
	MsgAuthRemoveRaftNodeResp MsgType = MsgAuthBase + 0x58001

	// MsgAuthOSAddCapsReq request type from ObjectNode to add caps
	MsgAuthOSAddCapsReq MsgType = MsgAuthBase + 0x61000

	// MsgAuthOSAddCapsResp request type from ObjectNode to add caps
	MsgAuthOSAddCapsResp MsgType = MsgAuthBase + 0x61001

	// MsgAuthOSDeleteCapsReq request type from ObjectNode to delete caps
	MsgAuthOSDeleteCapsReq MsgType = MsgAuthBase + 0x62000

	// MsgAuthOSDeleteCapsResp request type from ObjectNode to delete caps
	MsgAuthOSDeleteCapsResp MsgType = MsgAuthBase + 0x62001

	// MsgAuthOSGetCapsReq request type from ObjectNode to get caps
	MsgAuthOSGetCapsReq MsgType = MsgAuthBase + 0x63000

	// MsgAuthOSGetCapsResp response type from ObjectNode to get caps
	MsgAuthOSGetCapsResp MsgType = MsgAuthBase + 0x63001

	// MsgMasterAPIAccessReq request type for master api access
	MsgMasterAPIAccessReq MsgType = 0x60000

	// MsgMasterAPIAccessResp response type for master api access
	MsgMasterAPIAccessResp MsgType = 0x60001

	//Master API ClientVol
	MsgMasterFetchVolViewReq MsgType = MsgMasterAPIAccessReq + 0x10000
)

// HTTPAuthReply uniform response structure
type HTTPAuthReply struct {
	Code int32       `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

// MsgType2ResourceMap define the mapping from message type to resource
var MsgType2ResourceMap = map[MsgType]string{
	MsgAuthCreateKeyReq:      "auth:createkey",
	MsgAuthDeleteKeyReq:      "auth:deletekey",
	MsgAuthGetKeyReq:         "auth:getkey",
	MsgAuthAddCapsReq:        "auth:addcaps",
	MsgAuthDeleteCapsReq:     "auth:deletecaps",
	MsgAuthGetCapsReq:        "auth:getcaps",
	MsgAuthAddRaftNodeReq:    "auth:addnode",
	MsgAuthRemoveRaftNodeReq: "auth:removenode",
	MsgAuthOSAddCapsReq:      "auth:osaddcaps",
	MsgAuthOSDeleteCapsReq:   "auth:osdeletecaps",
	MsgAuthOSGetCapsReq:      "auth:osgetcaps",

	MsgMasterFetchVolViewReq: "master:getvol",
}

// AuthGetTicketReq defines the message from client to authnode
// use timestamp as verifier for MITM mitigation
// verifier is also used to verify the server identity
type AuthGetTicketReq struct {
	Type      MsgType `json:"type"`
	ClientID  string  `json:"client_id"`
	ServiceID string  `json:"service_id"`
	Verifier  string  `json:"verifier"`
}

// AuthGetTicketResp defines the message from authnode to client
type AuthGetTicketResp struct {
	Type       MsgType              `json:"type"`
	ClientID   string               `json:"client_id"`
	ServiceID  string               `json:"service_id"`
	Verifier   int64                `json:"verifier"`
	Ticket     string               `json:"ticket"`
	SessionKey cryptoutil.CryptoKey `json:"session_key"`
}

// APIAccessReq defines the request for access restful api
// use timestamp as verifier for MITM mitigation
// verifier is also used to verify the server identity
type APIAccessReq struct {
	Type      MsgType `json:"type"`
	ClientID  string  `json:"client_id"`
	ServiceID string  `json:"service_id"`
	Verifier  string  `json:"verifier"`
	Ticket    string  `json:"ticket"`
}

// APIAccessResp defines the response for access restful api
// use timestamp as verifier for MITM mitigation
// verifier is also used to verify the server identity
type APIAccessResp struct {
	Type      MsgType `json:"type"`
	ClientID  string  `json:"client_id"`
	ServiceID string  `json:"service_id"`
	Verifier  int64   `json:"verifier"`
}

// AuthAPIAccessReq defines Auth API request
type AuthAPIAccessReq struct {
	APIReq  APIAccessReq     `json:"api_req"`
	KeyInfo keystore.KeyInfo `json:"key_info"`
}

// AuthAPIAccessResp defines the response for creating an key in authnode
type AuthAPIAccessResp struct {
	APIResp APIAccessResp    `json:"api_resp"`
	KeyInfo keystore.KeyInfo `json:"key_info"`
}

// AuthRaftNodeInfo defines raft node information
type AuthRaftNodeInfo struct {
	ID   uint64 `json:"id"`
	Addr string `json:"addr"`
}

// AuthRaftNodeReq defines Auth API request for add/remove a raft node
type AuthRaftNodeReq struct {
	APIReq       APIAccessReq     `json:"api_req"`
	RaftNodeInfo AuthRaftNodeInfo `json:"node_info"`
}

// AuthRaftNodeResp defines Auth API response for add/remove a raft node
type AuthRaftNodeResp struct {
	APIResp APIAccessResp `json:"api_resp"`
	Msg     string        `json:"msg"`
}

// AuthAPIAccessKeystoreReq defines Auth API for put/delete Access Keystore vols
type AuthOSAccessKeyReq struct {
	APIReq APIAccessReq           `json:"api_req"`
	AKCaps keystore.AccessKeyCaps `json:"access_key_caps"`
}

// AuthAPIAccessKeystoreResp defines the response for put/delete Access Keystore vols
type AuthOSAccessKeyResp struct {
	APIResp APIAccessResp          `json:"api_resp"`
	AKCaps  keystore.AccessKeyCaps `json:"access_key_caps"`
}

// IsValidServiceID determine the validity of a serviceID
func IsValidServiceID(serviceID string) (err error) {
	if serviceID != AuthServiceID && serviceID != MasterServiceID && serviceID != MetaServiceID && serviceID != DataServiceID {
		err = fmt.Errorf("invalid service ID [%s]", serviceID)
		return
	}
	return
}

// IsValidMsgReqType determine the validity of a message type
func IsValidMsgReqType(serviceID string, msgType MsgType) (err error) {
	b := false
	switch serviceID {
	case "AuthService":
		fallthrough
	case "MasterService":
		if msgType|MsgAuthBase != 0 {
			b = true
		}
	}
	if !b {
		err = fmt.Errorf("invalid request type [%x] and serviceID[%s]", msgType, serviceID)
		return
	}
	return
}

// IsValidClientID determine the validity of a clientID
func IsValidClientID(id string) (err error) {
	re := regexp.MustCompile("^[A-Za-z]{1,1}[A-Za-z0-9_]{0,20}$")
	if !re.MatchString(id) {
		err = fmt.Errorf("clientID invalid format [%s]", id)
		return
	}
	return
}

// ParseAuthReply parse the response from auth
func ParseAuthReply(body []byte) (jobj HTTPAuthReply, err error) {
	if err = json.Unmarshal(body, &jobj); err != nil {
		return
	}
	if jobj.Code != 0 {
		err = fmt.Errorf(jobj.Msg)
		return
	}
	return
}

// GetDataFromResp extract data from response
func GetDataFromResp(body []byte, key []byte) (plaintext []byte, err error) {
	jobj, err := ParseAuthReply(body)
	if err != nil {
		return
	}
	data := fmt.Sprint(jobj.Data)

	if plaintext, err = cryptoutil.DecodeMessage(data, key); err != nil {
		return
	}

	return
}

// ParseAuthGetTicketResp parse and validate the auth get ticket resp
func ParseAuthGetTicketResp(body []byte, key []byte) (resp AuthGetTicketResp, err error) {
	var (
		plaintext []byte
	)

	if plaintext, err = GetDataFromResp(body, key); err != nil {
		return
	}

	if err = json.Unmarshal(plaintext, &resp); err != nil {
		return
	}

	return
}

// ParseAuthAPIAccessResp parse and validate the auth api access resp
func ParseAuthAPIAccessResp(body []byte, key []byte) (resp AuthAPIAccessResp, err error) {
	var (
		plaintext []byte
	)

	if plaintext, err = GetDataFromResp(body, key); err != nil {
		return
	}

	if err = json.Unmarshal(plaintext, &resp); err != nil {
		return
	}

	return
}

// ParseAuthRaftNodeResp parse and validate the auth raft node resp
func ParseAuthRaftNodeResp(body []byte, key []byte) (resp AuthRaftNodeResp, err error) {
	var (
		plaintext []byte
	)

	if plaintext, err = GetDataFromResp(body, key); err != nil {
		return
	}

	if err = json.Unmarshal(plaintext, &resp); err != nil {
		return
	}

	return
}

func ParseAuthOSAKResp(body []byte, key []byte) (resp AuthOSAccessKeyResp, err error) {
	var (
		plaintext []byte
	)

	if plaintext, err = GetDataFromResp(body, key); err != nil {
		return
	}

	if err = json.Unmarshal(plaintext, &resp); err != nil {
		return
	}

	return
}

func ExtractTicket(str string, key []byte) (ticket cryptoutil.Ticket, err error) {
	var (
		plaintext []byte
	)

	if plaintext, err = cryptoutil.DecodeMessage(str, key); err != nil {
		return
	}

	if err = json.Unmarshal(plaintext, &ticket); err != nil {
		return
	}

	return
}

func checkTicketCaps(ticket *cryptoutil.Ticket, kind string, cap string) (err error) {
	c := new(caps.Caps)
	if err = c.Init(ticket.Caps); err != nil {
		return
	}
	if b := c.ContainCaps(kind, cap); !b {
		err = fmt.Errorf("no permission to access %v", kind)
		return
	}
	return
}

// ParseVerifier checks the verifier structure for replay attack mitigation
func ParseVerifier(verifier string, key []byte) (ts int64, err error) {
	var (
		plainttext []byte
	)

	if plainttext, err = cryptoutil.DecodeMessage(verifier, key); err != nil {
		return
	}

	ts = int64(binary.LittleEndian.Uint64(plainttext))

	if time.Now().Unix()-ts >= reqLiveLength { // mitigate replay attack
		err = fmt.Errorf("req verifier is timeout [%d] >= [%d]", time.Now().Unix()-ts, reqLiveLength)
		return
	}

	return
}

// VerifyAPIAccessReqIDs verify the req IDs
func VerifyAPIAccessReqIDs(req *APIAccessReq) (err error) {
	if err = IsValidClientID(req.ClientID); err != nil {
		err = fmt.Errorf("IsValidClientID failed: %s", err.Error())
		return
	}

	if err = IsValidServiceID(req.ServiceID); err != nil {
		err = fmt.Errorf("IsValidServiceID failed: %s", err.Error())
		return
	}

	if err = IsValidMsgReqType(req.ServiceID, req.Type); err != nil {
		err = fmt.Errorf("IsValidMsgReqType failed: %s", err.Error())
		return
	}
	return
}

// ExtractAPIAccessTicket verify ticket validity
func ExtractAPIAccessTicket(req *APIAccessReq, key []byte) (ticket cryptoutil.Ticket, ts int64, err error) {
	if ticket, err = ExtractTicket(req.Ticket, key); err != nil {
		err = fmt.Errorf("extractTicket failed: %s", err.Error())
		return
	}

	if time.Now().Unix() >= ticket.Exp {
		err = ErrExpiredTicket
		return
	}

	if ts, err = ParseVerifier(req.Verifier, ticket.SessionKey.Key); err != nil {
		err = fmt.Errorf("parseVerifier failed: %s", err.Error())
		return
	}

	return
}

// CheckAPIAccessCaps checks capability
func CheckAPIAccessCaps(ticket *cryptoutil.Ticket, rscType string, mp MsgType, action string) (err error) {
	if _, ok := MsgType2ResourceMap[mp]; !ok {
		err = fmt.Errorf("MsgType2ResourceMap key not found [%d]", mp)
		return
	}

	rule := MsgType2ResourceMap[mp] + capSeparator + action

	if err = checkTicketCaps(ticket, rscType, rule); err != nil {
		err = fmt.Errorf("checkTicketCaps failed: %s", err.Error())
		return
	}
	return
}

func CheckVOLAccessCaps(ticket *cryptoutil.Ticket, volName string, action string, accessNode string) (err error) {

	rule := accessNode + capSeparator + volName + capSeparator + action

	if err = checkTicketCaps(ticket, OwnerVOLRsc, rule); err != nil {
		if err = checkTicketCaps(ticket, NoneOwnerVOLRsc, rule); err != nil {
			err = fmt.Errorf("checkTicketCaps failed: %s", err.Error())
			return
		}
	}

	return
}

// VerifyAPIRespComm client verifies commond attributes returned from server
func VerifyAPIRespComm(apiResp *APIAccessResp, msg MsgType, clientID string, serviceID string, ts int64) (err error) {
	if ts+1 != apiResp.Verifier {
		err = fmt.Errorf("verifier verification failed")
		return
	}

	if apiResp.Type != msg+1 {
		err = fmt.Errorf("msg verification failed")
		return
	}

	if apiResp.ClientID != clientID {
		err = fmt.Errorf("id verification failed")
		return
	}

	if apiResp.ServiceID != serviceID {
		err = fmt.Errorf("service id verification failed")
		return
	}
	return
}

// VerifyTicketRespComm verifies the ticket respose from server
func VerifyTicketRespComm(ticketResp *AuthGetTicketResp, msg MsgType, clientID string, serviceID string, ts int64) (err error) {
	if ts+1 != ticketResp.Verifier {
		err = fmt.Errorf("verifier verification failed")
		return
	}

	if ticketResp.Type != msg+1 {
		err = fmt.Errorf("msg verification failed")
		return
	}

	if ticketResp.ClientID != clientID {
		err = fmt.Errorf("id verification failed")
		return
	}

	if ticketResp.ServiceID != serviceID {
		err = fmt.Errorf("service id verification failed")
		return
	}
	return
}

// SendBytes send raw bytes target in http/https protocol
func SendBytes(client *http.Client, target string, data []byte) (res []byte, err error) {
	message := base64.StdEncoding.EncodeToString(data)
	resp, err := client.PostForm(target, url.Values{ClientMessage: {message}})
	if err != nil {
		err = fmt.Errorf("action[SendData] failed:" + err.Error())
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("action[doRealSend] failed:" + err.Error())
		return
	}
	res = body
	return
}

// SendData sends data to target
func SendData(client *http.Client, target string, data interface{}) (res []byte, err error) {
	messageJSON, err := json.Marshal(data)
	if err != nil {
		err = fmt.Errorf("action[doRealSend] failed:" + err.Error())
		return
	}
	if res, err = SendBytes(client, target, messageJSON); err != nil {
		return
	}
	return
}
