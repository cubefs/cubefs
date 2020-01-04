package auth

import (
	"encoding/json"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/auth"
	"github.com/chubaofs/chubaofs/util/cryptoutil"
	"github.com/chubaofs/chubaofs/util/keystore"
)

type API struct {
	ac *AuthClient
}

func (api *API) GetTicket(clientId string, clientKey string, serviceID string) (ticket *auth.Ticket, err error) {
	var (
		key      []byte
		ts       int64
		msgResp  proto.AuthGetTicketResp
		respData []byte
	)
	message := proto.AuthGetTicketReq{
		Type:      proto.MsgAuthTicketReq,
		ClientID:  clientId,
		ServiceID: proto.MasterServiceID,
	}
	if key, err = cryptoutil.Base64Decode(clientKey); err != nil {
		return
	}
	if message.Verifier, ts, err = cryptoutil.GenVerifier(key); err != nil {
		return
	}
	if respData, err = api.ac.request(clientId, clientKey, key, message, proto.ClientGetTicket); err != nil {
		return
	}
	if err = json.Unmarshal(respData, &msgResp); err != nil {
		return
	}
	if err = proto.VerifyTicketRespComm(&msgResp, proto.MsgAuthTicketReq, clientId, serviceID, ts); err != nil {
		return
	}
	ticket = &auth.Ticket{
		ID:         clientId,
		SessionKey: cryptoutil.Base64Encode(msgResp.SessionKey.Key),
		ServiceID:  cryptoutil.Base64Encode(msgResp.SessionKey.Key),
		Ticket:     msgResp.Ticket,
	}
	return
}

func (api *API) OSSAddCaps(clientID, clientKey, accessKey string, caps []byte) (newAKCaps *keystore.AccessKeyCaps, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	akCaps := &keystore.AccessKeyCaps{
		AccessKey: accessKey,
		Caps:      caps,
	}
	return api.ac.serveOSSRequest(clientID, clientKey, api.ac.ticket, akCaps, proto.MsgAuthOSAddCapsReq, proto.OSAddCaps)
}

func (api *API) OSSDeleteCaps(clientID, clientKey, accessKey string, caps []byte) (newAKCaps *keystore.AccessKeyCaps, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	akCaps := &keystore.AccessKeyCaps{
		AccessKey: accessKey,
		Caps:      caps,
	}
	return api.ac.serveOSSRequest(clientID, clientKey, api.ac.ticket, akCaps, proto.MsgAuthOSDeleteCapsReq, proto.OSDeleteCaps)
}

func (api *API) OSSGetCaps(clientID, clientKey, accessKey string) (caps *keystore.AccessKeyCaps, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	akCaps := &keystore.AccessKeyCaps{
		AccessKey: accessKey,
	}
	return api.ac.serveOSSRequest(clientID, clientKey, api.ac.ticket, akCaps, proto.MsgAuthOSGetCapsReq, proto.OSGetCaps)
}

func (api *API) AdminGetCaps(clientID, clientKey, userID string) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID: userID,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthGetCapsReq, proto.AdminGetCaps)
}
