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

func (api *API) GetTicket(id string, userKey string, serviceID string) (ticket *auth.Ticket, err error) {
	var (
		key      []byte
		ts       int64
		msgResp  proto.AuthGetTicketResp
		respData []byte
	)
	message := proto.AuthGetTicketReq{
		Type:      proto.MsgAuthTicketReq,
		ClientID:  id,
		ServiceID: proto.MasterServiceID,
	}
	if key, err = cryptoutil.Base64Decode(userKey); err != nil {
		return
	}
	if message.Verifier, ts, err = cryptoutil.GenVerifier(key); err != nil {
		return
	}
	if respData, err = api.ac.request(key, message, proto.ClientGetTicket); err != nil {
		return
	}
	if err = json.Unmarshal(respData, &msgResp); err != nil {
		return
	}
	if err = proto.VerifyTicketRespComm(&msgResp, proto.MsgAuthTicketReq, id, serviceID, ts); err != nil {
		return
	}
	ticket = &auth.Ticket{
		ID:         id,
		SessionKey: cryptoutil.Base64Encode(msgResp.SessionKey.Key),
		ServiceID:  cryptoutil.Base64Encode(msgResp.SessionKey.Key),
		Ticket:     msgResp.Ticket,
	}
	return
}

func (api *API) OSSAddCaps(ticket *auth.Ticket, accessKey string, caps []byte) (newAKCaps *keystore.AccessKeyCaps, err error) {
	akCaps := &keystore.AccessKeyCaps{
		AccessKey: accessKey,
		Caps:      caps,
	}
	return api.ac.serveOSSRequest(ticket.ID, ticket, akCaps, proto.MsgAuthOSAddCapsReq, proto.OSAddCaps)
}

func (api *API) OSSDeleteCaps(ticket *auth.Ticket, accessKey string, caps []byte) (newAKCaps *keystore.AccessKeyCaps, err error) {
	akCaps := &keystore.AccessKeyCaps{
		AccessKey: accessKey,
		Caps:      caps,
	}
	return api.ac.serveOSSRequest(ticket.ID, ticket, akCaps, proto.MsgAuthOSDeleteCapsReq, proto.OSDeleteCaps)
}

func (api *API) OSSGetCaps(ticket *auth.Ticket, accessKey string) (caps *keystore.AccessKeyCaps, err error) {
	akCaps := &keystore.AccessKeyCaps{
		AccessKey: accessKey,
	}
	return api.ac.serveOSSRequest(ticket.ID, ticket, akCaps, proto.MsgAuthOSGetCapsReq, proto.OSGetCaps)
}

func (api *API) AdminGetCaps(ticket *auth.Ticket, userID string) (res *keystore.KeyInfo, err error) {
	keyInfo := &keystore.KeyInfo{
		ID: userID,
	}
	return api.ac.serveAdminRequest(ticket.ID, ticket, keyInfo, proto.MsgAuthGetCapsReq, proto.AdminGetCaps)
}
