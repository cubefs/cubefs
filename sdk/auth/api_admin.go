package auth

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/keystore"
)

func (api *API) AdminCreateKey(clientID, clientKey, userID, role string, caps []byte) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID:   userID,
		Role: role,
		Caps: caps,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthCreateKeyReq, proto.AdminCreateKey)
}

func (api *API) AdminDeleteKey(clientID, clientKey, userID string) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID: userID,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthDeleteKeyReq, proto.AdminDeleteKey)
}

func (api *API) AdminGetKey(clientID, clientKey, userID string) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID: userID,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthGetKeyReq, proto.AdminGetKey)
}

func (api *API) AdminAddCaps(clientID, clientKey, userID string, caps []byte) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID:   userID,
		Caps: caps,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthAddCapsReq, proto.AdminAddCaps)
}

func (api *API) AdminDeleteCaps(clientID, clientKey, userID string, caps []byte) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID:   userID,
		Caps: caps,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthDeleteCapsReq, proto.AdminDeleteCaps)
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
