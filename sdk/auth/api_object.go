package auth

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/keystore"
)

type API struct {
	ac *AuthClient
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
