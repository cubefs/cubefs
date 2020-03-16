package objectnode

import (
	"net/http"

	"github.com/chubaofs/chubaofs/proto"

	"github.com/gorilla/mux"
)

const (
	ContextKeyRequestID     = "ctx_request_id"
	ContextKeyRequestAction = "ctx_request_action"
)

func SetRequestID(r *http.Request, requestID string) {
	mux.Vars(r)[ContextKeyRequestID] = requestID
}

func GetRequestID(r *http.Request) (id string) {
	return mux.Vars(r)[ContextKeyRequestID]
}

func SetRequestAction(r *http.Request, action proto.Action) {
	mux.Vars(r)[ContextKeyRequestAction] = action.String()
}

func GetActionFromContext(r *http.Request) (action proto.Action) {
	return proto.ParseAction(mux.Vars(r)[ContextKeyRequestAction])
}
