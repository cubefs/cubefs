package objectnode

import (
	"net/http"

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

func SetRequestAction(r *http.Request, action Action) {
	mux.Vars(r)[ContextKeyRequestAction] = action.String()
}

func GetActionFromContext(r *http.Request) (action Action) {
	return ActionFromString(mux.Vars(r)[ContextKeyRequestAction])
}
