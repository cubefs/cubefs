package mutil

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	graphql "github.com/graph-gophers/graphql-go"
)

type GraphqlHandler struct {
	Schema *graphql.Schema
}

type Params struct {
	Query         string                 `json:"query"`
	OperationName string                 `json:"operationName"`
	Variables     map[string]interface{} `json:"variables"`
}

func isGraphqlType(r *http.Request) bool {
	cType := r.Header.Get("Content-Type")
	if cType == "" {
		return false
	}
	return strings.HasPrefix(cType, "application/graphql")
}

func (h *GraphqlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	params := Params{}

	if isGraphqlType(r) {
		bs, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		params.Query = string(bs)
	} else {
		if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	ctx := r.Context()

	response := h.Schema.Exec(ctx, params.Query, params.OperationName, params.Variables)

	responseJSON, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}
