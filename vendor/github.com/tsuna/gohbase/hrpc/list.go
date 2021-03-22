package hrpc

import (
	"context"
	"errors"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// ListTableNames models a ListTableNames pb call
type ListTableNames struct {
	base
	regex            string
	includeSysTables bool
	namespace        string
}

// ListRegex sets a regex for ListTableNames
func ListRegex(regex string) func(Call) error {
	return func(c Call) error {
		l, ok := c.(*ListTableNames)
		if !ok {
			return errors.New("ListRegex option can only be used with ListTableNames")
		}
		l.regex = regex
		return nil
	}
}

// ListNamespace sets a namespace for ListTableNames
func ListNamespace(ns string) func(Call) error {
	return func(c Call) error {
		l, ok := c.(*ListTableNames)
		if !ok {
			return errors.New("ListNamespace option can only be used with ListTableNames")
		}
		l.namespace = ns
		return nil
	}
}

// ListSysTables includes sys tables for ListTableNames
func ListSysTables(b bool) func(Call) error {
	return func(c Call) error {
		l, ok := c.(*ListTableNames)
		if !ok {
			return errors.New("ListSysTables option can only be used with ListTableNames")
		}
		l.includeSysTables = b
		return nil
	}
}

// NewListTableNames creates a new GetTableNames request that will list tables in hbase.
//
// By default matchs all tables. Use the options (ListRegex, ListNamespace, ListSysTables) to
// set non default behaviour.
func NewListTableNames(ctx context.Context, opts ...func(Call) error) (*ListTableNames, error) {
	tn := &ListTableNames{
		base: base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		regex: ".*",
	}
	if err := applyOptions(tn, opts...); err != nil {
		return nil, err
	}
	return tn, nil
}

// Name returns the name of this RPC call.
func (tn *ListTableNames) Name() string {
	return "GetTableNames"
}

// ToProto converts the RPC into a protobuf message.
func (tn *ListTableNames) ToProto() proto.Message {
	return &pb.GetTableNamesRequest{
		Regex:            proto.String(tn.regex),
		IncludeSysTables: proto.Bool(tn.includeSysTables),
		Namespace:        proto.String(tn.namespace),
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (tn *ListTableNames) NewResponse() proto.Message {
	return &pb.GetTableNamesResponse{}
}
