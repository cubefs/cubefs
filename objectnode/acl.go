// Copyright 2019 The CubeFS Authors.
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

package objectnode

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/S3_ACLs_UsingACLs.html

import (
	"encoding/json"
	"encoding/xml"
	"net/http"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	AnonymousUser  = ""
	bucketRootPath = "/"
	maxGrantCount  = 100 // an acl can have up to 100 grants
)

var (
	ErrMissingSecurityHeader = &ErrorCode{ErrorCode: "MissingSecurityHeader", ErrorMessage: "Your request is missing a required header.", StatusCode: http.StatusBadRequest}
	ErrTooManyGrants         = &ErrorCode{ErrorCode: "ErrTooManyGrants", ErrorMessage: "Grant number exceed limits.", StatusCode: http.StatusBadRequest}
	ErrMissingGrants         = &ErrorCode{ErrorCode: "ErrMissingGrants", ErrorMessage: "Missing grant in request or grant info not supported.", StatusCode: http.StatusBadRequest}
	ErrInvalidCannedACL      = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "Invalid canned acl.", StatusCode: http.StatusBadRequest}
	ErrInvalidGroupUri       = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "Invalid group uri.", StatusCode: http.StatusBadRequest}
	ErrInvalidPermission     = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "Invalid permission, it is case sensitive.", StatusCode: http.StatusBadRequest}
	ErrMalformedACL          = &ErrorCode{ErrorCode: "MalformedACLError", ErrorMessage: "The XML you provided was not well-formed or did not validate against our published schema.", StatusCode: http.StatusBadRequest}
	ErrConflictAclHeader     = &ErrorCode{ErrorCode: "InvalidRequest", ErrorMessage: "Specifying both Canned ACLs and Header Grants is not allowed.", StatusCode: http.StatusBadRequest}
	ErrUnexpectedContent     = &ErrorCode{ErrorCode: "UnexpectedContent", ErrorMessage: "This request does not support content.", StatusCode: http.StatusBadRequest}
)

type Grantee struct {
	Xmlxsi      string `xml:"xmlns:xsi,attr" json:"xmlns,omitempty"`
	Xmlns       string `xml:"xsi,attr" json:"xsi,omitempty"`
	XsiType     string `xml:"xsi:type,attr" json:"xsi_type,omitempty"`
	Type        string `xml:"type,attr" json:"t"`
	Id          string `xml:"ID,omitempty" json:"i,omitempty"`
	URI         string `xml:"URI,omitempty" json:"u,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty" json:"d,omitempty"`
}

type Grant struct {
	Grantee    Grantee `xml:"Grantee,omitempty" json:"g"`
	Permission string  `xml:"Permission,omitempty" json:"p"`
}

type AccessControlList struct {
	Grants []Grant `xml:"Grant,omitempty" json:"gs"`
}

type Owner struct {
	Id          string `xml:"ID" json:"i"`
	DisplayName string `xml:"DisplayName" json:"d,omitempty"`
}

type AccessControlPolicy struct {
	Xmlns string            `xml:"xmlns,attr" json:"x,omitempty"`
	Owner Owner             `xml:"Owner,omitempty" json:"o,omitempty"`
	Acl   AccessControlList `xml:"AccessControlList,omitempty" json:"a,omitempty"`
}

func (g *Grantee) isValid() error {
	if g.Id == "" && g.URI == "" {
		return ErrMalformedACL
	}
	if g.Id != "" && g.URI != "" {
		return ErrMalformedACL
	}
	switch g.Type {
	case TypeGroup:
		if g.URI != GroupAllUser && g.URI != GroupAuthenticated {
			return ErrInvalidGroupUri
		}
	case TypeCanonicalUser:
		if g.URI != "" {
			return ErrMalformedACL
		}
	default:
		return ErrMalformedACL
	}
	return nil
}

func (g *Grant) isValid() error {
	if !g.isPermissionValid() {
		return ErrInvalidPermission
	}
	return g.Grantee.isValid()
}

func (g *Grant) isPermissionValid() bool {
	for _, v := range XAmzGrantToPermission {
		if g.Permission == v {
			return true
		}
	}
	return false
}

func (g *Grant) isAllowed(reqId string, apiName proto.Action) bool {
	permNeed, ok := apiToPermission[apiName]
	if !ok {
		return false
	}
	if !(g.Permission == PermissionFullControl || g.Permission == permNeed) {
		return false
	}
	switch g.Grantee.Type {
	case TypeCanonicalUser:
		return g.Grantee.Id == reqId
	case TypeGroup:
		if g.Grantee.URI == GroupAllUser {
			return true
		}
		return reqId != AnonymousUser // Group_Authenticated
	default:
		return false
	}
}

func (acp *AccessControlPolicy) IsValid() error {
	if len(acp.Acl.Grants) == 0 {
		return ErrMissingGrants
	}
	if len(acp.Acl.Grants) > maxGrantCount {
		return ErrTooManyGrants
	}
	for _, g := range acp.Acl.Grants {
		if err := g.isValid(); err != nil {
			return err
		}
	}
	return nil
}

func (acp *AccessControlPolicy) IsAllowed(reqUid string, apiName proto.Action) bool {
	for _, g := range acp.Acl.Grants {
		if g.isAllowed(reqUid, apiName) {
			return true
		}
	}
	if len(acp.Acl.Grants) == 0 && acp.Owner.Id == reqUid {
		return true
	}
	if isACLApi(apiName) {
		return acp.Owner.Id == reqUid
	}
	return false
}

func (acp *AccessControlPolicy) IsEmpty() bool {
	return len(acp.Acl.Grants) == 0
}

func (acp *AccessControlPolicy) SetOwner(owner string) {
	acp.Owner.Id = owner
}

func (acp *AccessControlPolicy) GetOwner() string {
	return acp.Owner.Id
}

func (acp *AccessControlPolicy) AddGrant(idUri, granteeType, permission string) {
	var g Grant
	switch granteeType {
	case TypeCanonicalUser:
		g.Grantee.Id = idUri
		g.Grantee.Type = TypeCanonicalUser
	case TypeGroup:
		g.Grantee.URI = idUri
		g.Grantee.Type = TypeGroup
	default:
		return
	}
	g.Grantee.Xmlns = XMLNS
	g.Permission = permission
	acp.Acl.Grants = append(acp.Acl.Grants, g)
}

func (acp *AccessControlPolicy) SetPrivate(ownerId string) {
	acp.SetOwner(ownerId)
	acp.AddGrant(ownerId, TypeCanonicalUser, PermissionFullControl)
}

func (acp *AccessControlPolicy) SetPublicRead(ownerId string) {
	acp.SetOwner(ownerId)
	acp.AddGrant(ownerId, TypeCanonicalUser, PermissionFullControl)
	acp.AddGrant(GroupAllUser, TypeGroup, PermissionRead)
}

func (acp *AccessControlPolicy) SetPublicReadWrite(ownerId string) {
	acp.SetOwner(ownerId)
	acp.AddGrant(ownerId, TypeCanonicalUser, PermissionFullControl)
	acp.AddGrant(GroupAllUser, TypeGroup, PermissionRead)
	acp.AddGrant(GroupAllUser, TypeGroup, PermissionWrite)
}

func (acp *AccessControlPolicy) SetAuthenticatedRead(ownerId string) {
	acp.SetOwner(ownerId)
	acp.AddGrant(ownerId, TypeCanonicalUser, PermissionFullControl)
	acp.AddGrant(GroupAuthenticated, TypeGroup, PermissionRead)
}

func (acp *AccessControlPolicy) XmlMarshal() ([]byte, error) {
	var grants []Grant
	for _, g := range acp.Acl.Grants {
		if g.Grantee.Xmlns == "" {
			g.Grantee.Xmlns = XMLNS
		}
		g.Grantee.Xmlxsi = g.Grantee.Xmlns
		g.Grantee.XsiType = g.Grantee.Type
		grants = append(grants, g)
	}
	acp.Acl.Grants = grants
	acp.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	data, err := xml.Marshal(acp)
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), data...), nil
}

func (acp *AccessControlPolicy) RemoveAttr() {
	var grants []Grant
	for _, g := range acp.Acl.Grants {
		g.Grantee.Xmlxsi = ""
		g.Grantee.XsiType = ""
		g.Grantee.Xmlns = ""
		grants = append(grants, g)
	}
	acp.Xmlns = ""
	acp.Acl.Grants = grants
}

func (acp *AccessControlPolicy) Encode() string {
	acp.RemoveAttr()
	data, err := json.Marshal(acp)
	if err != nil {
		log.LogWarnf("acl json marshal failed: %v", err)
	}
	return string(data)
}
