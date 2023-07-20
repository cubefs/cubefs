// Copyright 2023 The CubeFS Authors.
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

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"strings"

	"github.com/cubefs/cubefs/proto"
)

func HasAclInRequest(req *http.Request) bool {
	_, hasCannedAcl := req.Header[XAmzAcl]
	hasBodyAcl := req.ContentLength > 0
	hasGrantAcl := hasGrantAclHeader(req.Header)
	return hasCannedAcl || hasGrantAcl || hasBodyAcl
}

func IsApiSupportByACL(apiName proto.Action) bool {
	_, ok := apiToPermission[apiName]
	return ok
}

func IsApiSupportByObjectAcl(apiName proto.Action) bool {
	for _, a := range objectACLSupportedApis {
		if a == apiName {
			return true
		}
	}
	return false
}

func ParseACL(req *http.Request, owner string, hasBodyAcl, needDefault bool) (acl *AccessControlPolicy, err error) {
	cannedAcl, hasCannedAcl := req.Header[XAmzAcl]
	hasGrantAcl := hasGrantAclHeader(req.Header)
	if err = hasConflictAcl(hasCannedAcl, hasGrantAcl, hasBodyAcl); err != nil {
		return nil, err
	}
	switch {
	case hasCannedAcl:
		acl, err = ParseCannedAcl(cannedAcl[0], owner)
	case hasGrantAcl:
		acl, err = ParseGrantAclWithinHeader(req.Header, owner)
	case hasBodyAcl:
		acl, err = ParseAclFromRequestBody(req.Body, owner)
	default:
		if needDefault {
			return CreateDefaultACL(owner), nil
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return acl, acl.IsValid()
}

func ParseCannedAcl(cannedAcl, owner string) (*AccessControlPolicy, error) {
	acl := &AccessControlPolicy{}
	switch cannedAcl {
	case CannedPrivate:
		acl.SetPrivate(owner)
	case CannedPublicRead:
		acl.SetPublicRead(owner)
	case CannedPublicReadWrite:
		acl.SetPublicReadWrite(owner)
	case CannedAuthenticatedRead:
		acl.SetAuthenticatedRead(owner)
	default:
		return nil, ErrInvalidCannedACL
	}
	return acl, nil
}

func ParseGrantAclWithinHeader(reqHeader http.Header, owner string) (*AccessControlPolicy, error) {
	acl := &AccessControlPolicy{}
	for _, grantHeader := range XAmzGrantHeaders {
		if grantValue := reqHeader.Get(grantHeader); grantValue != "" {
			grants, err := parseGrantValue(grantValue)
			if err != nil {
				return nil, err
			}
			addGrants(acl, grants, XAmzGrantToPermission[grantHeader])
		}
	}
	acl.SetOwner(owner)
	return acl, nil
}

func ParseAclFromRequestBody(body io.Reader, owner string) (acl *AccessControlPolicy, err error) {
	if err = xml.NewDecoder(body).Decode(&acl); err != nil {
		return nil, ErrMalformedACL
	}
	if acl.GetOwner() != owner {
		return nil, AccessDenied
	}
	return acl, nil
}

func CreateDefaultACL(owner string) *AccessControlPolicy {
	acl := &AccessControlPolicy{}
	acl.SetPrivate(owner)
	return acl
}

func hasConflictAcl(hasCannedAcl, hasGrantAcl, hasBodyAcl bool) error {
	if hasCannedAcl && hasGrantAcl {
		return ErrConflictAclHeader
	}
	if hasBodyAcl && (hasCannedAcl || hasGrantAcl) {
		return ErrUnexpectedContent
	}
	return nil
}

func hasGrantAclHeader(h http.Header) bool {
	for _, grantHeader := range XAmzGrantHeaders {
		if _, ok := h[grantHeader]; ok {
			return true
		}
	}
	return false
}

func isACLApi(apiName proto.Action) bool {
	for _, a := range aclApiList {
		if a == apiName {
			return true
		}
	}
	return false
}

type grant struct {
	key   string
	value string
}

func parseGrantValue(grantValue string) (grants []grant, err error) {
	s := strings.ReplaceAll(grantValue, " ", "")
	pairs := strings.Split(s, ",")
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
			return nil, InvalidArgument
		}
		grants = append(grants, grant{key: kv[0], value: kv[1]})
	}
	return
}

func addGrants(acl *AccessControlPolicy, grants []grant, permission string) {
	for _, g := range grants {
		switch strings.ToLower(g.key) {
		case "id":
			acl.AddGrant(g.value, TypeCanonicalUser, permission)
		case "uri":
			acl.AddGrant(g.value, TypeGroup, permission)
		}
	}
}

func putBucketACL(vol *Volume, acp *AccessControlPolicy) error {
	acp.RemoveAttr()
	data, err := json.Marshal(acp)
	if err != nil {
		return err
	}
	return vol.store.Put(vol.name, bucketRootPath, XAttrKeyOSSACL, data)
}

func getObjectACL(vol *Volume, path string, needDefault bool) (*AccessControlPolicy, error) {
	xAttr, err := vol.GetXAttr(path, XAttrKeyOSSACL)
	if err != nil || xAttr == nil {
		return nil, err
	}
	var acp *AccessControlPolicy
	data := xAttr.Get(XAttrKeyOSSACL)
	if len(data) > 0 {
		err = json.Unmarshal(data, &acp)
	} else if needDefault {
		acp = CreateDefaultACL(vol.owner)
	}
	return acp, err
}

func putObjectACL(vol *Volume, path string, acp *AccessControlPolicy) error {
	acp.RemoveAttr()
	data, err := json.Marshal(acp)
	if err != nil {
		return err
	}
	return vol.SetXAttr(path, XAttrKeyOSSACL, data, false)
}
