// Copyright 2019 The ChubaoFS Authors.
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
	"encoding/xml"
	"errors"

	"github.com/cubefs/cubefs/proto"

	"github.com/cubefs/cubefs/util/log"
)

const (
	maxGrantCount  = 100 //ACL 可以拥有最多 100 个授权。
	bucketRootPath = "/"
)

const (
	//Permission Value
	ReadPermission        Permission = "READ"
	WritePermission                  = "WRITE"
	ReadACPPermission                = "READ_ACP"
	WriteACPPermission               = "WRITE_ACP"
	FullControlPermission            = "FULL_CONTROL"
)

// Mapping of ACL Permissions and Access Policy Permissions
// Reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/acl-overview.html
var (
	aclBucketPermissionActions = map[Permission]proto.Actions{
		ReadPermission: {
			proto.OSSListObjectsAction,
			proto.OSSListObjectVersionsAction,
			proto.OSSListMultipartUploadsAction,
		},
		WritePermission: {
			proto.OSSPutObjectAction,
			proto.OSSDeleteObjectAction,
			proto.OSSDeleteObjectsAction,
		},
		ReadACPPermission: {
			proto.OSSGetBucketAclAction,
		},
		WriteACPPermission: {
			proto.OSSPutBucketAclAction,
		},
		FullControlPermission: {
			proto.OSSListObjectsAction,
			proto.OSSListObjectVersionsAction,
			proto.OSSListMultipartUploadsAction,
			proto.OSSPutObjectAction,
			proto.OSSDeleteObjectAction,
			proto.OSSDeleteObjectsAction,
			proto.OSSGetBucketAclAction,
			proto.OSSPutBucketAclAction,
		},
	}
	aclObjectPermissionActions = map[Permission]proto.Actions{
		ReadPermission: {
			proto.OSSGetObjectAction,
			proto.OSSGetObjectTorrentAction,
		},
		WritePermission: {},
		ReadACPPermission: {
			proto.OSSGetObjectAclAction,
		},
		WriteACPPermission: {
			proto.OSSPutObjectAclAction},
		FullControlPermission: {
			proto.OSSGetObjectAction,
			proto.OSSGetObjectTorrentAction,
			proto.OSSGetObjectAclAction,
			proto.OSSPutObjectAclAction,
		},
	}
)

type StandardACL string

const (
	PrivateACL                StandardACL = "private"
	PublicReadACL                         = "public-read"
	PubliceReadWriteACL                   = "public-read-write"
	AwsExecReadACL                        = "aws-exec-read"
	AuthenticatedReadACL                  = "authenticated-read"
	BucketOwnerReadACL                    = "bucket-owner-read"
	BucketOwnerFullControlACL             = "bucket-owner-full-control"
	LogDeliveryWriteACL                   = "log-delivery-write"
)

type ResourceType string

const (
	bucketResource ResourceType = "bucket"
	objectResource              = "object"
)

type AclRole = string

const (
	objectOwnerRole AclRole = "owner"
	bucketOwnerRole         = "bucket-owner"
	allUsersRole            = "AllUsers"
	LogDeliveryRole         = "LogDelivery"
)

var (
	aclPermissions = map[StandardACL]map[ResourceType]map[string][]Permission{
		PrivateACL:                {"bucket": {"owner": {FullControlPermission}}, "object": {"owner": {FullControlPermission}}},
		PublicReadACL:             {"bucket": {"owner": {FullControlPermission}, "AllUsers": {ReadPermission}}, "object": {"owner": {FullControlPermission}, "AllUsers": {ReadPermission}}},
		PubliceReadWriteACL:       {"bucket": {"owner": {FullControlPermission}, "AllUsers": {ReadPermission, WritePermission}}, "object": {"owner": {FullControlPermission}, "AllUsers": {ReadPermission, WritePermission}}},
		AwsExecReadACL:            {"bucket": {"owner": {FullControlPermission}}, "object": {"owner": {FullControlPermission}}},
		AuthenticatedReadACL:      {"bucket": {"owner": {FullControlPermission}}, "object": {"owner": {FullControlPermission}}},
		BucketOwnerReadACL:        {"object": {"owner": {FullControlPermission}, "bucket-owner": {ReadPermission}}},
		BucketOwnerFullControlACL: {"object": {"owner": {FullControlPermission}, "bucket-owner": {FullControlPermission}}},
		LogDeliveryWriteACL:       {"bucket": {"LogDelivery": {WriteACPPermission, ReadACPPermission}}},
	}
)

//grant permission
type Permission string

// grantee
type Grantee struct {
	Xmlxsi       string `xml:"xmlns:xsi,attr"`
	Xmlns        string `xml:"xsi,attr"`
	XsiType      string `xml:"xsi:type,attr"`
	Type         string `xml:"type,attr"`
	Id           string `xml:"ID,omitempty"`
	URI          string `xml:"URI,omitempty"`
	DisplayName  string `xml:"DisplayName,omitempty"`
	EmailAddress string `xml:"EmailAddress,omitempty"`
}

// grant
type Grant struct {
	Grantee    Grantee    `xml:"Grantee,omitempty"`
	Permission Permission `xml:"Permission,omitempty"`
}

// access control list
type AccessControlList struct {
	Grants []Grant `xml:"Grant,omitempty"`
}

func (acl *AccessControlList) IsEmpty() bool {
	return len(acl.Grants) == 0
}

// owner
type Owner struct {
	Id          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// access control policy
type AccessControlPolicy struct {
	Xmlns string            `xml:"xmlns,attr"`
	Owner Owner             `xml:"Owner,omitempty"`
	Acl   AccessControlList `xml:"AccessControlList,omitempty"`
}

func (acp *AccessControlPolicy) IsAclEmpty() bool {
	return acp.Acl.IsEmpty()
}

func (acp *AccessControlPolicy) Validate(bucket string) (bool, error) {
	for _, grant := range acp.Acl.Grants {
		if !grant.Validate() {
			return false, nil
		}
	}

	return true, nil
}

func (acp *AccessControlPolicy) IsAllowed(param *RequestParam, isOwner bool) bool {
	log.LogDebugf("acl is allowed: %v param: %v", acp, param)
	if len(acp.Acl.Grants) == 0 {
		return true
	}
	if isOwner {
		return true
	}
	for _, grant := range acp.Acl.Grants {
		if grant.IsAllowed(param) {
			return true
		}
	}
	return false
}

var (
	aclGrantKeyPermissionMap = map[string]Permission{
		"x-amz-grant-full-control": FullControlPermission,
		"x-amz-grant-read":         ReadPermission,
		"x-amz-grant-read-acp":     ReadACPPermission,
		"x-amz-grant-write":        WritePermission,
		"x-amz-grant-write-acp":    WriteACPPermission,
	}
	aclRoleURIMap = map[string]string{
		"AllUsers":    "http://acs.amazonaws.com/groups/global/AllUsers",
		"LogDelivery": "http://acs.amazonaws.com/groups/s3/LogDelivery",
	}
)

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
func (acp *AccessControlPolicy) SetBucketStandardACL(param *RequestParam, acl string) {
	sacl := StandardACL(acl)
	var (
		rolePermissionsMap map[string][]Permission
		ok                 bool
	)

	if rolePermissionsMap, ok = aclPermissions[sacl]["bucket"]; !ok {
		return
	}
	for role, permissions := range rolePermissionsMap {
		grantee := Grantee{}
		if uri, ok := aclRoleURIMap[role]; ok {
			grantee.URI = uri
		} else {
			grantee.Id = param.accessKey
			grantee.DisplayName = param.accessKey
		}
		for _, p := range permissions {
			grant := Grant{
				Grantee:    grantee,
				Permission: p,
			}
			acp.Acl.Grants = append(acp.Acl.Grants, grant)
		}
	}
}

func (acp *AccessControlPolicy) SetBucketGrantACL(param *RequestParam, permission Permission) {
	grantee := Grantee{
		Id:          param.accessKey,
		DisplayName: param.accessKey,
	}
	grant := Grant{
		Grantee:    grantee,
		Permission: permission,
	}
	acp.Acl.Grants = append(acp.Acl.Grants, grant)
}

func (acp *AccessControlPolicy) Marshal() ([]byte, error) {
	data, err := xml.Marshal(acp)
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), data...), nil
}

func ParseACL(bytes []byte, bucket string) (*AccessControlPolicy, error) {
	acl := &AccessControlPolicy{}
	err2 := xml.Unmarshal(bytes, acl)
	if err2 != nil {
		return nil, err2
	}

	ok, err3 := acl.Validate(bucket)
	if err3 != nil {
		return nil, err3
	}
	if !ok {
		return nil, errors.New("")
	}

	return acl, nil
}

func storeBucketACL(bytes []byte, vol *Volume) (*AccessControlPolicy, error) {
	acl, err3 := ParseACL(bytes, vol.name)
	if err3 != nil {
		return nil, err3
	}

	err4 := vol.store.Put(vol.name, bucketRootPath, XAttrKeyOSSACL, bytes)
	if err4 != nil {
		return nil, err4
	}

	vol.metaLoader.storeACL(acl)

	return acl, nil
}

func (g Grant) Validate() bool {
	return true
}

func (g *Grant) IsAllowed(param *RequestParam) bool {
	if param.accessKey != g.Grantee.Id {
		return false
	}
	actions := aclBucketPermissionActions[g.Permission]
	return IsIntersectionActions(actions, param.Action())
}
