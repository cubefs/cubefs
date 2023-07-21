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

import (
	"encoding/xml"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestAccessControlPolicyXml(t *testing.T) {
	aclExample := `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Owner>
		<ID>id</ID>
		<DisplayName>display-id-name</DisplayName>
	</Owner>
	<AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
				<ID>id1</ID>
				<DisplayName>display-id1-name</DisplayName>
			</Grantee>
			<Permission>FULL_CONTROL</Permission>
		</Grant>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
				<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
			</Grantee>
			<Permission>READ</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var result AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &result)
	require.NoError(t, err)
	require.NoError(t, result.IsValid())

	require.Equal(t, "id", result.Owner.Id)
	require.Equal(t, "display-id-name", result.Owner.DisplayName)

	require.Equal(t, "FULL_CONTROL", result.Acl.Grants[0].Permission)
	require.Equal(t, "id1", result.Acl.Grants[0].Grantee.Id)
	require.Equal(t, "CanonicalUser", result.Acl.Grants[0].Grantee.Type)
	require.Equal(t, "", result.Acl.Grants[0].Grantee.URI)
	require.Equal(t, "display-id1-name", result.Acl.Grants[0].Grantee.DisplayName)

	require.Equal(t, "READ", result.Acl.Grants[1].Permission)
	require.Equal(t, "", result.Acl.Grants[1].Grantee.Id)
	require.Equal(t, "Group", result.Acl.Grants[1].Grantee.Type)
	require.Equal(t, "http://acs.amazonaws.com/groups/global/AllUsers", result.Acl.Grants[1].Grantee.URI)
	require.Equal(t, "", result.Acl.Grants[1].Grantee.DisplayName)
}

func TestAccessControlPolicyXml_NoGrantIDAndURI(t *testing.T) {
	aclExample := `<AccessControlPolicy>
	<Owner>
		<ID>id</ID>
	</Owner>
	<AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
				<ID></ID>
				<URI></URI>
			</Grantee>
			<Permission>FULL_CONTROL</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var result AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &result)
	require.NoError(t, err)
	require.Error(t, result.IsValid())
}

func TestAccessControlPolicyXml_BothHaveIDAndURI(t *testing.T) {
	aclExample := `<AccessControlPolicy>
	<Owner>
		<ID>id</ID>
	</Owner>
	<AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
				<ID>id1</ID>
				<URI>uri1</URI>
			</Grantee>
			<Permission>FULL_CONTROL</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var result AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &result)
	require.NoError(t, err)
	require.Error(t, result.IsValid())
}

func TestAccessControlPolicyXml_ErrType(t *testing.T) {
	aclExample := `<AccessControlPolicy>
	<Owner>
		<ID>id</ID>
	</Owner>
	<AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ErrType">
				<ID>id</ID>
			</Grantee>
			<Permission>FULL_CONTROL</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var result AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &result)
	require.NoError(t, err)
	require.Error(t, result.IsValid())
}

func TestAccessControlPolicyXml_ErrURI(t *testing.T) {
	aclExample := `<AccessControlPolicy>
	<Owner>
		<ID>id</ID>
	</Owner>
	<AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
				<URI>http://acs.amazonaws.com/groups/global/ErrRUI</URI>
			</Grantee>
			<Permission>READ</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var result AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &result)
	require.NoError(t, err)
	require.Error(t, result.IsValid())
}

func TestAccessControlPolicyXml_ErrMatchGroupToID(t *testing.T) {
	aclExample := `<AccessControlPolicy>
	<Owner>
		<ID>id</ID>
	</Owner>
	<AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
				<ID>id1</ID>
			</Grantee>
			<Permission>READ</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var result AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &result)
	require.NoError(t, err)
	require.Error(t, result.IsValid())
}

func TestAccessControlPolicyXml_ErrMatchCanonicalUserToURI(t *testing.T) {
	aclExample := `<AccessControlPolicy>
	<Owner>
		<ID>id</ID>
	</Owner>
	<AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
				<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
			</Grantee>
			<Permission>READ</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var result AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &result)
	require.NoError(t, err)
	require.Error(t, result.IsValid())
}

func TestAccessControlPolicyXml_ErrPermission(t *testing.T) {
	aclExample := `<AccessControlPolicy>
	<Owner>
		<ID>id</ID>
	</Owner>
	<AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
				<ID>id1</ID>
			</Grantee>
			<Permission>ERR_PERMISSION</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var result AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &result)
	require.NoError(t, err)
	require.Error(t, result.IsValid())
}

func TestAccessControlPolicyXml_NoGrant(t *testing.T) {
	aclExample := `<AccessControlPolicy>
	<Owner>
		<ID>id</ID>
	</Owner>
	<AccessControlList>
		<Grant>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var result AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &result)
	require.NoError(t, err)
	require.Error(t, result.IsValid())
}

func TestSetPrivate(t *testing.T) {
	acp := AccessControlPolicy{}
	acp.SetPrivate("id")
	require.Equal(t, "id", acp.Owner.Id)
	require.Equal(t, "", acp.Owner.DisplayName)
	require.Equal(t, "FULL_CONTROL", acp.Acl.Grants[0].Permission)
	require.Equal(t, "id", acp.Acl.Grants[0].Grantee.Id)
	require.Equal(t, "", acp.Acl.Grants[0].Grantee.URI)
	require.Equal(t, "", acp.Acl.Grants[0].Grantee.DisplayName)
	require.Equal(t, "CanonicalUser", acp.Acl.Grants[0].Grantee.Type)
}

func TestSetPublicRead(t *testing.T) {
	acp := AccessControlPolicy{}
	acp.SetPublicRead("id")
	require.Equal(t, "id", acp.Owner.Id)
	require.Equal(t, "", acp.Owner.DisplayName)
	require.Equal(t, "FULL_CONTROL", acp.Acl.Grants[0].Permission)
	require.Equal(t, "id", acp.Acl.Grants[0].Grantee.Id)
	require.Equal(t, "", acp.Acl.Grants[0].Grantee.URI)
	require.Equal(t, "", acp.Acl.Grants[0].Grantee.DisplayName)
	require.Equal(t, "CanonicalUser", acp.Acl.Grants[0].Grantee.Type)

	require.Equal(t, "READ", acp.Acl.Grants[1].Permission)
	require.Equal(t, "", acp.Acl.Grants[1].Grantee.Id)
	require.Equal(t, "http://acs.amazonaws.com/groups/global/AllUsers", acp.Acl.Grants[1].Grantee.URI)
	require.Equal(t, "", acp.Acl.Grants[1].Grantee.DisplayName)
	require.Equal(t, "Group", acp.Acl.Grants[1].Grantee.Type)
}

func TestSetPublicReadWrite(t *testing.T) {
	acp := AccessControlPolicy{}
	acp.SetPublicReadWrite("id")
	require.Equal(t, "id", acp.Owner.Id)
	require.Equal(t, "", acp.Owner.DisplayName)
	require.Equal(t, "FULL_CONTROL", acp.Acl.Grants[0].Permission)
	require.Equal(t, "id", acp.Acl.Grants[0].Grantee.Id)
	require.Equal(t, "", acp.Acl.Grants[0].Grantee.URI)
	require.Equal(t, "", acp.Acl.Grants[0].Grantee.DisplayName)
	require.Equal(t, "CanonicalUser", acp.Acl.Grants[0].Grantee.Type)

	require.Equal(t, "READ", acp.Acl.Grants[1].Permission)
	require.Equal(t, "", acp.Acl.Grants[1].Grantee.Id)
	require.Equal(t, "http://acs.amazonaws.com/groups/global/AllUsers", acp.Acl.Grants[1].Grantee.URI)
	require.Equal(t, "", acp.Acl.Grants[1].Grantee.DisplayName)
	require.Equal(t, "Group", acp.Acl.Grants[1].Grantee.Type)

	require.Equal(t, "WRITE", acp.Acl.Grants[2].Permission)
	require.Equal(t, "", acp.Acl.Grants[2].Grantee.Id)
	require.Equal(t, "http://acs.amazonaws.com/groups/global/AllUsers", acp.Acl.Grants[2].Grantee.URI)
	require.Equal(t, "", acp.Acl.Grants[2].Grantee.DisplayName)
	require.Equal(t, "Group", acp.Acl.Grants[2].Grantee.Type)
}

func TestSetAuthenticatedRead(t *testing.T) {
	acp := AccessControlPolicy{}
	acp.SetAuthenticatedRead("id")
	require.Equal(t, "id", acp.Owner.Id)
	require.Equal(t, "", acp.Owner.DisplayName)
	require.Equal(t, "FULL_CONTROL", acp.Acl.Grants[0].Permission)
	require.Equal(t, "id", acp.Acl.Grants[0].Grantee.Id)
	require.Equal(t, "", acp.Acl.Grants[0].Grantee.URI)
	require.Equal(t, "", acp.Acl.Grants[0].Grantee.DisplayName)
	require.Equal(t, "CanonicalUser", acp.Acl.Grants[0].Grantee.Type)

	require.Equal(t, "READ", acp.Acl.Grants[1].Permission)
	require.Equal(t, "", acp.Acl.Grants[1].Grantee.Id)
	require.Equal(t, "http://acs.amazonaws.com/groups/global/AuthenticatedUsers", acp.Acl.Grants[1].Grantee.URI)
	require.Equal(t, "", acp.Acl.Grants[1].Grantee.DisplayName)
	require.Equal(t, "Group", acp.Acl.Grants[1].Grantee.Type)
}

func TestAccessControlPolicy_FULL_CONTROL(t *testing.T) {
	aclExample := `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Owner>
		<ID>id</ID>
		<DisplayName>display-id-name</DisplayName>
	</Owner>
	<AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
				<ID>id1</ID>
				<DisplayName>display-id1-name</DisplayName>
			</Grantee>
			<Permission>FULL_CONTROL</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var acl AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &acl)
	require.NoError(t, err)
	require.NoError(t, acl.IsValid())
	// owner acl
	reqId := "id"
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSPutBucketAclAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSGetBucketAclAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSPutObjectAclAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSGetObjectAclAction))
	// owner other
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSHeadObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSListObjectsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSHeadBucketAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCopyObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSUploadPartAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSListMultipartUploadsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCreateMultipartUploadAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCompleteMultipartUploadAction))
	// non-owner id1 has FULL_CONTROL permission
	reqId = "id1"
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSPutBucketAclAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSGetBucketAclAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSPutObjectAclAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSGetObjectAclAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSGetObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSPutObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSHeadObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSListObjectsAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSHeadBucketAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSCopyObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSUploadPartAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSDeleteObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSDeleteObjectsAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSListMultipartUploadsAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSCreateMultipartUploadAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSCompleteMultipartUploadAction))
	// non-owner id2 has no permission
	reqId = "id2"
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetObjectAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSHeadObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSListObjectsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSHeadBucketAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCopyObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSUploadPartAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSListMultipartUploadsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCreateMultipartUploadAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCompleteMultipartUploadAction))
}

func TestAccessControlPolicy_WRITE(t *testing.T) {
	aclExample := `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Owner>
		<ID>id</ID>
		<DisplayName>display-id-name</DisplayName>
	</Owner>
	<AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
				<ID>id1</ID>
				<DisplayName>display-id1-name</DisplayName>
			</Grantee>
			<Permission>WRITE</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var acl AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &acl)
	require.NoError(t, err)
	require.NoError(t, acl.IsValid())
	// non-owner id1 has WRITE permission
	reqId := "id1"
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSPutObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSCopyObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSUploadPartAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSDeleteObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSDeleteObjectsAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSCreateMultipartUploadAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSCompleteMultipartUploadAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetObjectAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSListObjectsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSHeadBucketAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSHeadObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSListMultipartUploadsAction))
}

func TestAccessControlPolicy_PublicREAD(t *testing.T) {
	aclExample := `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Owner>
		<ID>id</ID>
		<DisplayName>display-id-name</DisplayName>
	</Owner>
	<AccessControlList>
		<Grant>
			 <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
                <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
            </Grantee>
            <Permission>READ</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var acl AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &acl)
	require.NoError(t, err)
	require.NoError(t, acl.IsValid())
	// non-owner has READ permission
	reqId := "id1"
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSListObjectsAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSHeadBucketAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSGetObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSHeadObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSListMultipartUploadsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCopyObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSUploadPartAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCreateMultipartUploadAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCompleteMultipartUploadAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetObjectAclAction))

	reqId = AnonymousUser
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSListObjectsAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSHeadBucketAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSGetObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSHeadObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSListMultipartUploadsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCopyObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSUploadPartAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCreateMultipartUploadAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCompleteMultipartUploadAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetObjectAclAction))
}

func TestAccessControlPolicy_AuthenticatedREAD(t *testing.T) {
	aclExample := `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Owner>
		<ID>id</ID>
		<DisplayName>display-id-name</DisplayName>
	</Owner>
	<AccessControlList>
		<Grant>
			 <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
                <URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI>
            </Grantee>
            <Permission>READ</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var acl AccessControlPolicy
	err := xml.Unmarshal([]byte(aclExample), &acl)
	require.NoError(t, err)
	require.NoError(t, acl.IsValid())
	// non-owner has READ permission
	reqId := "id1"
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSListObjectsAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSHeadBucketAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSGetObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSHeadObjectAction))
	require.Equal(t, true, acl.IsAllowed(reqId, proto.OSSListMultipartUploadsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCopyObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSUploadPartAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCreateMultipartUploadAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCompleteMultipartUploadAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetObjectAclAction))

	reqId = AnonymousUser
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSListObjectsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSHeadBucketAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSHeadObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSListMultipartUploadsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCopyObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSUploadPartAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSDeleteObjectsAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCreateMultipartUploadAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSCompleteMultipartUploadAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetBucketAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSPutObjectAclAction))
	require.Equal(t, false, acl.IsAllowed(reqId, proto.OSSGetObjectAclAction))
}
