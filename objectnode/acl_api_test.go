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
	"encoding/xml"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHasAclInRequest(t *testing.T) {
	aclExample := `<AccessControlPolicy>
    <Owner>
        <ID>user</ID>
    </Owner>
    <AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
				<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
			</Grantee>
			<Permission>WRITE</Permission>
		</Grant>
    </AccessControlList>
</AccessControlPolicy>`
	// both have body acl and canned acl
	req, _ := http.NewRequest("PUT", "http://bucket.s3.com?acl", strings.NewReader(aclExample))
	req.ContentLength = int64(len(aclExample))
	require.Equal(t, true, HasAclInRequest(req))

	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Set("x-amz-acl", "private")
	require.Equal(t, true, HasAclInRequest(req))

	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Set("x-amz-grant-read", "id=user")
	require.Equal(t, true, HasAclInRequest(req))

	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	require.Equal(t, false, HasAclInRequest(req))
}

func TestParseAcl_CannedAcl(t *testing.T) {
	// private
	req, _ := http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Add("x-amz-acl", "private")
	acp, err := ParseACL(req, "user", false, false)
	require.NoError(t, err)
	require.Equal(t, "user", acp.Owner.Id)
	require.Equal(t, "user", acp.Acl.Grants[0].Grantee.Id)
	require.Equal(t, "CanonicalUser", acp.Acl.Grants[0].Grantee.Type)
	require.Equal(t, "FULL_CONTROL", acp.Acl.Grants[0].Permission)
	// public-read
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Add("x-amz-acl", "public-read")
	acp, err = ParseACL(req, "user", false, false)
	require.NoError(t, err)
	require.Equal(t, "user", acp.Owner.Id)
	require.Equal(t, "user", acp.Acl.Grants[0].Grantee.Id)
	require.Equal(t, "CanonicalUser", acp.Acl.Grants[0].Grantee.Type)
	require.Equal(t, "FULL_CONTROL", acp.Acl.Grants[0].Permission)
	require.Equal(t, "", acp.Acl.Grants[1].Grantee.Id)
	require.Equal(t, "http://acs.amazonaws.com/groups/global/AllUsers", acp.Acl.Grants[1].Grantee.URI)
	require.Equal(t, "Group", acp.Acl.Grants[1].Grantee.Type)
	require.Equal(t, "READ", acp.Acl.Grants[1].Permission)
	// public-read-write
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Add("x-amz-acl", "public-read-write")
	acp, err = ParseACL(req, "user", false, false)
	require.NoError(t, err)
	require.Equal(t, "user", acp.Owner.Id)
	require.Equal(t, "user", acp.Acl.Grants[0].Grantee.Id)
	require.Equal(t, "CanonicalUser", acp.Acl.Grants[0].Grantee.Type)
	require.Equal(t, "FULL_CONTROL", acp.Acl.Grants[0].Permission)
	require.Equal(t, "", acp.Acl.Grants[1].Grantee.Id)
	require.Equal(t, "http://acs.amazonaws.com/groups/global/AllUsers", acp.Acl.Grants[1].Grantee.URI)
	require.Equal(t, "Group", acp.Acl.Grants[1].Grantee.Type)
	require.Equal(t, "READ", acp.Acl.Grants[1].Permission)
	require.Equal(t, "", acp.Acl.Grants[2].Grantee.Id)
	require.Equal(t, "http://acs.amazonaws.com/groups/global/AllUsers", acp.Acl.Grants[2].Grantee.URI)
	require.Equal(t, "Group", acp.Acl.Grants[2].Grantee.Type)
	require.Equal(t, "WRITE", acp.Acl.Grants[2].Permission)
	// authenticated-read
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Add("x-amz-acl", "authenticated-read")
	acp, err = ParseACL(req, "user", false, false)
	require.NoError(t, err)
	require.Equal(t, "user", acp.Owner.Id)
	require.Equal(t, "user", acp.Acl.Grants[0].Grantee.Id)
	require.Equal(t, "CanonicalUser", acp.Acl.Grants[0].Grantee.Type)
	require.Equal(t, "FULL_CONTROL", acp.Acl.Grants[0].Permission)
	require.Equal(t, "", acp.Acl.Grants[1].Grantee.Id)
	require.Equal(t, "http://acs.amazonaws.com/groups/global/AuthenticatedUsers", acp.Acl.Grants[1].Grantee.URI)
	require.Equal(t, "Group", acp.Acl.Grants[1].Grantee.Type)
	require.Equal(t, "READ", acp.Acl.Grants[1].Permission)
}

func TestParseAcl_GrantAclHeader(t *testing.T) {
	req, _ := http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Add("x-amz-grant-read", "uri=http://acs.amazonaws.com/groups/global/AllUsers, id=user1")
	req.Header.Add("x-amz-grant-write", "id = user2, id=user3")
	req.Header.Add("x-amz-grant-read-acp", "id=user4")
	req.Header.Add("x-amz-grant-write-acp", "id=user5")
	req.Header.Add("x-amz-grant-full-control", "id=user")

	acp, err := ParseACL(req, "user", false, false)
	require.NoError(t, err)
	_, err = xml.Marshal(acp)
	require.NoError(t, err)

	expectAclXml := `
<AccessControlPolicy>
    <Owner>
        <ID>user</ID>
    </Owner>
    <AccessControlList>
        <Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
                <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
            </Grantee>
            <Permission>READ</Permission>
		</Grant>
		<Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>user1</ID>
            </Grantee>
            <Permission>READ</Permission>
        </Grant>
        <Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>user2</ID>
            </Grantee>
            <Permission>WRITE</Permission>
		</Grant>
		<Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>user3</ID>
            </Grantee>
            <Permission>WRITE</Permission>
        </Grant>
        <Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>user4</ID>
            </Grantee>
            <Permission>READ_ACP</Permission>
        </Grant>
        <Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>user5</ID>
            </Grantee>
            <Permission>WRITE_ACP</Permission>
        </Grant>
        <Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>user</ID>
            </Grantee>
            <Permission>FULL_CONTROL</Permission>
        </Grant>
    </AccessControlList>
</AccessControlPolicy>`

	var expectAcp AccessControlPolicy
	err = xml.Unmarshal([]byte(expectAclXml), &expectAcp)
	require.NoError(t, err)
	require.Equal(t, &expectAcp, acp)

	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Add("x-amz-grant-read", "")
	_, err = ParseACL(req, "user", false, false)
	require.EqualError(t, err, ErrMissingGrants.Error())

	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Add("x-amz-grant-read", "uri=http://acs.amazonaws.com/wrong/uri")
	_, err = ParseACL(req, "user", false, false)
	require.EqualError(t, err, ErrInvalidGroupUri.Error())

	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Add("x-amz-grant-read", "id=user1 id=user2")
	_, err = ParseACL(req, "user", false, false)
	require.EqualError(t, err, InvalidArgument.Error())

	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Add("x-amz-grant-read", "id=")
	_, err = ParseACL(req, "user", false, false)
	require.EqualError(t, err, InvalidArgument.Error())

	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Add("x-amz-grant-read", "=user")
	_, err = ParseACL(req, "user", false, false)
	require.EqualError(t, err, InvalidArgument.Error())
}

func TestParseAcl_XmlBodyAcl(t *testing.T) {
	aclExample := `<AccessControlPolicy>
    <Owner>
        <ID>user</ID>
    </Owner>
    <AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
				<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
			</Grantee>
			<Permission>WRITE</Permission>
		</Grant>
        <Grant>
            <Grantee xmlns:XMLSchema-instance="http://www.w3.org/2001/XMLSchema-instance" XMLSchema-instance:type="CanonicalUser">
                <ID>user1</ID>
            </Grantee>
            <Permission>READ</Permission>
        </Grant>
    </AccessControlList>
</AccessControlPolicy>`

	req, _ := http.NewRequest("PUT", "http://bucket.s3.com?acl", strings.NewReader(aclExample))
	req.ContentLength = int64(len(aclExample))

	acp, err := ParseACL(req, "user", true, false)
	require.NoError(t, err)

	require.Equal(t, "user", acp.Owner.Id)
	require.Equal(t, "WRITE", acp.Acl.Grants[0].Permission)
	require.Equal(t, "Group", acp.Acl.Grants[0].Grantee.Type)
	require.Equal(t, "http://acs.amazonaws.com/groups/global/AllUsers", acp.Acl.Grants[0].Grantee.URI)
	require.Equal(t, "READ", acp.Acl.Grants[1].Permission)
	require.Equal(t, "CanonicalUser", acp.Acl.Grants[1].Grantee.Type)
	require.Equal(t, "user1", acp.Acl.Grants[1].Grantee.Id)

	aclExample = `<AccessControlPolicy>
    <Owner>
        <ID>user</ID>
    </Owner>
    <AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
				<WrongXML>http://acs.amazonaws.com/groups/global/AllUsers</WrongXML>
			</Grantee>
			<Permission>WRITE</Permission>
		</Grant>
    </AccessControlList>
</AccessControlPolicy>`
	// wrong xml format
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", strings.NewReader(aclExample))
	req.ContentLength = int64(len(aclExample))
	_, err = ParseACL(req, "user", true, false)
	require.EqualError(t, err, ErrMalformedACL.Error())

	aclExample = `<AccessControlPolicy>
    <Owner>
        <ID>user</ID>
    </Owner>
    <AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
				<URI></URI>
				<ID></ID>
			</Grantee>
			<Permission>WRITE</Permission>
		</Grant>
    </AccessControlList>
</AccessControlPolicy>`
	// both have no URI and ID
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", strings.NewReader(aclExample))
	req.ContentLength = int64(len(aclExample))
	_, err = ParseACL(req, "user", true, false)
	require.EqualError(t, err, ErrMalformedACL.Error())

	aclExample = `<AccessControlPolicy>
    <Owner>
        <ID>user</ID>
    </Owner>
    <AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>user1</ID>
				<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
            </Grantee>
            <Permission>READ_ACP</Permission>
		</Grant>
    </AccessControlList>
</AccessControlPolicy>`
	// both have ID and URI
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", strings.NewReader(aclExample))
	req.ContentLength = int64(len(aclExample))
	_, err = ParseACL(req, "user", true, false)
	require.EqualError(t, err, ErrMalformedACL.Error())

	aclExample = `<AccessControlPolicy>
    <Owner>
        <ID>user</ID>
    </Owner>
    <AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
				<URI>http://acs.amazonaws.com/groups/global/WrongRUI</URI>
            </Grantee>
            <Permission>READ_ACP</Permission>
		</Grant>
    </AccessControlList>
</AccessControlPolicy>`
	// Group but gave an invalid URI
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", strings.NewReader(aclExample))
	req.ContentLength = int64(len(aclExample))
	_, err = ParseACL(req, "user", true, false)
	require.EqualError(t, err, ErrInvalidGroupUri.Error())

	aclExample = `<AccessControlPolicy>
    <Owner>
        <ID>user</ID>
    </Owner>
    <AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
				<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
				<ID></ID>
            </Grantee>
            <Permission>READ_ACP</Permission>
		</Grant>
    </AccessControlList>
</AccessControlPolicy>`
	// CanonicalUser but given the URI
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", strings.NewReader(aclExample))
	req.ContentLength = int64(len(aclExample))
	_, err = ParseACL(req, "user", true, false)
	require.EqualError(t, err, ErrMalformedACL.Error())

	aclExample = `<AccessControlPolicy>
    <Owner>
        <ID>user</ID>
    </Owner>
    <AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ErrType">
				<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
				<ID></ID>
            </Grantee>
            <Permission>READ_ACP</Permission>
		</Grant>
    </AccessControlList>
</AccessControlPolicy>`
	// invalid type
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", strings.NewReader(aclExample))
	req.ContentLength = int64(len(aclExample))
	_, err = ParseACL(req, "user", true, false)
	require.EqualError(t, err, ErrMalformedACL.Error())

	aclExample = `<AccessControlPolicy>
    <Owner>
        <ID>UserIsNotOwner</ID>
    </Owner>
    <AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
				<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
			</Grantee>
			<Permission>WRITE</Permission>
		</Grant>
    </AccessControlList>
</AccessControlPolicy>`
	// user is not owner
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", strings.NewReader(aclExample))
	req.ContentLength = int64(len(aclExample))
	_, err = ParseACL(req, "user", true, false)
	require.EqualError(t, err, AccessDenied.Error())

}

func TestCreateDefaultACL(t *testing.T) {
	req, _ := http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	acp, err := ParseACL(req, "user", false, true)
	require.NoError(t, err)
	require.Equal(t, "user", acp.Owner.Id)
	require.Equal(t, "user", acp.Acl.Grants[0].Grantee.Id)
	require.Equal(t, "CanonicalUser", acp.Acl.Grants[0].Grantee.Type)
	require.Equal(t, "FULL_CONTROL", acp.Acl.Grants[0].Permission)
}

func TestConflictAcl(t *testing.T) {
	aclExample := `<AccessControlPolicy>
    <Owner>
        <ID>user</ID>
    </Owner>
    <AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
				<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
			</Grantee>
			<Permission>WRITE</Permission>
		</Grant>
    </AccessControlList>
</AccessControlPolicy>`
	// both have body acl and canned acl
	req, _ := http.NewRequest("PUT", "http://bucket.s3.com?acl", strings.NewReader(aclExample))
	req.ContentLength = int64(len(aclExample))
	req.Header.Set("x-amz-acl", "private")
	_, err := ParseACL(req, "user", true, false)
	require.EqualError(t, err, ErrUnexpectedContent.Error())

	// both have body acl and grant acl
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", strings.NewReader(aclExample))
	req.ContentLength = int64(len(aclExample))
	req.Header.Set("x-amz-grant-read", "id=user1")
	_, err = ParseACL(req, "user", true, false)
	require.EqualError(t, err, ErrUnexpectedContent.Error())

	// both have canned acl and grant acl
	req, _ = http.NewRequest("PUT", "http://bucket.s3.com?acl", nil)
	req.Header.Set("x-amz-grant-read", "id=user1")
	req.Header.Set("x-amz-acl", "public-read")
	_, err = ParseACL(req, "user", false, false)
	require.EqualError(t, err, ErrConflictAclHeader.Error())
}
