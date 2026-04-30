// Copyright 2026 The CubeFS Authors.
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
	"fmt"
	"strings"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func canonicalGrantForTest(id, permission string) Grant {
	return Grant{
		Grantee: Grantee{
			Type: TypeCanonicalUser,
			Id:   id,
		},
		Permission: permission,
	}
}

func groupGrantForTest(uri, permission string) Grant {
	return Grant{
		Grantee: Grantee{
			Type: TypeGroup,
			URI:  uri,
		},
		Permission: permission,
	}
}

func policyWithGrantsForTest(owner string, grants ...Grant) AccessControlPolicy {
	return AccessControlPolicy{
		Owner: Owner{Id: owner},
		Acl: AccessControlList{
			Grants: grants,
		},
	}
}

func requireGrantForTest(t *testing.T, grant Grant, id, uri, granteeType, permission string) {
	t.Helper()

	require.Equal(t, id, grant.Grantee.Id)
	require.Equal(t, uri, grant.Grantee.URI)
	require.Equal(t, granteeType, grant.Grantee.Type)
	require.Equal(t, permission, grant.Permission)
}

func requireAllowedForTest(t *testing.T, acl AccessControlPolicy, reqID string, action proto.Action) {
	t.Helper()

	require.Truef(t, acl.IsAllowed(reqID, action), "expected %q to be allowed for %q", reqID, action)
}

func requireDeniedForTest(t *testing.T, acl AccessControlPolicy, reqID string, action proto.Action) {
	t.Helper()

	require.Falsef(t, acl.IsAllowed(reqID, action), "expected %q to be denied for %q", reqID, action)
}

func TestGranteeIsValidExtendedCases(t *testing.T) {
	tests := []struct {
		name    string
		grantee Grantee
		wantErr error
	}{
		{
			name: "canonical user with id",
			grantee: Grantee{
				Type: TypeCanonicalUser,
				Id:   "user-1",
			},
		},
		{
			name: "all users group with uri",
			grantee: Grantee{
				Type: TypeGroup,
				URI:  GroupAllUser,
			},
		},
		{
			name: "authenticated users group with uri",
			grantee: Grantee{
				Type: TypeGroup,
				URI:  GroupAuthenticated,
			},
		},
		{
			name: "missing identity",
			grantee: Grantee{
				Type: TypeCanonicalUser,
			},
			wantErr: ErrMalformedACL,
		},
		{
			name: "both id and uri",
			grantee: Grantee{
				Type: TypeCanonicalUser,
				Id:   "user-1",
				URI:  GroupAllUser,
			},
			wantErr: ErrMalformedACL,
		},
		{
			name: "group with canonical id",
			grantee: Grantee{
				Type: TypeGroup,
				Id:   "user-1",
			},
			wantErr: ErrInvalidGroupUri,
		},
		{
			name: "group with unknown uri",
			grantee: Grantee{
				Type: TypeGroup,
				URI:  "http://acs.amazonaws.com/groups/global/UnknownUsers",
			},
			wantErr: ErrInvalidGroupUri,
		},
		{
			name: "canonical user with uri",
			grantee: Grantee{
				Type: TypeCanonicalUser,
				URI:  GroupAllUser,
			},
			wantErr: ErrMalformedACL,
		},
		{
			name: "unknown type with id",
			grantee: Grantee{
				Type: "EmailAddress",
				Id:   "user-1",
			},
			wantErr: ErrMalformedACL,
		},
		{
			name: "empty type with id",
			grantee: Grantee{
				Id: "user-1",
			},
			wantErr: ErrMalformedACL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.grantee.isValid()
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func TestGrantIsPermissionValidExtendedCases(t *testing.T) {
	tests := []struct {
		name       string
		permission string
		want       bool
	}{
		{
			name:       "read",
			permission: PermissionRead,
			want:       true,
		},
		{
			name:       "write",
			permission: PermissionWrite,
			want:       true,
		},
		{
			name:       "read acp",
			permission: PermissionReadAcp,
			want:       true,
		},
		{
			name:       "write acp",
			permission: PermissionWriteAcp,
			want:       true,
		},
		{
			name:       "full control",
			permission: PermissionFullControl,
			want:       true,
		},
		{
			name:       "lowercase read is invalid",
			permission: "read",
			want:       false,
		},
		{
			name:       "empty permission is invalid",
			permission: "",
			want:       false,
		},
		{
			name:       "unknown permission is invalid",
			permission: "DELETE",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grant := canonicalGrantForTest("user-1", tt.permission)
			require.Equal(t, tt.want, grant.isPermissionValid())
		})
	}
}

func TestGrantIsValidExtendedCases(t *testing.T) {
	tests := []struct {
		name    string
		grant   Grant
		wantErr error
	}{
		{
			name:  "canonical read grant",
			grant: canonicalGrantForTest("user-1", PermissionRead),
		},
		{
			name:  "group write grant",
			grant: groupGrantForTest(GroupAllUser, PermissionWrite),
		},
		{
			name:  "authenticated read acp grant",
			grant: groupGrantForTest(GroupAuthenticated, PermissionReadAcp),
		},
		{
			name: "invalid permission checked before grantee",
			grant: Grant{
				Grantee: Grantee{
					Type: TypeCanonicalUser,
				},
				Permission: "READ_WRITE",
			},
			wantErr: ErrInvalidPermission,
		},
		{
			name: "valid permission with malformed grantee",
			grant: Grant{
				Grantee: Grantee{
					Type: TypeCanonicalUser,
				},
				Permission: PermissionRead,
			},
			wantErr: ErrMalformedACL,
		},
		{
			name:    "zero grant has invalid permission",
			grant:   Grant{},
			wantErr: ErrInvalidPermission,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.grant.isValid()
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func TestAccessControlPolicyIsValidGrantCountBoundaries(t *testing.T) {
	t.Run("missing grants", func(t *testing.T) {
		acl := policyWithGrantsForTest("owner")

		require.Equal(t, ErrMissingGrants, acl.IsValid())
	})

	t.Run("exactly max grants", func(t *testing.T) {
		acl := policyWithGrantsForTest("owner")
		for i := 0; i < maxGrantCount; i++ {
			acl.AddGrant(fmt.Sprintf("user-%d", i), TypeCanonicalUser, PermissionRead)
		}

		require.Len(t, acl.Acl.Grants, maxGrantCount)
		require.NoError(t, acl.IsValid())
	})

	t.Run("more than max grants", func(t *testing.T) {
		acl := policyWithGrantsForTest("owner")
		for i := 0; i < maxGrantCount+1; i++ {
			acl.AddGrant(fmt.Sprintf("user-%d", i), TypeCanonicalUser, PermissionRead)
		}

		require.Len(t, acl.Acl.Grants, maxGrantCount+1)
		require.Equal(t, ErrTooManyGrants, acl.IsValid())
	})

	t.Run("invalid grant inside non-empty acl", func(t *testing.T) {
		acl := policyWithGrantsForTest(
			"owner",
			canonicalGrantForTest("user-1", PermissionRead),
			canonicalGrantForTest("", PermissionRead),
		)

		require.Equal(t, ErrMalformedACL, acl.IsValid())
	})
}

func TestAccessControlPolicyOwnerAndEmptyState(t *testing.T) {
	acl := AccessControlPolicy{}
	require.True(t, acl.IsEmpty())
	require.Empty(t, acl.GetOwner())

	acl.SetOwner("owner-1")
	require.Equal(t, "owner-1", acl.GetOwner())
	require.True(t, acl.IsEmpty())

	acl.AddGrant("user-1", TypeCanonicalUser, PermissionRead)
	require.False(t, acl.IsEmpty())
	require.Equal(t, "owner-1", acl.GetOwner())
}

func TestAccessControlPolicyAddGrantIgnoresUnsupportedType(t *testing.T) {
	acl := AccessControlPolicy{}
	acl.AddGrant("user-1", "EmailAddress", PermissionRead)
	require.Empty(t, acl.Acl.Grants)

	acl.AddGrant("user-1", TypeCanonicalUser, PermissionRead)
	require.Len(t, acl.Acl.Grants, 1)
	requireGrantForTest(t, acl.Acl.Grants[0], "user-1", "", TypeCanonicalUser, PermissionRead)

	acl.AddGrant(GroupAuthenticated, TypeGroup, PermissionReadAcp)
	require.Len(t, acl.Acl.Grants, 2)
	requireGrantForTest(t, acl.Acl.Grants[1], "", GroupAuthenticated, TypeGroup, PermissionReadAcp)
}

func TestCannedAclSettersExtended(t *testing.T) {
	tests := []struct {
		name       string
		setter     func(*AccessControlPolicy, string)
		wantGrants []Grant
	}{
		{
			name: "private",
			setter: func(acl *AccessControlPolicy, owner string) {
				acl.SetPrivate(owner)
			},
			wantGrants: []Grant{
				canonicalGrantForTest("owner-1", PermissionFullControl),
			},
		},
		{
			name: "public read",
			setter: func(acl *AccessControlPolicy, owner string) {
				acl.SetPublicRead(owner)
			},
			wantGrants: []Grant{
				canonicalGrantForTest("owner-1", PermissionFullControl),
				groupGrantForTest(GroupAllUser, PermissionRead),
			},
		},
		{
			name: "public read write",
			setter: func(acl *AccessControlPolicy, owner string) {
				acl.SetPublicReadWrite(owner)
			},
			wantGrants: []Grant{
				canonicalGrantForTest("owner-1", PermissionFullControl),
				groupGrantForTest(GroupAllUser, PermissionRead),
				groupGrantForTest(GroupAllUser, PermissionWrite),
			},
		},
		{
			name: "authenticated read",
			setter: func(acl *AccessControlPolicy, owner string) {
				acl.SetAuthenticatedRead(owner)
			},
			wantGrants: []Grant{
				canonicalGrantForTest("owner-1", PermissionFullControl),
				groupGrantForTest(GroupAuthenticated, PermissionRead),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acl := AccessControlPolicy{}
			tt.setter(&acl, "owner-1")

			require.Equal(t, "owner-1", acl.GetOwner())
			require.Equal(t, tt.wantGrants, acl.Acl.Grants)
			require.NoError(t, acl.IsValid())
		})
	}
}

func TestGrantIsAllowedCanonicalPermissionMatrix(t *testing.T) {
	tests := []struct {
		name       string
		permission string
		allowed    []proto.Action
		denied     []proto.Action
	}{
		{
			name:       "read",
			permission: PermissionRead,
			allowed: []proto.Action{
				proto.OSSListObjectsAction,
				proto.OSSHeadBucketAction,
				proto.OSSListMultipartUploadsAction,
				proto.OSSGetObjectAction,
				proto.OSSHeadObjectAction,
			},
			denied: []proto.Action{
				proto.OSSPutObjectAction,
				proto.OSSCopyObjectAction,
				proto.OSSPutBucketAclAction,
				proto.OSSGetBucketAclAction,
				proto.OSSPutObjectAclAction,
				proto.OSSGetObjectAclAction,
			},
		},
		{
			name:       "write",
			permission: PermissionWrite,
			allowed: []proto.Action{
				proto.OSSPutObjectAction,
				proto.OSSPostObjectAction,
				proto.OSSCopyObjectAction,
				proto.OSSCreateMultipartUploadAction,
				proto.OSSUploadPartAction,
				proto.OSSCompleteMultipartUploadAction,
				proto.OSSDeleteObjectAction,
				proto.OSSDeleteObjectsAction,
			},
			denied: []proto.Action{
				proto.OSSListObjectsAction,
				proto.OSSHeadBucketAction,
				proto.OSSGetObjectAction,
				proto.OSSPutBucketAclAction,
				proto.OSSGetBucketAclAction,
			},
		},
		{
			name:       "read acp",
			permission: PermissionReadAcp,
			allowed: []proto.Action{
				proto.OSSGetBucketAclAction,
				proto.OSSGetObjectAclAction,
			},
			denied: []proto.Action{
				proto.OSSPutBucketAclAction,
				proto.OSSPutObjectAclAction,
				proto.OSSGetObjectAction,
				proto.OSSPutObjectAction,
			},
		},
		{
			name:       "write acp",
			permission: PermissionWriteAcp,
			allowed: []proto.Action{
				proto.OSSPutBucketAclAction,
				proto.OSSPutObjectAclAction,
			},
			denied: []proto.Action{
				proto.OSSGetBucketAclAction,
				proto.OSSGetObjectAclAction,
				proto.OSSGetObjectAction,
				proto.OSSPutObjectAction,
			},
		},
		{
			name:       "full control",
			permission: PermissionFullControl,
			allowed: []proto.Action{
				proto.OSSListObjectsAction,
				proto.OSSHeadBucketAction,
				proto.OSSListMultipartUploadsAction,
				proto.OSSPutObjectAction,
				proto.OSSPostObjectAction,
				proto.OSSCopyObjectAction,
				proto.OSSCreateMultipartUploadAction,
				proto.OSSUploadPartAction,
				proto.OSSCompleteMultipartUploadAction,
				proto.OSSDeleteObjectAction,
				proto.OSSDeleteObjectsAction,
				proto.OSSGetObjectAction,
				proto.OSSHeadObjectAction,
				proto.OSSPutBucketAclAction,
				proto.OSSGetBucketAclAction,
				proto.OSSPutObjectAclAction,
				proto.OSSGetObjectAclAction,
			},
			denied: []proto.Action{
				proto.Action("unsupported-action"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acl := policyWithGrantsForTest("owner", canonicalGrantForTest("user-1", tt.permission))

			for _, action := range tt.allowed {
				requireAllowedForTest(t, acl, "user-1", action)
			}
			for _, action := range tt.denied {
				requireDeniedForTest(t, acl, "user-1", action)
			}

			requireDeniedForTest(t, acl, "user-2", proto.OSSGetObjectAction)
			requireDeniedForTest(t, acl, AnonymousUser, proto.OSSGetObjectAction)
		})
	}
}

func TestGrantIsAllowedGroupSemantics(t *testing.T) {
	tests := []struct {
		name      string
		grant     Grant
		reqID     string
		action    proto.Action
		wantAllow bool
	}{
		{
			name:      "all users allows anonymous read",
			grant:     groupGrantForTest(GroupAllUser, PermissionRead),
			reqID:     AnonymousUser,
			action:    proto.OSSGetObjectAction,
			wantAllow: true,
		},
		{
			name:      "all users allows authenticated read",
			grant:     groupGrantForTest(GroupAllUser, PermissionRead),
			reqID:     "user-1",
			action:    proto.OSSHeadObjectAction,
			wantAllow: true,
		},
		{
			name:      "authenticated group denies anonymous read",
			grant:     groupGrantForTest(GroupAuthenticated, PermissionRead),
			reqID:     AnonymousUser,
			action:    proto.OSSGetObjectAction,
			wantAllow: false,
		},
		{
			name:      "authenticated group allows signed read",
			grant:     groupGrantForTest(GroupAuthenticated, PermissionRead),
			reqID:     "user-1",
			action:    proto.OSSGetObjectAction,
			wantAllow: true,
		},
		{
			name:      "group write follows write permission",
			grant:     groupGrantForTest(GroupAllUser, PermissionWrite),
			reqID:     AnonymousUser,
			action:    proto.OSSPutObjectAction,
			wantAllow: true,
		},
		{
			name:      "group write does not allow read",
			grant:     groupGrantForTest(GroupAllUser, PermissionWrite),
			reqID:     "user-1",
			action:    proto.OSSGetObjectAction,
			wantAllow: false,
		},
		{
			name: "unknown grantee type is denied",
			grant: Grant{
				Grantee: Grantee{
					Type: "EmailAddress",
					Id:   "user@example.com",
				},
				Permission: PermissionRead,
			},
			reqID:     "user@example.com",
			action:    proto.OSSGetObjectAction,
			wantAllow: false,
		},
		{
			name:      "unsupported action is denied",
			grant:     groupGrantForTest(GroupAllUser, PermissionFullControl),
			reqID:     "user-1",
			action:    proto.Action("unsupported-action"),
			wantAllow: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantAllow, tt.grant.isAllowed(tt.reqID, tt.action))
		})
	}
}

func TestAccessControlPolicyIsAllowedOwnerFallbacks(t *testing.T) {
	t.Run("empty acl allows owner for non-acl action", func(t *testing.T) {
		acl := policyWithGrantsForTest("owner")

		requireAllowedForTest(t, acl, "owner", proto.OSSGetObjectAction)
		requireDeniedForTest(t, acl, "user-1", proto.OSSGetObjectAction)
	})

	t.Run("empty acl allows owner for acl action", func(t *testing.T) {
		acl := policyWithGrantsForTest("owner")

		requireAllowedForTest(t, acl, "owner", proto.OSSGetBucketAclAction)
		requireDeniedForTest(t, acl, "user-1", proto.OSSGetBucketAclAction)
	})

	t.Run("non-empty acl still lets owner manage acl", func(t *testing.T) {
		acl := policyWithGrantsForTest("owner", canonicalGrantForTest("user-1", PermissionRead))

		requireAllowedForTest(t, acl, "owner", proto.OSSGetBucketAclAction)
		requireAllowedForTest(t, acl, "owner", proto.OSSPutBucketAclAction)
		requireAllowedForTest(t, acl, "owner", proto.OSSGetObjectAclAction)
		requireAllowedForTest(t, acl, "owner", proto.OSSPutObjectAclAction)
		requireDeniedForTest(t, acl, "owner", proto.OSSGetObjectAction)
	})

	t.Run("grant match wins before owner acl fallback", func(t *testing.T) {
		acl := policyWithGrantsForTest(
			"owner",
			canonicalGrantForTest("user-1", PermissionReadAcp),
		)

		requireAllowedForTest(t, acl, "user-1", proto.OSSGetBucketAclAction)
		requireDeniedForTest(t, acl, "user-1", proto.OSSPutBucketAclAction)
		requireDeniedForTest(t, acl, "user-2", proto.OSSGetBucketAclAction)
	})
}

func TestAccessControlPolicyIsAllowedMultipleGrants(t *testing.T) {
	acl := policyWithGrantsForTest(
		"owner",
		canonicalGrantForTest("reader", PermissionRead),
		canonicalGrantForTest("writer", PermissionWrite),
		canonicalGrantForTest("acl-reader", PermissionReadAcp),
		canonicalGrantForTest("acl-writer", PermissionWriteAcp),
	)

	requireAllowedForTest(t, acl, "reader", proto.OSSGetObjectAction)
	requireDeniedForTest(t, acl, "reader", proto.OSSPutObjectAction)

	requireAllowedForTest(t, acl, "writer", proto.OSSPutObjectAction)
	requireDeniedForTest(t, acl, "writer", proto.OSSGetObjectAction)

	requireAllowedForTest(t, acl, "acl-reader", proto.OSSGetObjectAclAction)
	requireDeniedForTest(t, acl, "acl-reader", proto.OSSPutObjectAclAction)

	requireAllowedForTest(t, acl, "acl-writer", proto.OSSPutObjectAclAction)
	requireDeniedForTest(t, acl, "acl-writer", proto.OSSGetObjectAclAction)
}

func TestAccessControlPolicyXmlMarshalAddsNamespaceAttrs(t *testing.T) {
	acl := policyWithGrantsForTest(
		"owner",
		canonicalGrantForTest("user-1", PermissionFullControl),
		groupGrantForTest(GroupAllUser, PermissionRead),
	)
	acl.Owner.DisplayName = "owner display"
	acl.Acl.Grants[0].Grantee.DisplayName = "user display"

	data, err := acl.XmlMarshal()
	require.NoError(t, err)

	xmlText := string(data)
	require.True(t, strings.HasPrefix(xmlText, xml.Header))
	require.Contains(t, xmlText, `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`)
	require.Contains(t, xmlText, `xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"`)
	require.Contains(t, xmlText, `xsi:type="CanonicalUser"`)
	require.Contains(t, xmlText, `xsi:type="Group"`)
	require.Contains(t, xmlText, "<ID>owner</ID>")
	require.Contains(t, xmlText, "<DisplayName>owner display</DisplayName>")
	require.Contains(t, xmlText, "<DisplayName>user display</DisplayName>")
	require.Contains(t, xmlText, "<URI>"+GroupAllUser+"</URI>")

	var decoded AccessControlPolicy
	require.NoError(t, xml.Unmarshal(data, &decoded))
	require.Equal(t, "owner", decoded.Owner.Id)
	require.Equal(t, "owner display", decoded.Owner.DisplayName)
	require.Len(t, decoded.Acl.Grants, 2)
	requireGrantForTest(t, decoded.Acl.Grants[0], "user-1", "", TypeCanonicalUser, PermissionFullControl)
	requireGrantForTest(t, decoded.Acl.Grants[1], "", GroupAllUser, TypeGroup, PermissionRead)
}

func TestAccessControlPolicyEncodeRoundTrip(t *testing.T) {
	acl := policyWithGrantsForTest(
		"owner",
		canonicalGrantForTest("user-1", PermissionReadAcp),
		groupGrantForTest(GroupAuthenticated, PermissionRead),
	)
	acl.Owner.DisplayName = "owner display"

	encoded := acl.Encode()
	require.NotEmpty(t, encoded)
	require.NotContains(t, encoded, "Xmlns")
	require.NotContains(t, encoded, "Xmlxsi")
	require.NotContains(t, encoded, "XsiType")

	var decoded AccessControlPolicy
	require.NoError(t, json.Unmarshal([]byte(encoded), &decoded))
	require.Equal(t, acl, decoded)
	require.Equal(t, "owner", decoded.Owner.Id)
	require.Equal(t, "owner display", decoded.Owner.DisplayName)
	requireGrantForTest(t, decoded.Acl.Grants[0], "user-1", "", TypeCanonicalUser, PermissionReadAcp)
	requireGrantForTest(t, decoded.Acl.Grants[1], "", GroupAuthenticated, TypeGroup, PermissionRead)
}

func TestAccessControlPolicyXmlUnmarshalWhitespaceAndOptionalFields(t *testing.T) {
	aclXML := `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Owner>
		<ID>owner</ID>
	</Owner>
	<AccessControlList>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
				<ID>user-1</ID>
			</Grantee>
			<Permission>READ_ACP</Permission>
		</Grant>
		<Grant>
			<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
				<URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI>
			</Grantee>
			<Permission>READ</Permission>
		</Grant>
	</AccessControlList>
</AccessControlPolicy>`

	var acl AccessControlPolicy
	require.NoError(t, xml.Unmarshal([]byte(aclXML), &acl))
	require.NoError(t, acl.IsValid())
	require.Equal(t, "owner", acl.Owner.Id)
	require.Empty(t, acl.Owner.DisplayName)
	requireGrantForTest(t, acl.Acl.Grants[0], "user-1", "", TypeCanonicalUser, PermissionReadAcp)
	requireGrantForTest(t, acl.Acl.Grants[1], "", GroupAuthenticated, TypeGroup, PermissionRead)
}

func TestAccessControlPolicyXmlMarshalAfterSetter(t *testing.T) {
	acl := AccessControlPolicy{}
	acl.SetPublicReadWrite("owner")

	data, err := acl.XmlMarshal()
	require.NoError(t, err)

	var decoded AccessControlPolicy
	require.NoError(t, xml.Unmarshal(data, &decoded))
	require.Equal(t, "owner", decoded.Owner.Id)
	require.Len(t, decoded.Acl.Grants, 3)
	requireGrantForTest(t, decoded.Acl.Grants[0], "owner", "", TypeCanonicalUser, PermissionFullControl)
	requireGrantForTest(t, decoded.Acl.Grants[1], "", GroupAllUser, TypeGroup, PermissionRead)
	requireGrantForTest(t, decoded.Acl.Grants[2], "", GroupAllUser, TypeGroup, PermissionWrite)
}
