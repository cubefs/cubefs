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

/*

https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/example-bucket-policies.html
https://docs.aws.amazon.com/zh_cn/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html#Conditions_IPAddress

{
  "Version": "2012-10-17",
  "Id": "S3PolicyId1",
  "Statement": [
    {
      "Sid": "IPAllow",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": "arn:aws:s3:::examplebucket/*",
      "Condition": {
         "IpAddress": {"aws:SourceIp": "54.240.143.0/24"},
         "NotIpAddress": {"aws:SourceIp": "54.240.143.188/32"}
      }
    }
  ]
}

{
  "Id":"PolicyId2",
  "Version":"2012-10-17",
  "Statement":[
    {
      "Sid":"AllowIPmix",
      "Effect":"Allow",
      "Principal":"*",
      "Action":"s3:*",
      "Resource":"arn:aws:s3:::examplebucket/*",
      "Condition": {
        "IpAddress": {
          "aws:SourceIp": [
            "54.240.143.0/24",
            "2001:DB8:1234:5678::/64"
          ]
        },
        "NotIpAddress": {
          "aws:SourceIp": [
             "54.240.143.128/30",
             "2001:DB8:1234:5678:ABCD::/80"
          ]
        }
      }
    }
  ]
}

*/

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cubefs/cubefs/proto"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

type mockUserStore map[string]*proto.UserInfo

func (s mockUserStore) LoadUser(accessKey string) (*proto.UserInfo, error) {
	if userInfo, ok := s[accessKey]; ok {
		return userInfo, nil
	}
	return nil, proto.ErrAccessKeyNotExists
}

func newPolicyCheckVolume(name, owner string, policy *Policy) *Volume {
	synced := int32(1)
	return &Volume{
		name:       name,
		owner:      owner,
		metaLoader: &cacheMetaLoader{om: &OSSMeta{policy: policy}, synced: &synced},
	}
}

func newPolicyCheckNode(userStore UserInfoStore, vols ...*Volume) *ObjectNode {
	vm := &VolumeManager{}
	for i := range vm.loaders {
		vm.loaders[i] = &VolumeLoader{volumes: make(map[string]*Volume)}
	}
	for _, vol := range vols {
		vm.selectLoader(vol.name).volumes[vol.name] = vol
	}
	return &ObjectNode{vm: vm, userStore: userStore}
}

// A user who is only authorized on the target bucket by user policy must not be able to
// copy objects out of a source bucket they are not allowed to read.
func TestPolicyCheckCopySource(t *testing.T) {
	const (
		accessKey = "granteeAccessKey"
		uid       = "grantee"
	)
	denyRead, err := ParsePolicy([]byte(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Deny",
			"Principal": {"AWS": ["grantee"]},
			"Action": "s3:GetObject",
			"Resource": "arn:aws:s3:::private/*"
		}]
	}`))
	require.NoError(t, err)

	userInfo := &proto.UserInfo{
		UserID:    uid,
		AccessKey: accessKey,
		UserType:  proto.UserTypeNormal,
		Policy:    proto.NewUserPolicy(),
	}
	userInfo.Policy.AuthorizedVols["shared"] = []string{proto.BuiltinPermissionWritable.String()}
	userInfo.Policy.AuthorizedVols["readable"] = []string{proto.BuiltinPermissionReadOnly.String()}

	o := newPolicyCheckNode(mockUserStore{accessKey: userInfo},
		newPolicyCheckVolume("shared", "owner1", nil),
		newPolicyCheckVolume("readable", "owner2", nil),
		newPolicyCheckVolume("private", "owner3", denyRead))

	cases := []struct {
		action     proto.Action
		copySource string
		allowed    bool
	}{
		// source bucket is not readable by the request user
		{proto.OSSCopyObjectAction, "/private/secret", false},
		{proto.OSSUploadPartCopyAction, "/private/secret", false},
		// source bucket is authorized to the request user by user policy
		{proto.OSSCopyObjectAction, "/readable/doc", true},
		{proto.OSSUploadPartCopyAction, "/readable/doc", true},
		// source bucket is the target bucket
		{proto.OSSCopyObjectAction, "/shared/doc", true},
		{proto.OSSUploadPartCopyAction, "/shared/doc", true},
	}
	for _, cs := range cases {
		r := httptest.NewRequest(http.MethodPut, "http://s3.cubefs.com/shared/target", nil)
		r.Header.Set(XAmzCopySource, cs.copySource)
		r = mux.SetURLVars(r, map[string]string{
			ContextKeyBucket:    "shared",
			ContextKeyObject:    "target",
			ContextKeyAccessKey: accessKey,
		})
		SetRequestAction(r, cs.action)

		called := false
		w := httptest.NewRecorder()
		o.policyCheck(func(w http.ResponseWriter, r *http.Request) {
			called = true
		})(w, r)
		require.Equal(t, cs.allowed, called, "action(%v) copySource(%v)", cs.action, cs.copySource)
		if !cs.allowed {
			require.Equal(t, http.StatusForbidden, w.Code)
		}
	}
}
