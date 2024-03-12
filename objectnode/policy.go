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

// https://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"syscall"

	"github.com/gorilla/mux"

	"github.com/cubefs/cubefs/proto"
)

// https://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html
const (
	BucketPolicyLimitSize = 20 * 1024 // Bucket policies are limited to 20KB
	maxStatementNum       = 10
)

var (
	ErrMissingVersionInPolicy           = &ErrorCode{ErrorCode: "ErrMissingVersionInPolicy", ErrorMessage: "missing Version in policy", StatusCode: http.StatusBadRequest}
	ErrMissingStatementInPolicy         = &ErrorCode{ErrorCode: "MissingStatementInPolicy", ErrorMessage: "missing Statement in policy", StatusCode: http.StatusBadRequest}
	ErrMissingEffectInPolicy            = &ErrorCode{ErrorCode: "MissingEffectInPolicy", ErrorMessage: "missing Effect in policy", StatusCode: http.StatusBadRequest}
	ErrMissingPrincipalInPolicy         = &ErrorCode{ErrorCode: "MissingPrincipalInPolicy", ErrorMessage: "missing Principal in policy", StatusCode: http.StatusBadRequest}
	ErrMissingActionInPolicy            = &ErrorCode{ErrorCode: "MissingActionInPolicy", ErrorMessage: "missing Action in policy", StatusCode: http.StatusBadRequest}
	ErrMissingResourceInPolicy          = &ErrorCode{ErrorCode: "MissingResourceInPolicy", ErrorMessage: "missing Resource in policy", StatusCode: http.StatusBadRequest}
	ErrTooManyStatementInPolicy         = &ErrorCode{ErrorCode: "TooManyStatementInPolicy", ErrorMessage: "too many statement in policy", StatusCode: http.StatusBadRequest}
	ErrInvalidEffectValue               = &ErrorCode{ErrorCode: "InvalidEffectValue", ErrorMessage: "Effect can only be Allow or Deny", StatusCode: http.StatusBadRequest}
	ErrInvalidPricipalInPolicy          = &ErrorCode{ErrorCode: "InvalidPricipalInPolicy", ErrorMessage: "Invalid Principal in policy", StatusCode: http.StatusBadRequest}
	ErrInvalidActionInPolicy            = &ErrorCode{ErrorCode: "InvalidActionInPolicy", ErrorMessage: "Invalid Action in policy", StatusCode: http.StatusBadRequest}
	ErrInvalidResourceInPolicy          = &ErrorCode{ErrorCode: "InvalidResourceInPolicy", ErrorMessage: "Invalid Resource in policy", StatusCode: http.StatusBadRequest}
	ErrInvalidActionResourceCombination = &ErrorCode{ErrorCode: "InvalidActionResourceCombination", ErrorMessage: "Action does not apply to any resource in statement", StatusCode: http.StatusBadRequest}
)

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/example-bucket-policies.html

type Policy struct {
	Version    string      `json:"Version"`
	Id         string      `json:"Id,omitempty"`
	Statements []Statement `json:"Statement,omitempty"`
}

func (p *Policy) IsEmpty() bool {
	return len(p.Statements) == 0
}

func ParsePolicy(data []byte) (*Policy, error) {
	policy := new(Policy)
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func storeBucketPolicy(ctx context.Context, vol *Volume, policy []byte) error {
	// put policy bytes into store
	return vol.store.Put(ctx, vol.name, bucketRootPath, XAttrKeyOSSPolicy, policy)
}

func deleteBucketPolicy(ctx context.Context, vol *Volume) error {
	return vol.store.Delete(ctx, vol.name, bucketRootPath, XAttrKeyOSSPolicy)
}

func (p Policy) isValid() (bool, error) {
	if p.Version == "" {
		return false, ErrMissingVersionInPolicy
	}
	if len(p.Statements) == 0 {
		return false, ErrMissingStatementInPolicy
	}
	if len(p.Statements) > maxStatementNum {
		return false, ErrTooManyStatementInPolicy
	}
	return true, nil
}

func (p Policy) Validate(bucket string) (bool, error) {
	if ok, err1 := p.isValid(); !ok {
		return false, err1
	}

	for _, s := range p.Statements {
		if ok, err := s.Validate(bucket); !ok {
			return false, err
		}
	}

	return true, nil
}

// check policy is allowed for request
// https://docs.aws.amazon.com/zh_cn/IAM/latest/UserGuide/reference_policies_evaluation-logic.html
func (p *Policy) IsAllowed(params *RequestParam, reqUid, ownerUid string, conditionCheck map[string]string) PolicyCheckResult {
	result := POLICY_UNKNOW
	apiName := params.apiName
	// only bucket owner is allowed to put/get/delete bucket policy
	if isPolicyApi(apiName) {
		if reqUid == ownerUid {
			return POLICY_ALLOW
		}
		return POLICY_DENY
	}
	if !supportByPolicy(apiName) {
		return POLICY_UNKNOW
	}
	for _, statement := range p.Statements {
		if tmp := statement.CheckPolicy(apiName, reqUid, conditionCheck); tmp == POLICY_DENY {
			return POLICY_DENY
		} else if tmp == POLICY_ALLOW {
			result = POLICY_ALLOW
		}
	}
	return result
}

func (o *ObjectNode) policyCheck(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			err     error
			ec      *ErrorCode
			allowed bool
		)

		ctx := r.Context()
		span := spanWithOperation(ctx, "PolicyCheck")
		defer func() {
			if allowed {
				f(w, r)
			} else {
				if ec == nil && err == nil {
					ec = AccessDenied
				}
				o.errorResponse(w, r, err, ec)
			}
		}()

		param := ParseRequestParam(r)
		if param.Bucket() == "" {
			allowed = true
			return
		}

		// step1. The account level api does not need to check any user policy and volume policy.
		if IsAccountLevelApi(param.apiName) {
			if !isAnonymous(param.accessKey) {
				allowed = true
				return
			}
			span.Errorf("%v does not allow anonymous access", param.apiName)
			allowed = false
			return
		}
		if bucket := mux.Vars(r)[ContextKeyBucket]; len(bucket) > 0 {
			if _, err = o.getVol(ctx, bucket); err != nil {
				span.Errorf("load volume fail: volume(%v) err(%v)", bucket, err)
				allowed = false
				return
			}
		}

		// step2. Check user policy
		userInfo := new(proto.UserInfo)
		userPolicy := new(proto.UserPolicy)
		isOwner := false
		if isAnonymous(param.accessKey) && apiAllowAnonymous(param.apiName) {
			span.Debugf("anonymous user access to %v", param.apiName)
			goto policycheck
		}
		if isAnonymous(param.accessKey) && !apiAllowAnonymous(param.apiName) {
			span.Errorf("%v does not allow anonymous access", param.apiName)
			allowed = false
			return
		}
		userInfo, err = o.userStore.LoadUser(ctx, param.AccessKey())
		if err != nil {
			span.Errorf("load user info fail: accessKey(%v) err(%v)", param.AccessKey(), err)
			allowed = false
			return
		}
		// White list for admin and root user.
		if userInfo.UserType == proto.UserTypeRoot || userInfo.UserType == proto.UserTypeAdmin {
			span.Infof("accessed by admin user: userID(%v) accessKey(%v) volume(%v)",
				userInfo.UserID, param.AccessKey(), param.Bucket())
			allowed = true
			return
		}
		userPolicy = userInfo.Policy
		isOwner = userPolicy.IsOwn(param.Bucket())
		// The bucket is not owned by request user who has not been authorized, so bucket policy should be checked.
		if !isOwner && userPolicy.IsAuthorizedS3(param.Bucket(), param.apiName) {
			span.Infof("bucket is not owned by requester but authorized: reqUid(%v) api(%v) volume(%v) ownVols("+
				"%v) authorizedVols(%v)",
				userInfo.UserID, param.apiName, param.Bucket(), userPolicy.OwnVols, userPolicy.AuthorizedVols)
			allowed = true
			return
		}
		// copy api should check srcBucket policy additionally
		if param.apiName == COPY_OBJECT || param.apiName == UPLOAD_PART_COPY {
			err = o.allowedBySrcBucketPolicy(ctx, param, userInfo.UserID)
			if err != nil {
				return
			}
		}
		// batch delete will delay to check just before delete for each key
		if param.apiName == BATCH_DELETE {
			span.Infof("delete objects delay check: userID(%v) volume(%v)", userInfo.UserID, param.Bucket())
			allowed = true
			return
		}

		// step3. Check bucket policy
	policycheck:
		vol, acl, policy, err := o.loadBucketMeta(ctx, param.Bucket())
		if err != nil {
			span.Errorf("load bucket metadata fail: volume(%v) err(%v)", param.Bucket(), err)
			allowed = false
			return
		}
		span.Debugf("load bucket metadata: vol(%v) reqUid(%v) ownerUid(%v) userPolicy(%+v) ACL(%+v) policy(%+v)",
			vol.Name(), userInfo.UserID, vol.GetOwner(), userInfo.Policy, acl, policy)
		if vol != nil && policy != nil && !policy.IsEmpty() {
			conditionCheck := map[string]string{
				SOURCEIP: param.sourceIP,
				REFERER:  param.r.Referer(),
				HOST:     param.r.Host,
			}
			if !IsBucketApi(param.apiName) {
				conditionCheck[KEYNAME] = param.object
			}
			pcr := policy.IsAllowed(param, userInfo.UserID, vol.owner, conditionCheck)
			switch pcr {
			case POLICY_ALLOW:
				allowed = true
				return
			case POLICY_DENY:
				allowed = false
				span.Warnf("bucket policy disallowed: policy(%+v) reqUid(%v) ownerUid(%v) condition(%v)",
					policy, userInfo.UserID, vol.owner, conditionCheck)
				return
			case POLICY_UNKNOW:
				// policy check result is unknown so that acl should be checked
				span.Warnf("bucket policy unknown: policy(%+v) reqUid(%v) ownerUid(%v) condition(%v)",
					policy, userInfo.UserID, vol.owner, conditionCheck)
			default:
				// do nothing
			}
		}

		// step4. Check acl
		if IsApiSupportByACL(param.Action()) {
			if vol != nil && IsApiSupportByObjectAcl(param.Action()) {
				if param.Object() == "" {
					ec = InvalidKey
					span.Errorf("no object key specified: volume(%v) action(%v)",
						param.Bucket(), param.Action())
					return
				}
				if acl, err = getObjectACL(ctx, vol, param.object, true); err != nil && err != syscall.ENOENT {
					span.Errorf("get object ACL fail: volume(%v) key(%v) err(%v)",
						param.Bucket(), param.Object(), err)
					return
				}
				err = nil
			}
			if acl == nil && !isOwner {
				span.Warnf("empty ACL does not allow non-owner: reqUid(%v) ownerUid(%v) volume(%v) ownerVols(%v)",
					userInfo.UserID, vol.GetOwner(), param.Bucket(), userPolicy.OwnVols)
				allowed = false
				return
			}
			if acl != nil && !acl.IsAllowed(userInfo.UserID, param.Action()) {
				span.Warnf("ACL disallowed: volume(%v) key(%v) ACL(%+v) reqUid(%v) action(%v)",
					param.Bucket(), param.Object(), acl, userInfo.UserID, param.Action())
				allowed = false
				return
			}
		} else if !isOwner {
			allowed = false
			span.Warnf("action does not support ACL: action(%v)", param.Action())
			return
		}

		allowed = true
	}
}

func (o *ObjectNode) loadBucketMeta(ctx context.Context, bucket string) (vol *Volume, acl *AccessControlPolicy,
	policy *Policy, err error) {
	if vol, err = o.getVol(ctx, bucket); err != nil {
		return
	}
	if acl, err = vol.metaLoader.loadACL(ctx); err != nil {
		return
	}
	if policy, err = vol.metaLoader.loadPolicy(ctx); err != nil {
		return
	}
	return
}

func (o *ObjectNode) allowedBySrcBucketPolicy(ctx context.Context, param *RequestParam, reqUid string) (err error) {
	span := spanWithOperation(ctx, "allowedBySrcBucketPolicy")
	paramCopy := *param
	srcBucketId, srcKey, _, err := extractSrcBucketKey(paramCopy.r)
	if err != nil {
		span.Errorf("invalid %v: %v", XAmzCopySource, paramCopy.r.Header.Get(XAmzCopySource))
		return
	}

	vol, acl, policy, err := o.loadBucketMeta(ctx, srcBucketId)
	if err != nil {
		span.Errorf("load bucket metadata fail: bucket(%v) err(%v)", srcBucketId, err)
		return
	}

	paramCopy.apiName = GET_OBJECT
	paramCopy.action = proto.OSSGetObjectAction
	if vol != nil && policy != nil && !policy.IsEmpty() {
		conditionCheck := map[string]string{
			SOURCEIP: paramCopy.sourceIP,
			KEYNAME:  srcKey,
			REFERER:  paramCopy.r.Referer(),
			HOST:     paramCopy.r.Host,
		}
		pcr := policy.IsAllowed(&paramCopy, reqUid, vol.owner, conditionCheck)
		switch pcr {
		case POLICY_ALLOW:
			span.Debugf("source policy allowed: srcVol(%v) srcKey(%v) reqUid(%v) srcOwner(%v) action(%v)",
				srcBucketId, srcKey, reqUid, vol.owner, paramCopy.Action())
			return
		case POLICY_DENY:
			span.Warnf(" source policy disallowed: srcVol(%v) policy(%v) reqUid(%v) srcOwner(%v) cond(%v)",
				srcBucketId, policy, reqUid, vol.owner, conditionCheck)
			return AccessDenied
		case POLICY_UNKNOW:
			// policy check result is unknown so that acl should be checked
			span.Warnf(" source policy unknown: srcVol(%v) policy(%v) reqUid(%v) srcOwner(%v) cond(%v)",
				srcBucketId, policy, reqUid, vol.owner, conditionCheck)
		default:
			// do nothing
		}
	}

	isOwner := reqUid == vol.owner
	if acl, err = getObjectACL(ctx, vol, srcKey, true); err != nil && err != syscall.ENOENT {
		span.Errorf("get object ACL fail: srcVol(%v) srcKey(%v) err(%v)", srcBucketId, srcKey, err)
		return
	}
	err = nil
	if acl == nil && !isOwner {
		span.Warnf("empty ACL does not allow non-owner: srcVol(%v) srcKey(%v) reqUid(%v) srcOwner(%v)",
			srcBucketId, srcKey, reqUid, vol.owner)
		return AccessDenied
	}

	if acl != nil && !acl.IsAllowed(reqUid, paramCopy.Action()) {
		span.Warnf("source ACL disallowed: srcACL(%+v) srcVol(%v) srcKey(%v) reqUid(%v) action(%v)",
			acl, srcBucketId, srcKey, reqUid, paramCopy.Action())
		return AccessDenied
	}

	return
}
