/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 * Modifications copyright 2019 The CubeFS Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package objectnode

// https://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"

	"github.com/gorilla/mux"
)

// https://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html
const (
	BucketPolicyLimitSize = 20 * 1024 //Bucket policies are limited to 20KB
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

//https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/example-bucket-policies.html

type Policy struct {
	Version    string      `json:"Version"`
	Id         string      `json:"Id,omitempty"`
	Statements []Statement `json:"Statement,omitempty"`
}

func (p *Policy) IsEmpty() bool {
	return len(p.Statements) == 0
}

// write bucket policy into store and update vol policy meta
func storeBucketPolicy(bytes []byte, vol *Volume) (*Policy, error) {
	policy := &Policy{}
	err2 := json.Unmarshal(bytes, policy)
	if err2 != nil {
		log.LogErrorf("policy unmarshal err: %v", err2)
		return nil, err2
	}

	// validate policy
	ok, err3 := policy.Validate(vol.name)
	if err3 != nil {
		log.LogErrorf("policy validate err: %v", err2)
		return nil, err3
	}
	if !ok {
		return nil, errors.New("policy is invalid")
	}

	// put policy bytes into store
	err4 := vol.store.Put(vol.name, bucketRootPath, XAttrKeyOSSPolicy, bytes)
	if err4 != nil {
		return nil, err4
	}

	vol.metaLoader.storePolicy(policy)

	return policy, nil
}

func deleteBucketPolicy(vol *Volume) (err error) {
	if err = vol.store.Delete(vol.name, bucketRootPath, XAttrKeyOSSPolicy); err != nil {
		return err
	}
	return nil
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
	log.LogDebug("check policy syntax")
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
			log.LogDebugf("bucket policy check: statement denied: requestID(%v) statement(%v)", GetRequestID(params.r), statement)
			return POLICY_DENY
		} else if tmp == POLICY_ALLOW {
			log.LogDebugf("bucket policy check: statement allowed: requestID(%v) statement(%v)", GetRequestID(params.r), statement)
			result = POLICY_ALLOW
		}
	}
	return result
}

func (o *ObjectNode) policyCheck(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			err error
			ec  *ErrorCode
		)
		allowed := false
		defer func() {
			if allowed {
				f(w, r)
			} else {
				if ec == nil {
					ec = AccessDenied
				}
				o.errorResponse(w, r, err, ec)
			}
		}()

		param := ParseRequestParam(r)
		if param.Bucket() == "" {
			log.LogDebugf("policyCheck: no bucket specified: requestID(%v)", GetRequestID(r))
			allowed = true
			return
		}

		// step1. The account level api does not need to check any user policy and volume policy.
		if IsAccountLevelApi(param.apiName) {
			if !isAnonymous(param.accessKey) {
				allowed = true
				return
			}
			allowed = false
			return
		}
		var volume *Volume
		if bucket := mux.Vars(r)["bucket"]; len(bucket) > 0 {
			if volume, err = o.getVol(bucket); err != nil {
				allowed = false
				if err == proto.ErrVolNotExists {
					ec = NoSuchBucket
					return
				}
				ec = InternalErrorCode(err)
				return
			}
		}

		//step2. Check user policy
		userInfo := new(proto.UserInfo)
		isOwner := false
		if isAnonymous(param.accessKey) && apiAllowAnonymous(param.apiName) {
			log.LogDebugf("anonymous user: requestID(%v)", GetRequestID(r))
			goto policycheck
		}
		if isAnonymous(param.accessKey) && !apiAllowAnonymous(param.apiName) {
			log.LogDebugf("anonymous user is not allowed by api(%v) requestID(%v)", param.apiName, GetRequestID(r))
			allowed = false
			return
		}
		if userInfo, err = o.getUserInfoByAccessKey(param.AccessKey()); err == nil {
			// White list for admin and root user.
			if userInfo.UserType == proto.UserTypeRoot || userInfo.UserType == proto.UserTypeAdmin {
				log.LogDebugf("user policy check: user is admin: requestID(%v) userID(%v) accessKey(%v) volume(%v)",
					GetRequestID(r), userInfo.UserID, param.AccessKey(), param.Bucket())
				allowed = true
				return
			}
			var userPolicy = userInfo.Policy
			isOwner = userPolicy.IsOwn(param.Bucket())
			subdir := strings.TrimRight(param.Object(), "/")
			if subdir == "" {
				subdir = r.URL.Query().Get(ParamPrefix)
			}
			// The bucket is not owned by request user who has not been authorized, so bucket policy should be checked.
			if !isOwner && !userPolicy.IsAuthorized(param.Bucket(), subdir, param.Action()) {
				log.LogDebugf("user policy check:  permission unknown url(%v) subdir(%v) requestID(%v) userID(%v) accessKey(%v) volume(%v) object(%v) action(%v) authorizedVols(%v)",
					r.URL, subdir, GetRequestID(r), userInfo.UserID, param.AccessKey(), param.Bucket(), param.Object(), param.Action(), userPolicy.AuthorizedVols)
			}
		} else if (err == proto.ErrAccessKeyNotExists || err == proto.ErrUserNotExists) && volume != nil {
			if ak, _ := volume.OSSSecure(); ak != param.AccessKey() {
				allowed = false
				return
			}
			isOwner = true
		} else {
			log.LogErrorf("user policy check: load user policy from master fail: requestID(%v) accessKey(%v) err(%v)",
				GetRequestID(r), param.AccessKey(), err)
			allowed = false
			return
		}

		// copy api should check srcBucket policy additionally
		if param.apiName == COPY_OBJECT || param.apiName == UPLOAD_PART_COPY {
			err := o.allowedBySrcBucketPolicy(param, userInfo.UserID)
			if err != nil {
				return
			}
		}

		//step3. Check bucket policy
	policycheck:
		vol, acl, policy, vv, err := o.loadBucketMeta(param.Bucket())
		if err != nil {
			log.LogErrorf("bucket policy check: load bucket metadata fail: requestID(%v) err(%v)", GetRequestID(r), err)
			allowed = false
			ec = NoSuchBucket
			return
		}

		if vol != nil && policy != nil && !policy.IsEmpty() {
			log.LogDebugf("bucket policy check: requestID(%v) policy(%v)", GetRequestID(r), policy)
			conditionCheck := map[string]string{
				SOURCEIP: param.sourceIP,
				REFERER:  param.r.Referer(),
				HOST:     param.r.Host,
			}
			if !IsBucketApi(param.apiName) {
				conditionCheck[KEYNAME] = param.object
			}
			pcr := policy.IsAllowed(param, userInfo.UserID, vv.Owner, conditionCheck)
			switch pcr {
			case POLICY_ALLOW:
				allowed = true
				log.LogDebugf("bucket policy check: policy allowed: requestID(%v)", GetRequestID(r))
				return
			case POLICY_DENY:
				allowed = false
				log.LogWarnf("bucket policy check: policy not allowed: requestID(%v) ", GetRequestID(r))
				return
			case POLICY_UNKNOW:
				// policy check result is unknown so that acl should be checked
				log.LogWarnf("bucket policy check: policy unknown: requestID(%v) ", GetRequestID(r))
			}
		}

		//step4. Check acl
		//The default bucket acl should not be empty
		if acl == nil && !isOwner {
			allowed = false
			log.LogWarnf("bucket acl check: not allowed because of empty bucket ACL : requestID(%v) ", GetRequestID(r))
			return
		}
		if acl != nil && !acl.IsAclEmpty() {
			allowed = acl.IsAllowed(param, isOwner)
			if !allowed {
				log.LogWarnf("bucket acl check: bucket ACL not allowed: requestID(%v) acl(%v) accessKey(%v) volume(%v) action(%v)",
					GetRequestID(r), userInfo, acl, param.Bucket(), param.Action())
				return
			}
		}

		allowed = true
		log.LogDebugf("bucket acl check: action allowed: requestID(%v) userID(%v) accessKey(%v) volume(%v) action(%v)",
			GetRequestID(r), userInfo, param.AccessKey(), param.Bucket(), param.Action())
	}
}

func (o *ObjectNode) loadBucketMeta(bucket string) (vol *Volume, acl *AccessControlPolicy,
	policy *Policy, vv *proto.VolView, err error) {
	if vol, err = o.getVol(bucket); err != nil {
		return
	}
	if acl, err = vol.metaLoader.loadACL(); err != nil {
		return
	}
	if policy, err = vol.metaLoader.loadPolicy(); err != nil {
		return
	}
	if vv, err = o.mc.ClientAPI().GetVolumeWithoutAuthKey(bucket); err != nil {
		return
	}
	return
}

func (o *ObjectNode) allowedBySrcBucketPolicy(param *RequestParam, reqUid string) (err error) {
	srcBucketId, srcKey, _, err := extractSrcBucketKey(param.r)
	if err != nil {
		log.LogDebugf("copySource(%v) argument invalid: requestID(%v)", param.r.Header.Get(HeaderNameXAmzCopySource), GetRequestID(param.r))
		return
	}
	vol, acl, policy, vv, err := o.loadBucketMeta(srcBucketId)
	if err != nil {
		log.LogErrorf("srcBucket policy check: load bucket metadata fail: requestID(%v) err(%v)", GetRequestID(param.r), err)
		return
	}
	if vol != nil && policy != nil && !policy.IsEmpty() {
		conditionCheck := map[string]string{
			SOURCEIP: param.sourceIP,
			KEYNAME:  srcKey,
			REFERER:  param.r.Referer(),
			HOST:     param.r.Host,
		}
		pcr := policy.IsAllowed(param, reqUid, vv.Owner, conditionCheck)
		switch pcr {
		case POLICY_ALLOW:
			log.LogDebugf("srcBucket policy check: policy allowed: requestID(%v)", GetRequestID(param.r))
			return
		case POLICY_DENY:
			log.LogWarnf("srcBucket policy check: policy not allowed: requestID(%v) ", GetRequestID(param.r))
			return AccessDenied
		case POLICY_UNKNOW:
			// policy check result is unknown so that acl should be checked
			log.LogWarnf("srcBucket policy check: policy unknown: requestID(%v) ", GetRequestID(param.r))
		}
	}

	isOwner := reqUid == vv.Owner
	if acl == nil && !isOwner {
		log.LogWarnf("srcBucket acl check: not allowed because of empty bucket ACL : requestID(%v) ", GetRequestID(param.r))
		return AccessDenied
	}

	if vol != nil && acl != nil && !acl.IsAclEmpty() {
		allowed := acl.IsAllowed(param, isOwner)
		if !allowed {
			log.LogWarnf("srcBucket acl check: bucket ACL not allowed: requestID(%v) acl(%v) volume(%v) action(%v)",
				GetRequestID(param.r), acl, param.Bucket(), param.Action())
			return
		}
	}
	log.LogDebugf("srcBucket acl check: action allowed: requestID(%v) accessKey(%v) volume(%v) action(%v)",
		GetRequestID(param.r), param.AccessKey(), param.Bucket(), param.Action())
	return
}
