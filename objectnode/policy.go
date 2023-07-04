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
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"syscall"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"

	"github.com/gorilla/mux"
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

func storeBucketPolicy(vol *Volume, policy []byte) error {
	// put policy bytes into store
	return vol.store.Put(vol.name, bucketRootPath, XAttrKeyOSSPolicy, policy)
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
				if ec == nil && err == nil {
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
		if bucket := mux.Vars(r)["bucket"]; len(bucket) > 0 {
			if _, err = o.getVol(bucket); err != nil {
				allowed = false
				return
			}
		}

		// step2. Check user policy
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
		} else {
			log.LogErrorf("user policy check: load user policy from master fail: requestID(%v) accessKey(%v) err(%v)",
				GetRequestID(r), param.AccessKey(), err)
			allowed = false
			return
		}

		// copy api should check srcBucket policy additionally
		if param.apiName == COPY_OBJECT || param.apiName == UPLOAD_PART_COPY {
			err = o.allowedBySrcBucketPolicy(param, userInfo.UserID)
			if err != nil {
				return
			}
		}
		// batch delete will delay to check just before delete for each key
		if param.apiName == BATCH_DELETE {
			log.LogDebugf("user policy check: delete objects delay check: requestID(%v) userID(%v) volume(%v)",
				GetRequestID(r), userInfo.UserID, param.Bucket())
			allowed = true
			return
		}

		// step3. Check bucket policy
	policycheck:
		vol, acl, policy, err := o.loadBucketMeta(param.Bucket())
		if err != nil {
			log.LogErrorf("bucket policy check: load bucket metadata fail: requestID(%v) err(%v)", GetRequestID(r), err)
			allowed = false
			return
		}
		log.LogDebugf("bucket policy check: load bucket metadata, requestID(%v) userPolicy(%v/%+v) vol(%v/%v) acl(%+v) policy(%+v)",
			GetRequestID(r), userInfo.UserID, userInfo.Policy, vol.Name(), vol.GetOwner(), acl, policy)
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
			pcr := policy.IsAllowed(param, userInfo.UserID, vol.owner, conditionCheck)
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

		// step4. Check acl
		if IsApiSupportByACL(param.Action()) {
			if vol != nil && IsApiSupportByObjectAcl(param.Action()) {
				if param.Object() == "" {
					ec = InvalidKey
					log.LogErrorf("acl check: no object key specified: requestID(%v) volume(%v) action(%v)",
						GetRequestID(r), param.Bucket(), param.Action())
					return
				}
				if acl, err = getObjectACL(vol, param.object, true); err != nil && err != syscall.ENOENT {
					log.LogErrorf("acl check: get object acl fail: requestID(%v) volume(%v) action(%v) err(%v)",
						GetRequestID(r), param.Bucket(), param.Action(), err)
					return
				}
				err = nil
			}
			if acl == nil && !isOwner {
				allowed = false
				log.LogWarnf("acl check: empty acl disallows: requestID(%v) reqUid(%v) ownerUid(%v) volume(%v) action(%v)",
					GetRequestID(r), userInfo.UserID, vol.GetOwner(), param.Bucket(), param.Action())
				return
			}
			if acl != nil && !acl.IsAllowed(userInfo.UserID, param.Action()) {
				allowed = false
				log.LogWarnf("acl check: acl not allowed: requestID(%v) reqUid(%v) acl(%+v) volume(%v) action(%v)",
					GetRequestID(r), userInfo.UserID, acl, param.Bucket(), param.Action())
				return
			}
		} else if !isOwner {
			allowed = false
			log.LogWarnf("acl check: action not support acl: requestID(%v) reqUid(%v) ownerUid(%v) volume(%v) action(%v)",
				GetRequestID(r), userInfo.UserID, vol.GetOwner(), param.Bucket(), param.Action())
			return
		}

		allowed = true
		log.LogDebugf("bucket acl check: action allowed: requestID(%v) reqUid(%v) accessKey(%v) volume(%v) action(%v)",
			GetRequestID(r), userInfo, param.AccessKey(), param.Bucket(), param.Action())
	}
}

func (o *ObjectNode) loadBucketMeta(bucket string) (vol *Volume, acl *AccessControlPolicy, policy *Policy, err error) {
	if vol, err = o.getVol(bucket); err != nil {
		return
	}
	if acl, err = vol.metaLoader.loadACL(); err != nil {
		return
	}
	if policy, err = vol.metaLoader.loadPolicy(); err != nil {
		return
	}
	return
}

func (o *ObjectNode) allowedBySrcBucketPolicy(param *RequestParam, reqUid string) (err error) {
	paramCopy := *param
	srcBucketId, srcKey, _, err := extractSrcBucketKey(paramCopy.r)
	if err != nil {
		log.LogDebugf("copySource(%v) argument invalid: requestID(%v)", paramCopy.r.Header.Get(HeaderNameXAmzCopySource), GetRequestID(paramCopy.r))
		return
	}
	vol, acl, policy, err := o.loadBucketMeta(srcBucketId)
	if err != nil {
		log.LogErrorf("srcBucket policy check: load bucket metadata fail: requestID(%v) err(%v)", GetRequestID(paramCopy.r), err)
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
			log.LogDebugf("srcBucket policy check: policy allowed: requestID(%v)", GetRequestID(paramCopy.r))
			return
		case POLICY_DENY:
			log.LogWarnf("srcBucket policy check: policy not allowed: requestID(%v) ", GetRequestID(paramCopy.r))
			return AccessDenied
		case POLICY_UNKNOW:
			// policy check result is unknown so that acl should be checked
			log.LogWarnf("srcBucket policy check: policy unknown: requestID(%v) ", GetRequestID(paramCopy.r))
		}
	}

	isOwner := reqUid == vol.owner
	if acl, err = getObjectACL(vol, srcKey, true); err != nil && err != syscall.ENOENT {
		log.LogErrorf("srcBucket acl check: get object acl fail: requestID(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(paramCopy.r), srcBucketId, srcKey, err)
		return
	}
	err = nil
	if acl == nil && !isOwner {
		log.LogWarnf("srcBucket acl check: empty acl disallows: requestID(%v) reqUid(%v) ownerUid(%v) volume(%v) action(%v)",
			GetRequestID(paramCopy.r), reqUid, vol.owner, srcBucketId, paramCopy.Action())
		return AccessDenied
	}
	if acl != nil && !acl.IsAllowed(reqUid, paramCopy.Action()) {
		log.LogWarnf("srcBucket acl check: acl not allowed: requestID(%v) reqUid(%v) acl(%+v) volume(%v) path(%v) action(%v)",
			GetRequestID(paramCopy.r), reqUid, acl, srcBucketId, srcKey, paramCopy.Action())
		return AccessDenied
	}
	log.LogDebugf("srcBucket acl check: action allowed: requestID(%v) accessKey(%v) volume(%v) action(%v)",
		GetRequestID(paramCopy.r), paramCopy.AccessKey(), paramCopy.Bucket(), paramCopy.Action())
	return
}
