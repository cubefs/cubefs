/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 * Modifications copyright 2019 The ChubaoFS Authors.
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
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

//https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/example-bucket-policies.html
type ConditionValues map[string]StringSet
type Condition map[ConditionType]ConditionValues
type ConditionType string
type ConditionKey string

// https://docs.aws.amazon.com/zh_cn/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html
const (
	IpAddress                ConditionType = "IpAddress"
	NotIpAddress                           = "NotIpAddress"
	StringLike                             = "StringLike"
	StringNotLike                          = "StringNotLike"
	StringEquals                           = "StringEquals"
	StringNotEquals                        = "StringNotEquals"
	Bool                                   = "Bool"
	DateEquals                             = "DateEquals"
	DateNotEquals                          = "DateNotEquals"
	DateLessThan                           = "DateLessThan"
	DateLessThanEquals                     = "DateLessThanEquals"
	DateGreaterThan                        = "DateGreaterThan"
	DateGreaterThanEquals                  = "DateGreaterThanEquals"
	NumericEquals                          = "NumericEquals"
	NumericNotEquals                       = "NumericNotEquals"
	NumericLessThan                        = "NumericLessThan"
	NumericLessThanEquals                  = "NumericLessThanEquals"
	NumericGreaterThan                     = "NumericGreaterThan"
	NumericGreaterThanEquals               = "NumericGreaterThanEquals"
	ArnEquals                              = "ArnEquals"
	ArnLike                                = "ArnLike" //
	ArnNotEquals                           = "ArnNotEquals"
	ArnNotLike                             = "ArnNotLike" //
)

var (
	StringFuncs    = []ConditionFunc{StringEqualsFunc, StringNotEqualsFunc, StringLikeFunc, StringNotLikeFunc}
	DateFuncs      = []ConditionFunc{DateEqualsFunc, DateNotEqualsFunc, DateLessThanFunc, DateGreaterThanFunc, DateLessThanEqualsFunc, DateGreaterThanEqualsFunc}
	IpAddressFuncs = []ConditionFunc{IpAddressFunc, NotIpAddressFunc}
	BoolFuncs      = []ConditionFunc{BoolFunc}
	NumericFuncs   = []ConditionFunc{NumericEqualsFunc, NumericNotEqualsFunc, NumericLessThanFunc, NumericLessThanEqualsFunc, NumericGreaterThanFunc, NumericGreaterThanEqualsFunc}
	ArnFuncs       = []ConditionFunc{ArnEqualsFunc, ArnNotEqualsFunc, ArnLikeFunc, ArnNotLikeFunc}
)

type ConditionTypeSet map[ConditionType]null

var (
	StringType    = ConditionTypeSet{StringEquals: void, StringNotEquals: void, StringLike: void, StringNotLike: void}
	DateType      = ConditionTypeSet{DateEquals: void, DateNotEquals: void, DateLessThan: void, DateGreaterThan: void, DateLessThanEquals: void, DateGreaterThanEquals: void}
	IpAddressType = ConditionTypeSet{IpAddress: void, NotIpAddress: void}
	BoolType      = ConditionTypeSet{Bool: void}
	NumericType   = ConditionTypeSet{NumericEquals: void, NumericNotEquals: void, NumericLessThan: void, NumericLessThanEquals: void, NumericGreaterThan: void, NumericGreaterThanEquals: void}
	ArnType       = ConditionTypeSet{ArnEquals: void, ArnNotEquals: void, ArnLike: void, ArnNotLike: void}
)

// https://docs.aws.amazon.com/zh_cn/IAM/latest/UserGuide/reference_policies_condition-keys.html
const (
	AwsCurrentTime            ConditionKey = "aws:CurrentTime"
	AwsEpochTime                           = "aws:EpochTime"
	AwsMultiFactorAuthPresent              = "aws:MultiFactorAuthPresent"
	AwsPrincipalAccount                    = "aws:PrincipalAccount"
	AwsPrincipalArn                        = "aws:PrincipalArn"
	AwsPrincipalOrgID                      = "aws:PrincipalOrgID"
	AwsPrincipalTag                        = "aws:PrincipalTag"
	AwsPrincipalType                       = "aws:PrincipalType"
	AwsReferer                             = "aws:Referer"
	AwsRequestRegion                       = "aws:RequestRegion"
	AwsRequestTagKey                       = "aws:RequestTag/tag-key"
	AwsResourceTagKey                      = "aws:ResourceTag/tag-key"
	AwsSecureTransport                     = "aws:SecureTransport"
	AwsSourceAccout                        = "aws:AwsSourceAccout"
	AwsSourceArn                           = "aws:SourceArn"
	AwsSourceIp                            = "aws:SourceIp"
	AwsSourceVpc                           = "aws:SourceVpc"
	AwsSourceVpce                          = "aws:SourceVpce"
	AwsTagKeys                             = "aws:TagKeys"
	AwsTokenIssueTime                      = "aws:TokenIssueTime"
	AwsUserAgent                           = "aws:UserAgent"
	AwsUserId                              = "aws:userid"
	AwsUserName                            = "aws:username"
	AwsVpcSourceIp                         = "aws:VpcSourceIp"
)

var ConditionKeyType = map[ConditionKey]ConditionTypeSet{
	AwsCurrentTime:            DateType,
	AwsEpochTime:              DateType,
	AwsMultiFactorAuthPresent: BoolType,
	AwsPrincipalAccount:       StringType,
	AwsPrincipalArn:           ArnType,
	AwsPrincipalOrgID:         StringType,
	AwsPrincipalTag:           StringType,
	AwsPrincipalType:          StringType,
	AwsReferer:                StringType,
	AwsRequestRegion:          StringType,
	AwsRequestTagKey:          StringType,
	AwsResourceTagKey:         StringType,
	AwsSecureTransport:        BoolType,
	AwsSourceAccout:           StringType,
	AwsSourceArn:              ArnType,
	AwsSourceIp:               IpAddressType,
	AwsSourceVpc:              StringType,
	AwsSourceVpce:             StringType,
	AwsTagKeys:                StringType,
	AwsTokenIssueTime:         DateType,
	AwsUserAgent:              StringType,
	AwsUserId:                 StringType,
	AwsUserName:               StringType,
	AwsVpcSourceIp:            IpAddressType,
}

var ConditionFuncMap = map[ConditionType]ConditionFunc{
	IpAddress:             IpAddressFunc,
	NotIpAddress:          NotIpAddressFunc,
	StringLike:            StringLikeFunc,
	StringNotLike:         StringNotLikeFunc,
	StringEquals:          StringEqualsFunc,
	StringNotEquals:       StringNotEqualsFunc,
	Bool:                  BoolFunc,
	DateEquals:            DateEqualsFunc,
	DateNotEquals:         DateNotEqualsFunc,
	DateLessThan:          DateLessThanFunc,
	DateLessThanEquals:    DateLessThanEqualsFunc,
	DateGreaterThan:       DateGreaterThanFunc,
	DateGreaterThanEquals: DateGreaterThanEqualsFunc,
}

type ConditionFunc func(p *RequestParam, values ConditionValues) bool

var (
	awsTrimedPrefix = []string{"aws:", "jwt:", "s3:"}
)

func TrimAwsPrefixKey(key string) string {
	for _, prefix := range awsTrimedPrefix {
		if strings.HasPrefix(key, prefix) {
			return strings.TrimPrefix(key, prefix)
		}
	}

	return key
}

func getCondtionValues(r *http.Request) map[string][]string {
	currentTime := time.Now().UTC()
	authInfo := parseRequestAuthInfo(r)
	accessKey := authInfo.accessKey
	principalType := "User"
	if accessKey == "" {
		principalType = "Anonymous"
	}
	values := map[string][]string{
		"SourceIp":      {getRequestIP(r)},
		"UserAgent":     {r.UserAgent()},
		"Referer":       {r.Referer()},
		"CurrentTime":   {currentTime.Format(AMZTimeFormat)},
		"EpochTime":     {fmt.Sprintf("%d", currentTime.Unix())},
		"userid":        {accessKey},
		"username":      {accessKey},
		"PrincipalType": {principalType},
	}

	for k, v := range r.Header {
		if existsV, ok := values[k]; ok {
			values[k] = append(existsV, v...)
		} else {
			values[k] = v
		}
	}

	for k, v := range r.URL.Query() {
		if existsV, ok := values[k]; ok {
			values[k] = append(existsV, v...)
		} else {
			values[k] = v
		}
	}

	return values
}

func IpAddressFunc(p *RequestParam, value ConditionValues) bool {
	key := TrimAwsPrefixKey(AwsSourceIp)
	canonicalKey := http.CanonicalHeaderKey(key)
	sourceIP, ok := value[canonicalKey]
	if !ok {
		return false
	}
	for ipnet := range sourceIP.values {
		if ok, _ := isIPNetContainsIP(p.sourceIP, ipnet); ok {
			return true
		}
	}

	return true
}

func NotIpAddressFunc(p *RequestParam, values ConditionValues) bool {
	return !IpAddressFunc(p, values)
}

func StringLikeFunc(reqParam *RequestParam, storeCondVals ConditionValues) bool {
	for k, storeVals := range storeCondVals {
		key := TrimAwsPrefixKey(k)
		canonicalKey := http.CanonicalHeaderKey(key)
		if reqVals, ok := reqParam.conditionVars[canonicalKey]; ok {
			for _, rv := range reqVals {
				for sv := range storeVals.values {
					if match := patternMatch(rv, sv); match {
						return true
					}
				}
			}
		}
	}

	return false
}

func StringNotLikeFunc(p *RequestParam, values ConditionValues) bool {
	return !StringLikeFunc(p, values)
}

func StringEqualsFunc(reqParam *RequestParam, storeCondVals ConditionValues) bool {
	for k, storeVals := range storeCondVals {
		key := TrimAwsPrefixKey(k)
		canonicalKey := http.CanonicalHeaderKey(key)
		if reqVals, ok := reqParam.conditionVars[canonicalKey]; ok {
			for _, rv := range reqVals {
				if storeVals.Contains(rv) {
					return true
				}
			}
		} else if reqVals, ok := reqParam.conditionVars[key]; ok {
			for _, rv := range reqVals {
				if storeVals.Contains(rv) {
					return true
				}
			}
		}
	}

	return false
}

func StringNotEqualsFunc(p *RequestParam, values ConditionValues) bool {
	return !StringNotEqualsFunc(p, values)
}

// check statement conditions
func (s Statement) checkConditions(param *RequestParam) bool {
	if len(s.Condition) == 0 {
		return true
	}
	for k, v := range s.Condition {
		f, ok := ConditionFuncMap[k]
		if !ok {
			continue
		}
		if !f(param, v) {
			return false
		}
	}

	return true
}

func BoolFunc(p *RequestParam, policyCondtion ConditionValues) bool {
	if len(policyCondtion) == 0 {
		return true
	}
	for condKey, condVal := range policyCondtion {
		for vals := range condVal.values {
			val1, _ := strconv.ParseBool(vals)
			if cond, ok := p.conditionVars[condKey]; ok {
				for _, c := range cond {
					val2, _ := strconv.ParseBool(c)
					return val1 == val2
				}
			}
		}
	}

	return false
}

func DateEqualsFunc(p *RequestParam, policyVals ConditionValues) bool {
	for k, pVals := range policyVals {
		for pVal := range pVals.values {
			pDate, err := time.Parse(AMZTimeFormat, pVal)
			if err != nil {
				return false
			}
			if reqVals, ok := p.conditionVars[k]; ok {
				for _, reqVal := range reqVals {
					reqDate, err := time.Parse(AMZTimeFormat, reqVal)
					if err != nil {
						return false
					}
					return pDate.Equal(reqDate)
				}
			}
		}
	}

	return false
}

func DateNotEqualsFunc(p *RequestParam, value ConditionValues) bool {
	return !DateEqualsFunc(p, value)
}

func DateLessThanFunc(p *RequestParam, value ConditionValues) bool {
	for k, pVals := range value {
		for pVal := range pVals.values {
			pDate, err := time.Parse(AMZTimeFormat, pVal)
			if err != nil {
				return false
			}
			if reqVals, ok := p.conditionVars[k]; ok {
				for _, reqVal := range reqVals {
					reqDate, err := time.Parse(AMZTimeFormat, reqVal)
					if err != nil {
						return false
					}
					return reqDate.Before(pDate)
				}
			}
		}
	}
	return false
}

func DateLessThanEqualsFunc(p *RequestParam, value ConditionValues) bool {
	for k, pVals := range value {
		for pVal := range pVals.values {
			pDate, err := time.Parse(AMZTimeFormat, pVal)
			if err != nil {
				return false
			}
			if reqVals, ok := p.conditionVars[k]; ok {
				for _, reqVal := range reqVals {
					reqDate, err := time.Parse(AMZTimeFormat, reqVal)
					if err != nil {
						return false
					}
					return reqDate.Before(pDate) || reqDate.Equal(pDate)
				}
			}
		}
	}
	return true
}

func DateGreaterThanFunc(p *RequestParam, value ConditionValues) bool {
	return !DateLessThanEqualsFunc(p, value)
}

func DateGreaterThanEqualsFunc(p *RequestParam, value ConditionValues) bool {
	return !DateLessThanFunc(p, value)
}

func NumericEqualsFunc(p *RequestParam, value ConditionValues) bool {
	//TODO: numeric equal implements

	return true
}

func NumericNotEqualsFunc(p *RequestParam, value ConditionValues) bool {
	return !NumericEqualsFunc(p, value)
}

func NumericLessThanFunc(p *RequestParam, value ConditionValues) bool {
	//TODO: numeric less than implements

	return true
}

func NumericLessThanEqualsFunc(p *RequestParam, value ConditionValues) bool {
	return NumericLessThanFunc(p, value) || NumericEqualsFunc(p, value)
}

func NumericGreaterThanFunc(p *RequestParam, value ConditionValues) bool {
	return !NumericLessThanEqualsFunc(p, value)
}

func NumericGreaterThanEqualsFunc(p *RequestParam, value ConditionValues) bool {
	return !NumericLessThanFunc(p, value)
}

func ArnEqualsFunc(p *RequestParam, value ConditionValues) bool {
	//TODO: numeric equal implements

	return true
}

func ArnNotEqualsFunc(p *RequestParam, value ConditionValues) bool {
	return !ArnEqualsFunc(p, value)
}

func ArnLikeFunc(p *RequestParam, value ConditionValues) bool {
	//TODO: numeric equal implements

	return true
}

func ArnNotLikeFunc(p *RequestParam, value ConditionValues) bool {
	return !ArnLikeFunc(p, value)
}
