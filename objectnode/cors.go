package objectnode

// https://docs.aws.amazon.com/AmazonS3/latest/dev/cors.html

import (
	"encoding/xml"
	"sync/atomic"

	"github.com/chubaofs/chubaofs/util/errors"
)

var (
	methodsRequest = []string{"GET", "PUT", "HEAD", "POST", "DELETE", "*"}
	allMethods     = []string{"GET", "PUT", "HEAD", "POST", "DELETE"}
)

type CORSConfiguration struct {
	XMLName  xml.Name    `xml:"CORSConfiguration" json:"xml_name"`
	CORSRule []*CORSRule `xml:"CORSRule" json:"cors_rule"`
}

type CORSRule struct {
	AllowedHeader []string `xml:"AllowedHeader" json:"allowed_header"`
	AllowedMethod []string `xml:"AllowedMethod" json:"allowed_method"`
	AllowedOrigin []string `xml:"AllowedOrigin" json:"allowed_origin"`
	ExposeHeader  []string `xml:"ExposeHeader" json:"expose_header"`
	MaxAgeSeconds uint16   `xml:"MaxAgeSeconds" json:"max_age_seconds"`

	allowAllOrigin bool
	allowAllMethod bool
	allowAllHeader bool

	processed int32
}

func (rule *CORSRule) preprocess() {
	if !atomic.CompareAndSwapInt32(&rule.processed, 0, 1) {
		return
	}
	if len(rule.AllowedOrigin) == 0 || contains(rule.AllowedOrigin, "*") {
		rule.allowAllOrigin = true
	}
	if len(rule.AllowedMethod) == 0 || contains(rule.AllowedMethod, "*") {
		rule.allowAllMethod = true
	} else if len(rule.AllowedMethod) >= len(allMethods) {
		rule.allowAllMethod = true
		for _, method := range allMethods {
			if !contains(rule.AllowedMethod, method) {
				rule.allowAllMethod = false
				break
			}
		}
	}
	if len(rule.AllowedHeader) == 0 || contains(rule.AllowedHeader, "*") {
		rule.allowAllHeader = true
	}
}

func (rule *CORSRule) Match(origin, method string, headers []string) bool {
	return rule.matchOrigin(origin) && rule.matchMethod(method) && rule.matchHeaders(headers)
}

func (rule *CORSRule) matchOrigin(origin string) bool {
	rule.preprocess()
	return rule.allowAllOrigin || contains(rule.AllowedOrigin, origin)
}

func (rule *CORSRule) matchMethod(method string) bool {
	rule.preprocess()
	return rule.allowAllMethod || contains(rule.AllowedMethod, method)
}

func (rule *CORSRule) matchHeaders(headers []string) bool {
	rule.preprocess()
	if rule.allowAllHeader {
		return true
	}
	if len(headers) > 0 {
		for _, header := range headers {
			if !contains(rule.AllowedHeader, header) {
				return false
			}
		}
	}
	return true
}

func (corsConfig *CORSConfiguration) validate() bool {
	if len(corsConfig.CORSRule) > 100 {
		return false
	}
	for _, rule := range corsConfig.CORSRule {
		for _, method := range rule.AllowedMethod {
			if !contains(methodsRequest, method) {
				return false
			}
		}
		rule.preprocess()
	}
	return true
}

func parseCorsConfig(bytes []byte) (corsConfig *CORSConfiguration, err error) {
	corsConfig = &CORSConfiguration{}
	if err = xml.Unmarshal(bytes, corsConfig); err != nil {
		return
	}
	if ok := corsConfig.validate(); !ok {
		return nil, errors.New("invalid cors configuration")
	}
	return
}

func storeBucketCors(bytes []byte, vol *Volume) (err error) {
	if err = vol.store.Put(vol.name, bucketRootPath, XAttrKeyOSSCORS, bytes); err != nil {
		return
	}
	return nil
}

func deleteBucketCors(vol *Volume) (err error) {
	if err = vol.store.Delete(vol.name, bucketRootPath, XAttrKeyOSSCORS); err != nil {
		return err
	}
	return nil
}
