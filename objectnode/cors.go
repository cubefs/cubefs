package objectnode

// https://docs.aws.amazon.com/AmazonS3/latest/dev/cors.html

import (
	"encoding/xml"
	"regexp"
	"strings"
)

var methodsRequest = []string{"GET", "PUT", "HEAD", "POST", "DELETE", "*"}

type CORSConfiguration struct {
	XMLName  xml.Name    `xml:"CORSConfiguration" json:"xml_name"`
	CORSRule []*CORSRule `xml:"CORSRule" json:"cors_rule"`
}

type CORSRule struct {
	AllowedHeader []string `xml:"AllowedHeader" json:"allowed_header"`
	AllowedMethod []string `xml:"AllowedMethod" json:"allowed_method"`
	AllowedOrigin []string `xml:"AllowedOrigin" json:"allowed_origin"`
	ExposeHeader  []string `xml:"ExposeHeader" json:"expose_header"`
	MaxAgeSeconds uint16   `xml:"MaxAgeSeconds,omitempty" json:"max_age_seconds,omitempty"`
}

func (rule *CORSRule) match(origin, method string, headers []string) bool {
	if !matchOrigin(rule.AllowedOrigin, origin) {
		return false
	}
	if !matchMethod(rule.AllowedMethod, method) {
		return false
	}
	if len(headers) != 0 && !matchHeaders(rule.AllowedHeader, headers) {
		return false
	}
	return true
}

func (corsConfig *CORSConfiguration) validate() *ErrorCode {
	if len(corsConfig.CORSRule) == 0 {
		return NewError("BadRequest", "No CORS Rules found in request.", 400)
	}
	if len(corsConfig.CORSRule) > 10 {
		return TooManyCorsRules
	}
	for _, rule := range corsConfig.CORSRule {
		if err := valid(*rule); err != nil {
			return err
		}
	}
	return nil
}

func parseCorsConfig(bytes []byte) (corsConfig *CORSConfiguration, errCode *ErrorCode) {
	corsConfig = &CORSConfiguration{}
	if err := xml.Unmarshal(bytes, corsConfig); err != nil {
		return nil, MalformedXML
	}
	if err := corsConfig.validate(); err != nil {
		return nil, err
	}
	return corsConfig, nil
}

func valid(r CORSRule) *ErrorCode {
	if len(r.AllowedOrigin) == 0 {
		return NewError("InvalidCORSRule", "Missing AllowedOrigins.", 400)
	}
	if len(r.AllowedMethod) == 0 {
		return NewError("InvalidCORSRule", "Missing AllowedMethods.", 400)
	}
	//check origin, at most contain one *
	for _, origin := range r.AllowedOrigin {
		if strings.Count(origin, "*") > 1 {
			return NewError("InvalidCORSRule", "AllowedOrigin can not have more than one wildcard: "+origin, 400)
		}
	}
	//AllowedMethod is case sensitive
	for _, method := range r.AllowedMethod {
		if !StringListContain(methodsRequest, method) {
			return NewError("InvalidCORSRule", "AllowedMethod is unsupport: "+method, 400)
		}
	}
	//check allowedheaders, at most contain one *
	for _, header := range r.AllowedHeader {
		if strings.Count(header, "*") > 1 {
			return NewError("InvalidCORSRule", "AllowedHeaders can not have more than one wildcard: "+header, 400)
		}
	}
	//exposed headers can't include *
	for _, exheader := range r.ExposeHeader {
		if strings.Contains(exheader, "*") {
			return NewError("InvalidCORSRule", "ExposedHeaders can not include wildcard: "+exheader, 400)
		}
	}
	return nil
}

func StringListContain(list []string, element string) bool {
	for _, str := range list {
		if str == element {
			return true
		}
	}
	return false
}

func storeBucketCors(bytes []byte, vol *Volume) (err error) {
	return vol.store.Put(vol.name, bucketRootPath, XAttrKeyOSSCORS, bytes)
}

func deleteBucketCors(vol *Volume) (err error) {
	return vol.store.Delete(vol.name, bucketRootPath, XAttrKeyOSSCORS)
}

func matchOrigin(allowedOrigins []string, origin string) bool {
	for _, allowed := range allowedOrigins {
		if allowed == "*" {
			return true
		}
		pattern := makeRegexPattern(allowed)
		if ok, _ := regexp.MatchString(pattern, origin); ok {
			return true
		}
	}
	return false
}

func matchMethod(allowedMethod []string, method string) bool {
	return StringListContain(allowedMethod, method)
}

func matchHeaders(allowedHeaders []string, reqHeaders []string) bool {
	if len(allowedHeaders) == 0 {
		return false
	}
	for _, h := range reqHeaders {
		if !matchHeader(allowedHeaders, h) {
			return false
		}
	}
	return true
}

func matchHeader(allowedHeaders []string, reqHeader string) bool {
	for _, allowed := range allowedHeaders {
		if allowed == "*" {
			return true
		}
		pattern := makeRegexPattern(strings.ToLower(allowed))
		if ok, _ := regexp.MatchString(pattern, reqHeader); ok {
			return true
		}
	}
	return false
}
