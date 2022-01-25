package objectnode

// https://docs.aws.amazon.com/AmazonS3/latest/dev/cors.html

import (
	"encoding/xml"

	"github.com/cubefs/cubefs/util/errors"
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
	MaxAgeSeconds uint16   `xml:"MaxAgeSeconds" json:"max_age_seconds"`
}

func (rule *CORSRule) match(origin, method string, headers []string) bool {
	// todo if "*" are used in some text
	if !contains(rule.AllowedOrigin, "*") && !contains(rule.AllowedOrigin, origin) {
		return false
	}
	if !contains(rule.AllowedMethod, "*") && !contains(rule.AllowedMethod, method) {
		return false
	}
	if contains(rule.AllowedHeader, "*") {
		return true
	}
	for _, header := range headers {
		if !contains(rule.AllowedHeader, header) {
			return false
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
