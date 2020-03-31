package objectnode

import (
	"encoding/xml"

	"github.com/chubaofs/chubaofs/util/errors"
)

const OSS_CORS_KEY = "oss:cors"

var methodsRequest = []string{"GET", "PUT", "HEAD", "POST", "DELETE"}

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

func (corsConfig *CORSConfiguration) Validate() bool {
	for _, rule := range corsConfig.CORSRule {
		for _, method := range rule.AllowedMethod {
			if !contains(methodsRequest, method) {
				return false
			}
		}
	}
	return true
}

func ParseCorsConfig(bytes []byte) (corsConfig *CORSConfiguration, err error) {
	corsConfig = &CORSConfiguration{}
	if err = xml.Unmarshal(bytes, corsConfig); err != nil {
		return
	}
	if ok := corsConfig.Validate(); !ok {
		return nil, errors.New("invalid cors configuration")
	}
	return
}

func storeBucketCors(bytes []byte, vol *Volume) (err error) {
	if err = vol.store.Put(vol.name, bucketRootPath, OSS_CORS_KEY, bytes); err != nil {
		return
	}
	return nil
}

func deleteBucketCors(vol *Volume) (err error) {
	if err = vol.store.Delete(vol.name, bucketRootPath, OSS_CORS_KEY); err != nil {
		return err
	}
	return nil
}
