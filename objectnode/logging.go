// Copyright 2023 The CubeFS Authors.
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
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
)

var (
	ErrInvalidBucketRegionForLogging = &ErrorCode{
		ErrorCode:    "InvalidBucketRegionForLogging",
		ErrorMessage: "The region of the target bucket is different from the source bucket.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrInvalidTargetPrefixForLogging = &ErrorCode{
		ErrorCode:    "InvalidTargetPrefixForLogging",
		ErrorMessage: "The target prefix for bucket logging contains incorrectly formatted characters.",
		StatusCode:   http.StatusBadRequest,
	}
	ErrInvalidTargetBucketForLogging = &ErrorCode{
		ErrorCode:    "InvalidTargetBucketForLogging",
		ErrorMessage: "The target bucket for logging does not exist, is not owned by you, or does not have the appropriate grants for the log-delivery group.",
		StatusCode:   http.StatusBadRequest,
	}

	loggingPrefixRegexp = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-_/]{0,31}$`)
)

type LoggingMessage struct {
	TargetBucket string
	TargetPrefix string

	API         string
	Bucket      string
	BytesSent   string
	Host        string
	Object      string
	Owner       string
	Referer     string
	RemoteIP    string
	Requester   string
	RequestTime string
	RequestURI  string
	StatusCode  string
	TotalTime   string
	UserAgent   string
}

type Logging struct {
	XMLNS          string          `xml:"xmlns,attr,omitempty" json:"-"`
	XMLName        *xml.Name       `xml:"BucketLoggingStatus" json:"-"`
	LoggingEnabled *LoggingEnabled `xml:"LoggingEnabled,omitempty" json:"le,omitempty"`
}

type LoggingEnabled struct {
	TargetBucket string `xml:"TargetBucket" json:"tb"`
	TargetPrefix string `xml:"TargetPrefix" json:"tp"`
}

func isLoggingPrefixValid(prefix string) bool {
	if prefix == "" {
		return true
	}
	return loggingPrefixRegexp.MatchString(prefix)
}

func storeBucketLogging(vol *Volume, logging *Logging) error {
	data, err := json.Marshal(logging)
	if err != nil {
		return err
	}
	return vol.store.Put(vol.name, bucketRootPath, XAttrKeyOSSLogging, data)
}

func getBucketLogging(vol *Volume) (*Logging, error) {
	logging, err := vol.metaLoader.loadLogging()
	if err != nil {
		return nil, err
	}
	if logging == nil {
		logging = new(Logging)
	}
	logging.XMLNS = XMLNS
	return logging, nil
}

func deleteBucketLogging(vol *Volume) error {
	return vol.store.Delete(vol.name, bucketRootPath, XAttrKeyOSSLogging)
}

func makeLoggingMessage(bucket, prefix string, entry auditlog.LogEntry) *LoggingMessage {
	data := &LoggingMessage{
		TargetBucket: bucket,
		TargetPrefix: prefix,
	}

	data.Owner = stringRawOrHyphen(entry.Owner())
	data.Bucket = stringRawOrHyphen(entry.Bucket())
	data.RequestTime = stringRawOrHyphen(fmt.Sprintf("%v", entry.ReqTime()))
	data.Requester = stringRawOrHyphen(entry.Requester())
	data.API = stringRawOrHyphen(entry.ApiName())
	uri := fmt.Sprintf("%s %s", entry.Method(), entry.Path())
	if query := entry.RawQuery(); query != "" {
		uri += "?" + query
	}
	data.RequestURI = stringWrappedOrHyphen(uri, `"`)
	data.StatusCode = stringRawOrHyphen(entry.Code())
	data.BytesSent = stringRawOrHyphen(fmt.Sprintf("%v", entry.RespLength()))
	data.TotalTime = fmt.Sprintf("%v", entry.RespTime()/10000)
	data.UserAgent = stringWrappedOrHyphen(entry.UA(), `"`)
	data.Referer = stringWrappedOrHyphen(entry.Referer(), `"`)
	data.Host = stringRawOrHyphen(entry.ReqHost())
	data.RemoteIP = stringRawOrHyphen(entry.XRemoteIP())

	return data
}

func makeLoggingTimeName(t time.Time, grit int) string {
	year, mon, day := t.Date()
	hour, min, _ := t.Clock()
	if grit <= 0 {
		grit = 5
	}
	min = (min / grit) * grit
	return fmt.Sprintf("%d-%02d-%02d/%02d-%02d-00", year, mon, day, hour, min)
}

func makeLoggingNameData(msg *LoggingMessage, grit int) (name string, data []byte, err error) {
	sec, err := strconv.ParseInt(msg.RequestTime, 10, 64)
	if err != nil {
		return
	}
	reqTime := time.Unix(sec, 0).UTC()
	timeName := makeLoggingTimeName(reqTime, grit)
	name = fmt.Sprintf("%s/%s%s%s", msg.TargetBucket, msg.TargetPrefix, msg.Bucket, timeName)
	data = []byte(strings.Join([]string{
		reqTime.Format(ISO8601LayoutCompatible),
		msg.API,
		msg.Bucket,
		msg.RequestURI,
		msg.Requester,
		msg.Owner,
		msg.StatusCode,
		msg.BytesSent,
		msg.TotalTime,
		msg.Host,
		msg.RemoteIP,
		msg.Referer,
		msg.UserAgent,
	}, "\t") + "\n")

	return
}

func stringRawOrHyphen(raw string) string {
	if raw == "" {
		return "-"
	}
	return raw
}

func stringWrappedOrHyphen(raw, wc string) string {
	if raw == "" {
		return "-"
	}
	return fmt.Sprintf("%s%s%s", wc, raw, wc)
}
