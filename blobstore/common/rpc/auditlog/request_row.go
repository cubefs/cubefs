// Copyright 2022 The CubeFS Authors.
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

package auditlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	TAG_SERVICE         = 1
	TAG_TIME            = 2
	TAG_METHOD          = 3
	TAG_PATH            = 4
	TAG_HEADER          = 5
	TAG_PARAMS          = 6
	TAG_CODE            = 7
	TAG_RESPONSE_HEADER = 8
	TAG_RESPONSE        = 9
	TAG_RESPONSE_SIZE   = 10
	TAG_RESPONSE_TIME   = 11
)

const (
	LogTypeAudit = "audit"
)

type LogEntry interface {
	Service() string
	LogType() string
	Code() string
	Method() string
	Path() string
	RespTime() int64 // 100ns
	RespLength() int64
	ReqLength() int64
	ReqHost() string
	RemoteAddr() string
	Xlogs() []string
	UA() string
	XRespCode() string
	// ReqParams returns params of request, including params in raw query and body
	ReqParams() string
	Uid() uint32
	ApiName() string
}

func ErrInValidFieldCnt(msg string) error {
	return errors.New("invalid field count, " + msg)
}

var vre = regexp.MustCompile("^v[0-9]+$")

type ReqHeader struct {
	ContentLength string `json:"Content-Length"`
	BodySize      int64  `json:"bs"` // body size
	RawQuery      string `json:"RawQuery"`
	Host          string `json:"Host"`
	Token         *Token `json:"Token"`
	XRealIp       string `json:"X-Real-Ip"`
	XFromCdn      string `json:"X-From-Cdn"`
	XSrc          string `json:"X-Src"`
	IP            string `json:"IP"`
	UA            string `json:"User-Agent"`
}

type Token struct {
	AppId uint64 `json:"appid"`
	Uid   uint32 `json:"uid"`
	Utype uint32 `json:"utype"`
}

type SToken struct {
	Uid    uint32 `json:"uid"`
	Bucket string `json:"tbl"`
}

type RsInfo struct {
	Bucket   string `json:"bucket"`
	EntryURI string `json:"entryURI"`
}

type RespHeader struct {
	ContentLength     string           `json:"Content-Length"`
	Xlog              []string         `json:"X-Log"`
	XWarn             []string         `json:"X-Warn"`
	Bucket            string           `json:"Tbl"`
	Token             *Token           `json:"Token"`
	SToken            *SToken          `json:"SToken"`
	FileType          int              `json:"FileType"`
	BatchOps          map[string]int64 `json:"batchOps"`
	PreDelSize        map[string]int64 `json:"preDelSize"`
	PreDelArchiveSize map[string]int64 `json:"preDelArchiveSize"`
	OUid              uint32           `json:"ouid"` // owner uid
	RsInfo            *RsInfo          `json:"rs-info"`
	XRespCode         string           `json:"X-Resp-Code"` // return from dora
	BillTag           string           `json:"billtag"`     // must be same with definition  in billtag.go
	BatchDeletes      map[uint32]int64 `json:"batchDelete"`
	ApiName           string           `json:"api"` // api name of this auditlog
}

type RequestRow struct {
	data []string

	// the following fields will be loaded lazily and use get request to obtain them
	reqHeader  *ReqHeader
	respHeader *RespHeader
	rawQuery   *url.Values
}

type AdRowParser struct{}

func (self *AdRowParser) Parse(line string) (*RequestRow, error) {
	return ParseReqlogToAdrow(line)
}

func ParseReqlogToAdrow(line string) (*RequestRow, error) {
	idx := strings.Index(line, "REQ")
	if idx < 0 {
		return nil, errors.New("invalid reqlog")
	}
	line = strings.TrimSuffix(line[idx:], "\n")
	a := new(RequestRow)
	a.data = strings.Split(line, "\t")
	if len(a.data) != 12 && len(a.data) != 14 {
		return nil, ErrInValidFieldCnt(fmt.Sprintf("expect 12 or 14 while get %v", len(a.data)))
	}
	if a.data[0] != "REQ" {
		return nil, fmt.Errorf("invalid Head: %v", a.data[0])
	}
	return a, nil
}

func ParseReqlog(line string) (LogEntry, error) {
	return ParseReqlogToAdrow(line)
}

func (a *RequestRow) LogType() string {
	return LogTypeAudit
}

func (a *RequestRow) ApiName() string { // s3, apiName can get directly from auditlog
	respHeader := a.getRespHeader()
	if respHeader == nil {
		return ""
	}
	return respHeader.ApiName
}

func (a *RequestRow) Uid() uint32 {
	respSToken := a.RespSToken()
	if respSToken != nil {
		return respSToken.Uid
	}
	respToken := a.RespToken()
	if respToken != nil {
		return respToken.Uid
	}
	reqToken := a.ReqToken()
	if reqToken != nil {
		return reqToken.Uid
	}
	return 0
}

func (a *RequestRow) RespToken() *Token {
	respHeader := a.getRespHeader()
	if respHeader == nil {
		return nil
	}
	return respHeader.Token
}

func (a *RequestRow) RespSToken() *SToken {
	respHeader := a.getRespHeader()
	if respHeader == nil {
		return nil
	}
	return respHeader.SToken
}

func (a *RequestRow) UA() string {
	reqHeader := a.getReqHeader()
	if reqHeader != nil && reqHeader.UA != "" {
		return reqHeader.UA
	}
	return ""
}

func (a *RequestRow) Bucket() string {
	respHeader := a.getRespHeader()
	if respHeader != nil && respHeader.Bucket != "" {
		return respHeader.Bucket
	}
	return ""
}

func (a *RequestRow) getReqHeader() *ReqHeader {
	if a.reqHeader != nil {
		return a.reqHeader
	}
	err := json.Unmarshal([]byte(a.data[TAG_HEADER]), &a.reqHeader)
	if err != nil {
		log.Warnf("%q, Unmarshal ReqHeader err %v", a.data[TAG_HEADER], err)
		return nil
	}
	return a.reqHeader
}

func (a *RequestRow) getRawquery() *url.Values {
	reqHeader := a.getReqHeader()
	if reqHeader == nil {
		return nil
	}
	m, err := url.ParseQuery(reqHeader.RawQuery)
	if err != nil {
		return nil
	}
	a.rawQuery = &m
	return a.rawQuery
}

func (a *RequestRow) getRespHeader() *RespHeader {
	if a.respHeader != nil {
		return a.respHeader
	}
	err := json.Unmarshal([]byte(a.data[TAG_RESPONSE_HEADER]), &a.respHeader)
	if err != nil {
		log.Warnf("%q, Unmarshal RespHeader err %v", a.data[TAG_RESPONSE_HEADER], err)
		return nil
	}
	return a.respHeader
}

func (a *RequestRow) String() string {
	return strings.Join(a.data, "\t")
}

func (a *RequestRow) Code() string {
	if isType(Digital, a.data[TAG_CODE]) {
		return a.data[TAG_CODE]
	}
	return "unknown"
}

func (a *RequestRow) ReqToken() *Token {
	reqHeader := a.getReqHeader()
	if reqHeader == nil {
		return nil
	}
	return reqHeader.Token
}

// get unix second of time
func (a *RequestRow) ReqTime() int64 {
	t, err := strconv.ParseInt(a.data[TAG_TIME], 10, 0)
	if err != nil {
		log.Errorf("cannot parse time %v to int", a.data[TAG_TIME])
	}
	return t / 10000000
}

func (a *RequestRow) RemoteIp() string {
	reqHeader := a.getReqHeader()
	if reqHeader != nil {
		if reqHeader.XRealIp != "" {
			return strings.TrimSpace(reqHeader.XRealIp)
		} else {
			return strings.TrimSpace(reqHeader.IP)
		}
	}
	return ""
}

func (a *RequestRow) ReqCdn() string {
	reqHeader := a.getReqHeader()
	if reqHeader == nil {
		return ""
	}
	return reqHeader.XFromCdn
}

func (a *RequestRow) ReqSrc() string {
	reqHeader := a.getReqHeader()
	if reqHeader == nil {
		return ""
	}
	return reqHeader.XSrc
}

func (a *RequestRow) Service() string {
	return a.data[TAG_SERVICE]
}

func (a *RequestRow) Method() string {
	return a.data[TAG_METHOD]
}

func (a *RequestRow) Path() string {
	return a.data[TAG_PATH]
}

func (a *RequestRow) RawQuery() string {
	reqHeader := a.getReqHeader()
	if reqHeader == nil {
		return ""
	}
	return a.getReqHeader().RawQuery
}

func (a *RequestRow) RespTime() (respTime int64) {
	var err error
	respTime, err = strconv.ParseInt(a.data[TAG_RESPONSE_TIME], 10, 64)
	if err != nil {
		log.Error(err)
	}
	return respTime
}

func (a *RequestRow) RespLength() (respLength int64) {
	respLength, _ = strconv.ParseInt(a.data[TAG_RESPONSE_SIZE], 10, 64)
	return
}

func (a *RequestRow) XRespCode() string {
	respHeader := a.getRespHeader()
	if respHeader == nil {
		return ""
	}
	return respHeader.XRespCode
}

func (a *RequestRow) ReqLength() (reqLength int64) {
	reqHeader := a.getReqHeader()
	if reqHeader == nil {
		return
	}
	reqLength, _ = strconv.ParseInt(reqHeader.ContentLength, 10, 64)
	if reqLength > 0 {
		return reqLength
	}
	return reqHeader.BodySize
}

func (a *RequestRow) ReqHost() string {
	reqHeader := a.getReqHeader()
	if reqHeader == nil {
		return ""
	}
	return reqHeader.Host
}

func (a *RequestRow) ReqParams() string {
	return a.data[TAG_PARAMS]
}

func (a *RequestRow) ReqFsize() (fsize int64) {
	rawQuery := a.getRawquery()
	if rawQuery == nil {
		return
	}
	fsize, _ = strconv.ParseInt(rawQuery.Get("fsize"), 10, 64)
	return
}

func (a *RequestRow) RemoteAddr() string {
	return ""
}

func (a *RequestRow) XWarns() []string {
	respHeader := a.getRespHeader()
	if respHeader == nil {
		return nil
	}
	return respHeader.XWarn
}

func (a *RequestRow) BillTag() string {
	respHeader := a.getRespHeader()
	if respHeader == nil {
		return ""
	}
	return respHeader.BillTag
}

func (a *RequestRow) BatchDelete() map[uint32]int64 {
	respHeader := a.getRespHeader()
	if respHeader == nil {
		return nil
	}
	return respHeader.BatchDeletes
}

type Xlog struct {
	Name    string
	Err     string
	MsSpend uint64
}

func (a *RequestRow) Xlogs() []string {
	respHeader := a.getRespHeader()
	if respHeader == nil {
		return nil
	}
	return respHeader.Xlog
}

func (a *RequestRow) XlogSearch(name string) (rets []Xlog) {
	xlogs := a.Xlogs()
	for _, log := range xlogs {
		items := strings.Split(log, ";")
		for _, item := range items {
			if !strings.HasPrefix(item, name) {
				continue
			}
			ret := parseXlog(item)
			if ret.Name == name {
				rets = append(rets, ret)
			}
		}
	}
	return
}

func parseXlog(s string) (ret Xlog) {
	idx := strings.Index(s, "/")
	if idx > 0 {
		ret.Err = s[idx+1:]
		s = s[:idx]
	}
	idx = strings.LastIndex(s, ":")
	if idx > 0 {
		ret.MsSpend, _ = strconv.ParseUint(s[idx+1:], 10, 64)
		s = s[:idx]
	}
	ret.Name = s
	return
}

func (a *RequestRow) XlogTime(name string) (msSpeed uint64) {
	rets := a.XlogSearch(name)
	for _, ret := range rets {
		if ret.MsSpend > msSpeed {
			msSpeed = ret.MsSpend
		}
	}
	return
}

func (a *RequestRow) XlogsTime(names []string) (msSpeedTotal uint64) {
	for _, name := range names {
		msSpeedTotal += a.XlogTime(name)
	}
	return
}

// apiWithParams returns api information with maxApiLevel( default 2).
func apiWithParams(service, method, path, host, params string, maxApiLevel int) (api string) {
	if service == "" || method == "" {
		return "unknown.unknown"
	}
	stype := strings.ToLower(service)
	fields := strings.Split(strings.ToLower(path), "/")
	if len(fields) <= 1 {
		return stype + ".unknown"
	}

	firstPath := fields[1]
	firstPathIndex := 1

	switch stype {
	default:
		if (vre.MatchString(firstPath) || firstPath == "admin") && len(fields) > 2 && fields[2] != "" {
			firstPath = firstPath + "-" + fields[2]
			firstPathIndex = 2
		}

		// for defy api from apiserver
		if firstPath == "v2-tune" {
			return stype + ".v2-tune." + strings.Join(fields[firstPathIndex+1:], ".")
		}
		if !isValidApi(firstPath) {
			return stype + ".unknown"
		}
	}

	api = firstPath
	if maxApiLevel > 2 {
		level := 3
		index := firstPathIndex + 1
		length := len(fields)
		for level <= maxApiLevel && index < length {
			api += "." + fields[index]
			level += 1
			index += 1
		}
		if !isValidMultiPathApi(api) {
			return stype + ".unknown"
		}
	}

	return stype + "." + api
}

func isValidApi(api string) bool {
	return isType(ALPHA|Digital|SubLetter|Underline, api)
}

func isValidMultiPathApi(api string) bool {
	return isType(ALPHA|Digital|DotLetter|SubLetter|Underline, api)
}
