package meta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	requestTimeout = 30 * time.Second
)

type request struct {
	method string
	path   string
	params map[string]string
	header map[string]string
	body   []byte
}

func newAPIRequest(method string, path string) *request {
	return &request{
		method: method,
		path:   path,
		params: make(map[string]string),
		header: make(map[string]string),
	}
}

func (r *request) addParam(key, value string) {
	r.params[key] = value
}

func (r *request) addHeader(key, value string) {
	r.header[key] = value
}

func (r *request) addBody(body []byte) {
	r.body = body
}

type MetaHttpClient struct {
	sync.RWMutex
	useSSL bool
	host   string
}

// NewMasterHelper returns a new MasterClient instance.
func NewMetaHttpClient(host string, useSSL bool) *MetaHttpClient {
	return &MetaHttpClient{host: host, useSSL: useSSL}
}

func (c *MetaHttpClient) serveRequest(r *request) (respData []byte, err error) {
	var resp *http.Response
	var schema string
	if c.useSSL {
		schema = "https"
	} else {
		schema = "http"
	}
	var url = fmt.Sprintf("%s://%s%s", schema, c.host, r.path)
	resp, err = c.httpRequest(r.method, url, r.params, r.header, r.body)

	if err != nil {
		log.LogErrorf("serveRequest: send http request fail: method(%v) url(%v) err(%v)", r.method, url, err)
		return
	}
	stateCode := resp.StatusCode
	respData, err = ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		log.LogErrorf("serveRequest: read http response body fail: err(%v)", err)
		return
	}
	switch stateCode {
	case http.StatusOK:
		var body = &struct {
			Code int32           `json:"code"`
			Msg  string          `json:"msg"`
			Data json.RawMessage `json:"data"`
		}{}

		if err := json.Unmarshal(respData, body); err != nil {
			return nil, fmt.Errorf("unmarshal response body err:%v", err)

		}
		// o represent proto.ErrCodeSuccess
		if body.Code != http.StatusOK && body.Code != http.StatusSeeOther {
			return nil, fmt.Errorf("%v:%s", proto.ParseErrorCode(body.Code), body.Msg)
		}
		return []byte(body.Data), nil
	default:
		log.LogErrorf("serveRequest: unknown status: host(%v) uri(%v) status(%v) body(%s).",
			resp.Request.URL.String(), c.host, stateCode, strings.Replace(string(respData), "\n", "", -1))
	}
	return
}

func (c *MetaHttpClient) httpRequest(method, url string, param, header map[string]string, reqData []byte) (resp *http.Response, err error) {
	client := http.DefaultClient
	reader := bytes.NewReader(reqData)
	client.Timeout = requestTimeout
	var req *http.Request
	fullUrl := c.mergeRequestUrl(url, param)
	log.LogDebugf("httpRequest: merge request url: method(%v) url(%v) bodyLength[%v].", method, fullUrl, len(reqData))
	if req, err = http.NewRequest(method, fullUrl, reader); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err = client.Do(req)
	return
}

func (c *MetaHttpClient) mergeRequestUrl(url string, params map[string]string) string {
	if params != nil && len(params) > 0 {
		buff := bytes.NewBuffer([]byte(url))
		isFirstParam := true
		for k, v := range params {
			if isFirstParam {
				buff.WriteString("?")
				isFirstParam = false
			} else {
				buff.WriteString("&")
			}
			buff.WriteString(k)
			buff.WriteString("=")
			buff.WriteString(v)
		}
		return buff.String()
	}
	return url
}

type GetMPInfoResp struct {
	LeaderAddr     string          `json:"leaderAddr"`
	Peers          []proto.Peer    `json:"peers"`
	NodeId         uint64          `json:"nodeId"`
	Cursor         uint64          `json:"cursor"`
	ApplyId        uint64          `json:"apply_id"`
	InodeCount     uint64          `json:"inode_count"`
	DentryCount    uint64          `json:"dentry_count"`
	MultipartCount uint64          `json:"multipart_count"`
	ExtentCount    uint64          `json:"extent_count"`
	FreeListCount  int             `json:"free_list_count"`
	Leader         bool            `json:"leader"`
}
//msg["leaderAddr"] = leader
//msg["leader_term"] = leaderTerm
//msg["partition_id"] = conf.PartitionId
//msg["partition_type"] = conf.PartitionType
//msg["vol_name"] = conf.VolName
//msg["start"] = conf.Start
//msg["end"] = conf.End
//msg["peers"] = conf.Peers
//msg["nodeId"] = conf.NodeId
//msg["cursor"] = conf.Cursor
func (mc *MetaHttpClient) GetMetaPartition(pid uint64) (resp *GetMPInfoResp, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[GetMetaPartition],pid:%v,err:%v", pid, err)
		}
	}()
	req := newAPIRequest(http.MethodGet, "/getPartitionById")
	req.addParam("pid", fmt.Sprintf("%v", pid))
	respData, err := mc.serveRequest(req)
	log.LogInfof("err:%v,respData:%v\n", err, string(respData))
	if err != nil {
		return
	}
	resp = &GetMPInfoResp{}
	if err = json.Unmarshal(respData, resp); err != nil {
		return
	}
	return
}
