package util

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/chubaoio/cbfs/util/log"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	ErrNoValidMaster = errors.New("no valid master")
)

type MasterHelper interface {
	AddNode(address string)
	Nodes() []string
	Leader() string
	Request(method, path string, param map[string]string, body []byte) (data []byte, err error)
}

type masterHelper struct {
	masters   []string
	leaderIdx int
	skipMarks *Set
	sync.RWMutex
}

func (helper *masterHelper) AddNode(address string) {
	helper.Lock()
	defer helper.Unlock()
	helper.updateMaster(address)
}

func (helper *masterHelper) Leader() string {
	helper.RLock()
	defer helper.RUnlock()
	return helper.masters[helper.leaderIdx]
}

func (helper *masterHelper) Request(method, path string, param map[string]string, reqData []byte) (respData []byte, err error) {
	helper.Lock()
	defer helper.Unlock()
	respData, err = helper.request(method, path, param, reqData)
	helper.skipMarks.RemoveAll()
	return
}

func (helper *masterHelper) request(method, path string, param map[string]string, reqData []byte) (repsData []byte, err error) {
	for i := 0; i < len(helper.masters); i++ {
		var index int
		if i+int(helper.leaderIdx) < len(helper.masters) {
			index = i + int(helper.leaderIdx)
		} else {
			index = (i + int(helper.leaderIdx)) - len(helper.masters)
		}
		masterAddr := helper.masters[index]
		mark := int(crc32.ChecksumIEEE([]byte(masterAddr)))
		if helper.skipMarks.Has(mark) {
			log.LogErrorf("action[request] all master address has been tried.")
			break
		}
		helper.skipMarks.Add(mark)
		var resp *http.Response
		resp, err = helper.httpRequest(method, fmt.Sprintf("http://%s%s", masterAddr, path), param, reqData)
		if err != nil {
			continue
		}
		stateCode := resp.StatusCode
		repsData, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		switch stateCode {
		case http.StatusForbidden:
			curMasterAddr := strings.TrimSpace(string(repsData))
			curMasterAddr = strings.Replace(curMasterAddr, "\n", "", -1)
			if len(curMasterAddr) == 0 {
				err = ErrNoValidMaster
				return
			}
			helper.updateMaster(curMasterAddr)
			repsData, err = helper.request(method, path, param, reqData)
			return
		case http.StatusOK:
			return
		default:
			log.LogErrorf("action[request] master[%v] uri[%v] statusCode[%v] respBody[%v].",
				resp.Request.URL.String(), masterAddr, stateCode, string(repsData))
			continue
		}
	}
	err = ErrNoValidMaster
	return
}

func (helper *masterHelper) Nodes() []string {
	helper.RLock()
	defer helper.RUnlock()
	return helper.masters
}

func (helper *masterHelper) httpRequest(method, url string, param map[string]string, reqData []byte) (resp *http.Response, err error) {
	client := &http.Client{}
	reader := bytes.NewReader(reqData)
	client.Timeout = time.Second * 3
	var req *http.Request
	fullUrl := helper.mergeRequestUrl(url, param)
	log.LogDebugf("action[httpRequest] method[%v] url[%v] reqBodyLen[%v].", method, fullUrl, len(reqData))
	if req, err = http.NewRequest(method, fullUrl, reader); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	resp, err = client.Do(req)
	return
}

func (helper *masterHelper) updateMaster(address string) {
	contains := false
	for _, master := range helper.masters {
		if master == address {
			contains = true
			break
		}
	}
	if !contains {
		helper.masters = append(helper.masters, address)
	}
	for i, master := range helper.masters {
		if master == address {
			helper.leaderIdx = i
			break
		}
	}
}

func (helper *masterHelper) mergeRequestUrl(url string, params map[string]string) string {
	if params != nil && len(params) > 0 {
		buff := bytes.NewBuffer([]byte(url))
		firstParam := true
		for k, v := range params {
			if firstParam {
				buff.WriteString("?")
				firstParam = false
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

func NewMasterHelper() MasterHelper {
	return &masterHelper{
		masters:   make([]string, 0),
		skipMarks: NewSet(),
	}
}
