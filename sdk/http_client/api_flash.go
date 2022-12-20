package http_client

import (
	"encoding/json"
	"github.com/cubefs/cubefs/proto"
	"net/http"
	"sync"
	"time"
)

type FlashClient struct {
	sync.RWMutex
	useSSL bool
	host   string
}

// NewFlashClient returns a new FlashClient instance.
func NewFlashClient(host string, useSSL bool) *FlashClient {
	return &FlashClient{host: host, useSSL: useSSL}
}

func (client *FlashClient) RequestHttp(method, path string, param map[string]string) (respData []byte, err error) {
	req := newAPIRequest(method, path)
	for k, v := range param {
		req.addParam(k, v)
	}
	return serveRequest(client.useSSL, client.host, req)
}

func (client *FlashClient) GetStat() (stat *proto.CacheStatus, err error) {
	var d []byte
	for i := 0; i < 3; i++ {
		d, err = client.RequestHttp(http.MethodGet, "/stat", nil)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	stat = new(proto.CacheStatus)
	if err = json.Unmarshal(d, stat); err != nil {
		return
	}
	return
}
func (client *FlashClient) EvictVol(volume string) (err error) {
	params := make(map[string]string)
	params["volume"] = volume
	_, err = client.RequestHttp(http.MethodGet, "/evictVol", params)
	if err != nil {
		return
	}
	return
}

func (client *FlashClient) EvictAll() (err error) {
	_, err = client.RequestHttp(http.MethodGet, "/evictAll", nil)
	if err != nil {
		return
	}
	return
}
