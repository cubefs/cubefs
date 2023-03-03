// Copyright 2018 The CubeFS Authors.
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

package prom

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	RegisterPeriod = time.Duration(10) * time.Minute
	RegisterPath   = "/v1/agent/service/register"
)

// consulRegisterEntry for prometheus
// optional for user when set prometheus exporter
type consulRegisterEntry struct {
	Name    string   `json:"Name"`
	ID      string   `json:"ID"`
	Address string   `json:"Address"`
	Port    int64    `json:"Port"`
	Tags    []string `json:"Tags"`
}

// get consul id
func getConsulId(app string, role string, host string, port int64) string {
	return fmt.Sprintf("%s_%s_%s_%d", app, role, host, port)
}

func consoleRegisterWorker(consulAddress, app, role, cluster string, port int64) {
	defer wg.Done()
	if len(consulAddress) <= 0 {
		return
	}
	log.LogInfof("metrics consul register %v %v %v", consulAddress, cluster, port)
	ticker := time.NewTicker(RegisterPeriod)
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("RegisterConsul panic,err[%v]", err)
		}
		ticker.Stop()
	}()

	host, err := getLocalIpAddr()
	if err != nil {
		log.LogErrorf("get local ip error, %v", err.Error())
		return
	}

	client := &http.Client{}
	req := makeRegisterReq(host, consulAddress, app, role, cluster, port)
	if req == nil {
		log.LogErrorf("make register req error")
		return
	}

	if resp, _ := client.Do(req); resp != nil {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}

	for {
		select {
		case <-stopC:
			return
		case <-ticker.C:
			req := makeRegisterReq(host, consulAddress, app, role, cluster, port)
			if req == nil {
				log.LogErrorf("make register req error")
				return
			}
			if resp, _ := client.Do(req); resp != nil {
				ioutil.ReadAll(resp.Body)
				resp.Body.Close()
			}
		}
	}
}

// make a consul rest request
func makeRegisterReq(host, addr, app, role, cluster string, port int64) (req *http.Request) {
	id := getConsulId(app, role, host, port)
	url := addr + RegisterPath
	cInfo := &consulRegisterEntry{
		Name:    app,
		ID:      id,
		Address: host,
		Port:    port,
		Tags: []string{
			"app=" + app,
			"role=" + role,
			"cluster=" + cluster,
		},
	}
	cInfoBytes, err := json.Marshal(cInfo)
	if err != nil {
		log.LogErrorf("marshal error, %v", err.Error())
		return nil
	}
	req, err = http.NewRequest(http.MethodPut, url, bytes.NewBuffer(cInfoBytes))
	if err != nil {
		log.LogErrorf("new request error, %v", err.Error())
		return nil
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Close = true

	return
}

// GetLocalIpAddr returns the local IP address.
func getLocalIpAddr() (ipaddr string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.LogError("consul register get local ip failed, ", err)
		return
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("cannot get local ip")
}
