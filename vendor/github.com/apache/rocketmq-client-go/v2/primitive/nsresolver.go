/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package primitive

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path"
	"strings"
	"time"

	"github.com/apache/rocketmq-client-go/v2/rlog"
)

// resolver for nameserver, monitor change of nameserver and notify client
// consul or domain is common
type NsResolver interface {
	Resolve() []string
	Description() string
}

type StaticResolver struct {
}

var _ NsResolver = (*EnvResolver)(nil)

func NewEnvResolver() *EnvResolver {
	return &EnvResolver{}
}

type EnvResolver struct {
}

func (e *EnvResolver) Resolve() []string {
	if v := os.Getenv("NAMESRV_ADDR"); v != "" {
		return strings.Split(v, ";")
	}
	return nil
}

func (e *EnvResolver) Description() string {
	return "env resolver of var NAMESRV_ADDR"
}

type passthroughResolver struct {
	addr     []string
	failback NsResolver
}

func NewPassthroughResolver(addr []string) *passthroughResolver {
	return &passthroughResolver{
		addr:     addr,
		failback: NewEnvResolver(),
	}
}

func (p *passthroughResolver) Resolve() []string {
	if p.addr != nil {
		return p.addr
	}
	return p.failback.Resolve()
}

func (p *passthroughResolver) Description() string {
	return fmt.Sprintf("passthrough resolver of %v", p.addr)
}

const (
	DEFAULT_NAMESRV_ADDR = "http://jmenv.tbsite.net:8080/rocketmq/nsaddr"
)

var _ NsResolver = (*HttpResolver)(nil)

type HttpResolver struct {
	domain   string
	instance string
	cli      http.Client
	failback NsResolver
}

func NewHttpResolver(instance string, domain ...string) *HttpResolver {
	d := DEFAULT_NAMESRV_ADDR
	if len(domain) > 0 {
		d = domain[0]
	}
	client := http.Client{Timeout: 10 * time.Second}

	h := &HttpResolver{
		domain:   d,
		instance: instance,
		cli:      client,
		failback: NewEnvResolver(),
	}
	return h
}

func (h *HttpResolver) Resolve() []string {
	addrs := h.get()
	if len(addrs) > 0 {
		return addrs
	}

	addrs = h.loadSnapshot()
	if len(addrs) > 0 {
		return addrs
	}
	return h.failback.Resolve()
}

func (h *HttpResolver) Description() string {
	return fmt.Sprintf("passthrough resolver of domain:%v instance:%v", h.domain, h.instance)
}

func (h *HttpResolver) get() []string {
	resp, err := h.cli.Get(h.domain)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		data := map[string]interface{}{
			"NameServerDomain": h.domain,
			"err":              err,
		}
		if resp != nil {
			data["StatusCode"] = resp.StatusCode
		}
		rlog.Error("name server http fetch failed", data)
		return nil
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		rlog.Error("name server read http response failed", map[string]interface{}{
			"NameServerDomain": h.domain,
			"err":              err,
		})
		return nil
	}

	bodyStr := string(body)
	if bodyStr == "" {
		return nil
	}

	h.saveSnapshot(body)

	return strings.Split(string(body), ";")
}

func (h *HttpResolver) saveSnapshot(body []byte) error {
	filePath := h.getSnapshotFilePath(h.instance)
	err := ioutil.WriteFile(filePath, body, 0644)
	if err != nil {
		rlog.Error("name server snapshot save failed", map[string]interface{}{
			"filePath": filePath,
			"err":      err,
		})
		return err
	}

	rlog.Info("name server snapshot save successfully", map[string]interface{}{
		"filePath": filePath,
	})
	return nil
}

func (h *HttpResolver) loadSnapshot() []string {
	filePath := h.getSnapshotFilePath(h.instance)
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		rlog.Warning("name server snapshot local file not exists", map[string]interface{}{
			"filePath": filePath,
		})
		return nil
	}

	bs, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil
	}

	rlog.Info("load the name server snapshot local file", map[string]interface{}{
		"filePath": filePath,
	})
	return strings.Split(string(bs), ";")
}

func (h *HttpResolver) getSnapshotFilePath(instanceName string) string {
	homeDir := ""
	if usr, err := user.Current(); err == nil {
		homeDir = usr.HomeDir
	} else {
		rlog.Error("name server domain, can't get user home directory", map[string]interface{}{
			"err": err,
		})
	}
	storePath := path.Join(homeDir, "/logs/rocketmq-go/snapshot")
	if _, err := os.Stat(storePath); os.IsNotExist(err) {
		if err = os.MkdirAll(storePath, 0755); err != nil {
			rlog.Fatal("can't create name server snapshot directory", map[string]interface{}{
				"path": storePath,
				"err":  err,
			})
		}
	}
	filePath := path.Join(storePath, fmt.Sprintf("nameserver_addr-%s", instanceName))
	return filePath
}
