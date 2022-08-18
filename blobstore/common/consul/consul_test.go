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

package consul

import (
	"fmt"
	"net/http"
	"testing"

	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
)

func skipIfConsulNotRunning(t *testing.T, consulAddr string) {
	defConfig := api.DefaultConfig()
	defConfig.Address = consulAddr
	client, err := api.NewClient(defConfig)
	if err != nil {
		t.Skip("consul not running")
	}

	if err = checkConsulAgentRunning(client); err != nil {
		t.Skip(err)
	}
}

func TestServiceRegister(t *testing.T) {
	skipIfConsulNotRunning(t, "127.0.0.1:8500")
	ast := assert.New(t)
	{
		_, err := ServiceRegister("", nil)
		ast.Error(err)
	}

	{
		r, err := ServiceRegister("0.0.0.0:10220", nil)
		ast.NoError(err)
		services, _, err := r.Health().Service(r.ServiceName, "", false, nil)
		ast.NoError(err)
		ast.NotNil(services)
		ast.Equal(1, len(services))
		service := services[0]
		ast.Equal(r.ServiceName, service.Service.Service)
		ast.Equal(r.getId(), service.Service.ID)
		ast.Equal(r.ServiceIP, service.Service.Address)

		r.Close()
	}
}

func TestStartHealthCheckServ(t *testing.T) {
	ast := assert.New(t)
	_, port := StartHttpServerForHealthyCheck("", "/test")

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%v/test", port))
	ast.NoError(err)
	resp.Body.Close()
	ast.Equal(http.StatusOK, resp.StatusCode)
}
