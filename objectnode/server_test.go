// Copyright 2018 The ChubaoFS Authors.
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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
)

func TestObjectNode_Lifecycle(t *testing.T) {
	var err error
	cfgStr := `
{
	"listen": ":10004",
	"logDir": "/tmp/Logs/chubaofs",
	"module": "object",
	"masterAddr": [
    	"192.168.0.11:17010",
    	"192.168.0.12:17010",
    	"192.168.0.13:17010"
  	]
}
`
	// test log
	cfg := config.LoadConfigString(cfgStr)
	if _, err := log.InitLog(cfg.GetString("logDir"), "node", log.DebugLevel, nil); err != nil {
		fmt.Println("Fatal: failed to init log - ", err)
		os.Exit(1)
		return
	}

	node := NewServer()
	if err = node.Start(cfg); err != nil {
		t.Fatalf("start node server fail cause: %v", err)
	}
	go func() {
		time.Sleep(3 * time.Second)
		node.Shutdown()
	}()
	node.Sync()
}
