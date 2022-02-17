// Copyright 2022 The ChubaoFS Authors.
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

package main

import (
	"github.com/cubefs/cubefs/util/config"
	"testing"
)

func TestCheckConfMaster(t *testing.T) {
	t.Run("masters", func(t *testing.T) {
		var cfgJSON = `{"target":"/",
			"volumeName": "cold3",
			"masterAddr": "10.177.69.105:17010, 10.177.69.106:17010, 10.177.117.108:17010",
  			"logDir": "/home/ADC/80256477/adls/log/preload",
  			"logLevel": "debug",
			"ttl": "10",
			"action":"preload"}`
		cfg := config.LoadConfigString(cfgJSON)
		if checkConfig(cfg) == false {
			t.Fatalf("expected true, but got false")
		}
	})

	t.Run("masters_empty", func(t *testing.T) {
		var cfgJSON = `{"target":"/",
			"volumeName": "cold3",
  			"logDir": "/home/ADC/80256477/adls/log/preload",
  			"logLevel": "debug",
			"ttl": "10",
			"action":"preload"}`
		cfg := config.LoadConfigString(cfgJSON)
		if checkConfig(cfg) == true {
			t.Fatalf("expected false, but got true")
		}
	})
}

func TestCheckConfTarget(t *testing.T) {

	t.Run("target_empty", func(t *testing.T) {
		var cfgJSON = `{
			"volumeName": "cold3",
			"masterAddr": "10.177.69.105:17010, 10.177.69.106:17010, 10.177.117.108:17010",
  			"logDir": "/home/ADC/80256477/adls/log/preload",
  			"logLevel": "debug",
			"ttl": "10",
			"action":"preload"}`
		cfg := config.LoadConfigString(cfgJSON)
		if checkConfig(cfg) == true {
			t.Fatalf("expected false, but got true")
		}
	})
}

func TestCheckConfVol(t *testing.T) {
	t.Run("vol_empty", func(t *testing.T) {
		var cfgJSON = `{"target":"/",
			"masterAddr": "10.177.69.105:17010, 10.177.69.106:17010, 10.177.117.108:17010",
  			"logDir": "/home/ADC/80256477/adls/log/preload",
  			"logLevel": "debug",
			"ttl": "10",
			"action":"preload"}`
		cfg := config.LoadConfigString(cfgJSON)
		if checkConfig(cfg) == true {
			t.Fatalf("expected false, but got true")
		}
	})
}

func TestCheckConfLogDir(t *testing.T) {
	t.Run("logDir_empty", func(t *testing.T) {
		var cfgJSON = `{"target":"/",
			"volumeName": "cold3",
			"masterAddr": "10.177.69.105:17010, 10.177.69.106:17010, 10.177.117.108:17010",
  			"logLevel": "debug",
			"ttl": "10",
			"action":"preload"}`
		cfg := config.LoadConfigString(cfgJSON)
		if checkConfig(cfg) == true {
			t.Fatalf("expected false, but got true")
		}
	})
}

func TestCheckConfLogLevel(t *testing.T) {
	t.Run("logLevel_empty", func(t *testing.T) {
		var cfgJSON = `{"target":"/",
			"volumeName": "cold3",
			"masterAddr": "10.177.69.105:17010, 10.177.69.106:17010, 10.177.117.108:17010",
  			"logDir": "/home/ADC/80256477/adls/log/preload",
			"ttl": "10",
			"action":"preload"}`
		cfg := config.LoadConfigString(cfgJSON)
		if checkConfig(cfg) == true {
			t.Fatalf("expected false, but got true")
		}
	})
}

func TestCheckConfTTL(t *testing.T) {
	t.Run("ttl_empty", func(t *testing.T) {
		var cfgJSON = `{"target":"/",
			"volumeName": "cold3",
			"masterAddr": "10.177.69.105:17010, 10.177.69.106:17010, 10.177.117.108:17010",
  			"logDir": "/home/ADC/80256477/adls/log/preload",
  			"logLevel": "debug",
			"action":"preload"}`
		cfg := config.LoadConfigString(cfgJSON)
		if checkConfig(cfg) == true {
			t.Fatalf("expected false, but got true")
		}
	})
}

func TestCheckConfAction(t *testing.T) {
	t.Run("action_empty", func(t *testing.T) {
		var cfgJSON = `{"target":"/",
			"volumeName": "cold3",
			"masterAddr": "10.177.69.105:17010, 10.177.69.106:17010, 10.177.117.108:17010",
  			"logDir": "/home/ADC/80256477/adls/log/preload",
  			"logLevel": "debug",
			"ttl": "10"}`
		cfg := config.LoadConfigString(cfgJSON)
		if checkConfig(cfg) == true {
			t.Fatalf("expected false, but got true")
		}
	})
}
