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
