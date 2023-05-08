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

package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
)

const (
	DefaultConstConfigFile = "constcfg"
	ClusterVersionFile     = "CLUSTER-VERSION"
	ClusterUUID            = "ClusterUUID"
)

// Config defines the struct of a configuration in general.
type Config struct {
	data map[string]interface{}
	Raw  []byte
}

func newConfig() *Config {
	result := new(Config)
	result.data = make(map[string]interface{})
	return result
}

// LoadConfigFile loads config information from a JSON file.
func LoadConfigFile(filename string) (*Config, error) {
	result := newConfig()
	err := result.parse(filename)
	if err != nil {
		log.Printf("error loading config file %s: %s", filename, err)
	}
	return result, err
}

// LoadConfigString loads config information from a JSON string.
func LoadConfigString(s string) *Config {
	result := newConfig()
	err := json.Unmarshal([]byte(s), &result.data)
	if err != nil {
		log.Fatalf("error parsing config string %s: %s", s, err)
	}
	return result
}

func (c *Config) parse(fileName string) error {
	jsonFileBytes, err := ioutil.ReadFile(fileName)
	c.Raw = jsonFileBytes
	if err == nil {
		err = json.Unmarshal(jsonFileBytes, &c.data)
	}
	return err
}

// GetString returns a string for the config key.
func (c *Config) GetString(key string) string {
	x, present := c.data[key]
	if !present {
		return ""
	}
	if result, isString := x.(string); isString {
		return result
	}
	return ""
}

// GetString returns a string for the config key.
func (c *Config) SetString(key, val string) {
	c.data[key] = val
}

// GetFloat returns a float value for the config key.
func (c *Config) GetFloat(key string) float64 {
	x, present := c.data[key]
	if !present {
		return -1
	}
	if result, isFloat := x.(float64); isFloat {
		return result
	}
	return 0
}

// returns a bool value for the config key with default val when not present
func (c *Config) GetBoolWithDefault(key string, defval bool) bool {
	_, present := c.data[key]
	if !present {
		return defval
	}
	return c.GetBool(key)
}

// GetBool returns a bool value for the config key.
func (c *Config) GetBool(key string) bool {
	x, present := c.data[key]
	if !present {
		return false
	}
	if result, isBool := x.(bool); isBool {
		return result
	}
	if result, isString := x.(string); isString {
		if result == "true" {
			return true
		}
	}
	return false
}

// GetBool returns a int value for the config key.
func (c *Config) GetInt(key string) int {
	x, present := c.data[key]
	if !present {
		return 0
	}
	if result, isInt := x.(int); isInt {
		return result
	}
	return 0
}

// GetBool returns a int64 value for the config key.
func (c *Config) GetInt64(key string) int64 {
	x, present := c.data[key]
	if !present {
		return 0
	}
	if result, isInt := x.(int64); isInt {
		return result
	}
	if result, isFloat := x.(float64); isFloat {
		return int64(result)
	}
	if result, isString := x.(string); isString {
		r, err := strconv.ParseInt(result, 10, 64)
		if err == nil {
			return r
		}
	}
	return 0
}

// GetBool returns a int64 value for the config key.
func (c *Config) GetInt64WithDefault(key string, defaultVal int64) int64 {
	if val := c.GetInt64(key); val == 0 {
		return defaultVal
	} else {
		return val
	}
}

// GetSlice returns an array for the config key.
func (c *Config) GetSlice(key string) []interface{} {
	result, present := c.data[key]
	if !present {
		return []interface{}(nil)
	}
	return result.([]interface{})
}

func (c *Config) GetStringSlice(key string) []string {
	s := c.GetSlice(key)
	result := make([]string, 0, len(s))
	for _, item := range s {
		result = append(result, item.(string))
	}
	return result
}

// Check and get a string for the config key.
func (c *Config) CheckAndGetString(key string) (string, bool) {
	x, present := c.data[key]
	if !present {
		return "", false
	}
	if result, isString := x.(string); isString {
		return result, true
	}
	return "", false
}

// GetBool returns a bool value for the config key.
func (c *Config) CheckAndGetBool(key string) (bool, bool) {
	x, present := c.data[key]
	if !present {
		return false, false
	}
	if result, isBool := x.(bool); isBool {
		return result, true
	}
	// Take string value "true" and "false" as well.
	if result, isString := x.(string); isString {
		if result == "true" {
			return true, true
		}
		if result == "false" {
			return false, true
		}
	}
	return false, false
}

func NewIllegalConfigError(configKey string) error {
	return fmt.Errorf("illegal config %s", configKey)
}

type ConstConfig struct {
	Listen           string `json:"listen"`
	RaftReplicaPort  string `json:"raftReplicaPort"`
	RaftHeartbetPort string `json:"raftHeartbetPort"`
}

func (ccfg *ConstConfig) Equals(cfg *ConstConfig) bool {
	return (ccfg.Listen == cfg.Listen &&
		ccfg.RaftHeartbetPort == cfg.RaftHeartbetPort &&
		ccfg.RaftReplicaPort == cfg.RaftReplicaPort)
}

// check listen port, raft replica port and raft heartbeat port
func CheckOrStoreConstCfg(fileDir, fileName string, cfg *ConstConfig) (ok bool, err error) {
	var filePath = path.Join(fileDir, fileName)
	var buf []byte
	buf, err = ioutil.ReadFile(filePath)
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("read config file %v failed: %v", filePath, err)
	}
	if os.IsNotExist(err) || len(buf) == 0 {
		// Persist configuration to disk
		if buf, err = json.Marshal(cfg); err != nil {
			return false, fmt.Errorf("marshal const config failed: %v", err)
		}
		if err = os.MkdirAll(fileDir, 0755); err != nil {
			return false, fmt.Errorf("make directory %v filed: %v", fileDir, err)
		}
		var file *os.File
		if file, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0755); err != nil {
			return false, fmt.Errorf("create config file %v failed: %v", filePath, err)
		}
		defer func() {
			_ = file.Close()
			if err != nil {
				_ = os.Remove(filePath)
			}
		}()
		if _, err = file.Write(buf); err != nil {
			return false, fmt.Errorf("write config file %v failed: %v", filePath, err)
		}
		if err = file.Sync(); err != nil {
			return false, fmt.Errorf("sync config file %v failed: %v", filePath, err)
		}
		return true, nil
	}
	// Load and check stored const configuration
	storedConstCfg := new(ConstConfig)
	if err = json.Unmarshal(buf, storedConstCfg); err != nil {
		return false, fmt.Errorf("unmarshal const config %v failed: %v", filePath, err)
	}
	if ok := storedConstCfg.Equals(cfg); !ok {
		return false, fmt.Errorf("compare const config %v and %v failed: %v", storedConstCfg, cfg, err)
	}
	return true, nil
}

func CheckOrStoreClusterUuid(dirPath, id string, force bool) (err error) {
	dir, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("read dir %v failed: %v", dirPath, err.Error())
	}
	versionFile := path.Join(dirPath, ClusterVersionFile)
	if len(dir) == 0 || force {
		// store clusterUUID
		ClusterMap := map[string]interface{}{"ClusterUUID": id}
		data, err := json.Marshal(ClusterMap)
		if err = ioutil.WriteFile(versionFile, data, 0755); err != nil {
			return fmt.Errorf("write file %v failed: %v", versionFile, err.Error())
		}
	} else {
		// check clusterUUID
		cfg, err := LoadConfigFile(versionFile)
		if err != nil {
			return fmt.Errorf("read file %v failed: %v\n", versionFile, err.Error())
		}
		clusterUuId := cfg.GetString(ClusterUUID)
		if clusterUuId != id {
			return fmt.Errorf("file %v ClusterUUID %v not equal to %v\n",
				versionFile, clusterUuId, id)
		}
	}
	return
}
