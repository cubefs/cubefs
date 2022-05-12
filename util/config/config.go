// Copyright 2018 The Chubao Authors.
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

	CfgTracingsamplerType  = "tracingSamplerType"  // type of tracing
	CfgTracingsamplerParam = "tracingSamplerParam" // radio of tracing by type
	CfgTracingReportAddr   = "tracingReportAddr"   // jeagert addr
)

const (
	ConfigKeyLocalIP     = "localIP"
	ConfigKeyRole        = "role"
	ConfigKeyLogDir      = "logDir"
	ConfigKeyLogLevel    = "logLevel"
	ConfigKeyProfPort    = "prof"
	ConfigKeyClusterAddr = "clusterAddr"

	// mysql config key
	ConfigKeyMysql       = "mysql"
	ConfigKeyMysqlUrl    = "url"
	ConfigKeyUserName    = "userName"
	ConfigKeyPassword    = "password"
	ConfigKeyDatabase    = "database"
	ConfigKeyPort        = "port"
	ConfigKeyIdleConn    = "maxIdleConns"
	ConfigKeyOpenConn    = "maxOpenConns"
	ConfigKeyPassFile    = "passFile"
	ConfigKeyEncryptPass = "database.password"

	// leader elect config
	ConfigKeyHeartbeat        = "heartbeat"        // int
	ConfigKeyLeaderPeriod     = "leaderPeriod"     // int
	ConfigKeyFollowerPeriod   = "followerPeriod"   // int
	ConfigKeyWorkerHeartbeat  = "workerHeartbeat"  // int
	ConfigKeyWorkerPeriod     = "workerPeriod"     // int
	ConfigKeyWorkerTaskPeriod = "workerTaskPeriod" // int

	// HBase config
	ConfigKeyHBaseUrl = "hBaseUrl" // int
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
func (c *Config) GetInt(key string) int64 {
	x, present := c.data[key]
	if !present {
		return 0
	}
	if result, isInt := x.(int64); isInt {
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

func (c *Config) GetJsonObjectBytes(key string) []byte {
	x, present := c.data[key]
	if !present {
		return nil
	}
	result, err := json.Marshal(x.(map[string]interface{}))
	if err != nil {
		return nil
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

func (c *Config) SetStringSlice(key string, value []string) bool {
	if _, ok := c.data[key]; ok {
		return false
	}
	c.data[key] = value
	return true
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
		if err = ioutil.WriteFile(filePath, buf, 0755); err != nil {
			_ = os.Remove(filePath)
			return false, fmt.Errorf("write config file %v failed: %v", filePath, err)
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

// GetMap returns a map for the config key.
func (c *Config) GetMap(key string) map[string]interface{} {
	x, present := c.data[key]
	if !present {
		return nil
	}
	if result, isString := x.(map[string]interface{}); isString {
		return result
	}
	return nil
}

type MysqlConfig struct {
	Url          string
	Username     string
	Password     string
	Database     string
	Port         int
	MaxIdleConns int
	MaxOpenConns int
}

func NewMysqlConfig(url, username, password, database string, port, ics, ocs int) *MysqlConfig {
	return &MysqlConfig{
		Url:          url,
		Username:     username,
		Password:     password,
		Database:     database,
		Port:         port,
		MaxIdleConns: ics,
		MaxOpenConns: ocs,
	}
}

type ElectConfig struct {
	Heartbeat      int
	LeaderPeriod   int
	FollowerPeriod int
}

func NewElectConfig(hb, lp, fp int) *ElectConfig {
	return &ElectConfig{
		Heartbeat:      hb,
		LeaderPeriod:   lp,
		FollowerPeriod: fp,
	}
}

// WorkerHeartbeat: worker node heartbeat time,
// WorkerPeriod: N 和心跳周期workerNode没有更新，则认为节点宕机
type WorkerConfig struct {
	WorkerHeartbeat  int
	WorkerPeriod     int
	TaskCreatePeriod int
}

func NewWorkerConfig(whb, wp, wtp int) *WorkerConfig {
	return &WorkerConfig{
		WorkerHeartbeat:  whb,
		WorkerPeriod:     wp,
		TaskCreatePeriod: wtp,
	}
}

type HBaseConfig struct {
	Host string
}

func NewHBaseConfig(hBaseHost string) *HBaseConfig {
	return &HBaseConfig{
		Host: hBaseHost,
	}
}
