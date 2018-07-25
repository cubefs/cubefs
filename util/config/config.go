//json cfg file utility
package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

type Config struct {
	data map[string]interface{}
	Raw  []byte
}

func newConfig() *Config {
	result := new(Config)
	result.data = make(map[string]interface{})
	return result
}

// Loads config information from a JSON file
func LoadConfigFile(filename string) *Config {
	result := newConfig()
	err := result.parse(filename)
	if err != nil {
		log.Fatalf("error loading config file %s: %s", filename, err)
	}
	return result
}

// Loads config information from a JSON string
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

// Returns a string for the config variable key
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

// Returns a float for the config variable key
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

// Returns a bool for the config variable key
func (c *Config) GetBool(key string) bool {
	x, present := c.data[key]
	if !present {
		return false
	}
	if result, isBool := x.(bool); isBool {
		return result
	}
	return false
}

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

// Returns an array for the config variable key
func (c *Config) GetArray(key string) []interface{} {
	result, present := c.data[key]
	if !present {
		return []interface{}(nil)
	}
	return result.([]interface{})
}
