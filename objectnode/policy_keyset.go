// Copyright 2023 The CubeFS Authors.
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
	"encoding/json"
	"fmt"
	"strings"
)

// Key - conditional key which is used to fetch values for any condition.
// Refer https://docs.aws.amazon.com/IAM/latest/UserGuide/list_s3.html
// for more information about available condition keys.
type Key string

const (
	// AWSReferer - key representing Referer header of any API.
	AWSReferer Key = "aws:Referer"

	// AWSSourceIP - key representing client's IP address (not intermittent proxies) of any API.
	AWSSourceIP Key = "aws:SourceIp"

	//AWSHost - key representing client's request host of any API, this is not standard AWS key
	AWSHost Key = "aws:Host"
)

var AllSupportedKeys = []Key{
	AWSReferer,
	AWSSourceIP,
	AWSHost,
	// Add new supported condition keys.
}

func (key Key) IsValid() bool {
	for _, supKey := range AllSupportedKeys {
		if supKey == key {
			return true
		}
	}

	return false
}

//  encodes Key to JSON data.
func (key Key) MarshalJSON() ([]byte, error) {
	if !key.IsValid() {
		return nil, fmt.Errorf("unknown key: %v", key)
	}

	return json.Marshal(string(key))
}

//returns key operator which is stripped value of prefixes "aws:" and "s3:"
func (key Key) Name() string {
	keyString := string(key)

	if strings.HasPrefix(keyString, "aws:") {
		return strings.TrimPrefix(keyString, "aws:")
	}

	return strings.TrimPrefix(keyString, "s3:")
}

//  decodes string data to Key.
func (key *Key) UnmarshalText(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parsedKey, err := parseKey(s)
	if err != nil {
		return err
	}

	*key = parsedKey
	return nil
}

//  decodes JSON data to Key.
func (key *Key) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parsedKey, err := parseKey(s)
	if err != nil {
		return err
	}
	*key = parsedKey
	return nil
}

func parseKey(s string) (Key, error) {
	key := Key(s)

	if key.IsValid() {
		return key, nil
	}

	return key, fmt.Errorf(invalidConditionKey, s)
}

type KeySet map[Key]struct{}

func (set KeySet) Add(key Key) {
	set[key] = struct{}{}
}

func (set KeySet) AddAll(keys KeySet) {
	for key := range keys {
		set[key] = struct{}{}
	}

}

// returns a key set contains difference of two key set.
// Example:
//     keySet1 := ["one", "two", "three"]
//     keySet2 := ["two", "four", "three"]
//     keySet1.Difference(keySet2) == ["one"]
func (set KeySet) Difference(sset KeySet) KeySet {
	nset := make(KeySet)

	for k := range set {
		if _, ok := sset[k]; !ok {
			nset.Add(k)
		}
	}

	return nset
}

// returns whether key set is empty or not.
func (set KeySet) IsEmpty() bool {
	return len(set) == 0
}

func (set KeySet) String() string {
	return fmt.Sprintf("%v", set.ToSlice())
}

// ToSlice - returns slice of keys.
func (set KeySet) ToSlice() []Key {
	keys := []Key{}

	for key := range set {
		keys = append(keys, key)
	}

	return keys
}

// returns new KeySet contains given keys.
func NewKeySet(keys ...Key) KeySet {
	set := make(KeySet)
	for _, key := range keys {
		set.Add(key)
	}

	return set
}
