// Copyright 2018 The Cubefs Authors.
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

package cryptoutil

const (
	TicketVersion = 1
	TicketAge     = 24 * 60 * 60
)

// CryptoKey store the session key
type CryptoKey struct {
	Ctime int64  `json:"c_time"`
	Key   []byte `json:"key"`
}

/*
* MITM thread:
*      (1) talking to the right party (nonce, key encryption)
*      (2) replay attack (IP, timestamp constrains)
*
* Other thread: Client capability changes (ticket timestamp)
 */

// Ticket is a temperary struct to store permissions/caps for clients to
// access principle
type Ticket struct {
	Version    uint8     `json:"version"`
	ServiceID  string    `json:"service_id"`
	SessionKey CryptoKey `json:"session_key"`
	Exp        int64     `json:"exp"`
	IP         string    `json:"ip"`
	Caps       []byte    `json:"caps"`
}
