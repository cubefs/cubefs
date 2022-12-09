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
	"strconv"
)

type MQBrokerErr struct {
	ResponseCode int16
	ErrorMessage string
}

func (e MQBrokerErr) Error() string {
	return "CODE: " + strconv.Itoa(int(e.ResponseCode)) + "  DESC: " + e.ErrorMessage
}

func NewRemotingErr(s string) error {
	return &RemotingErr{s: s}
}

type RemotingErr struct {
	s string
}

func (e *RemotingErr) Error() string {
	return e.s
}

func NewMQClientErr(code int16, msg string) error {
	return &MQClientErr{code: code, msg: msg}
}

type MQClientErr struct {
	code int16
	msg  string
}

func (e MQClientErr) Error() string {
	return "CODE: " + strconv.Itoa(int(e.code)) + "  DESC: " + e.msg
}

func IsRemotingErr(err error) bool {
	_, ok := err.(*RemotingErr)
	return ok
}
