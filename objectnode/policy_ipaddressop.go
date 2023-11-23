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
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
)

// IP address operation. It checks whether value by Key in given
// values is in IP network.  Here Key must be AWSSourceIP.
type ipAddressOp struct {
	m map[Key][]*IPInfo
}
type IPInfo struct {
	IP  net.IP
	Net *net.IPNet
}

// evaluates to check whether IP address in values map for AWSSourceIP
// falls in one of network or not.
func (op ipAddressOp) evaluate(values map[string]string) bool {
	for k, v := range op.m {

		requestValue, ok := values[http.CanonicalHeaderKey(k.Name())]
		if !ok {
			requestValue = values[k.Name()]
		}
		IP := net.ParseIP(requestValue)
		if IP == nil {
			panic(fmt.Errorf("invalid IP address '%v'", requestValue))
		}
		nothingMatched := true //all values not matched
		for _, IPInfo := range v {
			if IPInfo.Net.Contains(IP) {
				nothingMatched = false
			}
		}
		if nothingMatched {
			return false
		}
	}
	return true
}

// returns condition key which is used by this condition operation.
// Key is always AWSSourceIP.
func (op ipAddressOp) keys() KeySet {

	keys := make(KeySet)
	for key := range op.m {
		keys.Add(key)
	}
	return keys
}

// operator() - returns "IpAddress" condition operator.
func (op ipAddressOp) operator() operator {
	return ipAddress
}

// toMap - returns map representation of this operation.
func (op ipAddressOp) toMap() map[Key]ValueSet {
	resultMap := make(map[Key]ValueSet)
	for k, v := range op.m {
		if !k.IsValid() {
			return nil
		}

		values := NewValueSet()
		for _, value := range v {
			leadingOne, _ := value.Net.Mask.Size()
			ip := value.IP.String() + "/" + strconv.Itoa(leadingOne)
			values.Add(NewStringValue(ip))
		}
		resultMap[k] = values
	}
	return resultMap
}

// Not IP address operation. It checks whether value by Key in given
// values is NOT in IP network.  Here Key must be AWSSourceIP.

type notIPAddressOp struct {
	ipAddressOp
}

// evaluates to check whether IP address in values map for AWSSourceIP
// does not fall in one of network.
func (op notIPAddressOp) evaluate(values map[string]string) bool {
	return !op.ipAddressOp.evaluate(values)
}

// returns "NotIpAddress" condition operator.
func (op notIPAddressOp) operator() operator {
	return notIPAddress
}

func valuesToIPNets(op operator, values ValueSet) ([]*IPInfo, error) {
	var IPInfos []*IPInfo
	for v := range values {
		s, err := v.GetString()
		if err != nil {
			return nil, fmt.Errorf(invalidCIDR, v, op)
		}

		if strings.Index(s, "/") < 0 {
			s += "/32"
		}
		IP, IPNet, err := net.ParseCIDR(s)
		if err != nil {
			return nil, fmt.Errorf(invalidCIDR, s, op)
		}
		IPInfo := &IPInfo{IP, IPNet}
		IPInfos = append(IPInfos, IPInfo)
	}

	return IPInfos, nil
}

// returns new IP address operation.
func newIPAddressOp(m map[Key]ValueSet) (Operation, error) {
	newMap := make(map[Key][]*IPInfo)
	for k, v := range m {
		IPNets, err := valuesToIPNets(ipAddress, v)
		if err != nil {
			return nil, err
		}
		newMap[k] = IPNets
	}

	return NewIPAddressOp(newMap)
}

//returns new IP address operation.
func NewIPAddressOp(m map[Key][]*IPInfo) (Operation, error) {
	for key := range m {
		if key != AWSSourceIP {
			return nil, fmt.Errorf(invalidIPKey, AWSSourceIP, ipAddress)
		}
	}

	return &ipAddressOp{m: m}, nil
}

//returns new Not IP address operation.
func newNotIPAddressOp(m map[Key]ValueSet) (Operation, error) {
	newMap := make(map[Key][]*IPInfo)
	for k, v := range m {
		IPNets, err := valuesToIPNets(notIPAddress, v)
		if err != nil {
			return nil, err
		}
		newMap[k] = IPNets
	}

	return NewNotIPAddressOp(newMap)
}

// returns new Not IP address operation.
func NewNotIPAddressOp(m map[Key][]*IPInfo) (Operation, error) {
	for key := range m {
		if key != AWSSourceIP {
			return nil, fmt.Errorf(invalidIPKey, AWSSourceIP, notIPAddress)
		}
	}

	return &notIPAddressOp{ipAddressOp{m: m}}, nil
}
