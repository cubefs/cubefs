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

import "testing"

func TestIPAddressOpEvalute(t *testing.T) {
	case1Operation, err := newIPAddressOp(map[Key]ValueSet{AWSSourceIP: NewValueSet(NewStringValue("192.168.1.0/24"))})
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	testCases := []struct {
		operation      Operation
		values         map[string]string
		expectedResult bool
	}{
		{case1Operation, map[string]string{"SourceIp": "192.168.1.10"}, true},
		{case1Operation, map[string]string{"SourceIp": "192.168.2.10"}, false},
	}

	for i, testCase := range testCases {
		result := testCase.operation.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestIPAddressOpBareIPv6IsSingleHost(t *testing.T) {
	// A bare IPv6 address must restrict to that single host, not be widened
	// to a /32 block.
	op, err := newIPAddressOp(map[Key]ValueSet{AWSSourceIP: NewValueSet(NewStringValue("2001:db8::1"))})
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	testCases := []struct {
		values         map[string]string
		expectedResult bool
	}{
		{map[string]string{"SourceIp": "2001:db8::1"}, true},
		{map[string]string{"SourceIp": "2001:db8:ffff:ffff::9999"}, false},
	}
	for i, testCase := range testCases {
		result := op.evaluate(testCase.values)
		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestIPAddressOpBareIPv4IsSingleHost(t *testing.T) {
	op, err := newIPAddressOp(map[Key]ValueSet{AWSSourceIP: NewValueSet(NewStringValue("192.168.1.1"))})
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	if op.evaluate(map[string]string{"SourceIp": "192.168.1.1"}) != true {
		t.Fatalf("expected 192.168.1.1 to match single-host 192.168.1.1\n")
	}
	if op.evaluate(map[string]string{"SourceIp": "192.168.1.2"}) != false {
		t.Fatalf("expected 192.168.1.2 not to match single-host 192.168.1.1\n")
	}
}

func TestIPAddressOpKeys(t *testing.T) {
	case1Operation, err := newIPAddressOp(map[Key]ValueSet{AWSSourceIP: NewValueSet(NewStringValue("192.168.1.0/24"))})
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	result := case1Operation.keys()
	expect := NewKeySet(AWSSourceIP)
	if len(result.Difference(expect)) != 0 || len(expect.Difference(result)) != 0 {
		t.Fatalf("expect two value set equal, expected: %v, got: %v\n", expect, result)
	}
}
