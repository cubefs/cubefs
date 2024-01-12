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
