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
