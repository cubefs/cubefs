package objectnode

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCondition_UnmarshalJSON(t *testing.T) {

	op1, err := newIPAddressOp(map[Key]ValueSet{AWSSourceIP: NewValueSet(NewStringValue("1.1.1.1"))})
	require.NoError(t, err)
	var expectCondition1 Condition = []Operation{op1}
	op2, err := newStringLikeOp(map[Key]ValueSet{AWSReferer: NewValueSet(NewStringValue("*.abc.com"))})
	require.NoError(t, err)
	var expectCondition2 Condition = []Operation{op2}
	op3, err := newStringLikeOp(map[Key]ValueSet{AWSHost: NewValueSet(NewStringValue("*.cba.com"))})
	require.NoError(t, err)
	var expectCondition3 Condition = []Operation{op3}
	testCases := []struct {
		Value     string
		expect    Condition
		expectErr error
	}{
		{`{"IpAddress": {"aws:SourceIp": ["1.1.1.1"]}}`, expectCondition1, nil},
		{`{"StringLike": {"aws:Referer": ["*.abc.com"]}}`, expectCondition2, nil},
		{`{"StringLike": {"aws:Host": ["*.cba.com"]}}`, expectCondition3, nil},
		{`[]`, nil, errors.New(" cannot unmarshal array")},
		{`{}`, nil, errors.New("condition must not be empty")},
		{`{"stringlike": {"aws:Host": ["*.cba.com"]}}`, nil, errors.New("invalid condition operator")},
		{`{"StringLike": {"aws:host": ["*.cba.com"]}}`, nil, errors.New("invalid condition key")},
		{`{"StringLike": {"aws:Host": [123]}}`, nil, errors.New("value must be a string")},
	}

	for i, testCase := range testCases {
		var result Condition
		resultErr := json.Unmarshal([]byte(testCase.Value), &result)
		if testCase.expectErr != nil {
			require.Error(t, resultErr, fmt.Sprintf("test case %v", i+1))
			require.Contains(t, resultErr.Error(), testCase.expectErr.Error(), fmt.Sprintf("test case %v", i+1))
		} else {
			t.Log(result)
			require.NoError(t, resultErr, fmt.Sprintf("test case %v", i+1))
			require.Equal(t, testCase.expect, result, fmt.Sprintf("test case %v", i+1))
		}
	}

}

func TestCondition_CheckValid(t *testing.T) {
	op1, err := newIPAddressOp(map[Key]ValueSet{AWSSourceIP: NewValueSet(NewStringValue("1.1.1.1"))})
	require.NoError(t, err)
	var Condition1 Condition = []Operation{op1}
	op2, err := newStringLikeOp(map[Key]ValueSet{AWSReferer: NewValueSet(NewStringValue("*.abc.com"))})
	require.NoError(t, err)
	var Condition2 Condition = []Operation{op2}
	op3, err := newStringLikeOp(map[Key]ValueSet{AWSHost: NewValueSet(NewStringValue("*.cba.com"))})
	require.NoError(t, err)
	var Condition3 Condition = []Operation{op3}
	op4, err := newStringLikeOp(map[Key]ValueSet{"host": NewValueSet(NewStringValue("*.cba.com"))})
	require.NoError(t, err)
	var Condition4 Condition = []Operation{op4}

	testCases := []struct {
		Value       Condition
		expectedErr error
	}{
		{Condition1, nil},
		{Condition2, nil},
		{Condition3, nil},
		{Condition4, errors.New("policy has invalid condition key")},
	}

	for i, testCase := range testCases {

		resultErr := testCase.Value.CheckValid()
		if testCase.expectedErr != nil {
			require.Error(t, resultErr, fmt.Sprintf("test case %v", i+1))
			require.Contains(t, resultErr.Error(), testCase.expectedErr.Error(), fmt.Sprintf("test case %v", i+1))
		} else {
			require.NoError(t, resultErr, fmt.Sprintf("test case %v", i+1))
		}
	}
}

func TestCondition_MarshalJSON(t *testing.T) {
	op1, err := newIPAddressOp(map[Key]ValueSet{AWSSourceIP: NewValueSet(NewStringValue("1.1.1.1"))})
	require.NoError(t, err)
	var Condition1 Condition = []Operation{op1}
	op2, err := newStringLikeOp(map[Key]ValueSet{AWSReferer: NewValueSet(NewStringValue("*.abc.com"))})
	require.NoError(t, err)
	var Condition2 Condition = []Operation{op2}
	op3, err := newStringLikeOp(map[Key]ValueSet{AWSHost: NewValueSet(NewStringValue("*.cba.com"))})
	require.NoError(t, err)
	var Condition3 Condition = []Operation{op3, op2}

	op4, err := newNotIPAddressOp(map[Key]ValueSet{AWSSourceIP: NewValueSet(NewStringValue("1.1.1.1"))})
	require.NoError(t, err)
	var Condition4 Condition = []Operation{op4}
	op5, err := newStringNotLikeOp(map[Key]ValueSet{AWSReferer: NewValueSet(NewStringValue("*.abc.com"))})
	require.NoError(t, err)
	var Condition5 Condition = []Operation{op5}
	op6, err := newStringNotLikeOp(map[Key]ValueSet{AWSHost: NewValueSet(NewStringValue("*.cba.com"))})
	require.NoError(t, err)
	var Condition6 Condition = []Operation{op6}
	testCases := []struct {
		Expect    string
		Value     Condition
		expectErr error
	}{
		{`{"IpAddress":{"aws:SourceIp":["1.1.1.1/32"]}}`, Condition1, nil},
		{`{"StringLike":{"aws:Referer":["*.abc.com"]}}`, Condition2, nil},
		{`{"StringLike":{"aws:Host":["*.cba.com"],"aws:Referer":["*.abc.com"]}}`, Condition3, nil},
		{`{"NotIpAddress":{"aws:SourceIp":["1.1.1.1/32"]}}`, Condition4, nil},
		{`{"StringNotLike":{"aws:Referer":["*.abc.com"]}}`, Condition5, nil},
		{`{"StringNotLike":{"aws:Host":["*.cba.com"]}}`, Condition6, nil},
	}

	for i, testCase := range testCases {

		b, resultErr := json.Marshal(testCase.Value)
		if testCase.expectErr != nil {
			require.Error(t, resultErr, fmt.Sprintf("test case %v", i+1))
			require.Contains(t, resultErr.Error(), testCase.expectErr.Error(), fmt.Sprintf("test case %v", i+1))
		} else {
			require.NoError(t, resultErr, fmt.Sprintf("test case %v", i+1))
			require.Equal(t, testCase.Expect, string(b), fmt.Sprintf("test case %v", i+1))
		}
	}
}

func TestCondition_Evaluate(t *testing.T) {
	op1, err := newIPAddressOp(map[Key]ValueSet{AWSSourceIP: NewValueSet(NewStringValue("1.1.1.1"))})
	require.NoError(t, err)
	var Condition1 Condition = []Operation{op1}
	op2, err := newStringLikeOp(map[Key]ValueSet{AWSReferer: NewValueSet(NewStringValue("*.abc.com"))})
	require.NoError(t, err)
	var Condition2 Condition = []Operation{op2}
	op3, err := newStringLikeOp(map[Key]ValueSet{AWSHost: NewValueSet(NewStringValue("*.cba.com"))})
	require.NoError(t, err)
	var Condition3 Condition = []Operation{op3, op2}

	op4, err := newNotIPAddressOp(map[Key]ValueSet{AWSSourceIP: NewValueSet(NewStringValue("1.1.1.1"))})
	require.NoError(t, err)
	var Condition4 Condition = []Operation{op4}
	op5, err := newStringNotLikeOp(map[Key]ValueSet{AWSReferer: NewValueSet(NewStringValue("*.abc.com"))})
	require.NoError(t, err)
	var Condition5 Condition = []Operation{op5}
	op6, err := newStringNotLikeOp(map[Key]ValueSet{AWSHost: NewValueSet(NewStringValue("*.cba.com"))})
	require.NoError(t, err)
	var Condition6 Condition = []Operation{op6}

	testCases := []struct {
		Values map[string]string
		Cond   Condition
		expect bool
	}{
		{map[string]string{"SourceIp": "1.1.1.1"}, Condition1, true},
		{map[string]string{"Referer": "www.abc.com"}, Condition2, true},
		{map[string]string{"Host": "www.cba.com", "Referer": "www.abc.com"}, Condition3, true},

		{map[string]string{"SourceIp": "1.1.1.2"}, Condition1, false},
		{map[string]string{"Referer": "www.abcd.com"}, Condition2, false},
		{map[string]string{"Host": "www.dcba.com", "Referer": "www.abc.com"}, Condition3, false},
		{map[string]string{"Host": "www.cba.com", "Referer": "www.abcd.com"}, Condition3, false},

		{map[string]string{"SourceIp": "1.1.1.1"}, Condition4, false},
		{map[string]string{"Referer": "www.abc.com"}, Condition5, false},
		{map[string]string{"Host": "www.cba.com"}, Condition6, false},

		{map[string]string{"SourceIp": "1.1.1.2"}, Condition4, true},
		{map[string]string{"Referer": "www.abcd.com"}, Condition5, true},
		{map[string]string{"Host": "www.dcba.com"}, Condition6, true},
	}

	for i, testCase := range testCases {

		b := testCase.Cond.Evaluate(testCase.Values)
		require.Equal(t, testCase.expect, b, fmt.Sprintf("test case %v", i+1))

	}
}

func TestNameIsValid(t *testing.T) {
	testCases := []struct {
		operator       operator
		expectedResult bool
	}{
		{stringLike, true},
		{stringNotLike, true},
		{ipAddress, true},
		{notIPAddress, true},
	}

	for i, testCase := range testCases {
		result := testCase.operator.IsValid()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNameMarshalJSON(t *testing.T) {
	testCases := []struct {
		operator       operator
		expectedResult []byte
		expectErr      bool
	}{
		{stringLike, []byte(`"StringLike"`), false},
		{stringNotLike, []byte(`"StringNotLike"`), false},
		{ipAddress, []byte(`"IpAddress"`), false},
		{notIPAddress, []byte(`"NotIpAddress"`), false},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.operator)
		expectErr := err != nil

		if testCase.expectErr != expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func TestNameUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult operator
		expectErr      bool
	}{
		{[]byte(`"StringLike"`), stringLike, false},
		{[]byte(`"foo"`), operator(""), true},
	}

	for i, testCase := range testCases {
		var result operator
		err := json.Unmarshal(testCase.data, &result)
		expectErr := (err != nil)

		if testCase.expectErr != expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if testCase.expectedResult != result {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}
