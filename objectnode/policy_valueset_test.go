package objectnode

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestValueGetBool(t *testing.T) {
	testCases := []struct {
		value          Value
		expectedResult bool
		expectErr      bool
	}{
		{NewBoolValue(true), true, false},
		{NewIntValue(7), false, true},
		{Value{}, false, true},
	}

	for i, testCase := range testCases {
		result, err := testCase.value.GetBool()
		expectErr := err != nil

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestValueGetInt(t *testing.T) {
	testCases := []struct {
		value          Value
		expectedResult int
		expectErr      bool
	}{
		{NewIntValue(7), 7, false},
		{NewBoolValue(true), 0, true},
		{Value{}, 0, true},
	}

	for i, testCase := range testCases {
		result, err := testCase.value.GetInt()
		expectErr := err != nil

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestValueGetString(t *testing.T) {
	testCases := []struct {
		value          Value
		expectedResult string
		expectErr      bool
	}{
		{NewStringValue("foo"), "foo", false},
		{NewBoolValue(true), "", true},
		{Value{}, "", true},
	}

	for i, testCase := range testCases {
		result, err := testCase.value.GetString()
		expectErr := err != nil

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestValueGetType(t *testing.T) {
	testCases := []struct {
		value          Value
		expectedResult reflect.Kind
	}{
		{NewBoolValue(true), reflect.Bool},
		{NewIntValue(7), reflect.Int},
		{NewStringValue("foo"), reflect.String},
		{Value{}, reflect.Invalid},
	}

	for i, testCase := range testCases {
		result := testCase.value.GetType()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}
func TestValueSet_UnmarshalJSON(t *testing.T) {
	testCases := []struct {
		value          []byte
		expectedResult ValueSet
		expectErr      bool
	}{
		//test duplicate value
		{[]byte(`["www.example.com","foo","foo"]`), NewValueSet(NewStringValue("www.example.com"), NewStringValue("foo"), NewStringValue("foo")), true},
		//test empty value
		{[]byte(`[]`), NewValueSet(), true},
		{[]byte(`["www.example.com",["foo"]]`), NewValueSet(NewStringValue("www.example.com")), true},
		//test correct multiple value
		{[]byte(`["www.example.com","foo","bar"]`), NewValueSet(NewStringValue("www.example.com"), NewStringValue("foo"), NewStringValue("bar")), false},
		{[]byte(`["www.example.com"]`), NewValueSet(NewStringValue("www.example.com")), false},
		{[]byte(`"www.example.com"`), NewValueSet(NewStringValue("www.example.com")), false},
		{[]byte(`["www.example.com",1,true]`), NewValueSet(NewStringValue("www.example.com"), NewIntValue(1), NewBoolValue(true)), false},
	}
	for i, testCase := range testCases {
		var result ValueSet
		err := json.Unmarshal(testCase.value, &result)
		resultErr := (err != nil)
		if resultErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, resultErr)
		}
		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestValueSet_MarshalJSON(t *testing.T) {
	testCases := []struct {
		value          ValueSet
		expectedResult []byte
		expectErr      bool
	}{
		//test duplicate value
		{NewValueSet(NewStringValue("www.example.com"), NewStringValue("foo"), NewStringValue("foo")), []byte(`["www.example.com","foo"]`), false},
		//test empty value
		{NewValueSet(), []byte(`[]`), true},

		//test correct multiple value
		{NewValueSet(NewStringValue("www.example.com"), NewStringValue("foo"), NewStringValue("bar")), []byte(`["www.example.com","foo","bar"]`), false},
		{NewValueSet(NewStringValue("www.example.com")), []byte(`["www.example.com"]`), false},
		{NewValueSet(NewStringValue("www.example.com"), NewIntValue(1), NewBoolValue(true)), []byte(`["www.example.com",1,true]`), false},
	}
	for i, testCase := range testCases {
		var result []byte
		result, err := json.Marshal(testCase.value)
		resultErr := (err != nil)
		if resultErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, resultErr)
		}
		if !testCase.expectErr {
			var r ValueSet
			json.Unmarshal(result, &r)
			if !reflect.DeepEqual(r, testCase.value) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}
