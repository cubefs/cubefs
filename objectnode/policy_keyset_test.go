package objectnode

import (
	"reflect"
	"testing"
)

func TestKeySetAdd(t *testing.T) {
	testCases := []struct {
		set            KeySet
		key            Key
		expectedResult KeySet
	}{
		{NewKeySet(), AWSReferer, NewKeySet(AWSReferer)},
		{NewKeySet(AWSReferer), AWSReferer, NewKeySet(AWSReferer)},
	}

	for i, testCase := range testCases {
		testCase.set.Add(testCase.key)

		if !reflect.DeepEqual(testCase.expectedResult, testCase.set) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, testCase.set)
		}
	}
}

func TestKeySetDifference(t *testing.T) {
	testCases := []struct {
		set            KeySet
		setToDiff      KeySet
		expectedResult KeySet
	}{
		{NewKeySet(), NewKeySet(AWSHost), NewKeySet()},
		{NewKeySet(AWSHost, AWSSourceIP, AWSReferer), NewKeySet(AWSSourceIP, AWSReferer), NewKeySet(AWSHost)},
	}

	for i, testCase := range testCases {
		result := testCase.set.Difference(testCase.setToDiff)

		if !reflect.DeepEqual(testCase.expectedResult, result) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestKeySetIsEmpty(t *testing.T) {
	testCases := []struct {
		set            KeySet
		expectedResult bool
	}{
		{NewKeySet(), true},
		{NewKeySet(AWSSourceIP), false},
	}

	for i, testCase := range testCases {
		result := testCase.set.IsEmpty()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestKeySetString(t *testing.T) {
	testCases := []struct {
		set            KeySet
		expectedResult string
	}{
		{NewKeySet(), `[]`},
		{NewKeySet(AWSHost), `[aws:Host]`},
	}

	for i, testCase := range testCases {
		result := testCase.set.String()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestKeySetToSlice(t *testing.T) {
	testCases := []struct {
		set            KeySet
		expectedResult []Key
	}{
		{NewKeySet(), []Key{}},
		{NewKeySet(AWSReferer), []Key{AWSReferer}},
	}

	for i, testCase := range testCases {
		result := testCase.set.ToSlice()

		if !reflect.DeepEqual(testCase.expectedResult, result) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}
