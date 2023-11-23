package objectnode

import (
	"fmt"
	"testing"
)

func TestNewStringSet(t *testing.T) {
	if ss := NewStringSet(); !ss.IsEmpty() {
		t.Fatalf("expected: true, got: false")
	}
}

func TestCreateStringSet(t *testing.T) {
	ss := CreateStringSet("foo")
	if str := ss.String(); str != `[foo]` {
		t.Fatalf("expected: %s, got: %s", `["foo"]`, str)
	}
}

func TestCopyStringSet(t *testing.T) {
	ss := CreateStringSet("foo")
	sscopy := CopyStringSet(ss)
	if !ss.Equals(sscopy) {
		t.Fatalf("expected: %s, got: %s", ss, sscopy)
	}
}

// StringSet.Add() is called with series of cases for valid and erroneous inputs and the result is validated.
func TestStringSetAdd(t *testing.T) {
	testCases := []struct {
		value          string
		expectedResult string
	}{
		// Test first addition.
		{"foo", `[foo]`},
		// Test duplicate addition.
		{"foo", `[foo]`},
		// Test new addition.
		{"bar", `[bar foo]`},
	}

	ss := NewStringSet()
	for _, testCase := range testCases {
		ss.Add(testCase.value)
		if str := ss.String(); str != testCase.expectedResult {
			t.Fatalf("expected: %s, got: %s", testCase.expectedResult, str)
		}
	}
}

func TestStringSetRemove(t *testing.T) {
	ss := CreateStringSet("foo", "bar")
	testCases := []struct {
		value          string
		expectedResult string
	}{
		// Test removing non-existen item.
		{"baz", `[bar foo]`},
		// Test remove existing item.
		{"foo", `[bar]`},
		// Test remove existing item again.
		{"foo", `[bar]`},
		// Test remove to make set to empty.
		{"bar", `[]`},
	}

	for _, testCase := range testCases {
		ss.Remove(testCase.value)
		if str := ss.String(); str != testCase.expectedResult {
			t.Fatalf("expected: %s, got: %s", testCase.expectedResult, str)
		}
	}
}

func TestStringSetContains(t *testing.T) {
	ss := CreateStringSet("foo")
	testCases := []struct {
		value          string
		expectedResult bool
	}{
		// Test to check non-existent item.
		{"bar", false},
		// Test to check existent item.
		{"foo", true},
		// Test to verify case sensitivity.
		{"Foo", false},
	}

	for _, testCase := range testCases {
		if result := ss.Contains(testCase.value); result != testCase.expectedResult {
			t.Fatalf("expected: %t, got: %t", testCase.expectedResult, result)
		}
	}
}

// StringSet.Equals() is called with series of cases for valid and erroneous inputs and the result is validated.
func TestStringSetEquals(t *testing.T) {
	testCases := []struct {
		set1           StringSet
		set2           StringSet
		expectedResult bool
	}{
		// Test equal set
		{CreateStringSet("foo", "bar"), CreateStringSet("foo", "bar"), true},
		// Test second set with more items
		{CreateStringSet("foo", "bar"), CreateStringSet("foo", "bar", "baz"), false},
		// Test second set with less items
		{CreateStringSet("foo", "bar"), CreateStringSet("bar"), false},
	}

	for _, testCase := range testCases {
		if result := testCase.set1.Equals(testCase.set2); result != testCase.expectedResult {
			t.Fatalf("expected: %t, got: %t", testCase.expectedResult, result)
		}
	}
}

// StringSet.Intersection() is called with series of cases for valid and erroneous inputs and the result is validated.
func TestStringSetIntersection(t *testing.T) {
	testCases := []struct {
		set1           StringSet
		set2           StringSet
		expectedResult StringSet
	}{
		// Test intersecting all values.
		{CreateStringSet("foo", "bar"), CreateStringSet("foo", "bar"), CreateStringSet("foo", "bar")},
		// Test intersecting all values in second set.
		{CreateStringSet("foo", "bar", "baz"), CreateStringSet("foo", "bar"), CreateStringSet("foo", "bar")},
		// Test intersecting different values in second set.
		{CreateStringSet("foo", "baz"), CreateStringSet("baz", "bar"), CreateStringSet("baz")},
		// Test intersecting none.
		{CreateStringSet("foo", "baz"), CreateStringSet("poo", "bar"), NewStringSet()},
	}

	for _, testCase := range testCases {
		if result := testCase.set1.Intersection(testCase.set2); !result.Equals(testCase.expectedResult) {
			t.Fatalf("expected: %s, got: %s", testCase.expectedResult, result)
		}
	}
}

// StringSet.Difference() is called with series of cases for valid and erroneous inputs and the result is validated.
func TestStringSetDifference(t *testing.T) {
	testCases := []struct {
		set1           StringSet
		set2           StringSet
		expectedResult StringSet
	}{
		// Test differing none.
		{CreateStringSet("foo", "bar"), CreateStringSet("foo", "bar"), NewStringSet()},
		// Test differing in first set.
		{CreateStringSet("foo", "bar", "baz"), CreateStringSet("foo", "bar"), CreateStringSet("baz")},
		// Test differing values in both set.
		{CreateStringSet("foo", "baz"), CreateStringSet("baz", "bar"), CreateStringSet("foo")},
		// Test differing all values.
		{CreateStringSet("foo", "baz"), CreateStringSet("poo", "bar"), CreateStringSet("foo", "baz")},
	}

	for _, testCase := range testCases {
		if result := testCase.set1.Difference(testCase.set2); !result.Equals(testCase.expectedResult) {
			t.Fatalf("expected: %s, got: %s", testCase.expectedResult, result)
		}
	}
}

// StringSet.Union() is called with series of cases for valid and erroneous inputs and the result is validated.
func TestStringSetUnion(t *testing.T) {
	testCases := []struct {
		set1           StringSet
		set2           StringSet
		expectedResult StringSet
	}{
		// Test union same values.
		{CreateStringSet("foo", "bar"), CreateStringSet("foo", "bar"), CreateStringSet("foo", "bar")},
		// Test union same values in second set.
		{CreateStringSet("foo", "bar", "baz"), CreateStringSet("foo", "bar"), CreateStringSet("foo", "bar", "baz")},
		// Test union different values in both set.
		{CreateStringSet("foo", "baz"), CreateStringSet("baz", "bar"), CreateStringSet("foo", "baz", "bar")},
		// Test union all different values.
		{CreateStringSet("foo", "baz"), CreateStringSet("poo", "bar"), CreateStringSet("foo", "baz", "poo", "bar")},
	}

	for _, testCase := range testCases {
		if result := testCase.set1.Union(testCase.set2); !result.Equals(testCase.expectedResult) {
			t.Fatalf("expected: %s, got: %s", testCase.expectedResult, result)
		}
	}
}

// StringSet.MarshalJSON() is called with series of cases for valid and erroneous inputs and the result is validated.
func TestStringSetMarshalJSON(t *testing.T) {
	testCases := []struct {
		set            StringSet
		expectedResult string
	}{
		// Test set with values.
		{CreateStringSet("foo", "bar"), `["bar","foo"]`},
		// Test empty set.
		{NewStringSet(), "[]"},
	}

	for _, testCase := range testCases {
		if result, _ := testCase.set.MarshalJSON(); string(result) != testCase.expectedResult {
			t.Fatalf("expected: %s, got: %s", testCase.expectedResult, string(result))
		}
	}
}

// StringSet.UnmarshalJSON() is called with series of cases for valid and erroneous inputs and the result is validated.
func TestStringSetUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult string
	}{
		{[]byte(`["bar","foo"]`), `[bar foo]`},
		{[]byte(`"bar"`), `[bar]`},
		{[]byte(`[]`), `[]`},
		{[]byte(`""`), `[]`},
	}

	for _, testCase := range testCases {
		var set StringSet
		set.UnmarshalJSON(testCase.data)
		if result := set.String(); result != testCase.expectedResult {
			t.Fatalf("expected: %s, got: %s", testCase.expectedResult, result)
		}
	}
}

// StringSet.String() is called with series of cases for valid and erroneous inputs and the result is validated.
func TestStringSetString(t *testing.T) {
	testCases := []struct {
		set            StringSet
		expectedResult string
	}{
		// Test empty set.
		{NewStringSet(), `[]`},
		// Test set with empty value.
		{CreateStringSet(""), `[]`},
		// Test set with value.
		{CreateStringSet("foo"), `[foo]`},
	}

	for _, testCase := range testCases {
		if str := testCase.set.String(); str != testCase.expectedResult {
			t.Fatalf("expected: %s, got: %s", testCase.expectedResult, str)
		}
	}
}

// StringSet.ToSlice() is called with series of cases for valid and erroneous inputs and the result is validated.
func TestStringSetToSlice(t *testing.T) {
	testCases := []struct {
		set            StringSet
		expectedResult string
	}{
		// Test empty set.
		{NewStringSet(), `[]`},
		// Test set with empty value.
		{CreateStringSet(""), `[]`},
		// Test set with value.
		{CreateStringSet("foo"), `[foo]`},
		// Test set with value.
		{CreateStringSet("foo", "bar"), `[bar foo]`},
	}

	for _, testCase := range testCases {
		sslice := testCase.set.ToSlice()
		if str := fmt.Sprintf("%s", sslice); str != testCase.expectedResult {
			t.Fatalf("expected: %s, got: %s", testCase.expectedResult, str)
		}
	}
}
