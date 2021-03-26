// Copyright (c) 2016-2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package atomic

import (
	"encoding/json"
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringNoInitialValue(t *testing.T) {
	atom := &String{}
	require.Equal(t, "", atom.Load(), "Initial value should be blank string")
}

func TestString(t *testing.T) {
	atom := NewString("")
	require.Equal(t, "", atom.Load(), "Expected Load to return initialized value")

	atom.Store("abc")
	require.Equal(t, "abc", atom.Load(), "Unexpected value after Store")

	atom = NewString("bcd")
	require.Equal(t, "bcd", atom.Load(), "Expected Load to return initialized value")

	t.Run("JSON/Marshal", func(t *testing.T) {
		bytes, err := json.Marshal(atom)
		require.NoError(t, err, "json.Marshal errored unexpectedly.")
		require.Equal(t, []byte(`"bcd"`), bytes, "json.Marshal encoded the wrong bytes.")
	})

	t.Run("JSON/Unmarshal", func(t *testing.T) {
		err := json.Unmarshal([]byte(`"abc"`), &atom)
		require.NoError(t, err, "json.Unmarshal errored unexpectedly.")
		require.Equal(t, "abc", atom.Load(), "json.Unmarshal didn't set the correct value.")
	})

	t.Run("JSON/Unmarshal/Error", func(t *testing.T) {
		err := json.Unmarshal([]byte("42"), &atom)
		require.Error(t, err, "json.Unmarshal didn't error as expected.")
		assertErrorJSONUnmarshalType(t, err,
			"json.Unmarshal failed with unexpected error %v, want UnmarshalTypeError.", err)
	})

	atom = NewString("foo")

	t.Run("XML/Marshal", func(t *testing.T) {
		bytes, err := xml.Marshal(atom)
		require.NoError(t, err, "xml.Marshal errored unexpectedly.")
		require.Equal(t, []byte("<String>foo</String>"), bytes, "xml.Marshal encoded the wrong bytes.")
	})

	t.Run("XML/Unmarshal", func(t *testing.T) {
		err := xml.Unmarshal([]byte("<String>bar</String>"), &atom)
		require.NoError(t, err, "xml.Unmarshal errored unexpectedly.")
		require.Equal(t, "bar", atom.Load(), "xml.Unmarshal didn't set the correct value.")
	})

	t.Run("String", func(t *testing.T) {
		atom := NewString("foo")
		assert.Equal(t, "foo", atom.String(),
			"String() returned an unexpected value.")
	})
}
