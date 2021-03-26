// Copyright (c) 2020 Uber Technologies, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFloat64(t *testing.T) {
	atom := NewFloat64(4.2)

	require.Equal(t, float64(4.2), atom.Load(), "Load didn't work.")

	require.True(t, atom.CAS(4.2, 0.5), "CAS didn't report a swap.")
	require.Equal(t, float64(0.5), atom.Load(), "CAS didn't set the correct value.")
	require.False(t, atom.CAS(0.0, 1.5), "CAS reported a swap.")

	atom.Store(42.0)
	require.Equal(t, float64(42.0), atom.Load(), "Store didn't set the correct value.")
	require.Equal(t, float64(42.5), atom.Add(0.5), "Add didn't work.")
	require.Equal(t, float64(42.0), atom.Sub(0.5), "Sub didn't work.")

	t.Run("JSON/Marshal", func(t *testing.T) {
		atom.Store(42.5)
		bytes, err := json.Marshal(atom)
		require.NoError(t, err, "json.Marshal errored unexpectedly.")
		require.Equal(t, []byte("42.5"), bytes, "json.Marshal encoded the wrong bytes.")
	})

	t.Run("JSON/Unmarshal", func(t *testing.T) {
		err := json.Unmarshal([]byte("40.5"), &atom)
		require.NoError(t, err, "json.Unmarshal errored unexpectedly.")
		require.Equal(t, float64(40.5), atom.Load(), "json.Unmarshal didn't set the correct value.")
	})

	t.Run("JSON/Unmarshal/Error", func(t *testing.T) {
		err := json.Unmarshal([]byte("\"40.5\""), &atom)
		require.Error(t, err, "json.Unmarshal didn't error as expected.")
		assertErrorJSONUnmarshalType(t, err,
			"json.Unmarshal failed with unexpected error %v, want UnmarshalTypeError.", err)
	})

	t.Run("String", func(t *testing.T) {
		assert.Equal(t, "42.5", NewFloat64(42.5).String(),
			"String() returned an unexpected value.")
	})
}
