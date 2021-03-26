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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUint32(t *testing.T) {
	atom := NewUint32(42)

	require.Equal(t, uint32(42), atom.Load(), "Load didn't work.")
	require.Equal(t, uint32(46), atom.Add(4), "Add didn't work.")
	require.Equal(t, uint32(44), atom.Sub(2), "Sub didn't work.")
	require.Equal(t, uint32(45), atom.Inc(), "Inc didn't work.")
	require.Equal(t, uint32(44), atom.Dec(), "Dec didn't work.")

	require.True(t, atom.CAS(44, 0), "CAS didn't report a swap.")
	require.Equal(t, uint32(0), atom.Load(), "CAS didn't set the correct value.")

	require.Equal(t, uint32(0), atom.Swap(1), "Swap didn't return the old value.")
	require.Equal(t, uint32(1), atom.Load(), "Swap didn't set the correct value.")

	atom.Store(42)
	require.Equal(t, uint32(42), atom.Load(), "Store didn't set the correct value.")

	t.Run("JSON/Marshal", func(t *testing.T) {
		bytes, err := json.Marshal(atom)
		require.NoError(t, err, "json.Marshal errored unexpectedly.")
		require.Equal(t, []byte("42"), bytes, "json.Marshal encoded the wrong bytes.")
	})

	t.Run("JSON/Unmarshal", func(t *testing.T) {
		err := json.Unmarshal([]byte("40"), &atom)
		require.NoError(t, err, "json.Unmarshal errored unexpectedly.")
		require.Equal(t, uint32(40), atom.Load(), "json.Unmarshal didn't set the correct value.")
	})

	t.Run("JSON/Unmarshal/Error", func(t *testing.T) {
		err := json.Unmarshal([]byte(`"40"`), &atom)
		require.Error(t, err, "json.Unmarshal didn't error as expected.")
		assertErrorJSONUnmarshalType(t, err,
			"json.Unmarshal failed with unexpected error %v, want UnmarshalTypeError.", err)
	})

	t.Run("String", func(t *testing.T) {
		// Use an integer with the signed bit set. If we're converting
		// incorrectly, we'll get a negative value here.
		atom := NewUint32(math.MaxUint32)
		assert.Equal(t, "4294967295", atom.String(),
			"String() returned an unexpected value.")
	})
}
