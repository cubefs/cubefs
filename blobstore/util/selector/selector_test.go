// Copyright 2022 The CubeFS Authors.
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

package selector

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSelector_NewSelector(t *testing.T) {
	{
		_, err := NewSelector(100, func() (strings []string, e error) {
			return nil, errors.New("no hosts")
		})
		require.Error(t, err)
	}
	{
		_, err := NewSelector(100, func() (strings []string, e error) {
			return nil, nil
		})
		require.Error(t, err)
	}
	{
		_, err := NewSelector(100, func() (strings []string, e error) {
			return []string{}, nil
		})
		require.Error(t, err)
	}
}

func TestSelector_GetRandomN(t *testing.T) {
	type fields struct {
		hosts []string
	}
	type args struct {
		n int
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		expectHosts []string
	}{
		{"1", fields{hosts: []string{"A"}}, args{0}, nil},
		{"2", fields{hosts: []string{"A"}}, args{-1}, nil},
		{"3", fields{hosts: []string{"A"}}, args{1}, []string{"A"}},
		{"4", fields{hosts: []string{"A"}}, args{2}, []string{"A"}},
		{"4", fields{hosts: []string{"A", "B"}}, args{2}, []string{"A", "B"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector, err := NewSelector(100, func() (strings []string, e error) {
				return tt.fields.hosts, nil
			})
			require.NoError(t, err)

			gotHosts := selector.GetRandomN(tt.args.n)
			sort.Strings(gotHosts)
			require.Equal(t, tt.expectHosts, gotHosts)
		})
	}
}

func TestSelector_GetHashN(t *testing.T) {
	type fields struct {
		hosts []string
	}
	type args struct {
		n   int
		key string
	}
	h := []string{"A", "B", "C", "D", "E"}

	key1 := []byte("1234")
	key2 := []byte("4321")
	key3 := []byte("")

	idx1 := keyHash(key1) % uint64(len(h))
	idx2 := keyHash(key2) % uint64(len(h))
	idx3 := keyHash(key3) % uint64(len(h))
	tests := []struct {
		name        string
		fields      fields
		args        args
		expectHosts []string
		expectErr   bool
	}{
		{"1", fields{hosts: []string{}}, args{n: 0}, nil, true},
		{"2", fields{hosts: []string{}}, args{n: -1}, nil, true},
		{"3", fields{hosts: []string{"A"}}, args{n: 1}, []string{"A"}, false},
		{"4", fields{hosts: []string{"A"}}, args{n: 2}, []string{"A"}, false},
		{"5", fields{hosts: []string{}}, args{n: 2}, nil, true},
		{"6", fields{hosts: h}, args{n: 1, key: "1234"}, []string{h[idx1]}, false},
		{"7", fields{hosts: h}, args{n: 1, key: "4321"}, []string{h[idx2]}, false},
		{"8", fields{hosts: h}, args{n: 1, key: ""}, []string{h[idx3]}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector, err := NewSelector(100, func() (strings []string, e error) {
				return tt.fields.hosts, nil
			})
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			gotHosts := selector.GetHashN(tt.args.n, []byte(tt.args.key))
			require.Equal(t, tt.expectHosts, gotHosts)
		})
	}
}

func TestSelector_GetRoundRobinN(t *testing.T) {
	first := true
	selector, err := NewSelector(500, func() (strings []string, e error) {
		if first {
			first = false
			return []string{"1", "2", "3"}, nil
		}

		return []string{"4", "5"}, nil
	})
	require.NoError(t, err)

	require.Nil(t, selector.GetRoundRobinN(-1))
	require.Nil(t, selector.GetRoundRobinN(0))

	require.Equal(t, []string{"1"}, selector.GetRoundRobinN(1))
	require.Equal(t, []string{"2"}, selector.GetRoundRobinN(1))
	require.Equal(t, []string{"3"}, selector.GetRoundRobinN(1))
	require.Equal(t, []string{"1", "2"}, selector.GetRoundRobinN(2))
	require.Equal(t, []string{"3", "1"}, selector.GetRoundRobinN(2))

	time.Sleep(time.Millisecond * 500)
	require.Equal(t, []string{"2"}, selector.GetRoundRobinN(1)) // cached
	time.Sleep(time.Millisecond * 100)
	require.Equal(t, []string{"4"}, selector.GetRoundRobinN(1))
	require.Equal(t, []string{"5", "4"}, selector.GetRoundRobinN(2))
	require.Equal(t, []string{"5", "4"}, selector.GetRoundRobinN(100))
}

func Benchmark_GetHashN(b *testing.B) {
	selector, _ := NewSelector(10000, func() (strings []string, e error) {
		return []string{"", "1", "2", "3"}, nil
	})
	key := []byte("key")
	for ii := 0; ii < b.N; ii++ {
		selector.GetHashN(1, key)
	}
}

func Benchmark_GetRandomN(b *testing.B) {
	selector, _ := NewSelector(10000, func() (strings []string, e error) {
		return []string{"", "1", "2", "3"}, nil
	})
	for ii := 0; ii < b.N; ii++ {
		selector.GetRandomN(1)
	}
}

func Benchmark_GetRoundRobinN(b *testing.B) {
	selector, _ := NewSelector(10000, func() (strings []string, e error) {
		return []string{"", "1", "2", "3"}, nil
	})
	for ii := 0; ii < b.N; ii++ {
		selector.GetRoundRobinN(1)
	}
}
