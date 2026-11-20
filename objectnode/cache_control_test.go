// Copyright 2026 The CubeFS Authors.
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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateCacheControl(t *testing.T) {
	valid := []string{
		"public",
		"private",
		"no-cache",
		"max-age=3600",
		"s-maxage=60",
		"public,max-age=3600",
		// directives are separated by a comma and optional whitespace
		"public, max-age=3600",
		"no-cache, no-store, must-revalidate",
		"public ,  max-age=3600",
		// max-age=0 is a common way to ask for revalidation
		"max-age=0",
		"no-cache, max-age=0",
		// directive names are case-insensitive
		"Public",
		"Max-Age=3600",
		"NO-CACHE, No-Store",
	}
	for _, cacheControl := range valid {
		t.Run("valid/"+cacheControl, func(t *testing.T) {
			require.True(t, ValidateCacheControl(cacheControl))
		})
	}

	invalid := []string{
		"unknown",
		"max-age",
		"max-age=",
		"max-age=-1",
		"max-age=abc",
		"max-age=1.5",
		"public; max-age=3600",
		"public, unknown",
		"public,",
	}
	for _, cacheControl := range invalid {
		t.Run("invalid/"+cacheControl, func(t *testing.T) {
			require.False(t, ValidateCacheControl(cacheControl))
		})
	}
}
