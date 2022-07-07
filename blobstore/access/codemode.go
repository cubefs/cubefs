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

package access

import (
	"fmt"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
)

// CodeModePair codemode with pair of tactic and policy
type CodeModePair struct {
	Policy codemode.Policy
	Tactic codemode.Tactic
}

// CodeModePairs map of CodeModePair
type CodeModePairs map[codemode.CodeMode]CodeModePair

// SelectCodeMode select codemode by size
func (c CodeModePairs) SelectCodeMode(size int64) codemode.CodeMode {
	for codeMode, pair := range c {
		policy := pair.Policy
		if !policy.Enable {
			continue
		}
		if size >= policy.MinSize && size <= policy.MaxSize {
			return codeMode
		}
	}

	panic(fmt.Sprintf("no codemode policy to be selected by size %d, %+v", size, c))
}
