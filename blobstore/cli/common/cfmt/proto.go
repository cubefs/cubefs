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

package cfmt

import (
	"fmt"

	"github.com/fatih/color"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// VuidF vuid fmt
func VuidF(vuid proto.Vuid) string {
	return fmt.Sprintf("%-20d (V:%10d I:%3d E:%10d)", vuid,
		vuid.Vid(), vuid.Index(), vuid.Epoch(),
	)
}

// VuidCF vuid fmt with color
func VuidCF(vuid proto.Vuid) string {
	c := color.New(color.Faint, color.Italic)
	return fmt.Sprintf("%-20d (%s%10d %s%3d %s%10d)", vuid,
		c.Sprint("V:"), vuid.Vid(),
		c.Sprint("I:"), vuid.Index(),
		c.Sprint("E:"), vuid.Epoch(),
	)
}

// SuidF suid fmt
func SuidF(suid proto.Suid) string {
	return fmt.Sprintf("%-20d (S:%10d I:%3d E:%10d)", suid,
		suid.ShardID(), suid.Index(), suid.Epoch(),
	)
}
