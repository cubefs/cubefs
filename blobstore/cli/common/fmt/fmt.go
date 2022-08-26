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

package fmt

import (
	"fmt"
	"io"
)

// alias some functions of system fmt package.
var (
	Errorf = fmt.Errorf

	Fprintf  = fmt.Fprintf
	Sprintf  = fmt.Sprintf
	Printf   = fmt.Printf
	Fprint   = fmt.Fprint
	Sprint   = fmt.Sprint
	Print    = fmt.Print
	Fprintln = fmt.Fprintln
	Sprintln = fmt.Sprintln
	Println  = fmt.Println
)

// SetOutput reset fmt output writer.
func SetOutput(w io.Writer) {
	Printf = func(format string, a ...interface{}) (int, error) { return Fprintf(w, format, a...) }
	Print = func(a ...interface{}) (int, error) { return Fprint(w, a...) }
	Println = func(a ...interface{}) (int, error) { return Fprintln(w, a...) }
}
