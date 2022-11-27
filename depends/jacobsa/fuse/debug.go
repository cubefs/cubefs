// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fuse

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jacobsa/fuse/fuseops"
)

func OpDescription(op interface{}) string {
	return describeRequest(op)
}

// Decide on the name of the given op.
func opName(op interface{}) string {
	// We expect all ops to be pointers.
	t := reflect.TypeOf(op).Elem()

	// Strip the "Op" from "FooOp".
	return strings.TrimSuffix(t.Name(), "Op")
}

func describeRequest(op interface{}) (s string) {
	v := reflect.ValueOf(op).Elem()

	// We will set up a comma-separated list of components.
	var components []string
	addComponent := func(format string, v ...interface{}) {
		components = append(components, fmt.Sprintf(format, v...))
	}

	// Include an inode number, if available.
	if f := v.FieldByName("Inode"); f.IsValid() {
		addComponent("inode %v", f.Interface())
	}

	// Include a parent inode number, if available.
	if f := v.FieldByName("Parent"); f.IsValid() {
		addComponent("parent %v", f.Interface())
	}

	// Include a name, if available.
	if f := v.FieldByName("Name"); f.IsValid() {
		addComponent("name %q", f.Interface())
	}

	// Handle special cases.
	switch typed := op.(type) {
	case *interruptOp:
		addComponent("fuseid 0x%08x", typed.FuseID)

	case *unknownOp:
		addComponent("opcode %d", typed.OpCode)

	case *fuseops.SetInodeAttributesOp:
		if typed.Size != nil {
			addComponent("size %d", *typed.Size)
		}

		if typed.Mode != nil {
			addComponent("mode %v", *typed.Mode)
		}

		if typed.Atime != nil {
			addComponent("atime %v", *typed.Atime)
		}

		if typed.Mtime != nil {
			addComponent("mtime %v", *typed.Mtime)
		}

		if typed.Uid != nil {
			addComponent("uid %v", *typed.Uid)
		}

		if typed.Gid != nil {
			addComponent("gid %v", *typed.Gid)
		}

	case *fuseops.ReadFileOp:
		addComponent("handle %d", typed.Handle)
		addComponent("offset %d", typed.Offset)
		addComponent("%d bytes", len(typed.Dst))

	case *fuseops.WriteFileOp:
		addComponent("handle %d", typed.Handle)
		addComponent("offset %d", typed.Offset)
		addComponent("%d bytes", len(typed.Data))

	case *fuseops.RemoveXattrOp:
		addComponent("name %s", typed.Name)

	case *fuseops.GetXattrOp:
		addComponent("name %s", typed.Name)

	case *fuseops.SetXattrOp:
		addComponent("name %s", typed.Name)
	}

	// Use just the name if there is no extra info.
	if len(components) == 0 {
		return opName(op)
	}

	// Otherwise, include the extra info.
	return fmt.Sprintf("%s (%s)", opName(op), strings.Join(components, ", "))
}

func describeResponse(op interface{}) string {
	v := reflect.ValueOf(op).Elem()

	// We will set up a comma-separated list of components.
	var components []string
	addComponent := func(format string, v ...interface{}) {
		components = append(components, fmt.Sprintf(format, v...))
	}

	// Include a resulting inode number, if available.
	if f := v.FieldByName("Entry"); f.IsValid() {
		if entry, ok := f.Interface().(fuseops.ChildInodeEntry); ok {
			addComponent("inode %v", entry.Child)
		}
	}

	return fmt.Sprintf("%s", strings.Join(components, ", "))
}
