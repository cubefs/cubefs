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

package flags

import "github.com/desertbit/grumble"

// VerboseRegister enable verbose mode
func VerboseRegister(f *grumble.Flags) {
	f.Bool("v", "verbose", false, "enable verbose mode")
}

// Verbose enable verbose mode
func Verbose(f grumble.FlagMap) bool {
	return f.Bool("verbose")
}

// VverboseRegister enable verbose verbose mode
func VverboseRegister(f *grumble.Flags) {
	f.BoolL("vv", false, "enable verbose verbose mode")
}

// Vverbose enable verbose verbose mode
func Vverbose(f grumble.FlagMap) bool {
	return f.Bool("vv")
}

// ConfigRegister config path
func ConfigRegister(f *grumble.Flags) {
	f.String("c", "config", "", "config path")
}

// Config config path
func Config(f grumble.FlagMap) string {
	return f.String("config")
}

// FilePathRegister file path, relative path is ok
func FilePathRegister(f *grumble.Flags) {
	f.String("f", "filepath", "", "file path")
}

// FilePath file path, relative path is ok
func FilePath(f grumble.FlagMap) string {
	return f.String("filepath")
}
