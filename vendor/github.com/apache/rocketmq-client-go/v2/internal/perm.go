/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package internal

const (
	permPriority = 0x1 << 3
	permRead     = 0x1 << 2
	permWrite    = 0x1 << 1
	permInherit  = 0x1 << 0
)

func queueIsReadable(perm int) bool {
	return (perm & permRead) == permRead
}

func queueIsWriteable(perm int) bool {
	return (perm & permWrite) == permWrite
}

func queueIsInherited(perm int) bool {
	return (perm & permInherit) == permInherit
}

func perm2string(perm int) string {
	bytes := make([]byte, 3)
	for i := 0; i < 3; i++ {
		bytes[i] = '-'
	}

	if queueIsReadable(perm) {
		bytes[0] = 'R'
	}

	if queueIsWriteable(perm) {
		bytes[1] = 'W'
	}

	if queueIsInherited(perm) {
		bytes[2] = 'X'
	}

	return string(bytes)
}
