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

package interrupt

import "fmt"

// inject interrupt just for test
var labels map[string]struct{}

type InterruptCfg struct {
	InterruptLabels []string `json:"interrupt_labels"`
}

func SetInterruptLabels(conf *InterruptCfg) {
	labels = make(map[string]struct{})
	for _, label := range conf.InterruptLabels {
		labels[label] = struct{}{}
	}
}

func Inject(label string) {
	if _, ok := labels[label]; ok {
		panic(fmt.Sprintf("InjectInterrupt label %s", label))
	}
}
