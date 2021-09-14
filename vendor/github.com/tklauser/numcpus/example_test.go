// Copyright 2018 Tobias Klauser
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

package numcpus_test

import (
	"fmt"
	"os"

	"github.com/tklauser/numcpus"
)

func ExampleGetOffline() {
	offline, err := numcpus.GetOffline()
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetOffline: %v\n", err)
	}
	fmt.Printf("# of offline CPUs: %v\n", offline)
}

func ExampleGetOnline() {
	online, err := numcpus.GetOnline()
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetOnline: %v\n", err)
	}
	fmt.Printf("# of online CPUs: %v\n", online)
}

func ExampleGetPossible() {
	possible, err := numcpus.GetPossible()
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetPossible: %v\n", err)
	}
	fmt.Printf("# of possible CPUs: %v\n", possible)
}

func ExampleGetPresent() {
	present, err := numcpus.GetPresent()
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetPresent: %v\n", err)
	}
	fmt.Printf("# of present CPUs: %v\n", present)
}
