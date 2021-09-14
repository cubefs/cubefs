// Copyright 2019-2020 Tobias Klauser
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
	"bytes"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/tklauser/numcpus"
)

func testGetconf(t *testing.T, got int, name, getconfWhich string) {
	getconf, err := exec.LookPath("getconf")
	if err != nil {
		t.Skipf("getconf not found in PATH: %v", err)
	}

	cmd := exec.Command(getconf, getconfWhich)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		t.Skipf("failed to invoke getconf: %v", err)
	}

	want, err := strconv.ParseInt(strings.TrimSpace(out.String()), 10, 64)
	if err != nil {
		t.Skipf("failed to parse getconf output: %v", err)
	}

	if got != int(want) {
		t.Errorf("%s returned %v, want %v (getconf %s)", name, got, want, getconfWhich)
	}
}

func TestGetKernelMax(t *testing.T) {
	n, err := numcpus.GetKernelMax()
	if err == numcpus.ErrNotSupported {
		t.Skipf("GetKernelMax not supported on %s", runtime.GOOS)
	} else if err != nil {
		t.Fatalf("GetKernelMax: %v", err)
	}
	t.Logf("KernelMax = %v", n)
}

func TestGetOffline(t *testing.T) {
	n, err := numcpus.GetOffline()
	if err == numcpus.ErrNotSupported {
		t.Skipf("GetOffline not supported on %s", runtime.GOOS)
	} else if err != nil {
		t.Fatalf("GetOffline: %v", err)
	}
	t.Logf("Offline = %v", n)
}

func TestGetOnline(t *testing.T) {
	n, err := numcpus.GetOnline()
	if err == numcpus.ErrNotSupported {
		t.Skipf("GetOnline not supported on %s", runtime.GOOS)
	} else if err != nil {
		t.Fatalf("GetOnline: %v", err)
	}
	t.Logf("Online = %v", n)

	testGetconf(t, n, "GetOnline", "_NPROCESSORS_ONLN")
}

func TestGetPossible(t *testing.T) {
	n, err := numcpus.GetPossible()
	if err == numcpus.ErrNotSupported {
		t.Skipf("GetPossible not supported on %s", runtime.GOOS)
	} else if err != nil {
		t.Fatalf("GetPossible: %v", err)
	}
	t.Logf("Possible = %v", n)
}

func TestGetPresent(t *testing.T) {
	n, err := numcpus.GetPresent()
	if err == numcpus.ErrNotSupported {
		t.Skipf("GetPresent not supported on %s", runtime.GOOS)
	} else if err != nil {
		t.Fatalf("GetPresent: %v", err)
	}
	t.Logf("Present = %v", n)

	testGetconf(t, n, "GetPresent", "_NPROCESSORS_CONF")
}
