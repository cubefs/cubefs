package testutil

import (
	"fmt"
	"os"
	"path"
	"testing"
)

type TempTestPath struct {
	testPath string
	t        *testing.T
}

func (p TempTestPath) String() string {
	return fmt.Sprintf("TempTestPath[test: %v, path: %v]", p.t.Name(), p.testPath)
}

func (p TempTestPath) Path() string {
	return p.testPath
}

func (p TempTestPath) Cleanup() {
	if err := os.RemoveAll(p.testPath); err != nil {
		p.t.Fatalf("%s cleanup failed: %v", p.String(), err)
	}
}

func InitTempTestPath(t *testing.T) *TempTestPath {
	var err error
	var testPath = path.Join(os.TempDir(), t.Name())
	var ttp = &TempTestPath{
		testPath: testPath,
		t:        t,
	}
	if err = os.RemoveAll(testPath); err != nil {
		t.Fatalf("%s cleanup on init failed: %v", ttp, testPath)
	}
	if err = os.MkdirAll(testPath, os.ModePerm); err != nil {
		t.Fatalf("%s make dirs on init failed: %v", ttp, testPath)
	}
	return ttp
}
