package util

import (
	"testing"
)

func TestGetDiskInfo(t *testing.T) {
	info, used, err := GetDiskInfo("./")

	if err != nil {
		t.Fatal(err)
	}

	if info <= 0 || used <= 0 {
		t.Fatal("get info has err")
	}

}
