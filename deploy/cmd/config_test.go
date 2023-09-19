package cmd

import (
	"fmt"
	"testing"
)

func TestReadConfig(t *testing.T) {
	_, err := readConfig()
	if err != nil {
		errStr := fmt.Sprintf("%s", err)
		t.Errorf(errStr)
	}

}
