package main

import "testing"

func Test_Main(t *testing.T) {
	if err := run(); err != nil {
		t.Fatalf("failed to run: %v", err)
	}
}
