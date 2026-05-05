//go:build !linux

package main

import "fmt"

func runBench(_ []string) { platformUnsupported("bench") }
func runRead(_ []string)  { platformUnsupported("read") }
func runWrite(_ []string) { platformUnsupported("write") }

func platformUnsupported(cmd string) {
	fmt.Printf("cfs-sync %s requires the CubeFS SDK and is only supported on Linux\n", cmd)
}
