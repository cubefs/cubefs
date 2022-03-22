# pacer

[![GoDoc](https://godoc.org/github.com/gammazero/workerpool/pacer?status.svg)](https://godoc.org/github.com/gammazero/workerpool/pacer)

A utility to limit the rate at which concurrent goroutines begin execution.  This addresses situations where running the concurrent goroutines is OK, as long as their execution does not start at the same time.

The pacer package is independent of the workerpool package.  Paced functions can be submitted to a workerpool or can be run as goroutines, and execution will be paced in both cases.
