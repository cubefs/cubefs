package main

import (
	"os"
	"syscall"
)

var SignalsIgnored = []os.Signal{
	syscall.SIGUSR1,
	syscall.SIGUSR2,
	syscall.SIGPIPE,
	syscall.SIGALRM,
	syscall.SIGXCPU,
	syscall.SIGXFSZ,
	syscall.SIGVTALRM,
	syscall.SIGPROF,
	syscall.SIGIO,
	syscall.SIGPWR,
}
