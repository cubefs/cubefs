package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"syscall"
)

func main() {
	flag.Parse()
	if !*version && *configFile == "" {
		fmt.Printf("Usage: %s -c {configFile}\n", os.Args[0])
		os.Exit(1)
	}
	var (
		err          error
		masterAddr   string
		downloadAddr string
		tarName      string
	)
	if *configFile != "" {
		masterAddr, err = parseMasterAddr(*configFile)
		if err != nil {
			fmt.Printf("parseMasterAddr err: %v\n", err)
			os.Exit(1)
		}
	} else {
		masterAddr = DefaultMasterAddr
	}

	downloadAddr, err = getClientDownloadAddr(masterAddr)
	if err != nil {
		fmt.Printf("get downloadAddr from master err: %v\n", err)
		os.Exit(1)
	}
	if !checkLibsExist() {
		if runtime.GOARCH == AMD64 {
			tarName = fmt.Sprintf("%s.tar.gz", TarNamePre)
		} else if runtime.GOARCH == ARM64 {
			tarName = fmt.Sprintf("%s_%s.tar.gz", TarNamePre, ARM64)
		} else {
			fmt.Printf("cpu arch %s not supported", runtime.GOARCH)
			os.Exit(1)
		}
		if !prepareLibs(downloadAddr, tarName) {
			os.Exit(1)
		}
	}
	if *useVersion != "" {
		if runtime.GOARCH == AMD64 {
			tarName = fmt.Sprintf("%s_%s.tar.gz", VersionTarPre, *useVersion)
		} else if runtime.GOARCH == ARM64 {
			tarName = fmt.Sprintf("%s_%s_%s.tar.gz", VersionTarPre, ARM64, *useVersion)
		}
		if !prepareLibs(downloadAddr, tarName) {
			os.Exit(1)
		}
	}

	exeFile := MainBinary
	start := !*version
	if start {
		exeFile = os.Args[0]
		if err = moveFile(MainBinary, exeFile+".tmp"); err != nil {
			fmt.Printf("%v\n", err.Error())
			os.Exit(1)
		}
		if err = os.Rename(exeFile+".tmp", exeFile); err != nil {
			fmt.Printf("%v\n", err.Error())
			os.Exit(1)
		}
	}

	env := os.Environ()
	args := make([]string, 0, len(os.Args))
	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "-u" {
			i++
		} else {
			args = append(args, os.Args[i])
		}
	}

	execErr := syscall.Exec(exeFile, args, env)
	if execErr != nil {
		fmt.Printf("exec %s %v error: %v\n", exeFile, os.Args, execErr)
		os.Exit(1)
	}
}

func checkLibsExist() bool {
	if _, err := os.Stat(MainBinary); err != nil {
		return false
	}
	if _, err := os.Stat(GolangLib); err != nil {
		return false
	}
	if _, err := os.Stat(ClientLib); err != nil {
		return false
	}
	return true
}
