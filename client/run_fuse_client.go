package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

const (
	GLIBCRequired = 214
)

func getGLIBCVersion() int64 {
	version, err := exec.Command("bash", "-c", "ldd --version |awk 'NR==1{print}' |awk '{print $NF}'").Output()
	if err != nil {
		fmt.Printf("get glibc version err: %v\n", err)
		os.Exit(1)
	}

	verStr := strings.Replace(string(version), "\n", "", -1)
	verStr = strings.Replace(verStr, ".", "", -1)
	ver, err := strconv.ParseInt(verStr, 10, 64)
	if err != nil {
		fmt.Printf("parse glibc version err: %v\n", err)
		os.Exit(1)
	}
	return ver
}

func main() {
	flag.Parse()
	if !*configVersion && *configFile == "" {
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

	useDynamicLibs := getGLIBCVersion() >= GLIBCRequired
	mainFile := MainBinary
	if !useDynamicLibs {
		mainFile = MainStaticBinary
	}
	exeFile := os.Args[0]
	if err = moveFile(mainFile, exeFile+".tmp"); err != nil {
		fmt.Printf("%v\n", err.Error())
		os.Exit(1)
	}
	if err = os.Rename(exeFile+".tmp", exeFile); err != nil {
		fmt.Printf("%v\n", err.Error())
		os.Exit(1)
	}

	env := os.Environ()
	execErr := syscall.Exec(exeFile, os.Args, env)
	if execErr != nil {
		fmt.Printf("exec %s %v error: %v\n", exeFile, os.Args, execErr)
		os.Exit(1)
	}
}

func checkLibsExist() bool {
	if _, err := os.Stat(MainBinary); err != nil {
		return false
	}
	if _, err := os.Stat(MainStaticBinary); err != nil {
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
