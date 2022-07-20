package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"plugin"
	"runtime"
	"time"

	"github.com/jacobsa/daemonize"
)

var (
	configFile       = flag.String("c", "", "FUSE client config file")
	configForeground = flag.Bool("f", false, "run foreground")
)

var (
	startClient func(string, *os.File, []byte) error
	stopClient  func() []byte
	getFuseFd   func() *os.File
)

func loadSym(handle *plugin.Plugin) {
	sym, _ := handle.Lookup("StartClient")
	startClient = sym.(func(string, *os.File, []byte) error)

	sym, _ = handle.Lookup("StopClient")
	stopClient = sym.(func() []byte)

	sym, _ = handle.Lookup("GetFuseFd")
	getFuseFd = sym.(func() *os.File)
}

func main() {
	flag.Parse()

	if !*configForeground {
		if err := startDaemon(); err != nil {
			fmt.Printf("%s\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	clientLib := "/usr/lib64/libcfssdk.so"
	handle, err := plugin.Open(clientLib)
	if err != nil {
		fmt.Printf("open plugin %s error: %s", clientLib, err.Error())
		os.Exit(1)
	}
	loadSym(handle)
	err = startClient(*configFile, nil, nil)
	if err != nil {
		fmt.Printf("\nStart fuse client failed: %v\n", err.Error())
		_ = daemonize.SignalOutcome(err)
		os.Exit(1)
	} else {
		_ = daemonize.SignalOutcome(nil)
	}
	fd := getFuseFd()
	for {
		time.Sleep(10 * time.Second)
		reload := os.Getenv("RELOAD_CLIENT")
		if reload != "1" && reload != "test" {
			continue
		}

		clientState := stopClient()
		plugin.Close(clientLib)
		if reload == "test" {
			runtime.GC()
			time.Sleep(10 * time.Second)
		}

		handle, err = plugin.Open(clientLib)
		if err != nil {
			fmt.Printf("open plugin %s error: %s", clientLib, err.Error())
			os.Exit(1)
		}
		loadSym(handle)
		err = startClient(*configFile, fd, clientState)
		if err != nil {
			fmt.Printf(err.Error())
			os.Exit(1)
		}
	}
}

func startDaemon() error {
	cmdPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("startDaemon failed: cannot get absolute command path, err(%v)", err)
	}

	if len(os.Args) <= 1 {
		return fmt.Errorf("startDaemon failed: cannot use null arguments")
	}

	args := []string{"-f"}
	args = append(args, os.Args[1:]...)

	if *configFile != "" {
		configPath, err := filepath.Abs(*configFile)
		if err != nil {
			return fmt.Errorf("startDaemon failed: cannot get absolute command path of config file(%v) , err(%v)", *configFile, err)
		}
		for i := 0; i < len(args); i++ {
			if args[i] == "-c" {
				// Since *configFile is not "", the (i+1)th argument must be the config file path
				args[i+1] = configPath
				break
			}
		}
	}

	env := os.Environ()
	buf := new(bytes.Buffer)
	err = daemonize.Run(cmdPath, args, env, buf)
	if err != nil {
		if buf.Len() > 0 {
			fmt.Println(buf.String())
		}
		return fmt.Errorf("startDaemon failed.\ncmd(%v)\nargs(%v)\nerr(%v)\n", cmdPath, args, err)
	}
	return nil
}
