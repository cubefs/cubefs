package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"plugin"
	"time"

	"github.com/jacobsa/daemonize"
)

var CommitID string
var (
	configFile       = flag.String("c", "", "FUSE client config file")
	configForeground = flag.Bool("f", false, "run foreground")
	versionUrlPtr    = flag.String("versionUrl", "", "url to get libcfssdk.so")
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

func parseVersionUrl() (string, error) {
	if *versionUrlPtr != "" {
		return *versionUrlPtr, nil
	}
	return ParseConfigString(*configFile, "versionUrl")
}

func ParseConfigString(fileName, key string) (string, error) {
	data := make(map[string]interface{})
	jsonFileBytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return "", err
	}
	if err = json.Unmarshal(jsonFileBytes, &data); err != nil {
		return "", err
	}

	value, present := data[key]
	if !present {
		return "", nil
	}
	if result, isString := value.(string); isString {
		return result, nil
	}
	return "", nil
}

func main() {
	flag.Parse()

	if !*configForeground {
		if err := startDaemon(); err != nil {
			fmt.Printf("Mount failed.\n%s\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	versionUrl, err := parseVersionUrl()
	if err != nil {
		fmt.Printf("parse config file %s error: %s", *configFile, err.Error())
		os.Exit(1)
	}
	sdkPath := "/usr/lib64/libfusesdk.so"
	handle, err := plugin.Open(sdkPath)
	if err != nil {
		fmt.Printf("open plugin %s error: %s", sdkPath, err.Error())
		os.Exit(1)
	}
	loadSym(handle)
	err = startClient(*configFile, nil, nil)
	if err != nil {
		_ = daemonize.SignalOutcome(err)
		os.Exit(1)
	} else {
		_ = daemonize.SignalOutcome(nil)
	}
	fd := getFuseFd()
	for {
		time.Sleep(10 * time.Second)
		data, err := ioutil.ReadFile(versionUrl + "/version")
		if err != nil {
			fmt.Printf(err.Error())
			os.Exit(1)
		}
		str := string(data)
		if str[0] == '0' {
			continue
		}
		if len(str) < 6 {
			continue
		}
		if str[2:6] == CommitID {
			continue
		}
		clientState := stopClient()
		plugin.Close(sdkPath)
		CommitID = str[2:6]
		sdkPath = versionUrl + "/libfusesdk_" + CommitID + ".so"
		handle, err = plugin.Open(sdkPath)
		if err != nil {
			fmt.Printf("open plugin %s error: %s", sdkPath, err.Error())
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
