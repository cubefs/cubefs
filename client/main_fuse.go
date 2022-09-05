package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"plugin"
	"runtime"
	"strings"
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

const (
	ClientLib             = "/usr/lib64/libcfssdk.so"
	GolangLib             = "/usr/lib64/libstd.so"
	MasterAddr            = "masterAddr"
	AdminGetClientPkgAddr = "/clientPkgAddr/get"
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
	if !checkLibsExist() {
		masterAddr, err := parseMasterAddr(*configFile)
		if err != nil {
			fmt.Printf("parseMasterAddr err: %v\n", err)
			os.Exit(1)
		}
		downloadAddr, err := getClientDownloadAddr(masterAddr)
		if err != nil {
			fmt.Printf("get downloadAddr from master err: %v\n", err)
			os.Exit(1)
		}
		if err = downloadClientLibs(downloadAddr); err != nil {
			fmt.Printf("download libs err: %v\n", err)
			os.Exit(1)
		}
	}

	if !*configForeground {
		if err := startDaemon(); err != nil {
			fmt.Printf("%s\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	handle, err := plugin.Open(ClientLib)
	if err != nil {
		fmt.Printf("open plugin %s error: %s", ClientLib, err.Error())
		_ = daemonize.SignalOutcome(err)
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
		plugin.Close(ClientLib)
		if reload == "test" {
			runtime.GC()
			time.Sleep(10 * time.Second)
		}

		handle, err = plugin.Open(ClientLib)
		if err != nil {
			fmt.Printf("open plugin %s error: %s", ClientLib, err.Error())
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

func checkLibsExist() bool {
	if _, err := os.Stat(GolangLib); err != nil {
		return false
	}
	if _, err := os.Stat(ClientLib); err != nil {
		return false
	}
	return true
}

func parseMasterAddr(configPath string) (string, error) {
	data := make(map[string]interface{})

	jsonFileBytes, err := ioutil.ReadFile(configPath)
	if err == nil {
		err = json.Unmarshal(jsonFileBytes, &data)
	}
	if err != nil {
		return "", err
	}

	x, present := data[MasterAddr]
	if !present {
		err = fmt.Errorf("lack argment %s in config file %s\n", MasterAddr, configPath)
		return "", err
	}
	result, isString := x.(string)
	if !isString {
		err = fmt.Errorf("argment %s must be string\n", MasterAddr)
		return "", err
	}
	return result, nil
}

func getClientDownloadAddr(masterAddr string) (string, error) {
	var masters = make([]string, 0)
	for _, master := range strings.Split(masterAddr, ",") {
		master = strings.TrimSpace(master)
		if master != "" {
			masters = append(masters, master)
		}
	}

	var (
		addr     string
		err      error
		req      *http.Request
		resp     *http.Response
		respData []byte
	)
	for _, host := range masters {
		var url = fmt.Sprintf("http://%s%s", host, AdminGetClientPkgAddr)
		client := http.Client{Timeout: 30 * time.Second}
		if req, err = http.NewRequest("GET", url, nil); err != nil {
			return addr, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Connection", "close")
		resp, err = client.Do(req)
		if err != nil {
			continue
		}
		stateCode := resp.StatusCode
		respData, err = ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			continue
		}
		switch stateCode {
		case http.StatusOK:
			var body = &struct {
				Code int32  `json:"code"`
				Msg  string `json:"msg"`
				Data string `json:"data"`
			}{}
			if err = json.Unmarshal(respData, body); err == nil {
				addr = body.Data
				break
			}
		default:
			err = fmt.Errorf("url(%v) status(%v) body(%s)", url, stateCode, strings.Replace(string(respData), "\n", "", -1))
		}
	}
	return addr, err
}

func downloadClientLibs(addr string) error {
	var url string
	for _, lib := range [2]string{GolangLib, ClientLib} {
		fileName := filepath.Base(lib)
		if addr[len(addr)-1] == '/' {
			url = fmt.Sprintf("%s%s", addr, fileName)
		} else {
			url = fmt.Sprintf("%s/%s", addr, fileName)
		}
		resp, err := http.Get(url)
		if err != nil {
			return fmt.Errorf("Download %s error: %v", url, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("Download %s error: %s", url, resp.Status)
		}

		tmpFile := lib + ".tmp"
		file, err := os.OpenFile(tmpFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return err
		}

		_, err = io.Copy(file, resp.Body)
		if err != nil {
			file.Close()
			return err
		}
		if err = file.Close(); err != nil {
			return err
		}
		if err := os.Rename(tmpFile, lib); err != nil {
			return err
		}
	}
	return nil
}
