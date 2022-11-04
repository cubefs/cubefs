package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

var (
	configFile       = flag.String("c", "", "FUSE client config file")
	configForeground = flag.Bool("f", false, "run foreground")
)

const (
	MainBinary            = "/usr/lib64/cfs-client-inner"
	ClientLib             = "/usr/lib64/libcfssdk.so"
	GolangLib             = "/usr/lib64/libstd.so"
	MasterAddr            = "masterAddr"
	AdminGetClientPkgAddr = "/clientPkgAddr/get"
)

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

	args := []string{filepath.Base(MainBinary)}
	args = append(args, os.Args[1:]...)
	env := os.Environ()
	execErr := syscall.Exec(MainBinary, args, env)
	if execErr != nil {
		fmt.Printf("exec %s %v error: %v\n", MainBinary, args, execErr)
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
	for _, lib := range [3]string{MainBinary, GolangLib, ClientLib} {
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
