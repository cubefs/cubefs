package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	configFile       = flag.String("c", "", "FUSE client config file")
	configForeground = flag.Bool("f", false, "run foreground")
	configVersion    = flag.Bool("v", false, "Show client version")
	configUseVersion = flag.String("u", "", "Use this client version, GLIBC_2.14 or newer is required")
)

const (
	MainBinary            = "/usr/lib64/cfs-client-inner"
	MainStaticBinary      = "/usr/lib64/cfs-client-static"
	ClientLib             = "/usr/lib64/libcfssdk.so"
	GolangLib             = "/usr/lib64/libstd.so"
	TmpLibsPath           = "/tmp/.cfs_client_fuse"
	FuseLibsPath          = "/usr/lib64"
	CheckFile             = "checkfile"
	MasterAddr            = "masterAddr"
	AdminGetClientPkgAddr = "/clientPkgAddr/get"
	RetryTimes            = 5
	TarNamePre            = "cfs-client-fuse"
	VersionTarPre         = "cfs-client-libs"
	AMD64                 = "amd64"
	ARM64                 = "arm64"
	DefaultMasterAddr     = "cn.chubaofs.jd.local"

	StartRetryMaxCount    = 10
	StartRetryIntervalSec = 5
)

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
	for retry := 0; retry < StartRetryMaxCount; retry++ {
		if err != nil {
			time.Sleep(StartRetryIntervalSec * time.Second)
		}
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
					return addr, err
				}
			default:
				err = fmt.Errorf("url(%v) status(%v) body(%s)", url, stateCode, strings.Replace(string(respData), "\n", "", -1))
			}
		}
	}

	return addr, err
}

func prepareLibs(downloadAddr, tarName string) bool {
	var err error
	tmpPath := TmpLibsPath + fmt.Sprintf("%d", time.Now().Unix())
	if err = os.MkdirAll(tmpPath, 0777); err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpPath)

	if err = downloadClientPkg(downloadAddr, tmpPath, tarName); err != nil {
		fmt.Printf("%v\n", err)
		return false
	}

	var fileNames []string
	if fileNames, err = untar(filepath.Join(tmpPath, tarName), tmpPath); err != nil {
		fmt.Printf("Untar %s err: %v", tarName, err)
		return false
	}

	var checkMap map[string]string
	if checkMap, err = readCheckfile(filepath.Join(tmpPath, CheckFile)); err != nil {
		fmt.Printf("Invalid checkfile: %v", err)
		return false
	}

	if !checkFiles(fileNames, checkMap, tmpPath) {
		fmt.Printf("check libs faild\n")
		return false
	}

	for _, fileName := range fileNames {
		if fileName == CheckFile {
			continue
		}

		src := filepath.Join(tmpPath, fileName)
		dst := filepath.Join(FuseLibsPath, fileName+".tmp")
		if err = moveFile(src, dst); err != nil {
			fmt.Printf("%v\n", err.Error())
			return false
		}
	}

	for _, fileName := range fileNames {
		if fileName == CheckFile {
			continue
		}
		src := filepath.Join(FuseLibsPath, fileName+".tmp")
		dst := filepath.Join(FuseLibsPath, fileName)
		if err = os.Rename(src, dst); err != nil {
			fmt.Printf("%v\n", err.Error())
			return false
		}
	}
	return true
}

func downloadClientPkg(addr, tmpPath, tarName string) (err error) {
	var url string
	var resp *http.Response
	if addr[len(addr)-1] == '/' {
		url = fmt.Sprintf("%s%s", addr, tarName)
	} else {
		url = fmt.Sprintf("%s/%s", addr, tarName)
	}
	for i := 0; i < RetryTimes; i++ {
		resp, err = http.Get(url)
		if err != nil {
			continue
		}
		if resp.StatusCode == 200 {
			err = nil
			break
		} else {
			err = fmt.Errorf("Download %s error: %s", url, resp.Status)
			resp.Body.Close()
			continue
		}
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dstFile := filepath.Join(tmpPath, tarName)
	file, err := os.OpenFile(dstFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
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
	return nil
}

func moveFile(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		input, err := ioutil.ReadFile(src)
		if err != nil {
			return err
		}

		if err = ioutil.WriteFile(dst, input, 0755); err != nil {
			return err
		}
		if err = os.Remove(src); err != nil {
			return err
		}
	}
	return nil
}

func untar(tarName, xpath string) ([]string, error) {
	tarFile, err := os.Open(tarName)
	if err != nil {
		return nil, err
	}
	defer tarFile.Close()

	tr := tar.NewReader(tarFile)
	if strings.HasSuffix(tarName, ".gz") || strings.HasSuffix(tarName, ".gzip") {
		gz, err := gzip.NewReader(tarFile)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		tr = tar.NewReader(gz)
	}

	fileNames := make([]string, 0, 3)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		finfo := hdr.FileInfo()
		if finfo.Mode().IsDir() {
			continue
		}

		fileName := filepath.Join(xpath, hdr.Name)
		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, finfo.Mode().Perm())
		if err != nil {
			return nil, err
		}

		n, err := io.Copy(file, tr)
		if err != nil {
			file.Close()
			return nil, err
		}
		if err = file.Close(); err != nil {
			return nil, err
		}

		if n != finfo.Size() {
			return nil, fmt.Errorf("unexpected bytes written: wrote %d, want %d", n, finfo.Size())
		}
		fileNames = append(fileNames, hdr.Name)
	}
	return fileNames, nil
}

func checkFiles(fileNames []string, checkMap map[string]string, tmpPath string) bool {
	for _, fileName := range fileNames {
		if fileName == CheckFile {
			continue
		}
		expected, exist := checkMap[fileName]
		if !exist {
			fmt.Printf("checkFiles: find no expected checksum of %s\n", fileName)
			return false
		}
		md5, err := getFileMd5(filepath.Join(tmpPath, fileName))
		if err != nil {
			fmt.Printf("checkFiles: get checksum of %s err: %v\n", fileName, err)
			return false
		}

		if md5 != expected {
			fmt.Printf("checkFiles: check md5 of %s failed. expected: %s, actually: %s\n", fileName, expected, md5)
			return false
		}
	}
	return true
}

func getFileMd5(fileName string) (string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return "", err
	}
	defer file.Close()

	md5h := md5.New()
	io.Copy(md5h, file)

	return hex.EncodeToString(md5h.Sum(nil)), nil
}

func readCheckfile(fileName string) (map[string]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	checkMap := make(map[string]string)

	body, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	for _, line := range strings.Split(string(body), "\n") {
		field := strings.Fields(line)
		if len(field) < 2 {
			continue
		}
		checkMap[field[1]] = field[0]
	}
	return checkMap, nil
}
