package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"

	"gopkg.in/ini.v1"
)

const (
	ControlVersion                 = "/version"
	ControlBroadcastRefreshExtents = "/broadcast/refreshExtents"
	ControlReadProcessRegister     = "/readProcess/register"
	ControlGetReadProcs            = "/get/readProcs"
	ControlSetReadWrite            = "/set/readwrite"
	ControlSetReadOnly             = "/set/readonly"
	ControlGetReadStatus           = "/get/readstatus"
	ControlSetUpgrade              = "/set/clientUpgrade"
	ControlUnsetUpgrade            = "/unset/clientUpgrade"
	ControlAccessRoot              = "/access/root"

	ControlCommandGetUmpCollectWay = "/umpCollectWay/get"
	ControlCommandSetUmpCollectWay = "/umpCollectWay/set"

	aliveKey         = "alive"
	volKey           = "vol"
	inoKey           = "ino"
	clientKey        = "client" // ip:port
	versionKey       = "version"
	currentKey       = "current"
	MaxRetry         = 5
	forceKey         = "force"
	CheckFile        = "checkfile"
	TmpLibsPath      = "/tmp/.cfs_client_libs_"
	FuseLibsPath     = "/usr/lib64"
	FuseLib          = "libcfssdk.so"
	TarNamePre       = "cfs-client-libs"
	AMD64            = "amd64"
	ARM64            = "arm64"
	fuseConfigType   = "json"
	bypassConfigType = "ini"

	pidFileSeparator = ";"
)

var (
	CommitID    string
	BranchName  string
	BuildTime   string
	NextVersion string
)

func lockPidFile(pidFile string) error {
	var (
		err   error
		data  []byte
		exist bool
	)
	if pidFile == "" {
		return nil
	}
	exist, err = checkFileExist(pidFile)
	if err != nil {
		return fmt.Errorf("Check pidFile %s failed: %v", pidFile, err)
	}
	if !exist {
		return writePidFile(pidFile)
	}
	data, err = ioutil.ReadFile(pidFile)
	if err != nil {
		return fmt.Errorf("Read pidFile %s failed: %v", pidFile, err)
	}
	pidCmd := strings.Split(string(data), pidFileSeparator)
	if len(pidCmd) < 2 {
		return fmt.Errorf("Invalid pidFile %s. Please remove it first", pidFile)
	}
	cmdFile := fmt.Sprintf("/proc/%s/cmdline", pidCmd[0])
	data, err = ioutil.ReadFile(cmdFile)
	if err != nil {
		return writePidFile(pidFile)
	}
	cmd := string(data)
	if strings.HasPrefix(cmd, pidCmd[1]) {
		return fmt.Errorf("pidFile %s has been locked by pid %s", pidFile, pidCmd[0])
	}
	return writePidFile(pidFile)
}

func checkFileExist(file string) (bool, error) {
	if _, err := os.Stat(file); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func writePidFile(file string) error {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.WriteString(f, fmt.Sprintf("%d%s%s", os.Getpid(), pidFileSeparator, os.Args[0]))
	return err
}

func GetVersionHandleFunc(w http.ResponseWriter, r *http.Request) {
	var resp = struct {
		Branch  string
		Version string
		Commit  string
		Build   string
	}{
		Branch:  BranchName,
		Version: proto.Version,
		Commit:  CommitID,
		Build:   fmt.Sprintf("%s %s %s %s", runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime),
	}
	var encoded []byte
	encoded, _ = json.Marshal(&resp)
	w.Write(encoded)
	return
}

func checkConfigFile(configFile, configFileType, volName, clusterName string) (err error) {
	var (
		actualVolName string
		masterAddr    string
		info          *proto.ClusterInfo
	)
	if configFileType == fuseConfigType {
		cfg, err := config.LoadConfigFile(configFile)
		if err != nil {
			return err
		}
		opt, err := parseMountOption(cfg)
		if err != nil {
			return err
		}
		actualVolName = opt.Volname
		masterAddr = opt.Master
	}

	if configFileType == bypassConfigType {
		cfg, err := ini.Load(configFile)
		if err != nil {
			return err
		}
		actualVolName = cfg.Section("").Key("volName").String()
		masterAddr = cfg.Section("").Key("masterAddr").String()
	}

	if actualVolName != volName {
		err = fmt.Errorf("actual volName: %s, expect: %s", actualVolName, volName)
		return
	}

	masters := strings.Split(masterAddr, ",")
	mc := master.NewMasterClient(masters, false)
	if info, err = mc.AdminAPI().GetClusterInfo(); err != nil {
		err = fmt.Errorf("get cluster info fail: err(%v). Please check masterAddr %s and retry.", err, masterAddr)
		return err
	}
	if info.Cluster != clusterName {
		err = fmt.Errorf("actual clusterName: %s, expect: %s", info.Cluster, clusterName)
		return
	}
	return nil
}

func downloadAndCheck(mc *master.MasterClient, tmpPath, version string) (fileNames []string, err error) {
	var tarName string
	if runtime.GOARCH == AMD64 {
		tarName = fmt.Sprintf("%s_%s.tar.gz", TarNamePre, version)
	} else if runtime.GOARCH == ARM64 {
		tarName = fmt.Sprintf("%s_%s_%s.tar.gz", TarNamePre, ARM64, version)
	} else {
		err = fmt.Errorf("cpu arch %s not supported", runtime.GOARCH)
		return nil, err
	}

	if err = downloadClientPkg(mc, tarName, tmpPath); err != nil {
		return nil, err
	}

	if fileNames, err = untar(filepath.Join(tmpPath, tarName), tmpPath); err != nil {
		return nil, fmt.Errorf("Untar %s err: %v", tarName, err)
	}

	var checkMap map[string]string
	if checkMap, err = readCheckfile(filepath.Join(tmpPath, CheckFile)); err != nil {
		return nil, fmt.Errorf("Invalid checkfile: %v", err)
	}

	if !checkFiles(fileNames, checkMap, tmpPath) {
		return nil, fmt.Errorf("check libs faild. Please try again.")
	}

	return fileNames, nil
}

func downloadClientPkg(mc *master.MasterClient, fileName, downloadPath string) (err error) {
	addr, err := mc.ClientAPI().GetClientPkgAddr()
	if err != nil {
		return err
	}
	var url string
	var resp *http.Response
	if addr[len(addr)-1] == '/' {
		url = fmt.Sprintf("%s%s", addr, fileName)
	} else {
		url = fmt.Sprintf("%s/%s", addr, fileName)
	}
	for i := 0; i < MaxRetry; i++ {
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

	dstFile := filepath.Join(downloadPath, fileName)
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

		err = ioutil.WriteFile(dst, input, 0755)
		if err != nil {
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
			log.LogWarnf("checkFiles: find no expected checksum of %s\n", fileName)
			return false
		}
		md5, err := getFileMd5(filepath.Join(tmpPath, fileName))
		if err != nil {
			log.LogWarnf("checkFiles: get checksum of %s err: %v\n", fileName, err)
			return false
		}

		if md5 != expected {
			log.LogWarnf("checkFiles: check md5 of %s failed. expected: %s, actually: %s\n", fileName, expected, md5)
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

func sendWithRetry(url string, maxRetry int) (reply *proto.HTTPReply, err error) {
	for i := 0; i < maxRetry; i++ {
		if reply, err = sendURL(url); err != nil {
			log.LogWarnf("sendURL failed, url(%v) err(%v) reply(%v). Retry......", url, err, reply)
			continue
		}
		break
	}
	return
}

func sendURL(reqURL string) (reply *proto.HTTPReply, err error) {
	client := http.DefaultClient
	client.Timeout = 500 * time.Millisecond
	resp, err := client.Get(reqURL)
	if err != nil {
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("resp status code[%v]", resp.StatusCode)
		return
	}
	reply = &proto.HTTPReply{}
	if err = json.Unmarshal(body, reply); err != nil {
		return
	}
	if reply.Code != http.StatusOK {
		err = fmt.Errorf("failed,msg[%v],data[%v]", reply.Msg, reply.Data)
		return
	}
	log.LogInfof("client send url(%v) reply(%v)", reqURL, reply)
	return
}

func buildSuccessResp(w http.ResponseWriter, data interface{}) {
	buildJSONResp(w, http.StatusOK, data, "")
}

func buildFailureResp(w http.ResponseWriter, code int, msg string) {
	buildJSONResp(w, code, nil, msg)
}

// Create response for the API request.
func buildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := &proto.HTTPReply{
		Code: int32(code),
		Msg:  msg,
		Data: data,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	_, _ = w.Write(jsonBody)
}

func GetUmpCollectWay(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf("%v\n", proto.UmpCollectByStr(ump.GetUmpCollectWay()))))
}

func SetUmpCollectWay(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	if way := r.FormValue("way"); way != "" {
		val, err := strconv.Atoi(way)
		if err != nil {
			w.Write([]byte("Set ump collect way failed\n"))
		} else {
			ump.SetUmpCollectWay(proto.UmpCollectBy(val))
			w.Write([]byte(fmt.Sprintf("Set ump collect way to %v successfully\n", proto.UmpCollectByStr(proto.UmpCollectBy(val)))))
		}
	} else {
		w.Write([]byte("Unrecognized way\n"))
	}
}

func UnsetClientUpgrade(w http.ResponseWriter, r *http.Request) {
	os.Unsetenv("RELOAD_CLIENT")
	NextVersion = ""
	buildSuccessResp(w, "Success")
}
