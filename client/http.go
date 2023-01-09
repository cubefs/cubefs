package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
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
	ControlBroadcastRefreshExtents = "/broadcast/refreshExtents"
	ControlReadProcessRegister     = "/readProcess/register"
	ControlGetReadProcs            = "/get/readProcs"
	ControlSetReadWrite            = "/set/readwrite"
	ControlSetReadOnly             = "/set/readonly"
	ControlGetReadStatus           = "/get/readstatus"
	ControlSetUpgrade              = "/set/clientUpgrade"
	ControlUnsetUpgrade            = "/unset/clientUpgrade"

	ControlCommandGetUmpCollectWay = "/umpCollectWay/get"
	ControlCommandSetUmpCollectWay = "/umpCollectWay/set"

	aliveKey         = "alive"
	volKey           = "vol"
	inoKey           = "ino"
	clientKey        = "client" // ip:port
	versionKey       = "version"
	MaxRetry         = 5
	forceKey         = "force"
	CheckFile        = "checkfile"
	TmpLibsPath      = "/tmp/.cfs_client_libs_"
	FuseLibsPath     = "/usr/lib64"
	FuseLib          = "libcfssdk.so"
	TarNamePre       = "cfs-client-libs"
	ADM64            = "amd64"
	ARM64            = "arm64"
	fuseConfigType   = "json"
	bypassConfigType = "ini"
)

var (
	CommitID    string
	BranchName  string
	BuildTime   string
	Debug       string
	NextVersion string
)

func GetVersionHandleFunc(w http.ResponseWriter, r *http.Request) {
	var resp = struct {
		Branch string
		Commit string
		Debug  string
		Build  string
	}{
		Branch: BranchName,
		Commit: CommitID,
		Debug:  Debug,
		Build:  fmt.Sprintf("%s %s %s %s", runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime),
	}
	var encoded []byte
	encoded, _ = json.Marshal(&resp)
	w.Write(encoded)
	return
}

func setReadWrite(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		force bool
	)
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	if err = r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter error: %v", err))
		return
	}
	if forceStr := r.FormValue(forceKey); forceStr != "" {
		if force, err = strconv.ParseBool(forceStr); err != nil {
			buildFailureResp(w, http.StatusBadRequest, "invalid parameter force")
			return
		}
	}
	err = c.mc.ClientAPI().ApplyVolMutex(c.volName, force)
	if err == proto.ErrVolWriteMutexUnable {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume %s not support WriteMutex", c.volName))
		return
	}
	if err == proto.ErrVolWriteMutexOccupied {
		clientIP, err := c.mc.ClientAPI().GetVolMutex(c.volName)
		if err != nil {
			buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume WriteMutex occupied, fail to get holder: %v", err.Error()))
			return
		}
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume WriteMutex occupied by %s", clientIP))
		return
	}
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("set readwrite failed: %v", err.Error()))
		return
	}
	c.readOnly = false
	log.LogInfof("Set ReadWrite %s successfully.", c.volName)
	buildSuccessResp(w, "success")
}

func setReadOnly(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	err := c.mc.ClientAPI().ReleaseVolMutex(c.volName)
	if err == proto.ErrVolWriteMutexUnable {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume %s not support WriteMutex", c.volName))
		return
	}
	if err == proto.ErrVolWriteMutexOccupied {
		clientIP, err := c.mc.ClientAPI().GetVolMutex(c.volName)
		if err != nil {
			buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume WriteMutex occupied by others, fail to get holder: %v", err.Error()))
			return
		}
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume WriteMutex occupied by %s", clientIP))
		return
	}
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("set readonly failed: %v", err.Error()))
		return
	}
	c.readOnly = true
	log.LogInfof("Set ReadOnly %s successfully.", c.volName)
	buildSuccessResp(w, "success")
}

func getReadStatus(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}

	if c.readOnly {
		buildSuccessResp(w, "read only.")
	} else {
		buildSuccessResp(w, "read write.")
	}
	return
}

func broadcastRefreshExtentsHandleFunc(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	if err := r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter error: %v", err))
		return
	}
	vol := r.FormValue(volKey)
	ino, err := strconv.ParseUint(r.FormValue(inoKey), 10, 64)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter [%v] error: %v", inoKey, err))
		return
	}
	if vol == c.volName {
		if err = c.ec.RefreshExtentsCache(context.Background(), ino); err != nil {
			log.LogWarnf("broadcastRefreshExtentsHandleFunc: err(%v), refresh ino(%v)", err, ino)
		}
		log.LogInfof("broadcastRefreshExtentsHandleFunc: refresh ino(%v) vol(%v)", ino, vol)
	}
	buildSuccessResp(w, "success")
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

func (c *fClient) SetClientUpgrade(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Parse parameter error: %v", err))
		return
	}
	version := r.FormValue(versionKey)
	if version == "" {
		buildFailureResp(w, http.StatusBadRequest, "Invalid version parameter.")
		return
	}

	if version == CommitID {
		buildFailureResp(w, http.StatusBadRequest, "Current version is same to expected.")
		return
	}
	if NextVersion != "" && version != NextVersion {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Last version %s is upgrading. please waiting.", NextVersion))
		return
	}
	if version == NextVersion {
		buildSuccessResp(w, "Please waiting")
		return
	}

	if err = checkConfigFile(c.configFile, fuseConfigType, c.volName, c.super.ClusterName()); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Invalid configFile %s: %v", c.configFile, err))
		return
	}

	NextVersion = version
	defer func() {
		if err != nil {
			NextVersion = ""
		}
	}()

	if version == "test" {
		os.Setenv("RELOAD_CLIENT", version)
		buildSuccessResp(w, "Set successful. Upgrading.")
		return
	}
	if version == "reload" {
		os.Setenv("RELOAD_CLIENT", "1")
		buildSuccessResp(w, "Set successful. Upgrading.")
		return
	}

	tmpPath := TmpLibsPath + fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano())
	if err = os.MkdirAll(tmpPath, 0777); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	defer os.RemoveAll(tmpPath)

	_, err = downloadAndCheck(c.mc, tmpPath, version)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	src := filepath.Join(tmpPath, FuseLib)
	dst := filepath.Join(FuseLibsPath, FuseLib+".tmp")
	if err = moveFile(src, dst); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	src = dst
	dst = filepath.Join(FuseLibsPath, FuseLib)
	if err = os.Rename(src, dst); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	os.Setenv("RELOAD_CLIENT", "1")
	buildSuccessResp(w, "Set successful. Upgrading.")
	return
}

func SetClientUpgrade(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	var err error
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	if err = r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Parse parameter error: %v", err))
		return
	}
	version := r.FormValue(versionKey)
	if version == "" {
		buildFailureResp(w, http.StatusBadRequest, "Invalid version parameter.")
		return
	}

	if version == CommitID {
		buildFailureResp(w, http.StatusBadRequest, "Current version is same to expected.")
		return
	}
	if NextVersion != "" && version != NextVersion {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Last version %s is upgrading. please waiting.", NextVersion))
		return
	}
	if version == NextVersion {
		buildSuccessResp(w, "Please waiting")
		return
	}

	if err = checkConfigFile(c.configPath, bypassConfigType, c.volName, c.mw.Cluster()); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Invalid configFile %s: %v", c.configPath, err))
		return
	}

	NextVersion = version
	defer func() {
		if err != nil {
			NextVersion = ""
		}
	}()

	if version == "test" {
		os.Setenv("RELOAD_CLIENT", version)
		buildSuccessResp(w, "Set successful. Upgrading.")
		return
	}
	if version == "reload" {
		os.Setenv("RELOAD_CLIENT", "1")
		buildSuccessResp(w, "Set successful. Upgrading.")
		return
	}

	tmpPath := TmpLibsPath + fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano())
	err = os.MkdirAll(tmpPath, 0777)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	defer os.RemoveAll(tmpPath)

	fileNames, err := downloadAndCheck(c.mc, tmpPath, version)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	targetPath := filepath.Dir(c.configPath)
	for _, fileName := range fileNames {
		if fileName == CheckFile {
			continue
		}

		src := filepath.Join(tmpPath, fileName)
		dst := filepath.Join(targetPath, fileName+".tmp")
		if err = moveFile(src, dst); err != nil {
			buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	for _, fileName := range fileNames {
		if fileName == CheckFile {
			continue
		}
		src := filepath.Join(targetPath, fileName+".tmp")
		dst := filepath.Join(targetPath, fileName)
		if err = os.Rename(src, dst); err != nil {
			buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	os.Setenv("RELOAD_CLIENT", "1")
	buildSuccessResp(w, "Set successful. Upgrading.")
	return
}

func downloadAndCheck(mc *master.MasterClient, tmpPath, version string) (fileNames []string, err error) {
	var tarName string
	if runtime.GOARCH == ADM64 {
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

func UnsetClientUpgrade(w http.ResponseWriter, r *http.Request) {
	os.Unsetenv("RELOAD_CLIENT")
	NextVersion = ""
	buildSuccessResp(w, "Success")
}

func registerReadProcStatusHandleFunc(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	if err := r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter error: %v", err))
		return
	}
	readClient := r.FormValue(clientKey)
	addr := strings.Split(readClient, ":")
	if len(addr) != 2 {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("invalid parameter %s", clientKey))
		return
	}
	if addr[0] == "" {
		readClient = strings.Split(r.RemoteAddr, ":")[0] + readClient
	}
	_, err := strconv.ParseUint(addr[1], 10, 64)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse port error: %v", err))
		return
	}
	alive, err := strconv.ParseBool(r.FormValue(aliveKey))
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter [%v] error: %v", aliveKey, err))
		return
	}

	c.readProcMapLock.Lock()
	if alive {
		if _, ok := c.readProcs[readClient]; !ok {
			c.readProcs[readClient] = time.Now().Format("2006-01-02 15:04:05")
		}
		log.LogInfof("registerReadClient: %s", readClient)
	} else {
		delete(c.readProcs, readClient)
		log.LogInfof("unregisterReadClient: %s", readClient)
	}
	c.readProcMapLock.Unlock()
	buildSuccessResp(w, "success")
}

func (c *client) broadcastRefreshExtents(readClient string, inode uint64) {
	url := fmt.Sprintf("http://%s%s?%s=%s&%s=%d", readClient, ControlBroadcastRefreshExtents, volKey, c.volName, inoKey, inode)
	reply, err := sendWithRetry(url, MaxRetry)
	if err != nil {
		log.LogErrorf("broadcastRefreshExtents: failed, send url(%v) err(%v) reply(%v)", url, err, reply)
	}
}

func (c *client) registerReadProcStatus(alive bool) {
	if len(c.masterClient) == 0 {
		return
	}
	url := fmt.Sprintf("http://%s%s?%s=:%d&%s=%t", c.masterClient, ControlReadProcessRegister, clientKey, gClientManager.profPort, aliveKey, alive)
	if reply, err := sendWithRetry(url, MaxRetry); err != nil {
		msg := fmt.Sprintf("send url(%v) err(%v) reply(%v)", url, err, reply)
		handleError(c, "registerReadProcStatus", msg)
	}
}

func getReadProcsHandleFunc(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	var encoded []byte
	c.readProcMapLock.Lock()
	encoded, _ = json.Marshal(&c.readProcs)
	c.readProcMapLock.Unlock()
	w.Write(encoded)
	return
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
