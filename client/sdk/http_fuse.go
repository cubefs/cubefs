package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
)

const (
	ControlAccessRoot = "/access/root"
)

func (c *fClient) SetClientUpgrade(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Parse parameter error: %v", err))
		return
	}

	vol := r.FormValue(volKey)
	if vol != "" && vol != c.volName {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume %s is not expected to update", c.volName))
		return
	}

	current := r.FormValue(currentKey)
	if current != "" && current != CommitID {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Current version %s is not expected to update", CommitID))
		return
	}

	version := r.FormValue(versionKey)
	if version == "" {
		buildFailureResp(w, http.StatusBadRequest, "Invalid version parameter.")
		return
	}

	if version == CommitID {
		buildSuccessResp(w, fmt.Sprintf("Current version %s is same to expected.", CommitID))
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

	if err = checkFuseConfigFile(c.configFile, c.volName, c.super.ClusterName()); err != nil {
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

func (c *fClient) AccessRoot(w http.ResponseWriter, r *http.Request) {
	if _, err := os.Stat(c.mountPoint); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Access root error: %v", err))
	} else {
		buildSuccessResp(w, "Success")
	}
	return
}

func checkFuseConfigFile(configFile, volName, clusterName string) (err error) {
	var (
		actualVolName string
		masterAddr    string
		info          *proto.ClusterInfo
	)
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
