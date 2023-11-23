package main

import (
	"fmt"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"k8s.io/mount-utils"
	"log"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"time"
)

type cfsOption struct {
	AccessKey            string `json:"accessKey"`
	AttrValid            string `json:"attrValid"`
	Authenticate         string `json:"authenticate"`
	AutoInvalData        string `json:"autoInvalData"`
	BcacheBatchCnt       string `json:"bcacheBatchCnt"`
	BcacheCheckIntervalS string `json:"bcacheCheckIntervalS"`
	BcacheDir            string `json:"bcacheDir"`
	BcacheFilterFiles    string `json:"bcacheFilterFiles"`
	BuffersTotalLimit    string `json:"buffersTotalLimit"`
	C                    string `json:"c"`
	CacheAction          string `json:"cacheAction"`
	CertFile             string `json:"certFile"`

	ClientKey            string `json:"clientKey"`
	DisableDcache        string `json:"disableDcache"`
	EbsBlockSize         string `json:"ebsBlockSize"`
	EbsEndpoint          string `json:"ebsEndpoint"`
	EbsServerPath        string `json:"ebsServerPath"`
	EnablePosixACL       string `json:"enablePosixACL"`
	EnSyncWrite          string `json:"enSyncWrite"`
	EnableAudit          string `json:"enableAudit"`
	EnableHTTPS          string `json:"enableHTTPS"`
	EnableSummary        string `json:"enableSummary"`
	EnableUnixPermission string `json:"enableUnixPermission"`
	EnableXattr          string `json:"enableXattr"`
	FollowerRead         string `json:"followerRead"`
	FsyncOnClose         string `json:"fsyncOnClose"`
	IcacheTimeout        string `json:"icacheTimeout"`
	Keepcache            string `json:"keepcache"`
	LogDir               string `json:"logDir"`
	LogLevel             string `json:"logLevel"`
	LookupValid          string `json:"lookupValid"`
	MasterAddr           string `json:"masterAddr"`
	MaxStreamerLimit     string `json:"maxStreamerLimit"`
	Maxcpus              string `json:"maxcpus"`
	MetaSendTimeout      string `json:"metaSendTimeout"`
	MountPoint           string `json:"mountPoint"`
	N                    string `json:"n"`
	NearRead             string `json:"nearRead"`
	Owner                string `json:"owner"`
	P                    string `json:"p"`
	ProfPort             string `json:"profPort"`
	R                    string `json:"r"`
	Rdonly               string `json:"rdonly"`
	ReadRate             string `json:"readRate"`
	ReadThreads          string `json:"readThreads"`
	S                    string `json:"s"`
	SecretKey            string `json:"secretKey"`
	Subdir               string `json:"subdir"`
	TicketHost           string `json:"ticketHost"`
	VolName              string `json:"volName"`
	VolType              string `json:"volType"`
	WarnLogDir           string `json:"warnLogDir"`
	WriteRate            string `json:"writeRate"`
	WriteThreads         string `json:"writeThreads"`
	Writecache           string `json:"writecache"`
}

func (c *cfsOption) ConvertToCliOptions() (s []string) {
	tp := reflect.TypeOf(*c)
	el := reflect.ValueOf(c).Elem()

	// run foreground, since "daemon start failed" issue
	s = append(s, "-f")

	for i := 0; i < tp.NumField(); i++ {
		fi := tp.Field(i)

		js := fi.Tag.Get("json")
		if js == "" {
			continue
		}

		v := strings.Trim(el.FieldByName(fi.Name).String(), " ")
		if v != "" {
			if js == "n" || js == "r" {
				s = append(s, "-"+js)
			} else {
				s = append(s, "-"+js, v)
			}
		}

	}

	return s

}

// cfsParse Parse from string
func cfsParse(mountPoint, options string) (c *cfsOption) {
	c = &cfsOption{MountPoint: mountPoint}

	sli := strings.Split(options, ",")

	el := reflect.ValueOf(c).Elem()

	cas := cases.Title(language.Und, cases.NoLower)

	for _, v := range sli {
		oSli := strings.Split(v, "=")
		if len(oSli) == 0 {
			continue
		}

		field := el.FieldByName(cas.String(oSli[0]))
		if !field.CanSet() {
			log.Println("Warn: field can not set. field: ", oSli[0])
			continue
		}

		if len(oSli) == 1 {
			field.SetString("1")
		} else {
			field.SetString(strings.Join(oSli[1:], "="))
		}

	}

	return
}

func cfsIsMounted(mountPoint string) bool {
	mo := mount.New("")

	mps, err := mo.List()
	if err != nil {
		log.Fatalf("mountListErr, err: %s", err.Error())
	}

	for _, v := range mps {
		if v.Path == mountPoint {
			return true
		}
	}

	return false

}

// cfsMountPre exception handling
func cfsMountPre(mountPoint string) error {
	if _, err := os.Stat(mountPoint); os.IsNotExist(err) {
		return err
	}

	if cfsIsMounted(mountPoint) {
		return cfsUmount(mountPoint)
	}

	return nil
}

func cfsList() error {
	mo := mount.New("")

	mps, err := mo.List()
	if err != nil {
		log.Fatalf("mountListErr, err: %s", err.Error())
	}

	for _, v := range mps {
		if v.Type == "fuse" || v.Type == "fuse.cubefs" {
			fmt.Println(fmt.Sprintf("%s on %s type %s (%s)", v.Device, v.Path, v.Type, strings.Join(v.Opts, ",")))
		}
	}

	return nil
}

func cfsMount(mountPoint, options string) error {
	err := cfsMountPre(mountPoint)
	if err != nil {
		return err
	}

	c := cfsParse(mountPoint, options)

	co := c.ConvertToCliOptions()

	log.Println("cfsMount commands: ", getCfsClientPath(), strings.Join(co, " "))
	cmd := exec.Command(getCfsClientPath(), co...)
	cmd.Env = append(cmd.Env, "PATH=/usr/bin/:/bin/:$PATH")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	log.Println("[info] cfsMount, env: ", cmd.Env)

	err = cmd.Start()
	if err != nil {
		log.Println("cfsMountCmdStartErr, err: ", err.Error())
		return err
	}

	log.Println("[info] cfsMount started")

	err = cfsMountPost(mountPoint)
	if err != nil {
		return err
	}

	// err = cmd.Wait()
	// if err != nil {
	//     log.Println("[error] cfsMountWaitErr, err: ", err)
	//     return nil
	// }

	return nil
}

// cfsMountPost async mount check
func cfsMountPost(mountPoint string) error {
	log.Println("[info] cfsMountPost run")

	for i := 0; i < 20*5; i++ {
		if cfsIsMounted(mountPoint) {
			log.Println("[info] cfsMount mounted")
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return nil

}

func cfsUmount(mountPoint string) error {
	mo := mount.New("")
	return mo.Unmount(mountPoint)
}
