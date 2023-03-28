package main

import "C"

import (
	syslog "log"
	"runtime"
	"unsafe"
)

//export InitModule
func InitModule(initTask unsafe.Pointer) {
	pluginpath, _, errstr := lastmoduleinit()
	doInit(initTask)
	if errstr != "" {
		syslog.Printf("module init res, pluginpath: %s, err: %s\n", pluginpath, errstr)
	}
}

//export FinishModule
func FinishModule(finiTask unsafe.Pointer) {
	doFini(finiTask)
	runtime.RemoveLastModuleitabs()
	runtime.RemoveLastModule()
}

//go:linkname doInit runtime.doInit
func doInit(t unsafe.Pointer) // t should be a *runtime.initTask

//go:linkname doFini runtime.doFini
func doFini(t unsafe.Pointer) // t should be a *runtime.finiTask

//go:linkname lastmoduleinit plugin.lastmoduleinit
func lastmoduleinit() (pluginpath string, syms map[string]interface{}, errstr string)
