package gohook

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"unsafe"
)

type HookInfo struct {
	Mode        int
	Info        *CodeInfo
	Target      reflect.Value
	Replacement reflect.Value
	Trampoline  reflect.Value
}

var (
	archMode = 64
	g_all    = make(map[uintptr]HookInfo)
)

func init() {
	sz := unsafe.Sizeof(uintptr(0))
	if sz == 4 {
		archMode = 32
	}
}

func GetArchMode() int {
	return archMode
}

// valueStruct is taken from runtime source code.
// it may be changed in later release
type valueStruct struct {
	typ uintptr
	ptr uintptr
}

func getDataPtrFromValue(v reflect.Value) uintptr {
	return (uintptr)((*valueStruct)(unsafe.Pointer(&v)).ptr)
}

func ShowDebugInfo() string {
	buff := bytes.NewBuffer(make([]byte, 0, 256))
	for k, v := range g_all {
		s := fmt.Sprintf("hook function at addr:%x, how:%s, num of instruction fixed:%d\n", k, v.Info.How, len(v.Info.Fix))

		buff.WriteString(s)
		for _, f := range v.Info.Fix {
			s = fmt.Sprintf("==@%08x    new inst:", f.Addr)
			buff.WriteString(s)
			for _, c := range f.Code {
				s = fmt.Sprintf("%02x ", c)
				buff.WriteString(s)
			}
			s = fmt.Sprintf("\n")
			buff.WriteString(s)
		}
	}

	return string(buff.Bytes())
}

func Hook(target, replacement, trampoline interface{}) error {
	t := reflect.ValueOf(target)
	r := reflect.ValueOf(replacement)
	t2 := reflect.ValueOf(trampoline)
	return doHook(archMode, false, t, r, t2)
}

func HookByIndirectJmp(target, replacement, trampoline interface{}) error {
	t := reflect.ValueOf(target)
	r := reflect.ValueOf(replacement)
	t2 := reflect.ValueOf(trampoline)
	return doHook(archMode, true, t, r, t2)
}

func UnHook(target interface{}) error {
	t := reflect.ValueOf(target)
	return doUnHook(t.Pointer())
}

func HookMethod(instance interface{}, method string, replacement, trampoline interface{}) error {
	target := reflect.TypeOf(instance)
	m, ok := target.MethodByName(method)
	if !ok {
		return fmt.Errorf("unknown method %s.%s()", target.Name(), method)
	}
	r := reflect.ValueOf(replacement)
	t := reflect.ValueOf(trampoline)
	return doHook(archMode, false, m.Func, r, t)
}

func UnHookMethod(instance interface{}, methodName string) error {
	target := reflect.TypeOf(instance)
	m, ok := target.MethodByName(methodName)
	if !ok {
		return errors.New(fmt.Sprintf("unknown method %s", methodName))
	}

	return UnHook(m.Func.Interface())
}

func doUnHook(target uintptr) error {
	info, ok := g_all[target]
	if !ok {
		return errors.New("target not exist")
	}

	CopyInstruction(target, info.Info.Origin)

	if info.Info.How == "fix" {
		for _, v := range info.Info.Fix {
			CopyInstruction(v.Addr, v.Code)
		}
	}

	if info.Trampoline.IsValid() {
		CopyInstruction(info.Trampoline.Pointer(), info.Info.TrampolineOrig)
	}

	delete(g_all, target)

	return nil
}

func doHook(mode int, rdxIndirect bool, target, replacement, trampoline reflect.Value) error {
	if target.Kind() != reflect.Func {
		return fmt.Errorf("target must be a Func")
	}

	if replacement.Kind() != reflect.Func {
		return fmt.Errorf("replacement must be a Func")
	}

	if target.Type() != replacement.Type() {
		return fmt.Errorf("target and replacement must have the same type %s != %s", target.Type().Name(), replacement.Type().Name())
	}

	tp := uintptr(0)
	if trampoline.IsValid() {
		if trampoline.Kind() != reflect.Func {
			return fmt.Errorf("replacement must be a Func")
		}

		if target.Type() != trampoline.Type() {
			return fmt.Errorf("target and trampoline must have the same type %s != %s", target.Type().Name(), trampoline.Type().Name())
		}

		tp = trampoline.Pointer()
	}

	doUnHook(target.Pointer())

	replaceAddr := replacement.Pointer()
	if rdxIndirect {
		// get data ptr out of a reflect value.
		replaceAddr = getDataPtrFromValue(replacement)
	}

	info, err := hookFunction(mode, rdxIndirect, target.Pointer(), replaceAddr, tp)
	if err != nil {
		return err
	}

	g_all[target.Pointer()] = HookInfo{Mode: mode, Info: info, Target: target, Replacement: replacement, Trampoline: trampoline}

	return nil
}
