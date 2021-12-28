package gohook

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

//go:noinline
func foo_short_call(a int) (int, error) {
	//fmt.Printf("calling short call origin func\n")
	return 3 + foo_short_call2(a), nil
}

//go:noinline
func foo_short_call2(a int) int {
	fmt.Printf("in short call2\n")
	return 3 + a
}

//go:noinline
func foo_short_call_replace(a int) (int, error) {
	fmt.Printf("calling short call replace func\n")
	r, _ := foo_short_call_trampoline(a)
	return a + 1000 + r, nil
}

func dummy_delimiter(id string) string {
	for {
		fmt.Printf("calling victim trampoline")
		if id == "miliao" {
			return "done"
		}
		break
	}

	ret := "miliao"
	ret += foo_for_inplace_fix("test")
	ret += foo_for_inplace_fix("test")
	ret += foo_for_inplace_fix("test")
	ret += foo_for_inplace_fix("test")

	fmt.Printf("len1:%d\n", len(id))
	fmt.Printf("len2:%d\n", len(ret))

	ret += foo_for_inplace_fix_delimiter(id)

	return id + ret
}

//go:noinline
func foo_short_call_trampoline(a int) (int, error) {
	for {
		fmt.Printf("printing a:%d\n", a)
		a++
		if a > 233 {
			fmt.Printf("done printing a:%d\n", a)
			break
		}
	}

	dummy_delimiter("miliao")

	return a + 233, nil
}

func TestShortCall(t *testing.T) {
	r, _ := foo_short_call(32)
	assert.Equal(t, 38, r)

	addr := GetFuncAddr(foo_short_call)
	sz1 := GetFuncInstSize(foo_short_call)
	addr2 := addr + uintptr(sz1)
	fmt.Printf("start hook real short call func, start:%x, end:%x\n", addr, addr2)

	err := Hook(foo_short_call, foo_short_call_replace, foo_short_call_trampoline)
	assert.Nil(t, err)

	r1, _ := foo_short_call(22)
	assert.Equal(t, 1050, r1)

	UnHook(foo_short_call)

	r2, _ := foo_short_call(32)
	assert.Equal(t, 38, r2)

	code := make([]byte, 0, sz1)
	for i := 0; i < int(sz1); i++ {
		code = append(code, 0x90)
	}

	code1 := []byte{0xeb, 0x4}
	code2 := []byte{0xeb, 0x5}

	copy(code, code1)
	copy(code[2:], code2)

	ret := sz1 - 5
	jmp1 := sz1 - 4
	jmp2 := sz1 - 2

	if sz1 > 0x7f {
		ret = 0x70 - 5
		jmp1 = 0x70 - 4
		jmp2 = 0x70 - 2
	}

	code[ret] = byte(0xc3)

	code3 := []byte{0xeb, byte(-jmp1 - 2)}
	code4 := []byte{0xeb, byte(-jmp2 - 2)}

	copy(code[jmp1:], code3)
	copy(code[jmp2:], code4)

	assert.Equal(t, code[:4], append(code1, code2...))

	CopyInstruction(addr, code)

	err = Hook(foo_short_call, foo_short_call_replace, foo_short_call_trampoline)
	assert.Nil(t, err)

	fmt.Printf("fix code for foo_short_call:\n%s\n", ShowDebugInfo())

	foo_short_call(22)

	addr3 := addr2 + uintptr(2)
	fc := runtime.FuncForPC(addr3)

	assert.NotNil(t, fc)

	fmt.Printf("func name get from addr beyond scope:%s\n", fc.Name())
	assert.Equal(t, addr, fc.Entry())

	f, l := fc.FileLine(addr2 + uintptr(3))
	assert.Equal(t, 0, l)
	assert.Equal(t, "?", f)
	fmt.Printf("file:%s, line:%d\n", f, l)
}
