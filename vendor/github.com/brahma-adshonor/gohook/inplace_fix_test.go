package gohook

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func inplaceFix(a, b, c int, e, f, g string) int {
	fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)

	for {
		if (a % 2) != 0 {
			fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
		} else {
			a++
		}

		if a+b > 100 {
			break
		}

		buff := bytes.NewBufferString("something weird")
		fmt.Printf("len:%d\n", buff.Len())
	}

	return 1
}

func TestRipRelativeAddr(t *testing.T) {
	toAddr := uintptr(0x10101042)
	code := []byte{0x48, 0x8b, 0x05, 0x11, 0x22, 0x33, 0x44}
	addr := calcJumpToAbsAddr(GetArchMode(), toAddr, code)

	assert.Equal(t, toAddr+7+0x44332211, addr)
}

func TestFixInplace(t *testing.T) {
	d0 := int32(0x01b1)
	d1 := byte(0xb2) // byte(-78)
	d2 := byte(0xa8) // byte(-88)

	prefix1 := []byte{
		0x64, 0x48, 0x8b, 0x0c, 0x25, 0xf8, 0xff, 0xff, 0xff, // 9
		0x48, 0x3b, 0x61, 0x10, // 4
	}

	jc0 := []byte{
		0x0f, 0x86, byte(d0), byte(d0 >> 8), 0x00, 0x00, // jmp
	}

	prefix2 := []byte{
		0x48, 0x83, 0xec, 0x58, // 4
		0x48, 0x89, 0x6c, 0x24, 0x50, // 5
		0x48, 0x8d, 0x6c, 0x24, 0x50, // 5
		0x90, // 1
	}

	prefix3rip := []byte{
		0x48, 0x8b, 0x05, 0xc7, 0x5f, 0x16, 0x00, // 7  mov $rip offset
	}

	prefix4rip := []byte{
		0x48, 0x8d, 0x0d, 0x50, 0x85, 0x07, 0x00, // 7 lea $rip offset
	}

	prefix5 := []byte{
		0x48, 0x89, 0x0c, 0x24, // 4
		0x48, 0x89, 0x44, 0x24, 0x08, // 5
	}

	prefix6rip := []byte{
		0x48, 0x8d, 0x05, 0x9f, 0xe0, 0x04, 0x00, // 7   lea $rip offset
	}

	prefix7 := []byte{
		0x48, 0x89, 0x44, 0x24, 0x10, // 5
		0x48, 0xc7, 0x44, 0x24, 0x18, 0x07, 0x00, 0x00, 0x00, // 9
	}
	// totoal 78 bytes

	// short jump
	jc1 := []byte{0xeb, d1} // 2

	mid := []byte{
		0x0f, 0x57, 0xc0, // 3
		0x0f, 0x11, 0x44, 0x24, 0x28, // 5
	}

	jc2 := []byte{
		// condition jump
		0x77, d2, // 2
	}

	posfix := []byte{
		// trailing
		0xcc, 0xcc, 0xcc, 0xcc,
		0xcc, 0xcc, 0xcc, 0xcc,
		0xcc, 0xcc, 0xcc, 0xcc,
		0xcc, 0xcc, 0xcc, 0xcc,
	}

	var fc []byte
	all := [][]byte{prefix1, jc0, prefix2, prefix3rip, prefix4rip, prefix5, prefix6rip, prefix7, jc1, mid, jc2, posfix}
	for k := range all {
		fc = append(fc, all[k]...)
	}

	info := &CodeInfo{}
	addr := GetFuncAddr(inplaceFix)
	size := len(fc)
	mvSize := 0x09
	toAddr := GetFuncAddr(inplaceFix2)

	curAddr1 := addr + uintptr(78)
	curAddr2 := addr + uintptr(78) + uintptr(10)

	CopyInstruction(addr, fc)

	fs := makeSliceFromPointer(addr, len(fc))
	raw := make([]byte, len(fc))
	copy(raw, fs)

	fmt.Printf("src func:%x, target func:%x\n", addr, toAddr)

	err := doFixFuncInplace(64, addr, toAddr, int(size), mvSize, info, 5)

	assert.Nil(t, err)
	assert.True(t, len(raw) >= len(info.Origin))
	raw = raw[:len(info.Origin)]
	assert.Equal(t, raw, info.Origin)
	assert.Equal(t, 18, len(info.Fix))
	assert.Equal(t, prefix1[:5], fs[:5])

	off0 := d0 + 4
	fix0, _ := adjustInstructionOffset(jc0, int64(off0))
	fmt.Printf("inplace fix, off0:%x, sz:%d\n", off0, len(fix0))

	rip3Addr := addr + uintptr(len(prefix1)+len(jc0)+len(prefix2))
	ripOff3 := calcJumpToAbsAddr(GetArchMode(), rip3Addr, prefix3rip) - uintptr(len(prefix3rip)) - rip3Addr + uintptr(4)
	prefix3ripFix, _ := adjustInstructionOffset(prefix3rip, int64(ripOff3))

	rip4Addr := addr + uintptr(len(prefix1)+len(jc0)+len(prefix2)+len(prefix3rip))
	ripOff4 := calcJumpToAbsAddr(GetArchMode(), rip4Addr, prefix4rip) - uintptr(len(prefix4rip)) - rip4Addr + uintptr(4)
	prefix4ripFix, _ := adjustInstructionOffset(prefix4rip, int64(ripOff4))

	rip6Addr := addr + uintptr(len(prefix1)+len(jc0)+len(prefix2)+len(prefix3rip)+len(prefix4rip)+len(prefix5))
	ripOff6 := calcJumpToAbsAddr(GetArchMode(), rip6Addr, prefix6rip) - uintptr(len(prefix6rip)) - rip6Addr + uintptr(4)
	prefix6ripFix, _ := adjustInstructionOffset(prefix6rip, int64(ripOff6))

	to1 := curAddr1 + uintptr(2) + uintptr(int32(int8(d1)))
	newTo1 := toAddr + to1 - addr
	off1 := int64(newTo1 - (curAddr1 - uintptr(4)) - 5)
	fix1, _ := translateJump(off1, jc1)
	fmt.Printf("inplace fix, off1:%x, sz:%d\n", off1, len(fix1))

	to2 := curAddr2 + uintptr(2) + uintptr(int32(int8(d2)))
	newTo2 := toAddr + to2 - addr
	off2 := int64(newTo2 - (curAddr2 + uintptr(3) - uintptr(4)) - 6)
	fix2, _ := translateJump(off2, jc2)
	fmt.Printf("inplace fix, off2:%x, sz:%d\n", off2, len(fix2))

	all2 := [][]byte{prefix1[:5], prefix1[9:], fix0, prefix2, prefix3ripFix, prefix4ripFix, prefix5, prefix6ripFix, prefix7, fix1, mid, fix2, posfix}
	var fc2 []byte
	for k := range all2 {
		fc2 = append(fc2, all2[k]...)
	}
	assert.Equal(t, len(fc)-4+3+4, len(fc2))

	fs = makeSliceFromPointer(addr, len(fc2)-len(posfix))
	assert.Equal(t, fc2[:len(fc2)-len(posfix)], fs)
}

func inplaceFix2(a, b, c int, e, f, g string) int {
	fmt.Printf("calling inplacefix2()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling inplacefix2()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling inplacefix2()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling inplacefix2()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling inplacefix2()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling inplacefix2()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling inplacefix2()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling inplacefix2()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling inplacefix2()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
	fmt.Printf("calling inplacefix2()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)

	for {
		if (a % 2) != 0 {
			fmt.Printf("calling victim()(%d,%s,%s,%x):%dth\n", a, e, f, c, 0x23)
		} else {
			a++
		}

		if a+b > 100 {
			break
		}

		buff := bytes.NewBufferString("something weird")
		fmt.Printf("len:%d\n", buff.Len())
	}

	return 1
}

func foo_for_inplace_fix(id string) string {
	c := 0
	for {
		fmt.Printf("calling victim\n")
		if id == "miliao" {
			return "done"
		}

		c++
		if c > len(id) {
			break
		}
	}

	fmt.Printf("len:%d\n", len(id))
	return id + "xxx"
}

func foo_for_inplace_fix_delimiter(id string) string {
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

	return id + ret
}

func foo_for_inplace_fix_replace(id string) string {
	c := 0
	for {
		fmt.Printf("calling foo_for_inplace_fix_replace\n")
		if id == "miliao" {
			return "done"
		}
		c++
		if c > len(id) {
			break
		}
	}

	// TODO uncomment following
	foo_for_inplace_fix_trampoline("miliao")

	fmt.Printf("len:%d\n", len(id))
	return id + "xxx2"
}

func foo_for_inplace_fix_trampoline(id string) string {
	c := 0
	for {
		fmt.Printf("calling foo_for_inplace_fix_trampoline\n")
		if id == "miliao" {
			return "done"
		}
		c++
		if c > len(id) {
			break
		}
	}

	fmt.Printf("len:%d\n", len(id))
	return id + "xxx3"
}

func TestInplaceFixAtMoveArea(t *testing.T) {
	code := []byte{
		/*
			0x48, 0x8b, 0x48, 0x08, // mov 0x8(%rax),%rcx
			0x74, 0x4, // jbe
			0x48, 0x8b, 0x48, 0x18, // sub 0x18(%rax), %rcx
			0x48, 0x89, 0x4c, 0x24, 0x10, // %rcx, 0x10(%rsp)
			0xc3, // retq
			0xcc, 0xcc,
		*/
		0x90, 0x90,
		0xeb, 0x04, // jmp 4
		0x90, 0x90, 0x90, 0x90, 0x90,
		0x90, 0x90, 0x90, 0x90, 0x90,
		0xc3,
		0x74, 0xf0, // jbe -16
		0xcc, 0xcc, 0xcc, 0xcc,
		0xcc, 0xcc, 0xcc, 0xcc,
	}

	target := GetFuncAddr(foo_for_inplace_fix)
	replace := GetFuncAddr(foo_for_inplace_fix_replace)
	trampoline := GetFuncAddr(foo_for_inplace_fix_trampoline)

	assert.True(t, isByteOverflow((int32)(trampoline-target)))

	CopyInstruction(target, code)

	fmt.Printf("short call target:%x, replace:%x, trampoline:%x\n", target, replace, trampoline)
	err1 := Hook(foo_for_inplace_fix, foo_for_inplace_fix_replace, foo_for_inplace_fix_trampoline)
	assert.Nil(t, err1)

	fmt.Printf("debug info:%s\n", ShowDebugInfo())

	msg1 := foo_for_inplace_fix("txt")

	fmt.Printf("calling foo inplace fix func\n")

	assert.Equal(t, "txtxxx2", msg1)

	sz1 := 5
	na1 := trampoline + uintptr(2)
	ta1 := target + uintptr(2+5+4-3)
	off1 := ta1 - (na1 + uintptr(sz1))

	sz2 := 6
	na2 := target + uintptr(15+3-3)
	ta2 := trampoline + uintptr(1)
	off2 := ta2 - (na2 + uintptr(sz2))

	fmt.Printf("off1:%x, off2:%x\n", off1, off2)

	ret := []byte{
		0x90, 0x90,
		0xe9, 0x74, 0xfc, 0xff, 0xff,
		0x90, 0x90, 0x90, 0x90, 0x90,
		0x90, 0x90, 0x90, 0x90, 0x90,
		0xc3,
		0x0f, 0x84, 0x80, 0x03, 0x00, 0x00,
		0xcc, 0xcc, 0xcc,
	}

	ret[3] = byte(off1)
	ret[4] = byte(off1 >> 8)
	ret[5] = byte(off1 >> 16)
	ret[6] = byte(off1 >> 24)

	ret[20] = byte(off2)
	ret[21] = byte(off2 >> 8)
	ret[22] = byte(off2 >> 16)
	ret[23] = byte(off2 >> 24)

	fc1 := makeSliceFromPointer(target, len(ret))
	fc2 := makeSliceFromPointer(trampoline, len(ret))

	assert.Equal(t, ret[:8], fc2[:8])
	assert.Equal(t, byte(0xe9), fc2[8])
	assert.Equal(t, ret[8:], fc1[5:len(ret)-3])

	code2 := []byte{
		0x90, 0x90, 0x90, 0x90,
		0x74, 0x04,
		0x90, 0x90, 0x90, 0x90,
		0x90, 0x90, 0x90, 0x90, 0x90,
		0xc3, 0xcc, 0x90,
	}

	err2 := UnHook(foo_for_inplace_fix)
	assert.Nil(t, err2)

	msg2 := foo_for_inplace_fix_trampoline("txt")
	assert.Equal(t, "txtxxx3", msg2)

	msg3 := foo_for_inplace_fix_replace("txt2")
	assert.Equal(t, "txt2xxx2", msg3)

	CopyInstruction(target, code2)

	fsz, _ := GetFuncSizeByGuess(GetArchMode(), target, false)
	assert.Equal(t, len(code2)-1, int(fsz))
}
