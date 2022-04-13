package gohook

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"unsafe"

	"golang.org/x/arch/x86/x86asm"
)

type CodeFix struct {
	Code    []byte
	Addr    uintptr
	Foreign bool
}

var (
	minJmpCodeSize = 0
	elfInfo, _     = NewElfInfo()

	errInplaceFixSizeNotEnough = fmt.Errorf("func size exceed during inplace fix")

	funcPrologue32 = defaultFuncPrologue32
	funcPrologue64 = defaultFuncPrologue64

	// ======================condition jump instruction========================
	// JA JAE JB JBE JCXZ JE JECXZ JG JGE JL JLE JMP JNE JNO JNP JNS JO JP JRCXZ JS

	// one byte opcode, one byte relative offset
	twoByteCondJmp = []byte{0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f, 0xe3}
	// two byte opcode, four byte relative offset
	sixByteCondJmp = []uint16{0x0f80, 0x0f81, 0x0f82, 0x0f83, 0x0f84, 0x0f85, 0x0f86, 0x0f87, 0x0f88, 0x0f89, 0x0f8a, 0x0f8b, 0x0f8c, 0x0f8d, 0x0f8e, 0x0f8f}

	// ====================== jump instruction========================
	// one byte opcode, one byte relative offset
	twoByteJmp = []byte{0xeb}
	// one byte opcode, four byte relative offset
	fiveByteJmp = []byte{0xe9}

	// ====================== call instruction========================
	// one byte opcode, 4 byte relative offset
	fiveByteCall = []byte{0xe8}

	// ====================== ret instruction========================
	// return instruction, no operand
	oneByteRet = []byte{0xc3, 0xcb}
	// return instruction, one byte opcode, 2 byte operand
	threeByteRet = []byte{0xc2, 0xca}
)

const (
	FT_CondJmp  = 1
	FT_JMP      = 2
	FT_CALL     = 3
	FT_RET      = 4
	FT_OTHER    = 5
	FT_INVALID  = 6
	FT_SKIP     = 7
	FT_OVERFLOW = 8
)

func SetMinJmpCodeSize(sz int) {
	minJmpCodeSize = sz
}

func ResetFuncPrologue() {
	funcPrologue32 = defaultFuncPrologue32
	funcPrologue64 = defaultFuncPrologue64
}

func SetFuncPrologue(mode int, data []byte) {
	if mode == 32 {
		funcPrologue32 = make([]byte, len(data))
		copy(funcPrologue32, data)
	} else {
		funcPrologue64 = make([]byte, len(data))
		copy(funcPrologue64, data)
	}
}

func GetInsLenGreaterThan(mode int, data []byte, least int) int {
	if len(data) < least {
		return 0
	}

	curLen := 0
	d := data[curLen:]
	for {
		if len(d) <= 0 {
			break
		}

		if curLen >= least {
			break
		}

		inst, err := x86asm.Decode(d, mode)
		if err != nil || (inst.Opcode == 0 && inst.Len == 1 && inst.Prefix[0] == x86asm.Prefix(d[0])) {
			break
		}

		if inst.Len == 1 && d[0] == 0xcc {
			// 0xcc -> int3, trap to debugger, padding to function end
			break
		}

		curLen = curLen + inst.Len
		d = data[curLen:]
	}

	return curLen
}

func isByteOverflow(v int32) bool {
	if v > 0 {
		if v > math.MaxInt8 {
			return true
		}
	} else {
		if v < math.MinInt8 {
			return true
		}
	}

	return false
}

func isIntOverflow(v int64) bool {
	if v > 0 {
		if v > math.MaxInt32 {
			return true
		}
	} else {
		if v < math.MinInt32 {
			return true
		}
	}

	return false
}

func calcOffset(insSz int, startAddr, curAddr, to uintptr, to_sz int, offset int32) int64 {
	newAddr := curAddr
	absAddr := curAddr + uintptr(insSz) + uintptr(offset)

	if curAddr < startAddr+uintptr(to_sz) {
		newAddr = to + (curAddr - startAddr)
	}

	if absAddr >= startAddr && absAddr < startAddr+uintptr(to_sz) {
		absAddr = to + (absAddr - startAddr)
	}

	return int64(uint64(absAddr) - uint64(newAddr) - uint64(insSz))
}

func translateJump(off int64, code []byte) ([]byte, error) {
	if code[0] == 0xe3 {
		return nil, errors.New("not supported JCXZ instruction(0xe3)")
	}

	if code[0] == 0xeb {
		ret := make([]byte, 5)
		ret[0] = 0xe9

		ret[1] = byte(off)
		ret[2] = byte(off >> 8)
		ret[3] = byte(off >> 16)
		ret[4] = byte(off >> 24)
		return ret, nil
	} else if code[0] >= 0x70 && code[0] <= 0x7f {
		ret := make([]byte, 6)
		ret[0] = 0x0f
		ret[1] = 0x80 + code[0] - 0x70

		ret[2] = byte(off)
		ret[3] = byte(off >> 8)
		ret[4] = byte(off >> 16)
		ret[5] = byte(off >> 24)
		return ret, nil
	} else {
		return nil, errors.New("cannot fix unsupported jump instruction inplace")
	}
}

func adjustInstructionOffset(code []byte, off int64) ([]byte, error) {
	if code[0] == 0xe3 || code[0] == 0xeb || (code[0] >= 0x70 && code[0] <= 0x7f) {
		offset := int(int8(code[1]))
		if offset == int(off) {
			return code, nil
		}
		if isByteOverflow(int32(off)) {
			return nil, fmt.Errorf("byte overflow in adjusting offset")
		}
		code[1] = byte(int8(off))
	} else if code[0] == 0x0f && (code[1] >= 0x80 && code[1] <= 0x8f) {
		offset := int(int32(uint32(code[2]) | (uint32(code[3]) << 8) | (uint32(code[4]) << 16) | (uint32(code[5]) << 24)))
		if offset == int(off) {
			return code, nil
		}
		if isIntOverflow(off) {
			return nil, fmt.Errorf("int overflow in adjusting 6-bytes inst offset")
		}
		code[2] = byte(off)
		code[3] = byte(off >> 8)
		code[4] = byte(off >> 16)
		code[5] = byte(off >> 24)
	} else if code[0] == 0xe9 || code[0] == 0xe8 {
		offset := int(int32(uint32(code[1]) | (uint32(code[2]) << 8) | (uint32(code[3]) << 16) | (uint32(code[4]) << 24)))
		if offset == int(off) {
			return code, nil
		}
		if isIntOverflow(off) {
			return nil, fmt.Errorf("int overflow in adjusting 5-bytes inst offset")
		}
		code[1] = byte(off)
		code[2] = byte(off >> 8)
		code[3] = byte(off >> 16)
		code[4] = byte(off >> 24)
	} else if code[0] == 0x48 && (code[1] == 0x8b || code[1] == 0x8d) && (code[2]&0x05) == 0x05 { // mod == 00 r/m == 101
		// rip relative addressing: mov/lea
		// intel software development manual: `Addressing-Mode Encoding of ModR/M and SIB Bytes` && `RIP-Relative Addressing`
		// or https://www.cs.uaf.edu/2016/fall/cs301/lecture/09_28_machinecode.html
		offset := int(int32(uint32(code[3]) | (uint32(code[4]) << 8) | (uint32(code[5]) << 16) | (uint32(code[6]) << 24)))
		if offset == int(off) {
			return code, nil
		}
		code[3] = byte(off)
		code[4] = byte(off >> 8)
		code[5] = byte(off >> 16)
		code[6] = byte(off >> 24)
	} else {
		return nil, fmt.Errorf("not jump instruction")
	}

	return code, nil
}

func calcJumpToAbsAddr(mode int, addr uintptr, code []byte) uintptr {
	sz := 0
	offset := 0

	if code[0] == 0xe3 || code[0] == 0xeb || (code[0] >= 0x70 && code[0] <= 0x7f) {
		sz = 2
		offset = int(int8(code[1]))
	}

	if code[0] == 0x0f && (code[1] >= 0x80 && code[1] <= 0x8f) {
		sz = 6
		offset = int(int32(uint32(code[2]) | (uint32(code[3]) << 8) | (uint32(code[4]) << 16) | (uint32(code[5]) << 24)))
	}

	if code[0] == 0xe9 || code[0] == 0xe8 {
		sz = 5
		offset = int(int32(uint32(code[1]) | (uint32(code[2]) << 8) | (uint32(code[3]) << 16) | (uint32(code[4]) << 24)))
	}

	if code[0] == 0x48 && (code[1] == 0x8b || code[1] == 0x8d) && (code[2]&0x05) == 0x05 { // mod == 00 r/m == 101
		// rip relative addressing: mov/lea
		// intel software development manual: `Addressing-Mode Encoding of ModR/M and SIB Bytes` && `RIP-Relative Addressing`
		// or https://www.cs.uaf.edu/2016/fall/cs301/lecture/09_28_machinecode.html
		sz = 7
		offset = int(int32(uint32(code[3]) | (uint32(code[4]) << 8) | (uint32(code[5]) << 16) | (uint32(code[6]) << 24)))
	}

	if sz == 0 {
		return uintptr(0)
	}

	return addr + uintptr(sz) + uintptr(offset)
}

func FixOneInstruction(mode int, fix_recursive_call bool, startAddr, curAddr uintptr, code []byte, to uintptr, to_sz int) (int, int, []byte) {
	nc := make([]byte, len(code))
	copy(nc, code)

	if code[0] == 0xe3 || code[0] == 0xeb || (code[0] >= 0x70 && code[0] <= 0x7f) {
		// two byte condition jump, two byte jmp
		nc = nc[:2]
		off := calcOffset(2, startAddr, curAddr, to, to_sz, int32(int8(code[1])))
		if off != int64(int8(nc[1])) {
			if isByteOverflow(int32(off)) {
				// overfloat, cannot fix this with one byte operand
				return 2, FT_OVERFLOW, nc
			}
			nc[1] = byte(off)
			return 2, FT_CondJmp, nc
		}
		return 2, FT_SKIP, nc
	}

	if code[0] == 0x0f && (code[1] >= 0x80 && code[1] <= 0x8f) {
		// six byte condition jump
		nc = nc[:6]
		off1 := (uint32(code[2]) | (uint32(code[3]) << 8) | (uint32(code[4]) << 16) | (uint32(code[5]) << 24))
		off2 := uint64(calcOffset(6, startAddr, curAddr, to, to_sz, int32(off1)))
		if uint64(int32(off1)) != off2 {
			if isIntOverflow(int64(off2)) {
				// overfloat, cannot fix this with four byte operand
				return 6, FT_OVERFLOW, nc
			}
			nc[2] = byte(off2)
			nc[3] = byte(off2 >> 8)
			nc[4] = byte(off2 >> 16)
			nc[5] = byte(off2 >> 24)
			return 6, FT_CondJmp, nc
		}
		return 6, FT_SKIP, nc
	}

	if code[0] == 0xe9 || code[0] == 0xe8 {
		// five byte jmp, five byte call
		nc = nc[:5]
		off1 := (uint32(code[1]) | (uint32(code[2]) << 8) | (uint32(code[3]) << 16) | (uint32(code[4]) << 24))

		off2 := uint64(0)
		if !fix_recursive_call && code[0] == 0xe8 && startAddr == (curAddr+uintptr(5)+uintptr(int32(off1))) {
			// don't fix recursive call
			off2 = uint64(int32(off1))
		} else {
			off2 = uint64(calcOffset(5, startAddr, curAddr, to, to_sz, int32(off1)))
		}

		if uint64(int32(off1)) != off2 {
			if isIntOverflow(int64(off2)) {
				// overfloat, cannot fix this with four byte operand
				return 5, FT_OVERFLOW, nc
			}
			nc[1] = byte(off2)
			nc[2] = byte(off2 >> 8)
			nc[3] = byte(off2 >> 16)
			nc[4] = byte(off2 >> 24)
			return 5, FT_JMP, nc
		}
		return 5, FT_SKIP, nc
	}

	// ret instruction just return, no fix is needed.
	if code[0] == 0xc3 || code[0] == 0xcb {
		// one byte ret
		nc = nc[:1]
		return 1, FT_RET, nc
	}

	if code[0] == 0xc2 || code[0] == 0xca {
		// three byte ret
		nc = nc[:3]
		return 3, FT_RET, nc
	}

	inst, err := x86asm.Decode(code, mode)
	if err != nil || (inst.Opcode == 0 && inst.Len == 1 && inst.Prefix[0] == x86asm.Prefix(code[0])) {
		return 0, FT_INVALID, nc
	}

	if inst.Len == 1 && code[0] == 0xcc {
		return 0, FT_INVALID, nc
	}

	sz := inst.Len
	nc = nc[:sz]
	return sz, FT_OTHER, nc
}

func doFixTargetFuncCode(all bool, mode int, start uintptr, funcSz int, to uintptr, move_sz int, inst []CodeFix) ([]CodeFix, error) {
	fix := make([]CodeFix, 0, 64)

	curSz := 0
	curAddr := start

	i := 0
	for i = 0; i < len(inst); i++ {
		if curSz >= move_sz {
			break
		}

		code := inst[i].Code
		sz, ft, nc := FixOneInstruction(mode, false, start, curAddr, code, to, move_sz)

		if sz == 0 && ft == FT_INVALID {
			// the end or unrecognized instruction
			return nil, errors.New(fmt.Sprintf("invalid instruction scanned, addr:0x%x", curAddr))
		} else if sz == 5 && nc[0] == 0xe8 {
			// call instruction is not allowed to move.
			// this will mess up with golang stack reallocation.
			return nil, fmt.Errorf("call instruction is not allowed to move")
		}

		if ft == FT_RET {
			return nil, errors.New(fmt.Sprintf("ret instruction in patching erea is not allowed, addr:0x%x", curAddr))
		}

		if ft == FT_OVERFLOW {
			return nil, errors.New(fmt.Sprintf("jmp instruction in patching erea overflow, addr:0x%x", curAddr))
		}

		if ft != FT_OTHER && ft != FT_SKIP {
			fix = append(fix, CodeFix{Code: nc, Addr: curAddr, Foreign: true})
		} else if all {
			fix = append(fix, CodeFix{Code: nc, Addr: curAddr, Foreign: true})
		}

		curSz += sz
		curAddr = start + uintptr(curSz)
	}

	for ; i < len(inst); i++ {
		if funcSz > 0 && int(curAddr-start) >= funcSz {
			break
		}

		code := inst[i].Code
		sz, ft, nc := FixOneInstruction(mode, false, start, curAddr, code, to, move_sz)

		if sz == 0 && ft == FT_INVALID {
			// the end or unrecognized instruction
			break
		}

		if ft == FT_OVERFLOW {
			return nil, errors.New(fmt.Sprintf("jmp instruction in body overflow, addr:0x%x", curAddr))
		}

		if ft != FT_OTHER && ft != FT_RET && ft != FT_SKIP {
			fix = append(fix, CodeFix{Code: nc, Addr: curAddr, Foreign: false})
		} else if all {
			fix = append(fix, CodeFix{Code: nc, Addr: curAddr, Foreign: false})
		}

		curSz += sz
		curAddr = start + uintptr(curSz)
	}

	return fix, nil
}

// FixTargetFuncCode fix function code starting at address [start]
// parameter 'funcSz' may not specify, in which case, we need to find out the end by scanning next prologue or finding invalid instruction.
// 'to' specifys a new location, to which 'move_sz' bytes instruction will be copied
// since move_sz byte instructions will be copied, those relative jump instruction need to be fixed.
func FixTargetFuncCode(mode int, start uintptr, funcSz uint32, to uintptr, move_sz int) ([]CodeFix, error) {
	inst, _ := parseInstruction(mode, start, int(funcSz), false)
	return doFixTargetFuncCode(false, mode, start, int(funcSz), to, move_sz, inst)
}

func GetFuncSizeByGuess(mode int, start uintptr, minimal bool) (uint32, error) {
	funcPrologue := funcPrologue64
	if mode == 32 {
		funcPrologue = funcPrologue32
	}

	prologueLen := len(funcPrologue)
	code := makeSliceFromPointer(start, 16) // instruction takes at most 16 bytes

	/* prologue is not required
	if !bytes.Equal(funcPrologue, code[:prologueLen]) { // not valid function start or invalid prologue
		return 0, errors.New(fmt.Sprintf("no func prologue, addr:0x%x", start))
	}
	*/

	int3_found := false
	curLen := uint32(0)

	for {
		inst, err := x86asm.Decode(code, mode)
		if err != nil || (inst.Opcode == 0 && inst.Len == 1 && inst.Prefix[0] == x86asm.Prefix(code[0])) {
			break
		}

		if inst.Len == 1 && code[0] == 0xcc {
			// 0xcc -> int3, trap to debugger, padding to function end
			if minimal {
				break
			}
			int3_found = true
		} else if int3_found {
			break
		}

		curLen = curLen + uint32(inst.Len)
		code = makeSliceFromPointer(start+uintptr(curLen), 16) // instruction takes at most 16 bytes

		if bytes.Equal(funcPrologue, code[:prologueLen]) {
			break
		}
	}

	return curLen, nil
}

// sz size of source function
// WARNING: copy function won't work in copystack(since go 1.3).
// runtime will copy stack to new area and fix those weird stuff(pointer/rbp etc), this will crash trampoline function.
// since copying function makes trampoline a completely different function, with completely different stack layout which is
// not known to runtime.
// solution to this is, we should just copy those non-call instructions to trampoline. in this way we don't mess up with runtime.
// TODO/FIXME
func copyFuncInstruction(mode int, from, to uintptr, sz int, allowCall bool) ([]CodeFix, error) {
	curSz := 0
	curAddr := from
	fix := make([]CodeFix, 0, 256)

	for {
		if curSz >= sz {
			break
		}

		code := makeSliceFromPointer(curAddr, 16) // instruction takes at most 16 bytes
		sz, ft, nc := FixOneInstruction(mode, true, from, curAddr, code, to, sz)

		if sz == 0 && ft == FT_INVALID {
			// the end or unrecognized instruction
			break
		} else if !allowCall && sz == 5 && nc[0] == 0xe8 {
			// call instruction is not allowed to move.
			// this will mess up with golang stack reallocation.
			return nil, fmt.Errorf("call instruction is not allowed to copy")
		}

		if ft == FT_OVERFLOW {
			return nil, fmt.Errorf("overflow instruction in copying function, addr:0x%x", curAddr)
		}

		to_addr := (to + (curAddr - from))
		fix = append(fix, CodeFix{Code: nc, Addr: to_addr})

		curSz += sz
		curAddr = from + uintptr(curSz)
	}

	to_addr := (to + (curAddr - from))
	fix = append(fix, CodeFix{Code: []byte{0xcc}, Addr: to_addr})
	return fix, nil
}

func adjustJmpOffset(mode int, start, delem uintptr, funcSize, moveSize int, inst []CodeFix) error {
	funcEnd := start + uintptr(funcSize)
	for i := range inst {
		code := inst[i].Code
		curAddr := inst[i].Addr
		absAddr := calcJumpToAbsAddr(mode, curAddr, code)

		if curAddr > delem && curAddr < funcEnd {
			inst[i].Addr = curAddr + uintptr(moveSize)
		}

		if absAddr != uintptr(0) {
			delta := absAddr - curAddr - uintptr(len(code))
			off := int64(delta)
			if unsafe.Sizeof(uintptr(0)) == unsafe.Sizeof(int32(0)) {
				off = int64(int32(delta))
			}

			// fmt.Printf("adjust inst at:%x, sz:%d, delem:%x, target:%x, funcEnd:%x, off:%x\n", curAddr, len(code), delem, absAddr, funcEnd, uintptr(off))

			if (curAddr < delem || curAddr >= funcEnd) && absAddr > delem && absAddr < funcEnd {
				off += int64(moveSize)
			} else if (curAddr >= delem && curAddr < funcEnd) && (absAddr <= delem || absAddr >= funcEnd) {
				off -= int64(moveSize)
			} else {
				// do nothing
			}

			c, err := adjustInstructionOffset(code, off)
			if err != nil {
				return fmt.Errorf("err occurs adjusting inst, addr:%x,off:%x,err:%s", curAddr, off, err.Error())
			}

			inst[i].Code = c
			// absAddr = calcJumpToAbsAddr(mode, inst[i].Addr, code)
			// fmt.Printf("after adjust inst, old addr:%x, new addr:%x, target:%x\n", curAddr, inst[i].Addr, absAddr)
		}
	}

	return nil
}

func translateShortJump(mode int, addr, to uintptr, inst []CodeFix, funcSz, move_sz, jumpSize int) (int, []CodeFix, error) {
	newSz := 0
	fix := make([]CodeFix, 0, 256)

	for i := range inst {
		code := inst[i].Code
		curAddr := inst[i].Addr
		sz, ft, _ := FixOneInstruction(mode, false, addr, curAddr, code, to, move_sz)

		if sz == 0 && ft == FT_INVALID {
			// the end or unrecognized instruction
			break
		}

		foreign := false
		if curAddr < addr+uintptr(move_sz) {
			foreign = true
		}

		if ft == FT_OVERFLOW {
			if sz != 2 {
				return 0, nil, fmt.Errorf("inst overflow with size != 2")
			}

			nc, err := translateJump(int64(int8(code[1])), code)
			if err != nil {
				return 0, nil, err
			}

			delta := len(nc) - len(code)
			if curAddr < addr+uintptr(move_sz) {
				move_sz += delta
			}

			inst[i].Code = nc
			// fmt.Printf("extent overflow inst at:%x, sz:%d, move sz:%d\n", curAddr, len(nc), move_sz)

			err = adjustJmpOffset(mode, addr, curAddr, funcSz, delta, inst[i:])
			if err != nil {
				return 0, nil, err
			}

			err = adjustJmpOffset(mode, addr, curAddr, funcSz, delta, fix)
			if err != nil {
				return 0, nil, err
			}
		}

		newSz += len(inst[i].Code)
		fix = append(fix, CodeFix{Code: inst[i].Code, Addr: inst[i].Addr, Foreign: foreign})
	}

	if newSz-move_sz > funcSz-jumpSize {
		return move_sz, fix, errInplaceFixSizeNotEnough
	}

	return move_sz, fix, nil
}

func parseInstruction(mode int, addr uintptr, funcSz int, minimal bool) ([]CodeFix, error) {
	funcPrologue := funcPrologue64
	if mode == 32 {
		funcPrologue = funcPrologue32
	}

	prologueLen := len(funcPrologue)
	code := makeSliceFromPointer(addr, 16) // instruction takes at most 16 bytes

	curLen := 0
	int3_found := false

	ret := make([]CodeFix, 0, 258)

	for {
		if funcSz > 0 && curLen >= funcSz {
			break
		}

		inst, err := x86asm.Decode(code, mode)
		if err != nil || (inst.Opcode == 0 && inst.Len == 1 && inst.Prefix[0] == x86asm.Prefix(code[0])) {
			break
		}

		if inst.Len == 1 && code[0] == 0xcc {
			// 0xcc -> int3, trap to debugger, padding to function end
			if minimal {
				break
			}
			int3_found = true
		} else if int3_found {
			break
		}

		c := make([]byte, inst.Len)
		copy(c, code)
		cf := CodeFix{Addr: addr + uintptr(curLen), Code: c}
		ret = append(ret, cf)

		curLen = curLen + inst.Len
		code = makeSliceFromPointer(addr+uintptr(curLen), 16)

		if bytes.Equal(funcPrologue, code[:prologueLen]) {
			break
		}
	}

	return ret, nil
}

func fixFuncInstructionInplace(mode int, addr, to uintptr, funcSz int, move_sz int, jumpSize int) ([]CodeFix, error) {
	/*
		trail := makeSliceFromPointer(addr+uintptr(funcSz), 1024)
		for i := 0; i < len(trail); i++ {
			if trail[i] != 0xcc {
				break
			}
			funcSz++
		}
	*/

	code, _ := parseInstruction(mode, addr, funcSz, false)
	move_sz, fix, err := translateShortJump(mode, addr, to, code, funcSz, move_sz, jumpSize)

	if err != nil {
		return nil, err
	}

	fix, err1 := doFixTargetFuncCode(true, mode, addr, funcSz, to, move_sz, fix)

	if err1 != nil {
		return fix, err1
	}

	curAddr := to
	firstBody := addr
	for i := range fix {
		if !fix[i].Foreign {
			firstBody = fix[i].Addr
			break
		}

		// fmt.Printf("foreign addr:%x, sz:%d\n", curAddr, len(fix[i].Code))

		fix[i].Addr = curAddr
		curAddr += uintptr(len(fix[i].Code))
	}

	mvAddr := addr + uintptr(jumpSize)
	msz := -int(firstBody - mvAddr)

	if msz != 0 {
		// fmt.Printf("now move to the front, msz:%d\n", msz)
		err2 := adjustJmpOffset(mode, addr, mvAddr, funcSz, msz, fix)
		if err2 != nil {
			// fmt.Printf("error in fixing inplace\n")
			return nil, err2
		}
	}

	// fmt.Printf("done fixing inplace\n")
	return fix, nil
}

func genJumpCode(mode int, rdxIndirect bool, to, from uintptr) []byte {
	// 1. use relaive jump if |from-to| < 2G
	// 2. otherwise, push target, then ret

	var code []byte

	if rdxIndirect {
		// rdx indirect jump.
		// 'to' :data pointer from reflect.Value, pointed to a funcValue, and the first field of funcval is a pointer to the real func.
		// 'from': this is the instruction code addr of the target function.

		// by convention, rdx is the context register pointed to a funcval.
		// funcval of a closure function contains extra information used by compiler and runtime.
		// so using indirect jmp by rdx makes it possible to hook closure func and func created by reflect.MakeFunc

		// caution: 'to' funcval must stay alive after hook is installed.
		if mode == 32 {
			code = []byte{
				0xBA,
				byte(to),
				byte(to >> 8),
				byte(to >> 16),
				byte(to >> 24), // mov edx,to
				0xFF, 0x22,     // jmp DWORD PTR [edx]
			}
		} else {
			code = []byte{
				0x48, 0xBA,
				byte(to),
				byte(to >> 8),
				byte(to >> 16),
				byte(to >> 24),
				byte(to >> 32),
				byte(to >> 40),
				byte(to >> 48),
				byte(to >> 56), // movabs rdx,to
				0xFF, 0x22,     // jmp QWORD PTR [rdx]
			}
		}
	} else {
		delta := int64(from - to)
		if unsafe.Sizeof(uintptr(0)) == unsafe.Sizeof(int32(0)) {
			delta = int64(int32(from - to))
		}

		relative := (delta <= 0x7fffffff)

		if delta < 0 {
			delta = -delta
			relative = (delta <= 0x80000000)
		}

		// relative = false

		if relative {
			var dis uint32
			if to > from {
				dis = uint32(int32(to-from) - 5)
			} else {
				dis = uint32(-int32(from-to) - 5)
			}
			code = []byte{
				0xe9,
				byte(dis),
				byte(dis >> 8),
				byte(dis >> 16),
				byte(dis >> 24),
			}
		} else if mode == 32 {
			code = []byte{
				0x68, // push
				byte(to),
				byte(to >> 8),
				byte(to >> 16),
				byte(to >> 24),
				0xc3, // retn
			}
		} else if mode == 64 {
			// push does not operate on 64bit imm, workarounds are:
			// 1. move to register(eg, %rdx), then push %rdx, however, overwriting register may cause problem if not handled carefully.
			// 2. push twice, preferred.
			/*
			   code = []byte{
			       0x48, // prefix
			       0xba, // mov to %rdx
			       byte(to), byte(to >> 8), byte(to >> 16), byte(to >> 24),
			       byte(to >> 32), byte(to >> 40), byte(to >> 48), byte(to >> 56),
			       0x52, // push %rdx
			       0xc3, // retn
			   }
			*/
			code = []byte{
				0x68, //push
				byte(to), byte(to >> 8), byte(to >> 16), byte(to >> 24),
				0xc7, 0x44, 0x24, // mov $value, 4%rsp
				0x04, // rsp + 4
				byte(to >> 32), byte(to >> 40), byte(to >> 48), byte(to >> 56),
				0xc3, // retn
			}
		} else {
			panic("invalid mode")
		}
	}

	sz := len(code)
	if minJmpCodeSize > 0 && sz < minJmpCodeSize {
		nop := make([]byte, 0, minJmpCodeSize-sz)
		for {
			if len(nop) >= minJmpCodeSize-sz {
				break
			}
			nop = append(nop, 0x90)
		}

		code = append(code, nop...)
	}

	return code
}
