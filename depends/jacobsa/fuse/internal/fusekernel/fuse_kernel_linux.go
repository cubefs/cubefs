package fusekernel

import "time"

type Attr struct {
	Ino       uint64
	Size      uint64
	Blocks    uint64
	Atime     uint64
	Mtime     uint64
	Ctime     uint64
	AtimeNsec uint32
	MtimeNsec uint32
	CtimeNsec uint32
	Mode      uint32
	Nlink     uint32
	Uid       uint32
	Gid       uint32
	Rdev      uint32
	Blksize   uint32
	padding   uint32
}

func (a *Attr) Crtime() time.Time {
	return time.Time{}
}

func (a *Attr) SetCrtime(s uint64, ns uint32) {
	// Ignored on Linux.
}

func (a *Attr) SetFlags(f uint32) {
	// Ignored on Linux.
}

type SetattrIn struct {
	setattrInCommon
}

func (in *SetattrIn) BkupTime() time.Time {
	return time.Time{}
}

func (in *SetattrIn) Chgtime() time.Time {
	return time.Time{}
}

func (in *SetattrIn) Flags() uint32 {
	return 0
}

func openFlags(flags uint32) OpenFlags {
	// on amd64, the 32-bit O_LARGEFILE flag is always seen;
	// on i386, the flag probably depends on the app
	// requesting, but in any case should be utterly
	// uninteresting to us here; our kernel protocol messages
	// are not directly related to the client app's kernel
	// API/ABI
	flags &^= 0x8000

	return OpenFlags(flags)
}

type GetxattrIn struct {
	getxattrInCommon
}

type SetxattrIn struct {
	setxattrInCommon
}
