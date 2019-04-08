package fusekernel

import (
	"time"
)

type Attr struct {
	Ino        uint64
	Size       uint64
	Blocks     uint64
	Atime      uint64
	Mtime      uint64
	Ctime      uint64
	Crtime_    uint64 // OS X only
	AtimeNsec  uint32
	MtimeNsec  uint32
	CtimeNsec  uint32
	CrtimeNsec uint32 // OS X only
	Mode       uint32
	Nlink      uint32
	Uid        uint32
	Gid        uint32
	Rdev       uint32
	Flags_     uint32 // OS X only; see chflags(2)
	Blksize    uint32
	padding    uint32
}

func (a *Attr) SetCrtime(s uint64, ns uint32) {
	a.Crtime_, a.CrtimeNsec = s, ns
}

func (a *Attr) SetFlags(f uint32) {
	a.Flags_ = f
}

type SetattrIn struct {
	setattrInCommon

	// OS X only
	Bkuptime_    uint64
	Chgtime_     uint64
	Crtime       uint64
	BkuptimeNsec uint32
	ChgtimeNsec  uint32
	CrtimeNsec   uint32
	Flags_       uint32 // see chflags(2)
}

func (in *SetattrIn) BkupTime() time.Time {
	return time.Unix(int64(in.Bkuptime_), int64(in.BkuptimeNsec))
}

func (in *SetattrIn) Chgtime() time.Time {
	return time.Unix(int64(in.Chgtime_), int64(in.ChgtimeNsec))
}

func (in *SetattrIn) Flags() uint32 {
	return in.Flags_
}

func openFlags(flags uint32) OpenFlags {
	return OpenFlags(flags)
}

type GetxattrIn struct {
	getxattrInCommon

	// OS X only
	Position uint32
	Padding  uint32
}

func (g *GetxattrIn) GetPosition() uint32 {
	return g.Position
}

type SetxattrIn struct {
	setxattrInCommon

	// OS X only
	Position uint32
	Padding  uint32
}

func (s *SetxattrIn) GetPosition() uint32 {
	return s.Position
}
