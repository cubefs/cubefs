package util

type BooleanFlag struct {
    flag byte
}

func NewBooleanFlag(flag byte) *BooleanFlag {
    return &BooleanFlag{
        flag: flag,
    }
}

func (b *BooleanFlag) Check(flagBits byte) bool {
    return (flagBits & b.flag) != 0
}

func (b *BooleanFlag) Get(available bool) byte {
    if available {
        return b.flag
    }
    return 0
}