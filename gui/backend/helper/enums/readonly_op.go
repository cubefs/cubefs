package enums

type ReadOnlyOp int16

var (
	NullParam ReadOnlyOp = 0
	ReadOnly  ReadOnlyOp = 1
	WriteOnly ReadOnlyOp = 2
)
