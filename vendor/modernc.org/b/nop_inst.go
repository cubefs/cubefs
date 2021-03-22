// +build !b.instrumentation

package b

type dTree struct {
}

type treeInst struct {
}

func (t *Tree) didCopy(n int) {
}

func (d *d) didCopy(n int) {
}

func (t *Tree) countCopies() int64 {
	return 0
}

func (d *d) setTree(t *Tree) {
}
