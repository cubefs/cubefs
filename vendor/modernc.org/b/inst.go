// +build b.instrumentation

package b

type dTree struct {
	t *Tree
}

type treeInst struct {
	deCopied int64
}

func (t *Tree) didCopy(n int) {
	t.deCopied += int64(n)
}

func (d *d) didCopy(n int) {
	d.t.deCopied += int64(n)
}

func (t *Tree) countCopies() int64 {
	return t.deCopied
}

func (d *d) setTree(t *Tree) {
	d.t = t
}
