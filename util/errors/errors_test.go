package errors

import (
	"testing"
)

func init() {
}

func TestError01(t *testing.T) {
	err := New("first")
	err = NewError(err)
	err = Trace(err, "second(%v %v)", 2, 3)
	err = Trace(err, "third(%v %v)", 4, 5)
	t.Log(err)
	t.Log(Stack(err))
}

func TestError02(t *testing.T) {
	var err error

	err = Trace(nil, "first(%v %v)", 2, 3)
	err = Trace(err, "second(%v %v)", 4, 5)
	t.Log(err)
	t.Log(Stack(err))
}

func TestError03(t *testing.T) {
	var err error

	err = Trace(nil, "first")
	err = Trace(err, "second(%v %v)", 4, 5)
	t.Log(err)
	t.Log(Stack(err))
}
