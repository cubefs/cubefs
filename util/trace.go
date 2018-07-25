package util

import (
	"fmt"
	"runtime"
)

func GetFuncTrace() (m string) {
	pc := make([]uintptr, 15)
	n := runtime.Callers(2, pc)
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()
	m = fmt.Sprintf("func[%v]_line[%v]", frame.Function, frame.Line)
	return
}
