package version

import (
	"fmt"
	"io/ioutil"
	"os"
)

var (
	version string      = ""
	fPerm   os.FileMode = 0600
)

func init() {
	if len(os.Args) > 1 && os.Args[1] == "-version" {
		fmt.Println("version:", version)
		os.Exit(0)
	}
	writeFile(".version", version)
}

func Version() string {
	return version
}

func writeFile(fname, field string) {
	if field != "" {
		ioutil.WriteFile(fname, []byte(field), fPerm)
	}
}
