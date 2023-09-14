package util

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

func LoadSpecifiedPartitions() (ids []uint64) {
	ids = make([]uint64, 0)
	buf := make([]byte, 2048)
	idsF, err := os.Open("ids")
	if err != nil {
		return
	}
	defer idsF.Close()
	o := bufio.NewReader(idsF)
	for {
		buf, _, err = o.ReadLine()
		if err == io.EOF {
			break
		}
		id, _ := strconv.ParseUint(string(buf), 10, 64)
		if id > 0 {
			ids = append(ids, id)
		}
	}
	return
}

func LoadSpecifiedVolumes() (vols []string) {
	volsF, err := os.Open("vols")
	if err != nil {
		return
	}
	defer volsF.Close()
	r := bufio.NewReader(volsF)
	vols = make([]string, 0)
	buf := make([]byte, 2048)
	for {
		buf, _, err = r.ReadLine()
		if err == io.EOF {
			break
		}
		vols = append(vols, string(buf))
	}
	return
}
