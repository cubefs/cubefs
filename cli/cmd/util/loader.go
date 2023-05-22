package util

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"
)

func LoadSpecifiedPartitions() (ids []uint64) {
	ids = make([]uint64, 0)
	buf := make([]byte, 2048)
	var err error
	idsF, _ := os.OpenFile("ids", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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

func LoadSpecifiedVolumes(volFilter, volExcludeFilter string) (vols []string) {
	volsF, err := os.OpenFile("vols", os.O_RDONLY, 0666)
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
		if volFilter != "" {
			if !strings.Contains(string(buf), volFilter) {
				continue
			}
		}
		if volExcludeFilter != "" {
			if strings.Contains(string(buf), volExcludeFilter) {
				continue
			}
		}
		vols = append(vols, string(buf))
	}
	return
}
