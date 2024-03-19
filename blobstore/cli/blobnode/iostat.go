// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blobnode

import (
	"errors"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
)

type vdevice struct {
	Name   string
	Viewer iostat.IOViewer
}

type rwStat struct {
	name  string
	items []float64
}

func parseIostatName(name string) (pid string, key string, err error) {
	arr := strings.SplitN(name, ".", 3)
	switch len(arr) {
	case 2:
		pid, key = arr[0], ""
	case 3:
		pid, key = arr[0], arr[1]
	default:
		return "", "", errors.New("invalid iostat file")
	}
	return
}

func newVDevice(dir, name string) (*vdevice, error) {
	pid, key, err := parseIostatName(name)
	if err != nil {
		return nil, err
	}

	sm, err := iostat.StatInitWithShareMemory(path.Join(dir, name), 1)
	if err != nil {
		return nil, err
	}
	iov := &iostat.Viewer{}
	iov.Stat = sm.Stat

	dev := &vdevice{}
	dev.Viewer = iov

	pName := ""
	if data, err := os.ReadFile(fmt.Sprintf("/proc/%s/comm", pid)); err == nil {
		pName = strings.TrimSpace(string(data)) + "."
	}
	devName := pName + pid
	if key != "" {
		devName = devName + "." + key
	}
	dev.Name = devName

	return dev, nil
}

func showVDevices(devices []*vdevice, extend, megabytes bool, descend, number int) {
	units := float64(1 << 10)
	if megabytes {
		units = 1 << 20
	}

	rw := make([]rwStat, 0, len(devices))
	for _, dev := range devices {
		r := dev.Viewer.ReadStat()
		w := dev.Viewer.WriteStat()
		st := []float64{
			float64(r.Iops), float64(w.Iops),
			float64(r.Bps) / units, float64(w.Bps) / units,
			float64(r.Avgrq) / 512, float64(w.Avgrq) / 512,
			float64(r.Avgqu), float64(w.Avgqu),
			float64(r.Await) / 1000, float64(w.Await) / 1000,
		}
		rw = append(rw, rwStat{name: dev.Name, items: st})
	}

	if descend >= 1 && descend <= 10 {
		sort.Slice(rw, func(i, j int) bool {
			return rw[i].items[descend-1] > rw[j].items[descend-1]
		})
	}

	if number > 0 && number < len(rw) {
		rw = rw[:number]
	}

	fmt.Printf("%-48s%12s%12s", "Process:", "r/s", "w/s")
	if megabytes {
		fmt.Printf("%12s%12s", "rMB/s", "wMB/s")
	} else {
		fmt.Printf("%12s%12s", "rKB/s", "wKB/s")
	}
	if extend {
		fmt.Printf("%10s%10s%10s%10s%10s%10s",
			"rrq_sz", "wrq_sz", "rqu-sz", "wqu-sz", "r_await", "w_await")
	}
	fmt.Println()

	for _, st := range rw {
		f := st.items
		fmt.Printf("%-48s%12.2f%12.2f%12.2f%12.2f", st.name, f[0], f[1], f[2], f[3])
		if extend {
			fmt.Printf("%10.2f%10.2f%10.2f%10.2f%10.2f%10.2f", f[4], f[5], f[6], f[7], f[8], f[9])
		}
		fmt.Println()
		fmt.Println()
	}
}

func scanVDevices(dir string, filters []string) ([]*vdevice, error) {
	fis, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var devices []*vdevice
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}
		name := fi.Name()

		pass := true
		for _, filter := range filters {
			if !strings.Contains(name, filter) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		dev, err := newVDevice(dir, name)
		if err != nil {
			return nil, err
		}
		devices = append(devices, dev)
	}

	return devices, nil
}

func showIOStat(c *grumble.Context) error {
	path := c.Flags.String("path")
	filter := c.Flags.String("filter")
	devices, err := scanVDevices(path, strings.Split(filter, ","))
	if err != nil {
		return err
	}

	interval := c.Args.Int("interval")
	count := c.Args.Int("count")

	extend := c.Flags.Bool("extend")
	megabytes := c.Flags.Bool("megabytes")
	descend := c.Flags.Int("descend")
	number := c.Flags.Int("number")

	interrupt := make(chan struct{})
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Duration(interval) * time.Second)
		defer ticker.Stop()

		for ii := 0; ii < count; ii++ {
			select {
			case <-interrupt:
				return
			default:
			}

			for _, dev := range devices {
				dev.Viewer.Update()
			}
			showVDevices(devices, extend, megabytes, descend, number)

			<-ticker.C
		}

		close(done)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
		close(interrupt)
	case <-done:
	}
	return nil
}

func addCmdIOStat(cmd *grumble.Command) {
	iostatCommand := &grumble.Command{
		Name: "iostat",
		Help: "iostat of blobnode",
		LongHelp: "show iostat of localhost running process\n\n" +
			"  iostat [-x] [-m] [-n number] [-d index] [-p path] [-j filter] -- [interval [count]]",
		Args: func(a *grumble.Args) {
			a.Int("interval", "show interval seconds", grumble.Default(1))
			a.Int("count", "show total seconds", grumble.Default(100000))
		},
		Flags: func(f *grumble.Flags) {
			f.Bool("x", "extend", false, "Display extended statistics.")
			f.Bool("m", "megabytes", false, "Display statistics in megabytes.")
			f.Int("d", "descend", 0, "Descend by which index.")
			f.Int("n", "number", 1000, "Number items of max display.")
			f.String("p", "path", iostat.IOSTAT_DIRECTORY, "Path of stat directory.")
			f.String("j", "filter", "", "Filter keys, mulit-keys split by comma.")
		},
		Run: showIOStat,
	}
	cmd.AddCommand(iostatCommand)
}
