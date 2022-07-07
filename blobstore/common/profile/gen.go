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

package profile

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

// MEConfig metric config with yaml
type MEConfig struct {
	URL    string            `yaml:"url"`
	Labels map[string]string `yaml:"labels"`
}

func genMetricExporter() {
	fn := os.Args[0] + suffixMetricExporter
	f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	labels := map[string]string{"scrapejob": "profile"}
	fields := strings.Split(os.Getenv(envMetricExporterLabels), ",")
	for _, item := range fields {
		s := strings.Split(item, "=")
		if len(s) != 2 {
			continue
		}
		labels[s[0]] = s[1]
	}

	content, err := yaml.Marshal(MEConfig{
		URL:    profileAddr + "/metrics",
		Labels: labels,
	})
	if err != nil {
		log.Println(err)
		return
	}

	f.Write(content)
	f.Sync()
}

func genDumpScript() {
	fn := os.Args[0] + suffixDumpScript
	f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	fmt.Fprint(f, "#!/bin/sh\n")
	fmt.Fprint(f, "echo $(date) dumping runtime status\n")
	fmt.Fprint(f, "set -x\n")
	fmt.Fprintf(f, "DIR=profile/$(date '+%%Y%%m%%d%%H%%M%%S')\n")
	fmt.Fprintf(f, "PREFIX=$DIR/%s_%d_%s_\n",
		filepath.Base(os.Args[0]), os.Getpid(), time.Now().Format("20060102_150405"))
	fmt.Fprint(f, "mkdir -p ${DIR}\n")
	fmt.Fprintf(f, "curl -sS '%s/metrics' -o ${PREFIX}metrics\n", profileAddr)
	fmt.Fprintf(f, "curl -sS '%s/debug/vars?seconds=5' -o ${PREFIX}vars\n", profileAddr)
	for _, p := range pprof.Profiles() {
		name := p.Name()
		switch name {
		case "heap", "allocs":
			fmt.Fprintf(f, "curl -sS '%s/debug/pprof/%s' -o ${PREFIX}%s\n",
				profileAddr, name, name)
		case "goroutine":
			fmt.Fprintf(f, "curl -sS '%s/debug/pprof/%s?debug=1' -o ${PREFIX}%s_debug_1\n",
				profileAddr, name, name)
			fmt.Fprintf(f, "curl -sS '%s/debug/pprof/%s?debug=2' -o ${PREFIX}%s_debug_2\n",
				profileAddr, name, name)
			fallthrough
		default:
			fmt.Fprintf(f, "curl -sS '%s/debug/pprof/%s?seconds=5' -o ${PREFIX}%s\n",
				profileAddr, name, name)
		}
	}
	fmt.Fprintf(f, "curl -sS '%s/debug/pprof/profile?seconds=5' -o ${PREFIX}profile\n", profileAddr)
	fmt.Fprintf(f, "curl -sS '%s/debug/pprof/trace?seconds=5' -o ${PREFIX}trace\n", profileAddr)
	fmt.Fprint(f, "tar -cvzf ${DIR}.tar.gz ${DIR}\n")
	f.Sync()
}

func genListenAddr() {
	fn := os.Args[0] + suffixListenAddr
	f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	f.WriteString(strings.TrimPrefix(profileAddr, "http://"))
	f.Sync()
}
