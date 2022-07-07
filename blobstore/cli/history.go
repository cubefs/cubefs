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

package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/desertbit/grumble"
	"github.com/fatih/color"
)

func cmdHistory(path string) func(c *grumble.Context) error {
	hisColor := color.New(color.FgHiMagenta)
	return func(c *grumble.Context) error {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		l := c.Args.Int("line")
		lines := make([]string, 0, l)

		reader := bufio.NewReader(file)
		n := 1
		for {
			line, _, err := reader.ReadLine()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			lines = append(lines, fmt.Sprintf("%5.d: %s", n, line))
			n++
		}
		if len(lines) > l {
			lines = lines[len(lines)-l:]
		}

		for _, line := range lines {
			hisColor.Println(line)
		}
		return nil
	}
}

func registerHistory(app *grumble.App) {
	historyCommand := &grumble.Command{
		Name:     "history",
		Help:     "show history commands",
		LongHelp: "show history commands in file " + app.Config().HistoryFile,
		Args: func(a *grumble.Args) {
			a.Int("line", "show lines at the tail", grumble.Default(40))
		},
		Run: cmdHistory(app.Config().HistoryFile),
	}
	app.AddCommand(historyCommand)
}
