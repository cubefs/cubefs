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
	"io"
	"os"
	"path"

	"github.com/desertbit/grumble"
	"github.com/fatih/color"

	"github.com/cubefs/cubefs/blobstore/cli/access"
	"github.com/cubefs/cubefs/blobstore/cli/blobnode"
	"github.com/cubefs/cubefs/blobstore/cli/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/cli/proxy"
	"github.com/cubefs/cubefs/blobstore/cli/scheduler"
	"github.com/cubefs/cubefs/blobstore/cli/sdk"
	"github.com/cubefs/cubefs/blobstore/cli/shardnode"
	"github.com/cubefs/cubefs/blobstore/cli/toolbox"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/version"
)

// App blobstore command app
var App = grumble.New(&grumble.Config{
	Name:                  "blobstore manager",
	Description:           "A command manager of blobstore",
	HistoryFile:           path.Join(os.TempDir(), ".blobstore_cli.history"),
	HistoryLimit:          10000,
	ErrorColor:            color.New(color.FgRed, color.Bold, color.Faint),
	HelpHeadlineColor:     color.New(color.FgGreen),
	HelpHeadlineUnderline: false,
	HelpSubCommands:       true,
	Prompt:                "BS $> ",
	PromptColor:           color.New(color.FgBlue, color.Bold),
	ASCIILogoColor:        color.New(color.Bold),
	Flags: func(f *grumble.Flags) {
		flags.ConfigRegister(f)
		flags.VerboseRegister(f)
		flags.VverboseRegister(f)
		f.BoolL("silence", false, "disable print output")
		f.IntL("loglevel", -1, "trace log level")
	},
})

func init() {
	log.SetOutputLevel(log.Lpanic)

	App.OnInit(func(a *grumble.App, fm grumble.FlagMap) error {
		if path := flags.Config(fm); path != "" {
			config.LoadConfig(path)
		}
		if flags.Verbose(fm) {
			config.Set("Flag-Verbose", true)
		}
		if flags.Vverbose(fm) {
			config.Set("Flag-Vverbose", true)
		}
		if fm.Bool("silence") {
			color.Output = io.Discard
			fmt.SetOutput(io.Discard)
			log.SetOutput(io.Discard)
		}
		if loglevel := fm.Int("loglevel"); loglevel >= 0 {
			config.Set("Flag-Loglevel", loglevel)
		}
		log.SetOutputLevel(log.Level(config.Get("Flag-Loglevel").(int)))
		// build-in flag in grumble
		if fm.Bool("nocolor") {
			color.NoColor = true
		}
		return nil
	})

	App.SetPrintASCIILogo(func(a *grumble.App) {
		fmt.Printf("|-> Blobstore-cli (%s) <-|\n",
			color.YellowString("%s", version.Version()))
	})

	registerHistory(App)
	registerConfig(App)
	registerRpc2(App)
	registerUtil(App)

	access.Register(App)
	clustermgr.Register(App)
	scheduler.Register(App)
	blobnode.Register(App)
	proxy.Register(App)
	shardnode.Register(App)
	sdk.Register(App)

	toolbox.Register(App)
}
