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

package access

import (
	"os"

	"github.com/desertbit/grumble"
	"github.com/hashicorp/consul/api"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
)

func newAccessClient() (access.API, error) {
	return access.New(access.Config{
		ConnMode:       access.RPCConnectMode(config.AccessConnMode()),
		Consul:         access.ConsulConfig{Address: config.AccessConsulAddr()},
		PriorityAddrs:  config.AccessPriorityAddrs(),
		MaxSizePutOnce: config.AccessMaxSizePutOnce(),
		MaxPartRetry:   config.AccessMaxPartRetry(),
		MaxHostRetry:   config.AccessMaxHostRetry(),

		ServiceIntervalS: config.AccessServiceIntervalS(),

		HostTryTimes:       config.AccessHostTryTimes(),
		FailRetryIntervalS: config.AccessFailRetryIntervalS(),
		MaxFailsPeriodS:    config.AccessMaxFailsPeriodS(),
	})
}

func readLocation(f grumble.FlagMap) (loc access.Location, err error) {
	if f.String("location") != "" {
		loc, err = cfmt.ParseLocation(f.String("location"))
		return
	}

	filepath := f.String("locationpath")
	if filepath == "" {
		err = fmt.Errorf("no location and locationpath setting")
		return
	}

	file, err := os.Open(filepath)
	if err != nil {
		return
	}
	defer file.Close()

	err = common.NewDecoder(file).Decode(&loc)
	return
}

func accessFlags(f *grumble.Flags) {
	// readable
	flags.VerboseRegister(f)

	// location
	f.String("p", "locationpath", "", "location file path")
	f.String("l", "location", "", "location string by [json|hex|base64]")
}

// Register register access
func Register(app *grumble.App) {
	accessCommand := &grumble.Command{
		Name:     "access",
		Help:     "access tools",
		LongHelp: "blobstore access api tools",
		Run: func(c *grumble.Context) error {
			if config.AccessConsulAddr() == "" {
				fmt.Println("access services: ")
				for index, node := range config.AccessPriorityAddrs() {
					fmt.Printf("node %d: %s\n", index+1, node)
				}
				return nil
			}
			cli, err := config.NewConsulClient(config.AccessConsulAddr())
			if err != nil {
				return err
			}
			services, _, err := cli.Health().Service("access", "", true,
				&api.QueryOptions{RequireConsistent: true})
			if err != nil {
				return err
			}
			fmt.Println("discovery access services on")
			fmt.Println(common.Readable(services))
			fmt.Println()
			return nil
		},
	}
	app.AddCommand(accessCommand)

	accessCommand.AddCommand(&grumble.Command{
		Name: "put",
		Help: "put file",
		Run:  putFile,
		Flags: func(f *grumble.Flags) {
			accessFlags(f)
			f.String("d", "data", "", "raw data body")
			f.String("f", "filepath", "", "put file path")
			f.Int64("", "size", 0, "put file size, 0 means file size")
			f.Uint("", "hashes", 0, "put file hashes")
		},
	})
	accessCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get file",
		Run:  getFile,
		Flags: func(f *grumble.Flags) {
			accessFlags(f)
			f.String("f", "filepath", "", "save file path")
			f.Uint64("", "offset", 0, "get file offset")
			f.Uint64("", "readsize", 0, "get file read size, 0 means file size")
		},
	})
	accessCommand.AddCommand(&grumble.Command{
		Name: "del",
		Help: "del file",
		Run:  delFile,
		Flags: func(f *grumble.Flags) {
			accessFlags(f)
		},
	})
	accessCommand.AddCommand(&grumble.Command{
		Name:     "cluster",
		Help:     "show cluster",
		LongHelp: "show cluster in [region]",
		Run:      showClusters,
		Args: func(a *grumble.Args) {
			a.String("region", "show cluster of region", grumble.Default(""))
		},
	})
	accessCommand.AddCommand(&grumble.Command{
		Name:     "ec",
		Help:     "show ec buffer size",
		LongHelp: "show ec buffer size with [blobsize]",
		Run:      showECbuffer,
		Args: func(a *grumble.Args) {
			a.Int("blobsize", "show ec buffer with blobsize", grumble.Default(0))
		},
		Flags: func(f *grumble.Flags) {
			f.IntL("codemode", 0, "on special codemode")
		},
	})
}
