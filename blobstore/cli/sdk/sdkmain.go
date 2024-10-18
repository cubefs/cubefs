// Copyright 2024 The CubeFS Authors.
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

package sdk

import (
	"os"

	"github.com/desertbit/grumble"

	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/sdk"
)

var sdkCli acapi.Client

func Register(app *grumble.App) {
	cmCommand := &grumble.Command{
		Name:     "sdk",
		Help:     "sdk tools",
		LongHelp: "blobstore access sdk tools",
		Run:      newSdkClient,
		Flags: func(f *grumble.Flags) {
			sdkFlags(f)
		},
	}
	app.AddCommand(cmCommand)

	addCmdGetBlob(cmCommand)
	addCmdDelBlob(cmCommand)
	addCmdPutBlob(cmCommand)
	addCmdListBlob(cmCommand)
}

func sdkFlags(f *grumble.Flags) {
	// readable
	flags.VerboseRegister(f)
	f.String("c", "config", "sdk.conf", "config path(json type file)")
}

func newSdkClient(c *grumble.Context) error {
	if sdkCli != nil {
		return nil
	}
	confPath := c.Flags.String("config")
	if confPath == "" {
		return fmt.Errorf("no config path setting")
	}

	file, err := os.Open(confPath)
	if err != nil {
		return err
	}
	defer file.Close()

	sdkConf := sdk.Config{}
	err = common.NewDecoder(file).Decode(&sdkConf)
	if err != nil {
		return err
	}
	if flags.Verbose(c.Flags) {
		fmt.Printf("load path:%s, config:%s \n", confPath, common.Readable(sdkConf))
	}

	newCli, err := sdk.New(&sdkConf)
	if err != nil {
		return err
	}

	sdkCli = newCli
	fmt.Println("----new sdk client OK----")
	return nil
}

func getSdkClient() (acapi.Client, error) {
	if sdkCli != nil {
		return sdkCli, nil
	}
	return nil, fmt.Errorf("empty sdk client, please new the client with conf")
}

func getOrNewClient(c *grumble.Context) (acapi.Client, error) {
	if sdkCli != nil {
		return sdkCli, nil
	}

	err := newSdkClient(c)
	return sdkCli, err
}
