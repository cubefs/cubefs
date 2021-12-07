// Copyright 2020 The Chubao Authors.
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

package cmd

import (
	"context"
	"fmt"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/jacobsa/daemonize"
	"github.com/spf13/cobra"
	"regexp"
)

const (
	cmdTrashUse   = "trash [COMMAND]"
	cmdTrashShort = "Manage trash"
)

var (
	RegexpFileIsDeleted, _ = regexp.Compile("_(\\d{14})+.+(\\d{6})+$")
	dentryNameTimeFormat   = "20060102150405.000000"
	ctx                    = context.Background()
	gTrashEnv              *TrashEnv
	forceFlag              = false
	isRecursive            = false
)

type TrashEnv struct {
	VolName      string
	metaWrapper  *meta.MetaWrapper
	masterClient *master.MasterClient
}

func (env *TrashEnv) String() string {
	return fmt.Sprintf("master: %v, vol: %v", env.masterClient.Nodes(), env.VolName)
}

func (env *TrashEnv) init() (err error) {
	if env.metaWrapper != nil {
		return
	}
	config := new(meta.MetaConfig)
	config.Masters = env.masterClient.Nodes()
	config.Volume = env.VolName
	env.metaWrapper, err = meta.NewMetaWrapper(config)
	if err != nil {
		log.LogErrorf("failed to new trash env: %v", env)
	}
	return
}

func newTrashEnv(client *master.MasterClient, vol string) (err error) {
	if gTrashEnv != nil {
		return
	}
	gTrashEnv = new(TrashEnv)
	gTrashEnv.masterClient = client
	gTrashEnv.VolName = vol
	config := new(meta.MetaConfig)
	config.Masters = client.Nodes()
	config.Volume = vol
	gTrashEnv.metaWrapper, err = meta.NewMetaWrapper(config)
	if err != nil {
		log.LogErrorf("failed to new trash env: %v, err: %v", gTrashEnv, err.Error())
		return
	}
	return
}

func NewTrashCmd(client *master.MasterClient) *cobra.Command {
	var c = &cobra.Command{
		Use:   cmdTrashUse,
		Short: cmdTrashShort,
		Args:  cobra.MinimumNArgs(0),
	}
	c.AddCommand(
		newFSListCmd(client),
		newFSRecoverCmd(client),
		newFSCleanCmd(client),
		newStatCmd(client),
		newTestCmd(client),
		newTest2Cmd(client),
		newUSToStrCmd(),
		newDateToUSCmd(),
	)
	return c
}

func initLog() (err error) {
	level := log.DebugLevel
	_, err = log.InitLog("./logs", "trash", level, nil)
	if err != nil {
		daemonize.SignalOutcome(err)
		return
	}
	return
}
