// Copyright 2018 The Chubao Authors.
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
	"fmt"
	"os"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdUserUse   = "user [COMMAND]"
	cmdUserShort = "Manage cluster users"
)

func newUserCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdUserUse,
		Short: cmdUserShort,
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(
		newUserCreateCmd(client),
		newUserInfoCmd(client),
		newUserPermCmd(client),
	)
	return cmd
}

const (
	cmdUserCreateUse   = "create [USER ID]"
	cmdUserCreateShort = "Create a new user"
)

func newUserCreateCmd(client *master.MasterClient) *cobra.Command {
	var optPassword string
	var optAccessKey string
	var optSecretKey string
	var optUserType string
	var optYes bool
	var cmd = &cobra.Command{
		Use:   cmdUserCreateUse,
		Short: cmdUserCreateShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var userID = args[0]
			var password = optPassword
			var accessKey = optAccessKey
			var secretKey = optSecretKey
			var userType = proto.UserTypeFromString(optUserType)

			if !userType.Valid() {
				errout("Invalid user type.")
				os.Exit(1)
			}

			// display information before create
			var displayPassword = "[default]"
			if optPassword != "" {
				displayPassword = optPassword
			}
			var displayAccessKey = "[auto generate]"
			var displaySecretKey = "[auto generate]"
			if optAccessKey != "" && optSecretKey != "" {
				displayAccessKey = optAccessKey
				displaySecretKey = optSecretKey
			}
			var displayUserType = userType.String()
			fmt.Printf("Create a new ChubaoFS cluster user\n")
			stdout("  User ID   : %v\n", userID)
			stdout("  Password  : %v\n", displayPassword)
			stdout("  Access Key: %v\n", displayAccessKey)
			stdout("  Secret Key: %v\n", displaySecretKey)
			stdout("  Type      : %v\n", displayUserType)

			// ask user for confirm
			if !optYes {
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}

			var param = proto.UserCreateParam{
				ID:        userID,
				Password:  password,
				AccessKey: accessKey,
				SecretKey: secretKey,
				Type:      userType,
			}
			var akPolicy *proto.AKPolicy
			if akPolicy, err = client.UserAPI().Create(&param); err != nil {
				errout("Create user failed: %v\n", err)
				os.Exit(1)
			}

			// display operation result
			stdout("Create user success:\n")
			printUserInfo(akPolicy)
			return
		},
	}
	cmd.Flags().StringVar(&optPassword, "password", "", "Specify user password")
	cmd.Flags().StringVar(&optAccessKey, "access-key", "", "Specify user access key for object storage interface authentication")
	cmd.Flags().StringVar(&optSecretKey, "secret-key", "", "Specify user secret key for object storage interface authentication")
	cmd.Flags().StringVar(&optUserType, "user-type", "normal", "Specify user type [normal | admin]")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

const (
	cmdUserInfoUse   = "info [USER ID]"
	cmdUserInfoShort = "Show detail information about specified user"
)

func newUserInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdUserInfoUse,
		Short: cmdUserInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var userID = args[0]
			var akp *proto.AKPolicy
			if akp, err = client.UserAPI().GetUserInfo(userID); err != nil {
				errout("Get user info failed: %v\n", err)
				os.Exit(1)
			}
			printUserInfo(akp)
		},
	}

	return cmd
}

const (
	cmdUserPermUse   = "perm [USER ID] [VOLUME] [PERM]"
	cmdUserPermShort = "Setup volume permission for a user"
)

func newUserPermCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdUserPermUse,
		Short: cmdUserPermShort,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var userID = args[0]
			var volume = args[1]
			var perm proto.Permission
			switch args[2] {
			case "ro":
				perm = proto.BuiltinPermissionReadOnly
			case "rw":
				perm = proto.BuiltinPermissionWritable
			case "none":
				perm = proto.NonePermission
			default:
				stdout("Permission must be on of ro, rw, none")
				return
			}
			stdout("Setup volume permission\n")
			stdout("  User ID   : %v\n", userID)
			stdout("  Volume    : %v\n", volume)
			stdout("  Permission: %v\n", perm.ReadableString())

			// ask user for confirm
			stdout("\nConfirm (yes/no)[yes]: ")
			var userConfirm string
			_, _ = fmt.Scanln(&userConfirm)
			if userConfirm != "yes" && len(userConfirm) != 0 {
				stdout("Abort by user.\n")
				return
			}
			var err error
			defer func() {
				if err != nil {
					errout("Setup permission failed:\n%v\n", err)
					os.Exit(1)
				}
			}()
			var akp *proto.AKPolicy
			if akp, err = client.UserAPI().GetUserInfo(userID); err != nil {
				return
			}
			if _, err = client.AdminAPI().GetVolumeSimpleInfo(volume); err != nil {
				return
			}
			var newUserPolicy = proto.NewUserPolicy()
			newUserPolicy.SetPerm(volume, perm)
			if perm.IsNone() {
				akp, err = client.UserAPI().DeletePolicy(akp.AccessKey, newUserPolicy)
			} else {
				akp, err = client.UserAPI().AddPolicy(akp.AccessKey, newUserPolicy)
			}
			if err != nil {
				return
			}
			printUserInfo(akp)
		},
	}
	return cmd
}

func printUserInfo(akp *proto.AKPolicy) {
	stdout("\n[Summary]\n")
	stdout("  User ID    : %v\n", akp.UserID)
	stdout("  Access Key : %v\n", akp.AccessKey)
	stdout("  Secret Key : %v\n", akp.SecretKey)
	stdout("  Type       : %v\n", akp.UserType)
	stdout("  Create Time: %v\n", akp.CreateTime)
	if akp.Policy == nil {
		return
	}
	stdout("\n[Own volumes]\n")
	if len(akp.Policy.OwnVols) != 0 {
		for _, vol := range akp.Policy.OwnVols {
			stdout("  %s\n", vol)
		}
	} else {
		stdout("  None\n")
	}
	stdout("\n[Authorized volumes]\n")

	if len(akp.Policy.AuthorizedVols) != 0 {
		stdout("  %10v\t%10v\n", "VOLUME", "PERMISSION")
		for vol, perms := range akp.Policy.AuthorizedVols {
			stdout("  %10v\t%10v\n", vol, perms)
		}
	} else {
		stdout("  None\n")
	}
}
