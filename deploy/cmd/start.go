package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

var (
	ip           string
	allStart     bool
	datanodeDisk string
	disk         string
)

var StartCmd = &cobra.Command{
	Use:   "start",
	Short: "start service",
	Long:  `This command will start services.`,
	Run: func(cmd *cobra.Command, args []string) {
		if allStart {
			fmt.Println("start all services......")
			err := startAllMaster()
			if err != nil {
				log.Println(err)
			}

			err = startAllMetaNode()
			if err != nil {
				log.Println(err)
			}

			err = startAllDataNode()
			if err != nil {
				log.Println(err)
			}

		} else {
			fmt.Println(cmd.UsageString())
		}
	},
}

var startMasterCommand = &cobra.Command{
	Use:   "master",
	Short: "start master",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		err := startAllMaster()
		if err != nil {
			log.Println(err)
		}
	},
}

var startFromDockerCompose = &cobra.Command{
	Use:   "test",
	Short: "start test for on node",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		err := startALLFromDockerCompose(disk)
		if err != nil {
			log.Println(err)
		}
	},
}

var startMetanodeCommand = &cobra.Command{
	Use:   "metanode",
	Short: "start metanode",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		if cmd.Flags().Changed("ip") {
			err := startMetanodeInSpecificNode(ip)
			if err != nil {
				log.Println(err)
			}
		} else {
			err := startAllMetaNode()
			if err != nil {
				log.Println(err)
			}
		}
	},
}

var startDatanodeCommand = &cobra.Command{
	Use:   "datanode",
	Short: "start datanode",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		if cmd.Flags().Changed("ip") {
			startDatanodeInSpecificNode(ip)
			fmt.Println("start datanode in ", ip)

		} else {
			err := startAllDataNode()
			if err != nil {
				log.Println(err)
			}
		}
	},
}

func init() {
	StartCmd.AddCommand(startMasterCommand)
	StartCmd.AddCommand(startMetanodeCommand)
	StartCmd.AddCommand(startDatanodeCommand)
	StartCmd.AddCommand(startFromDockerCompose)
	startFromDockerCompose.PersistentFlags().StringVarP(&disk, "disk", "d", "", "disk option description")
	startFromDockerCompose.MarkPersistentFlagRequired("disk")
	StartCmd.Flags().BoolVarP(&allStart, "all", "a", false, "start all services")
	StartCmd.PersistentFlags().StringVarP(&ip, "ip", "", "", "specify an IP address to start services")
	startDatanodeCommand.Flags().StringVarP(&datanodeDisk, "disk", "d", "", "specify the disk where datanode mount")
}
