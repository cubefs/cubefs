package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

var allStop bool

var StopCmd = &cobra.Command{
	Use:   "stop",
	Short: "start service",
	Long:  `This command will start service.`,
	Run: func(cmd *cobra.Command, args []string) {
		if allStop {
			fmt.Println("stop all services......")
			err := stopAllMaster()
			if err != nil {
				log.Println(err)
			}
			fmt.Println("stop all master services")

			err = stopAllMetaNode()
			if err != nil {
				log.Println(err)
			}
			fmt.Println("stop all metanode services")
			err = stopAllDataNode()
			if err != nil {
				log.Println(err)
			}
			fmt.Println("stop all datanode services")

		} else {
			fmt.Println(cmd.UsageString())
		}
	},
}

var stopFromDockerCompose = &cobra.Command{
	Use:   "test",
	Short: "start test for on node",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		err := stopALLFromDockerCompose()
		if err != nil {
			log.Println(err)
		}
	},
}

var stopMasterCommand = &cobra.Command{
	Use:   "master",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		if cmd.Flags().Changed("ip") {
			fmt.Println("ip:", ip)
		} else {
			err := stopAllMaster()
			if err != nil {
				log.Println(err)
			}
			log.Println("stop all master services")
		}
	},
}

var stopMetanodeCommand = &cobra.Command{
	Use:   "metanode",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		if cmd.Flags().Changed("ip") {
			err := stopMetanodeInSpecificNode(ip)
			if err != nil {
				log.Println(err)
			}
			log.Println("stop metanode in ", ip)
		} else {
			err := stopAllMetaNode()
			if err != nil {
				log.Println(err)
			}
			log.Println("stop all metanode services")
		}
	},
}

var stopDatanodeCommand = &cobra.Command{
	Use:   "datanode",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		if cmd.Flags().Changed("ip") {
			stopDatanodeInSpecificNode(ip)
			fmt.Println("stop datanode in ", ip)
		} else {
			err := stopAllDataNode()
			if err != nil {
				log.Println(err)
			}
			fmt.Println("stop all datanode services")
		}
	},
}

func init() {
	StopCmd.AddCommand(stopMasterCommand)
	StopCmd.AddCommand(stopMetanodeCommand)
	StopCmd.AddCommand(stopDatanodeCommand)
	StopCmd.AddCommand(stopFromDockerCompose)
	StopCmd.Flags().BoolVarP(&allStop, "all", "a", false, "stop all services")
	StopCmd.PersistentFlags().StringVarP(&ip, "ip", "", "", "specify an IP address to start services")
	stopDatanodeCommand.Flags().StringVarP(&datanodeDisk, "disk", "d", "", "specify the disk where datanode mount")
}
