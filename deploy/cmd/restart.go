package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

var allRestart bool

var RestartCmd = &cobra.Command{
	Use:   "restart",
	Short: "start service",
	Long:  `This command will start service.`,
	Run: func(cmd *cobra.Command, args []string) {
		if allRestart {
			fmt.Println("restart all services......")
			err := stopAllMaster()
			if err != nil {
				log.Println(err)
			}
			err = startAllMaster()
			if err != nil {
				log.Println(err)
			}
			err = startAllMetaNode()
			if err != nil {
				log.Println(err)
			}
			err = stopAllMetaNode()
			if err != nil {
				log.Println(err)
			}

			err = startAllDataNode()
			if err != nil {
				log.Println(err)
			}
			err = stopAllDataNode()
			if err != nil {
				log.Println(err)
			}

		} else {
			fmt.Println(cmd.UsageString())
		}
	},
}

var restartMasterCommand = &cobra.Command{
	Use:   "master",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		err := stopAllMaster()
		if err != nil {
			log.Println(err)
		}
		err = startAllMaster()
		if err != nil {
			log.Println(err)
		}
		log.Println("restart all master")
	},
}

var restartMetanodeCommand = &cobra.Command{
	Use:   "metanode",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		if cmd.Flags().Changed("ip") {
			err := stopMetanodeInSpecificNode(ip)
			if err != nil {
				log.Println(err)
			}
			err = startMetanodeInSpecificNode(ip)
			if err != nil {
				log.Println(err)
			}
			log.Println("restart metanode in ", ip)
		} else {
			err := stopAllMetaNode()
			if err != nil {
				log.Println(err)
			}
			err = startAllMetaNode()
			if err != nil {
				log.Println(err)
			}
			log.Println("restart all metanode")
		}
	},
}

var restartDatanodeCommand = &cobra.Command{
	Use:   "datanode",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		if cmd.Flags().Changed("ip") {
			err := stopDatanodeInSpecificNode(ip)
			if err != nil {
				log.Println(err)
			}
			err = startDatanodeInSpecificNode(ip)
			if err != nil {
				log.Println(err)
			}
			log.Println("restart datanode in ", ip)

		} else {
			err := stopAllDataNode()
			if err != nil {
				log.Println(err)
			}
			err = startAllDataNode()
			if err != nil {
				log.Println(err)
			}
			log.Println("restart all datanode")
		}
	},
}

func init() {
	RestartCmd.AddCommand(restartMasterCommand)
	RestartCmd.AddCommand(restartMetanodeCommand)
	RestartCmd.AddCommand(restartDatanodeCommand)
	RestartCmd.Flags().BoolVarP(&allRestart, "all", "a", false, "restart all services")
	RestartCmd.PersistentFlags().StringVarP(&ip, "ip", "", "", "specify an IP address to start services")
	restartDatanodeCommand.Flags().StringVarP(&datanodeDisk, "disk", "d", "", "specify the disk where datanode mount")
}
