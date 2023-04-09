package main

import (
    "fmt"
    "github.com/spf13/cobra"
    "log"
    "os"
    "time"
)

var buildVersion = "DebugVersion"
var buildDate = time.Now().String()

var logFile *os.File

var rootCmd = &cobra.Command{
    Use:                "cfsauto",
    Short:              "",
    Long:               ``,
    FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
    Run: func(c *cobra.Command, args []string) {
        var err error
        // version
        v, _ := c.Flags().GetBool("version")
        if v {
            fmt.Println(fmt.Sprintf("Version: %s, BuildDate: %s", buildVersion, buildDate))
            return
        }

        // mount list
        if len(args) == 0 {
            err = cfsList()
            if err != nil {
                log.Fatal("cfsListErr, err: ", err)
            }
            return
        }

        // mount
        options, err := c.Flags().GetString("options")
        if err != nil {
            log.Fatal("optionsErr, err: ", err)
        }

        log.Println("[info] cfsauto options: ", options)
        log.Println("[info] cfsauto args: ", args)

        err = cfsMount(args[len(args)-1], options)
        if err != nil {
            log.Fatal("cfsMountErr, err: ", err)
        }

    },
}

func main() {

    rootCmd.PersistentFlags().BoolP("no-mtab", "n", false, "Mount without writing in /etc/mtab")
    rootCmd.PersistentFlags().StringP("options", "o", "", "Options, followed by a comma separated string of options")
    rootCmd.PersistentFlags().BoolP("version", "V", false, "Output version")

    if err := rootCmd.Execute(); err != nil {
        os.Exit(1)
    }
}

func init() {
    // logFile
    var err error
    logFile, err = os.OpenFile(getEnv(EnvLogFile, LogFile), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
    if err != nil {
        log.Fatalf("error opening file: %v", err)
    }
    // defer logFile.Close()

    log.SetOutput(logFile)
    log.SetFlags(log.Lshortfile | log.Ltime | log.Ldate)
}
