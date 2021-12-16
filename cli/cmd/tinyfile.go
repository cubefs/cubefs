package cmd

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"hash/crc32"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	cmdTinyFileUse      = "tinyfile [command]"
	cmdTinyFileShort    = "Read-write-check tiny file"
	cmdTinyFileRWCUse   = "rwc [CFS mountPoint]"
	cmdTinyFileRWCShort = "read-write-check tiny file"
)

func newReadWriteCheckTinyFileCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdTinyFileUse,
		Short: cmdTinyFileShort,
		Args:  cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(
		newTinyFileCheckCmd(),
	)
	return cmd
}

func newTinyFileCheckCmd() *cobra.Command {
	var (
		cycleNum        int
		tmpDir          string
		jobsNum         int
		concurrentNum   int
		rwcCount        int
		remainMountFile bool
	)
	var cmd = &cobra.Command{
		Use:   cmdTinyFileRWCUse,
		Short: cmdTinyFileRWCShort,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			mountPoint := args[0]
			crcPath := path.Join(mountPoint, "crc")
			for i := 0; i < cycleNum; i++ {
				readWriteCheckTinyFile(crcPath, tmpDir, jobsNum, concurrentNum, rwcCount, remainMountFile, i)
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().IntVar(&cycleNum, "cycleNum", 1, "Number of cycles")
	cmd.Flags().StringVar(&tmpDir, "tmpDir", "./tmp/crc", "tmpDir")
	cmd.Flags().IntVar(&jobsNum, "jNum", 10, "Maximum number of jobs")
	cmd.Flags().IntVar(&concurrentNum, "cNum", 50, "Maximum concurrent number")
	cmd.Flags().IntVar(&rwcCount, "rwcCount", 100, "Number of read-write-check per goroutine")
	cmd.Flags().BoolVar(&remainMountFile, "remainMountFile", false, "Remain mounted directory file")
	return cmd
}

func readWriteCheckTinyFile(cfsDir, tmpDir string, jobsNum, concurrentNum, rwcCount int, remainMountFile bool, cycleIndex int) {
	defer func() {
		msg := fmt.Sprintf("begin read-write-check tiny file")
		if r := recover(); r != nil {
			var stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			stdout("%s,%s\n", msg, stack)
		}
	}()
	startTime := time.Now()
	stdout("begin read-write-check tiny file, begin time:%v, cycleIndex:%v\n", startTime.Unix(), cycleIndex)
	stdout("number of jobs:%v number of concurrent:%v every read-write-check count:%v\n", jobsNum, concurrentNum, rwcCount)

	if concurrentNum <= 0 {
		concurrentNum = 1
	}
	wg := sync.WaitGroup{}
	ch := make(chan struct{}, concurrentNum)
	for i := 0; i < jobsNum; i++ {
		ch <- struct{}{}
		wg.Add(1)
		go func(index int) {
			defer func() {
				wg.Done()
				<-ch
			}()
			checkCrcFail := false
			everyGoroutineStartTime := time.Now()
			for i := 0; i < rwcCount; i++ {
				cfsFileName := getFileName(cfsDir, index, i)
				tmpFileName := getFileName(tmpDir, index, i)
				if cfsFileName == "" || tmpFileName == "" {
					continue
				}
				bufStr := strings.Repeat("1", rand.Intn(2047)+1)
				bytes := []byte(bufStr)
				cfsWriteSuc := writeFile(cfsFileName, bufStr)
				tmpWriteSuc := writeFile(tmpFileName, bufStr)
				if cfsWriteSuc && tmpWriteSuc {
					ok := crcCheck(cfsFileName, tmpFileName, bytes)
					if !ok {
						checkCrcFail = true
					}
				}
				if !remainMountFile {
					err := os.Remove(cfsFileName)
					if err != nil {
						stdout("remove cfsFileName[%v] failed, please remove it manually, err: %v\n", cfsFileName, err)
					}
				}
				err := os.Remove(tmpFileName)
				if err != nil {
					stdout("remove tmpFileName[%v] failed, please remove it manually, err: %v\n", tmpFileName, err)
				}
			}
			if checkCrcFail {
				stdoutRed(fmt.Sprintf("goroutineId:%v rwc count:%v time consuming:%v, crc check unequal", index, rwcCount, time.Since(everyGoroutineStartTime)))
			} else {
				stdoutGreen(fmt.Sprintf("goroutineId:%v rwc count:%v time consuming:%v, crc check equal", index, rwcCount, time.Since(everyGoroutineStartTime)))
			}
		}(i)
	}
	wg.Wait()
	stdout("clean tmp file......\n")
	if !remainMountFile {
		err := os.RemoveAll(cfsDir)
		if err != nil {
			stdout("remove dir[%v] failed, please remove it manually, err: %v\n", cfsDir, err)
		}
	}
	err := os.RemoveAll(tmpDir)
	if err != nil {
		stdout("remove dir[%v] failed, please remove it manually, err: %v\n", tmpDir, err)
	}
	stdout("finished read-write-check tiny file, time consuming: %v\n", time.Since(startTime))
}

func getFileName(rootDir string, index, no int) (fileName string) {
	subDir := path.Join(rootDir, strconv.Itoa(index))
	err := os.MkdirAll(subDir, 0666)
	if err != nil {
		stdout("err: %v\n", err)
		os.Exit(1)
	}
	fileName = path.Join(subDir, fmt.Sprintf("%v_%v_%v", strconv.FormatInt(time.Now().UnixNano(), 10), no, index))
	return
}

func writeFile(fileName string, buf string) bool {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND|os.O_SYNC, 0664)
	if err != nil {
		stdout("os.OpenFile:%v\n", err)
		return false
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	_, err = w.WriteString(buf)
	if err != nil {
		stdout("w.WriteString:%v\n", err)
		return false
	}
	err = w.Flush()
	if err != nil {
		stdout("w.Flush:%v\n", err)
		return false
	}
	return true
}

func readFileData(fileName string) []byte {
	data, _ := ioutil.ReadFile(fileName)
	return data
}

func crcCheck(cfsFileName, tmpFileName string, bytes []byte) bool {
	cfsData := readFileData(cfsFileName)
	tmpData := readFileData(tmpFileName)
	cfsCrc := crc32.ChecksumIEEE(cfsData)
	tmpCrc := crc32.ChecksumIEEE(tmpData)
	byteCrc := crc32.ChecksumIEEE(bytes)
	return cfsCrc == tmpCrc && cfsCrc == byteCrc
}
