package cmd

import (
	"fmt"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"
)

var (
	test2RootDir = "/cfs/mnt/test2"
)

func newTest2Cmd(client *master.MasterClient) *cobra.Command {
	var vol string
	var c = &cobra.Command{
		Use:   "test2",
		Short: "run the unit tests",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			err := newTrashEnv(client, vol)
			if err != nil {
				return
			}
			err = test2()
			if err != nil {
				fmt.Println(err.Error())
				return
			}
		},
	}
	c.Flags().StringVarP(&vol, "vol", "v", "", "volume Name")
	c.MarkFlagRequired("vol")

	return c
}

func test2() (err error) {
	mixTest()
	return nil
}

type MixTestItem struct {
	dirIndex  int
	fileIndex int
}

func mixTest() {
	fmt.Println("mixTest")
	err := os.MkdirAll(test2RootDir, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	dataChan := make(chan *MixTestItem, 1000)
	ts := time.Now().Unix()
	fmt.Println("start mixTestRead")
	go mixTestScanExistDir(fmt.Sprintf("mixTest_%v", ts))
	go mixTestRead(dataChan, ts)
	fmt.Println("start mixTestOp")

	mixTestOp(dataChan, ts)
}

func mixTestScanExistDir(exclusive string) {
	for i := 0; i < 10000000; i++ {
		count := 0
		dirs, err := ioutil.ReadDir(test2RootDir)
		if err != nil {
			panic(err)
		}
		for _, dir := range dirs {
			if dir.Name() == exclusive {
				continue
			}
			dirPath := path.Join(test2RootDir, dir.Name())
			subDirs, e := ioutil.ReadDir(dirPath)
			if e != nil {
				perr := fmt.Errorf("dir: %v, err: %v", dirPath, e.Error())
				panic(perr)
			}

			for _, subDir := range subDirs {
				subDirPath := path.Join(dirPath, subDir.Name())
				files, e := ioutil.ReadDir(subDirPath)
				if e != nil {
					perr := fmt.Errorf("dir: %v, err: %v", subDirPath, e.Error())
					panic(perr)
				}

				for _, file := range files {
					if strings.HasSuffix(file.Name(), "sym") {
						continue
					}
					if strings.HasPrefix(file.Name(), "d_") {
						continue
					}
					fPath := path.Join(subDirPath, file.Name())
					_, e = ioutil.ReadFile(fPath)
					if e != nil {
						perr := fmt.Errorf("file: %v, err: %v", fPath, e.Error())
						panic(perr)
					}
					count++
					log.LogDebugf("Scan: %v", fPath)
					if count%100 == 0 {
						log.LogDebugf("Scan: %v", fPath)
					}
				}
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func mixTestRead(dataChan chan *MixTestItem, ts int64) {
	name := fmt.Sprintf("mixTest_%v", ts)
	rootDir := path.Join(test2RootDir, name)
	for {
		select {
		case item := <-dataChan:
			{
				var files = [5]int{0, 3, 5, 6}
				for _, i := range files {
					dir := path.Join(rootDir, fmt.Sprintf("d_%v", item.dirIndex))
					f := path.Join(dir, fmt.Sprintf("f_%v_%v", item.fileIndex, i))
					_, err := ioutil.ReadFile(f)
					if err != nil {
						log.LogErrorf("file: %v, err: %v", f, err.Error())
						fmt.Printf(err.Error())
						panic(err)
					}
				}
			}
			{
				dir := path.Join(rootDir, fmt.Sprintf("d_%v", item.dirIndex))
				f := path.Join(dir, fmt.Sprintf("f_%v_2_hard", item.fileIndex))
				_, err := ioutil.ReadFile(f)
				if err != nil {
					log.LogErrorf("file: %v, err: %v", f, err.Error())
					fmt.Printf(err.Error())
					panic(err)
				}
			}
			{
				dir := path.Join(rootDir, fmt.Sprintf("d_%v", item.dirIndex))
				f := path.Join(dir, fmt.Sprintf("f_%v_4_sym", item.fileIndex))
				_, err := ioutil.ReadFile(f)
				if err == nil {
					err = fmt.Errorf("file: %v expect a error ", f)
					log.LogErrorf(err.Error())
					fmt.Printf(err.Error())
					panic(err)
				}
			}
			fmt.Printf("Dir: %v, File: %v\n", item.dirIndex, item.fileIndex)
			log.LogDebugf("Dir: %v, File: %v\n", item.dirIndex, item.fileIndex)
		}
	}
}

func mixTestOp(dataChan chan *MixTestItem, ts int64) (err error) {
	name := fmt.Sprintf("mixTest_%v", ts)
	rootDir := path.Join(test2RootDir, name)
	err = os.MkdirAll(rootDir, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	dirs := 5000
	files := 50000
	m := 0
	i := 1
	for m = 0; m < dirs; m++ {
		dir := path.Join(rootDir, fmt.Sprintf("d_%v", m))
		err = os.Mkdir(dir, 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		for ; i < files; i++ {
			f := path.Join(dir, fmt.Sprintf("f_%v_0", i))
			err = ioutil.WriteFile(f, []byte("trash-cli-test"), 0755)
			if err != nil {
				log.LogError(err.Error())
				return
			}

			f1 := path.Join(dir, fmt.Sprintf("f_%v_1", i))
			err = ioutil.WriteFile(f1, []byte("trash-cli-test"), 0755)
			if err != nil {
				log.LogError(err.Error())
				return
			}
			err = os.Remove(f1)
			if err != nil {
				log.LogError(err.Error())
				return
			}

			{
				f2 := path.Join(dir, fmt.Sprintf("f_%v_2", i))
				err = ioutil.WriteFile(f2, []byte("trash-cli-test"), 0755)
				if err != nil {
					log.LogError(err.Error())
					return
				}
				f2HardPath := path.Join(dir, fmt.Sprintf("f_%v_2_hard", i))
				info, e := os.Stat(f2HardPath)
				if e == nil {
					log.LogErrorf("info.Name: %v", info.Name())
					err = fmt.Errorf("file: %v expect a error ", f2HardPath)
					log.LogErrorf(err.Error())
					return
				}
				err = os.Link(f2, f2HardPath)
				if err != nil {
					log.LogError(err.Error())
					return
				}
				err = os.Remove(f2)
				if err != nil {
					log.LogError(err.Error())
					return
				}
			}

			{
				f2 := path.Join(dir, fmt.Sprintf("f_%v_3", i))
				err = ioutil.WriteFile(f2, []byte("trash-cli-test"), 0755)
				if err != nil {
					log.LogError(err.Error())
					return
				}
				f2HardPath := path.Join(dir, fmt.Sprintf("f_%v_3_hard", i))
				err = os.Link(f2, f2HardPath)
				if err != nil {
					log.LogError(err.Error())
					return
				}
				err = os.Remove(f2HardPath)
				if err != nil {
					log.LogError(err.Error())
					return
				}
			}

			{
				f3 := path.Join(dir, fmt.Sprintf("f_%v_4", i))
				f3SymPath := path.Join(dir, fmt.Sprintf("f_%v_4_sym", i))
				err = ioutil.WriteFile(f3, []byte("trash-cli-test"), 0755)
				if err != nil {
					log.LogError(err.Error())
					return
				}
				err = os.Symlink(f3, f3SymPath)
				if err != nil {
					log.LogError(err.Error())
					return
				}
				err = os.Remove(f3)
				if err != nil {
					log.LogError(err.Error())
					return
				}
			}
			{
				f3 := path.Join(dir, fmt.Sprintf("f_%v_5", i))
				f3SymPath := path.Join(dir, fmt.Sprintf("f_%v_5_sym", i))
				err = ioutil.WriteFile(f3, []byte("trash-cli-test"), 0755)
				if err != nil {
					log.LogError(err.Error())
					return
				}
				err = os.Symlink(f3, f3SymPath)
				if err != nil {
					log.LogError(err.Error())
					return
				}
				err = os.Remove(f3SymPath)
				if err != nil {
					log.LogError(err.Error())
					return
				}
			}

			f4 := path.Join(dir, fmt.Sprintf("f_%v_6", i))
			err = ioutil.WriteFile(f4, []byte("trash-cli-test"), 0755)
			if err != nil {
				log.LogError(err.Error())
				return
			}
			f5 := path.Join(dir, fmt.Sprintf("f_%v_7", i))
			err = ioutil.WriteFile(f5, []byte("trash-cli"), 0755)
			if err != nil {
				log.LogError(err.Error())
				return
			}
			err = os.Rename(f5, f4)
			if err != nil {
				log.LogError(err.Error())
				return
			}

			d0 := path.Join(dir, fmt.Sprintf("d_%v_0", i))
			err = os.Mkdir(d0, 0755)
			for n := 0; n < 5; n++ {
				df := path.Join(d0, fmt.Sprintf("f_%v", n))
				err = ioutil.WriteFile(df, []byte("trash-cli"), 0755)
				if err != nil {
					log.LogError(err.Error())
					return
				}
			}
			err = os.RemoveAll(d0)
			if err != nil {
				log.LogError(err.Error())
				return
			}
			item := new(MixTestItem)
			item.dirIndex = m
			item.fileIndex = i
			dataChan <- item
			//time.Sleep(5 * time.Second)
		}
	}
	return
}
