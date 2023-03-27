package cmd

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"
)

var (
	cfsDir      = fmt.Sprintf("%v_%v_%v", "/trash_cli_test", getTimeStr(time.Now().UnixNano()/1000), "a")
	testRootDir = fmt.Sprintf("%v%v", "/cfs/mnt", cfsDir)
)

type listRow struct {
	Seq      int
	Name     string
	INode    uint64
	FileType string
	Size     uint64
	IsDel    string
	TS       string
}

func newTestCmd(client *master.MasterClient) *cobra.Command {
	var vol string
	var c = &cobra.Command{
		Use:   "test",
		Short: "run the unit tests",
		Run: func(cmd *cobra.Command, args []string) {
			err := newTrashEnv(client, vol)
			if err != nil {
				return
			}
			err = test()
			if err != nil {
				fmt.Println("err: " + err.Error())
				return
			}
			fmt.Println("Test is done")
		},
	}
	c.Flags().StringVarP(&vol, "vol", "v", "", "volume Name")
	c.MarkFlagRequired("vol")
	return c
}

func test() (err error) {
	err = prepare()
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = basicTest()
	if err != nil {
		log.LogError(err.Error())
		return
	}

	/*
		err = recursiveTest()
		if err != nil {
			log.LogError(err.Error())
			return
		}

	*/

	err = sameNameTest()
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = recursiveRecoverTest()
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = RenameToNotExistFileTest()
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = RenameToExistFileTest()
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = RenameDirTest()
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = hardLinkTestCleanOneLink()
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = hardLinkTestCleanTwoLink()
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = symLinkTest()
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = batchDeleteFileTest()
	if err != nil {
		log.LogError(err.Error())
		return
	}

	fmt.Println("=====================>")
	err = readDirTest()
	if err != nil {
		log.LogError(err.Error())
		return
	}
	return
}

func prepare() (err error) {
	// 01. mkdir /trash_cli_test
	err = os.Mkdir(testRootDir, 0755)
	if err != nil {
		return
	}

	// 02. mkdir /trash_cli_test/d0,.../d4
	for i := 0; i < 5; i++ {
		d := path.Join(testRootDir, fmt.Sprintf("d%v", i))
		err = os.Mkdir(d, 0755)
		if err != nil {
			return
		}
	}

	for i := 0; i < 3; i++ {
		d := path.Join(testRootDir, fmt.Sprintf("d%v", i))
		for m := 0; m < 5; m++ {
			f := path.Join(d, fmt.Sprintf("f_%v_%v", i, m))
			err = ioutil.WriteFile(f, []byte("trash-cli-test"), 0755)
			if err != nil {
				return
			}
		}
	}

	// 03. touch /trash_cli_test/f5.../f7
	for i := 5; i < 8; i++ {
		f := path.Join(testRootDir, fmt.Sprintf("f%v", i))
		err = ioutil.WriteFile(f, []byte("trash-cli-test"), 0755)
		if err != nil {
			return
		}
	}

	var rows, delRows []*listRow
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = checkListResSize(rows, delRows, 8, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	return
}

func basicTest() (err error) {
	isRecursive = false
	forceFlag = false
	//case:  rm -f /trash_cli_test/f5, than recover it
	var link int
	link, err = getLinks(testRootDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	cfsf5 := path.Join(cfsDir, "f5")
	f5 := path.Join(testRootDir, "f5")
	err = os.Remove(f5)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	var rows, delRows []*listRow
	err, rows, delRows = ListPath(cfsf5, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 0, 1)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 7, 1)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = recoverPath(cfsf5)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkLinks(f5, 1)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 8, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsf5, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 1, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	// case: rm -f /trash_cli_test/f5, than recover /trash_cli_test
	err = os.Remove(f5)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkLinks(testRootDir, link-1)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = recoverPath(cfsDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	/*
		err = checkLinks(testRootDir, link)
		if err != nil {
			log.LogError(err.Error())
			return
		}

	*/
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 8, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsf5, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 1, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	// case: rm -f /trash_cli_test/f5, than clean the deleted f5
	err = os.Remove(f5)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = cleanPath(cfsf5)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkLinks(testRootDir, link-1)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 7, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsf5, true)
	if err == nil {
		err = errors.New("expect a error")
		return
	}

	// case: rm -rf /trash_cli_test/d4 && recover /trash_cli_test
	cfsd4 := path.Join(cfsDir, "d4")
	d4 := path.Join(testRootDir, "d4")
	err = os.Remove(d4)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkLinks(testRootDir, link-2)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	children := 7
	err, rows, delRows = ListPath(cfsd4, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 0, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, children-1, 1)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = recoverPath(cfsd4)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	/*
		err = checkLinks(testRootDir, link-1)
		if err != nil {
			log.LogError(err.Error())
			return
		}

	*/
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, children, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	// case: rm -rf /trash_cli_test/d4 && recover /trash_cli_test
	err = os.Remove(d4)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkLinks(testRootDir, link-2)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = recoverPath(cfsd4)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	/*
		err = checkLinks(testRootDir, link-1)
		if err != nil {
			log.LogError(err.Error())
			return
		}

	*/
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, children, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsd4, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 0, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	// case: rm -rf /trash_cli_test/d4, than clean the deleted d4
	err = os.Remove(d4)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = cleanPath(cfsd4)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkLinks(testRootDir, link-2)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, children-1, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsd4, true)
	if err == nil {
		err = errors.New("expect a error")
		return
	}
	return nil
}

func sameNameTest() (err error) {
	isRecursive = false
	forceFlag = false
	links := 1
	links, err = getLinks(testRootDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	subs := 6
	cfsf6 := path.Join(cfsDir, "f6")
	f6 := path.Join(testRootDir, "f6")
	err = os.Remove(f6)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkLinks(testRootDir, links-1)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	var rows, delRows []*listRow
	err, rows, delRows = ListPath(cfsf6, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 0, 1)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, subs-1, 1)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = ioutil.WriteFile(f6, []byte("trash-cli-test"), 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsf6, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 1, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, subs, 1)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = recoverPath(cfsf6)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkLinks(testRootDir, links+1)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, subs+1, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = os.Remove(f6)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkLinks(testRootDir, links)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, subs, 1)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = cleanPath(cfsf6)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, subs, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	d2 := path.Join(testRootDir, "d2")
	err = os.RemoveAll(d2)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = os.Mkdir(d2, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	for m := 0; m < 5; m++ {
		f := path.Join(d2, fmt.Sprintf("fa_%v_%v", 2, m))
		err = ioutil.WriteFile(f, []byte("trash-cli-test"), 0755)
		if err != nil {
			return
		}
	}
	err = os.RemoveAll(d2)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = os.Mkdir(d2, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	for m := 0; m < 5; m++ {
		f := path.Join(d2, fmt.Sprintf("fb_%v_%v", 2, m))
		err = ioutil.WriteFile(f, []byte("trash-cli-test"), 0755)
		if err != nil {
			return
		}
	}
	err = os.RemoveAll(d2)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, subs-1, 3)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	forceFlag = true
	err = recoverPath(cfsDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, subs+2, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	for _, row := range rows {
		fmt.Println(row.Name)
		if strings.HasPrefix(row.Name, "d2") {
			d := path.Join(testRootDir, row.Name)
			err = os.Remove(d)
			if err != nil {
				log.LogError(err)
				return
			}
		}
	}
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, subs-1, 3)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	isRecursive = true
	for _, row := range delRows {
		if strings.HasPrefix(row.Name, "d2") {
			d := path.Join(cfsDir, row.Name)
			err = recoverPath(d)
			if err != nil {
				log.LogError(err)
				return
			}
			d2 := path.Join(cfsDir, row.Name[0:len(row.Name)-22])
			err, res, delRes := ListPath(d2, true)
			if err != nil {
				log.LogError(err)
				return err
			}
			err = checkListResSize(res, delRes, 5, 0)
			if err != nil {
				log.LogError(err.Error())
				return err
			}
		}
	}
	err, rows, delRows = ListPath(cfsDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, subs+2, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	return
}

func recursiveRecoverTest() (err error) {
	isRecursive = false
	name := fmt.Sprintf("recursiveRecoverTest_%v", time.Now().Unix())
	rootDir := path.Join(testRootDir, name)
	cfsDir := path.Join(cfsDir, name)
	err = os.Mkdir(rootDir, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	d := path.Join(rootDir, "d")
	cfsd := path.Join(cfsDir, "d")
	err = os.Mkdir(d, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	dfiles := 5
	for i := 0; i < dfiles; i++ {
		f := path.Join(d, fmt.Sprintf("f_%v", i))
		err = ioutil.WriteFile(f, []byte("trash-cli-test"), 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	d1 := path.Join(d, "d1")
	cfsd1 := path.Join(cfsd, "d1")
	err = os.Mkdir(d1, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	d1files := 3
	for i := 0; i < d1files; i++ {
		f := path.Join(d1, fmt.Sprintf("f1_%v", i))
		err = ioutil.WriteFile(f, []byte("trash-cli-test"), 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	links := 3
	err = os.RemoveAll(d)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkLinks(rootDir, links-1)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	var rows, delRows []*listRow
	err, rows, delRows = ListPath(cfsd, true)
	err = checkListResSize(rows, delRows, 0, dfiles+1)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsd1, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 0, d1files)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = recoverPath(cfsd)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsd, true)
	err = checkListResSize(rows, delRows, dfiles+1, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsd1, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, 0, d1files)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = os.RemoveAll(d)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	isRecursive = true
	err = recoverPath(cfsd)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsd, true)
	err = checkListResSize(rows, delRows, dfiles+1, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsd1, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = checkListResSize(rows, delRows, d1files, 0)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = os.RemoveAll(d)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = cleanPath(cfsd)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	return
}

func readDirTest() (err error) {
	name := fmt.Sprintf("readDirTest_%v", time.Now().Unix())
	rootDir := path.Join(testRootDir, name)
	cDir := path.Join(cfsDir, name)
	log.LogDebugf("ReadDirTest, rootdir: %v, cfsDir: %v", rootDir, cDir)
	err = os.Mkdir(rootDir, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	for i := 0; i < 2100; i++ {
		f := path.Join(rootDir, fmt.Sprintf("f_%v", i))
		err = ioutil.WriteFile(f, []byte("trash-cli-test"), 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}
	err = os.RemoveAll(rootDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	var rows []*listRow
	err, _, rows = ListPath(cDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	if len(rows) != 2100 {
		err = fmt.Errorf("ReadDir: %v", len(rows))
		return
	}
	return
}

func symLinkTest() (err error) {
	name := fmt.Sprintf("symLinkTest_%v", time.Now().Unix())
	rootDir := path.Join(testRootDir, name)
	cDir := path.Join(cfsDir, name)
	err = os.Mkdir(rootDir, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	for i := 0; i < 5; i++ {
		f1 := path.Join(rootDir, fmt.Sprintf("f_%v", i))
		err = ioutil.WriteFile(f1, []byte(f1), 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	{
		f := path.Join(rootDir, "f_0")
		symPath := path.Join(rootDir, "f_0_sym")
		err = os.Symlink(f, symPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		_, err = os.Stat(symPath)
		//if err != syscall.ENOENT {
		if err == nil {
			err = fmt.Errorf("expected a error, when stat %v", symPath)
			log.LogError(err.Error())
			return
		}

		err = recoverPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		_, err = ioutil.ReadFile(symPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(symPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		_, err = ioutil.ReadFile(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = recoverPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		err = cleanPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		err = os.Remove(symPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		err = recoverPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		_, err = os.Stat(symPath)
		//if err != syscall.ENOENT {
		if err == nil {
			err = fmt.Errorf("expected a error, when stat %v", symPath)
			log.LogError(err.Error())
			return
		}
		err = os.Remove(symPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		err = cleanPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	{
		f := path.Join(rootDir, "f_1")
		symPath := path.Join(rootDir, "f_1_sym")
		err = os.Symlink(f, symPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(symPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = cleanPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = recoverPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		_, err = ioutil.ReadFile(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = recoverPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		_, err = ioutil.ReadFile(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		err = cleanPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}
	err = os.RemoveAll(rootDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = cleanPath(cDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	return
}

/*
1. create 10 empty, and batch delete them, and recover
2. create 10 files, and batch delete tem, and recover
*/
func batchDeleteFileTest() (err error) {
	name := fmt.Sprintf("batchDeletedFileTest%v", time.Now().Unix())
	rootDir := path.Join(testRootDir, name)
	cDir := path.Join(cfsDir, name)
	err = os.Mkdir(rootDir, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	{
		log.LogDebug("==> batchDeleteFileTest: test the files")
		d1 := path.Join(rootDir, "d1")
		cfsd1 := path.Join(cDir, "d1")
		err = os.Mkdir(d1, 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		d1files := 20
		for i := 0; i < d1files; i++ {
			f := path.Join(d1, fmt.Sprintf("f1_%v", i))
			err = ioutil.WriteFile(f, []byte("trash-cli-test"), 0755)
			if err != nil {
				log.LogError(err.Error())
				return
			}
		}

		var ino uint64
		ino, err = gTrashEnv.metaWrapper.LookupPath(ctx, cfsd1)
		if err != nil {
			log.LogErrorf(err.Error())
			return
		}
		dens := make([]proto.Dentry, 0)
		dens, err = gTrashEnv.metaWrapper.ReadDir_ll(ctx, ino)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		inos := make([]uint64, 0, len(dens))
		for _, den := range dens {
			inos = append(inos, den.Inode)
		}

		res := make(map[uint64]int, 0)
		_, err = gTrashEnv.metaWrapper.BatchEvictInodeUntest(ctx, inos, false)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		res, err = gTrashEnv.metaWrapper.BatchUnlinkInodeUntest(ctx, inos, false)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if len(res) != len(inos) {
			err = fmt.Errorf("failed to BatchUnlinkInodeUntest, path: %v, ino: %v, len: %v ", d1, ino, len(res))
			log.LogError(err.Error())
			return
		}

		res, err = gTrashEnv.metaWrapper.BatchDeleteDentryUntest(ctx, ino, dens, false)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		/*
			if len(res) != 0 {
				err = fmt.Errorf("failed to BatchDeleteDentryUntest, path: %v, ino: %v, len: %v ", d1, ino, len(res))
				log.LogError(err.Error())
				return
			}

		*/

		var rows, delRows []*listRow
		err, rows, delRows = ListPath(cfsd1, true)
		err = checkListResSize(rows, delRows, 0, d1files)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = recoverPath(cfsd1)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		err, rows, delRows = ListPath(cfsd1, true)
		err = checkListResSize(rows, delRows, d1files, 0)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	{
		log.LogDebug("==> batchDeleteFileTest: test the dirs")
		d1 := path.Join(rootDir, "d2")
		cfsd1 := path.Join(cDir, "d2")
		err = os.Mkdir(d1, 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		d1files := 20
		for i := 0; i < d1files; i++ {
			d := path.Join(d1, fmt.Sprintf("d2_%v", i))
			err = os.Mkdir(d, 0755)
			if err != nil {
				log.LogError(err.Error())
				return
			}
		}

		var ino uint64
		ino, err = gTrashEnv.metaWrapper.LookupPath(ctx, cfsd1)
		if err != nil {
			log.LogErrorf(err.Error())
			return
		}
		dens := make([]proto.Dentry, 0)
		dens, err = gTrashEnv.metaWrapper.ReadDir_ll(ctx, ino)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		inos := make([]uint64, 0, len(dens))
		for _, den := range dens {
			inos = append(inos, den.Inode)
		}

		res := make(map[uint64]int, 0)
		/*
			res, err = metaWrapper.BatchEvictInodeUntest(ctx, inos)
			if err != nil {
				log.LogError(err.Error())
				return
			}
			if len(res) > 0 {
				err = fmt.Errorf("failed to BatchEvictInodeUntest, path: %v, ino: %v, len: %v ", d1, ino, len(res))
				log.LogError(err.Error())
				return
			}

		*/
		res, err = gTrashEnv.metaWrapper.BatchUnlinkInodeUntest(ctx, inos, false)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if len(res) != len(inos) {
			err = fmt.Errorf("failed to BatchUnlinkInodeUntest, path: %v, ino: %v, len: %v ", d1, ino, len(res))
			log.LogError(err.Error())
			return
		}

		res, err = gTrashEnv.metaWrapper.BatchDeleteDentryUntest(ctx, ino, dens, false)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		/*
			if len(res) > 0 {
				err = fmt.Errorf("failed to BatchDeleteDentryUntest, path: %v, ino: %v, len: %v ", d1, ino, len(res))
				log.LogError(err.Error())
				return
			}

		*/

		var rows, delRows []*listRow
		err, rows, delRows = ListPath(cfsd1, true)
		err = checkListResSize(rows, delRows, 0, d1files)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = recoverPath(cfsd1)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		err, rows, delRows = ListPath(cfsd1, true)
		err = checkListResSize(rows, delRows, d1files, 0)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	err = os.RemoveAll(rootDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = cleanPath(cDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	return
}

func hardLinkTestCleanOneLink() (err error) {
	name := fmt.Sprintf("hardLinkTest_%v", time.Now().Unix())
	rootDir := path.Join(testRootDir, name)
	cDir := path.Join(cfsDir, name)
	err = os.Mkdir(rootDir, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	for i := 0; i < 5; i++ {
		f1 := path.Join(rootDir, fmt.Sprintf("f_%v", i))
		err = ioutil.WriteFile(f1, []byte(f1), 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	{
		f := path.Join(rootDir, fmt.Sprintf("f_%v", 0))
		hardPath := path.Join(rootDir, "f_0_hard")
		err = os.Link(f, hardPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = cleanPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		_, err = ioutil.ReadFile(hardPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(hardPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = recoverPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		_, err = ioutil.ReadFile(hardPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	{
		f := path.Join(rootDir, fmt.Sprintf("f_%v", 1))
		hardPath := path.Join(rootDir, "f_1_hard")
		err = os.Link(f, hardPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(hardPath)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = cleanPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		_, err = ioutil.ReadFile(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = recoverPath(cDir)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		_, err = ioutil.ReadFile(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}
	err = os.RemoveAll(rootDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = cleanPath(cDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	return
}

func hardLinkTestCleanTwoLink() (err error) {
	name := fmt.Sprintf("hardLinkTest_%v", time.Now().Unix())
	rootDir := path.Join(testRootDir, name)
	cDir := path.Join(cfsDir, name)
	err = os.Mkdir(rootDir, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	d1 := path.Join(rootDir, "d1")
	cfsd1 := path.Join(cDir, "d1")
	err = os.Mkdir(d1, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	testFiles := 5
	for i := 0; i < testFiles; i++ {
		f := path.Join(d1, fmt.Sprintf("f1_%v", i))
		err = ioutil.WriteFile(f, []byte(f), 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	d2 := path.Join(rootDir, "d2")
	cfsd2 := path.Join(cDir, "d2")
	err = os.Mkdir(d2, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	{
		source := path.Join(d1, "f1_0")
		target := path.Join(d2, "f2_0_hard")
		err = os.Link(source, target)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(source)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(target)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = recoverPath(cfsd1)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		_, err = ioutil.ReadFile(source)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = recoverPath(cfsd2)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		_, err = ioutil.ReadFile(target)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(source)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = os.Remove(target)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = cleanPath(cfsd1)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		err = recoverPath(cfsd2)
		if err != nil {
			log.LogError(err.Error())
			return
		}

		_, err = os.Stat(target)
		if err == nil {
			err = fmt.Errorf("expected a error, when stat %v", target)
			log.LogError(err.Error())
			return
		}

		err = cleanPath(cfsd2)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		var delRows []*listRow
		err, _, delRows = ListPath(cfsd2, true)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if len(delRows) != 0 {
			err = fmt.Errorf("link: %v, del: %v is not 0", cfsd2, len(delRows))
			return
		}
	}

	err = os.RemoveAll(rootDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = cleanPath(cDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	return
}

func RenameToNotExistFileTest() (err error) {
	name := fmt.Sprintf("renameToNotExistFile_%v", time.Now().Unix())
	rootDir := path.Join(testRootDir, name)
	cDir := path.Join(cfsDir, name)
	err = os.Mkdir(rootDir, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	d1 := path.Join(rootDir, "d1")
	cfsd1 := path.Join(cDir, "d1")
	err = os.Mkdir(d1, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	testFiles := 5
	d2 := path.Join(rootDir, "d2")
	cfsd2 := path.Join(cDir, "d2")
	err = os.Mkdir(d2, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	for i := 0; i < testFiles; i++ {
		f := path.Join(d2, fmt.Sprintf("f2_%v", i))
		err = ioutil.WriteFile(f, []byte(f), 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	{
		target := path.Join(d1, fmt.Sprintf("f2_%v", 0))
		source := path.Join(d2, fmt.Sprintf("f2_%v", 0))
		err = os.Rename(source, target)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		var rows, delRows []*listRow
		err, rows, delRows = ListPath(cfsd1, true)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if len(delRows) != 0 {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d1, len(delRows), 1)
			return
		}
		if len(rows) != 1 {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d2, len(rows), testFiles)
			log.LogError(err.Error())
			return
		}
		if rows[0].Name != "f2_0" {
			err = fmt.Errorf("Name: %v", rows[0].Name)
			log.LogError(err.Error())
			return
		}
		f := path.Join(d1, rows[0].Name)
		var data []byte
		data, err = ioutil.ReadFile(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if string(data) != source {
			err = fmt.Errorf("file data: %v, expected: %v", string(data), target)
			return
		}

		err, rows, delRows = ListPath(cfsd2, true)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if len(rows) != testFiles-1 {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d2, len(rows), testFiles)
			log.LogError(err.Error())
			return
		}
		if len(delRows) != 0 {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d1, len(delRows), 1)
			log.LogError(err.Error())
			return
		}

		// recover the target
		err = recoverPath(cfsd1)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		err, rows, delRows = ListPath(cfsd1, true)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if len(rows) != 1 {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d1, len(rows), testFiles)
			log.LogError(err.Error())
			return
		}
		if rows[0].Name != "f2_0" {
			err = fmt.Errorf("Name: %v", rows[0].Name)
			log.LogError(err.Error())
			return
		}
		f = path.Join(d1, rows[0].Name)
		data, err = ioutil.ReadFile(f)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if string(data) != source {
			err = fmt.Errorf("file data: %v, expected: %v", string(data), target)
			return
		}

		// recover source file
		err = recoverPath(cfsd2)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		err, rows, delRows = ListPath(cfsd2, true)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if len(rows) != testFiles-1 {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d1, len(rows), testFiles)
			return
		}
	}
	err = os.RemoveAll(rootDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = cleanPath(cDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	return
}

func RenameToExistFileTest() (err error) {
	name := fmt.Sprintf("reNameToExistFile_%v", time.Now().Unix())
	rootDir := path.Join(testRootDir, name)
	cDir := path.Join(cfsDir, name)
	err = os.Mkdir(rootDir, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	d1 := path.Join(rootDir, "d1")
	cfsd1 := path.Join(cDir, "d1")
	err = os.Mkdir(d1, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	testFiles := 5
	for i := 0; i < testFiles; i++ {
		f := path.Join(d1, fmt.Sprintf("f1_%v", i))
		err = ioutil.WriteFile(f, []byte(f), 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	d2 := path.Join(rootDir, "d2")
	cfsd2 := path.Join(cDir, "d2")
	err = os.Mkdir(d2, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	for i := 0; i < testFiles; i++ {
		f := path.Join(d2, fmt.Sprintf("f2_%v", i))
		err = ioutil.WriteFile(f, []byte(f), 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	{
		target := path.Join(d1, fmt.Sprintf("f1_%v", 0))
		source := path.Join(d2, fmt.Sprintf("f2_%v", 0))
		err = os.Rename(source, target)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		var rows, delRows []*listRow
		err, rows, delRows = ListPath(cfsd1, true)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if len(delRows) != 0 {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d1, len(delRows), 1)
			return
		}
		if len(rows) != testFiles {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d2, len(rows), testFiles)
			log.LogError(err.Error())
			return
		}
		for _, row := range rows {
			if strings.HasPrefix(row.Name, "f1_0") {
				f := path.Join(d1, row.Name)
				var data []byte
				data, err = ioutil.ReadFile(f)
				if err != nil {
					log.LogError(err.Error())
					return
				}
				if string(data) != source {
					err = fmt.Errorf("file data: %v, expected: %v", string(data), target)
					return
				}
			}
		}

		err, rows, delRows = ListPath(cfsd2, true)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if len(rows) != testFiles-1 {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d2, len(rows), testFiles)
			log.LogError(err.Error())
			return
		}
		if len(delRows) != 0 {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d1, len(delRows), 1)
			log.LogError(err.Error())
			return
		}

		// recover the target
		err = recoverPath(cfsd1)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		err, rows, delRows = ListPath(cfsd1, true)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if len(rows) != testFiles {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d1, len(rows), testFiles)
			log.LogError(err.Error())
			return
		}
		for _, row := range rows {
			if strings.HasPrefix(row.Name, "f1_0") {
				f := path.Join(d1, row.Name)
				var data []byte
				data, err = ioutil.ReadFile(f)
				if err != nil {
					log.LogError(err.Error())
					return
				}
				if string(data) != source {
					err = fmt.Errorf("file data: %v, expected: %v", string(data), target)
					return
				}
			}
		}

		// recover source file
		err = recoverPath(cfsd2)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		err, rows, delRows = ListPath(cfsd2, true)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		if len(rows) != testFiles-1 {
			err = fmt.Errorf("list %v, files: %v, expected: %v", d1, len(rows), testFiles)
			return
		}
	}
	err = os.RemoveAll(rootDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = cleanPath(cDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	return
}

func RenameDirTest() (err error) {
	isRecursive = false
	name := fmt.Sprintf("rename_dir_test_%v", time.Now().Unix())
	rootDir := path.Join(testRootDir, name)
	cDir := path.Join(cfsDir, name)
	err = os.Mkdir(rootDir, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	d1 := path.Join(rootDir, "d1")
	err = os.Mkdir(d1, 0755)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	d2 := path.Join(rootDir, "d2")
	cfsd2 := path.Join(cDir, "d2")
	err = os.Rename(d1, d2)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	var rows, delRows []*listRow
	err, rows, delRows = ListPath(cDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	if len(rows) != 1 {
		err = fmt.Errorf("len(rows): %v", len(rows))
		return
	}
	if rows[0].Name != "d2" {
		err = fmt.Errorf("Name: %v", rows[0].Name)
		return
	}
	if len(delRows) != 0 {
		err = fmt.Errorf("len(del rows): %v", len(delRows))
		return
	}

	err = os.Remove(d2)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	if len(rows) != 0 {
		err = fmt.Errorf("len(rows): %v", len(rows))
		log.LogError(err.Error())
		return
	}
	if len(delRows) != 1 {
		err = fmt.Errorf("len(del rows): %v", len(delRows))
		return
	}
	if strings.HasPrefix(delRows[0].Name, "d2") == false {
		err = fmt.Errorf("nam: %v", delRows[0].Name)
		log.LogError(err.Error())
		return
	}

	err = recoverPath(cfsd2)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err, rows, delRows = ListPath(cDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	if len(rows) != 1 {
		err = fmt.Errorf("len(rows): %v", len(rows))
		return
	}
	if rows[0].Name != "d2" {
		err = fmt.Errorf("Name: %v", rows[0].Name)
		return
	}
	if len(delRows) != 0 {
		err = fmt.Errorf("len(del rows): %v", len(delRows))
		return
	}

	testFiles := 5
	for i := 0; i < testFiles; i++ {
		f := path.Join(d2, fmt.Sprintf("f1_%v", i))
		err = ioutil.WriteFile(f, []byte(f), 0755)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}
	d3 := path.Join(rootDir, "d3")
	cfsd3 := path.Join(cDir, "d3")
	err = os.Rename(d2, d3)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cDir, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	if len(rows) != 1 {
		err = fmt.Errorf("len(rows): %v", len(rows))
		log.LogError(err.Error())
		return
	}
	if rows[0].Name != "d3" {
		err = fmt.Errorf("Name: %v", delRows[0].Name)
		log.LogError(err.Error())
		return
	}
	if len(delRows) != 0 {
		err = fmt.Errorf("len(del rows): %v", len(delRows))
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsd3, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	if len(rows) != 5 || len(delRows) != 0 {
		err = fmt.Errorf("len(rows): %v, len(delRows): %v", len(rows), len(delRows))
		log.LogError(err.Error())
		return
	}

	err = os.Remove(d3)
	if err == nil {
		err = fmt.Errorf("d2 should be not deleted")
		log.LogError(err.Error())
		return
	}
	err = os.RemoveAll(d3)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = recoverPath(cfsd3)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err, rows, delRows = ListPath(cfsd3, true)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	if len(rows) != 5 || len(delRows) != 0 {
		err = fmt.Errorf("len(rows): %v, len(delRows): %v", len(rows), len(delRows))
		log.LogError(err.Error())
		return
	}

	subD3 := path.Join(d1, "d3")
	err = os.MkdirAll(subD3, 0644)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = os.Rename(d3, d1)
	if err == nil {
		err = fmt.Errorf("rename %v to %v should be failed", d3, d1)
		log.LogError(err.Error())
		return
	}

	err = os.RemoveAll(rootDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = cleanPath(cDir)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	return
}

func checkListResSize(rows, delRows []*listRow, num, delNum int) (err error) {
	if len(rows) != num {
		err = fmt.Errorf("the list res [%v] is not [%v]", len(rows), num)
		return
	}

	if len(delRows) != delNum {
		err = fmt.Errorf("the list deleted res [%v] is not [%v]", len(delRows), delNum)
		return
	}

	return
}

func checkLinks(path string, expect int) (err error) {
	var link int
	link, err = getLinks(path)
	if err != nil {
		log.LogErrorf("checkLinks,  path: %v", path)
		return
	}

	if link != expect {
		err = fmt.Errorf("links check error, expect: %v, real: %v", expect, link)
	}
	return
}

func getLinks(path string) (link int, err error) {
	//time.Sleep(20*time.Second)
	time.Sleep(300 * time.Millisecond)
	arg := fmt.Sprintf("stat %v | grep Links | awk -F\"Links: \" '{print $2}'", path)
	cmd := exec.Command("/bin/bash", "-c", arg)
	var out []byte
	out, err = cmd.CombinedOutput()
	if err != nil {
		return
	}
	out = out[0 : len(out)-1]
	link, err = strconv.Atoi(string(out))
	if err != nil {
		log.LogErrorf("getLinks, path: %v, out: %v, err: %v", cmd.String(), string(out), err.Error())
		return
	}

	return
}
