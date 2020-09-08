package main

import (
	"github.com/shurcooL/vfsgen"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
)

const FileName string = "html.go"

func main() {

	_, file, _, _ := runtime.Caller(0)
	sourceDir := path.Dir(file)
	consoleDir := sourceDir[:len(sourceDir)-12]
	jsDir := path.Join(sourceDir, "dist", "static", "js")

	println("sourceDir", sourceDir, "consoleDir", consoleDir, "jsDir", jsDir)

	dir, err := ioutil.ReadDir(jsDir)
	if err != nil {
		panic(err)
	}

	var fileName string
	var mode os.FileMode

	for _, file := range dir {
		if strings.HasPrefix(file.Name(), "app.") {
			fileName = file.Name()
			mode = file.Mode()
			break
		}
	}

	if fileName == "" {
		panic("can not find app.*.js file in html dir")
	}

	appPath := path.Join(jsDir, fileName)

	appjs, err := os.Open(appPath)
	if err != nil {
		panic(err)
	}

	all, err := ioutil.ReadAll(appjs)
	if err != nil {
		panic(err)
	}

	appjsfixed := strings.ReplaceAll(string(all), "/proxy/", "/")
	if err := ioutil.WriteFile(appPath, []byte(appjsfixed), mode); err != nil {
		panic(err)
	}

	assets := getAssets(path.Join(sourceDir, "dist"))

	err = vfsgen.Generate(assets, vfsgen.Options{
		PackageName:  "console",
		BuildTags:    "!dev",
		VariableName: "Assets",
		Filename:     FileName,
	})

	if err != nil {
		panic(err)
	}

	println("move", FileName, "to", path.Join(consoleDir, FileName))

	if err := os.Rename(FileName, path.Join(consoleDir, FileName)); err != nil {
		panic(err)
	}
	println("success!!!!!")
}

func getAssets(htmlDir string) http.FileSystem {
	var assets http.FileSystem = http.Dir(htmlDir)
	return assets
}
