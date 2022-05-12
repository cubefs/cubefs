package mysql

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/config"
	"io"
	"io/ioutil"
	"testing"
)

const (
	username = "root"
	password = "123456"
	host     = "11.97.57.230"
	database = "smart"
	port     = 3306
	//username = "jfsops"
	//password = "Lizhendong7&"
	//host     = "10.170.240.34"
	//database = "storage_object"
	//port     = 3306
)

func init() {
	mc := &config.MysqlConfig{
		Url:      host,
		Username: username,
		Password: password,
		Database: database,
		Port:     port,
	}
	if err := InitMysqlClient(mc); err != nil {
		fmt.Println(fmt.Sprintf("init mysql client failed, err(%v)", err))
		return
	}
}

func TestFormatTime(t *testing.T) {
	dataString := "2022-03-31 12:09:17"
	dateTime, err := FormatTime(dataString)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(dateTime.String())
}

func TestParsePassword1(t *testing.T) {
	configKey := "database.password"
	testLine := "database.password={{db:U2FsdGVkX18FLKuVorfnx6rejNcOIIXefpMtyA==}}"
	file, err := ioutil.TempFile("", "important-*.properties")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = file.WriteString(testLine)
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = file.WriteString("\n")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_ = file.Close()
	_ = file.Sync()
	pass, err := ParsePassword(file.Name(), configKey)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if pass == "" {
		t.Fatalf("parsed value is empty")
	}
	fmt.Printf("password: %v\n", pass)
}

func TestParsePassword2(t *testing.T) {
	configKey := "database.password"
	testLine := "database.username={{db:U2FsdGVkX18FLKuVorfnx6rejNcOIIXefpMtyA==}}"
	file, err := ioutil.TempFile("", "important-*.properties")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = file.WriteString(testLine)
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = file.WriteString("\n")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_ = file.Close()
	_ = file.Sync()
	pass, err := ParsePassword(file.Name(), configKey)
	if err != nil && err != io.EOF {
		t.Fatalf(err.Error())
	}
	if pass != "" {
		t.Fatalf("parsed value shoule be empty")
	}
}

func TestParsePassword3(t *testing.T) {
	configKey := "database.password"
	file, err := ioutil.TempFile("", "important-*.properties")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = file.WriteString("\n")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_ = file.Close()
	_ = file.Sync()
	pass, err := ParsePassword(file.Name(), configKey)
	if err != nil && err != io.EOF {
		t.Fatalf(err.Error())
	}
	if pass != "" {
		t.Fatalf("parsed value shoule be empty")
	}
}