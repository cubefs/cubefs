package storage

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func createFile(name string,data []byte)(err error){
	os.RemoveAll(name)
	fp,err:=os.OpenFile(name,os.O_RDWR|os.O_CREATE,0777)
	if err!=nil {
		return
	}
	fp.Write(data)
	fp.Close()
	return
}

func removeFile(name string) {
	os.RemoveAll(name)
}

func computeMd5(data []byte)(string) {
	h := md5.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}


func compareMd5(s *ExtentStore,extent,offset,size uint64,expectMD5 string)(err error) {
	actualMD5,err:=s.ComputeMd5Sum(extent,offset,size)
	if err!=nil {
		return fmt.Errorf("ComputeMd5Sum failed on extent(%v)," +
			"offset(%v),size(%v) expect(%v) actual(%v) err(%v)",extent,offset,size,expectMD5,actualMD5,err)
	}
	if actualMD5!=expectMD5{
		return fmt.Errorf("ComputeMd5Sum failed on extent(%v)," +
			"offset(%v),size(%v) expect(%v) actual(%v) err(%v)",extent,offset,size,expectMD5,actualMD5,err)
	}
	return nil
}

func TestExtentStore_ComputeMd5Sum(t *testing.T) {
	allData:=[]byte(RandStringRunes(100*1024))
	allMd5:=computeMd5(allData)
	dataPath:="/tmp"
	extentID:=3675
	err:=createFile(path.Join(dataPath,strconv.Itoa(extentID)),allData)
	if err!=nil {
		t.Logf("createFile failed %v",err)
		t.FailNow()
	}
	s:=new(ExtentStore)
	s.dataPath=dataPath
	err=compareMd5(s,uint64(extentID),0,0,allMd5)
	if err!=nil {
		t.Logf("compareMd5 failed %v",err)
		t.FailNow()
	}

	for i:=0;i<100;i++{
		data:=allData[i*1024:(i+1)*1024]
		expectMD5:=computeMd5(data)
		err=compareMd5(s,uint64(extentID),uint64(i*1024),1024,expectMD5)
		if err!=nil {
			t.Logf("compareMd5 failed %v",err)
			t.FailNow()
		}
	}
	removeFile(path.Join(dataPath,strconv.Itoa(extentID)))

}

