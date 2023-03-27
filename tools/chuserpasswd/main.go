package main

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/unit"
	"io"
)

const (
	LRUCacheSize    = 3 << 30
	WriteBufferSize = 4 * unit.MB
)

func main() {

	var masterDir string
	var user string
	var password string

	flag.StringVar(&masterDir, "master-dir", "", "master data dir path")
	flag.StringVar(&user, "user", "", "user to change passwd")
	flag.StringVar(&password, "password", "", "new password")

	flag.Parse()

	if masterDir == "" || user == "" || password == "" {
		flag.Usage()
		return
	}

	var err error

	var store *raftstore.RocksDBStore
	if store, err = raftstore.NewRocksDBStore(masterDir, LRUCacheSize, WriteBufferSize); err != nil {
		fmt.Printf("Open DB %v failed: %v\n", masterDir, err)
		return
	}

	var value interface{}
	var key = fmt.Sprintf("#user#%v", user)
	if value, err = store.Get(key); err != nil {
		fmt.Printf("Load value of key %v from DB failed: %v\n", key, err)
		return
	}

	var valueBytes = value.([]byte)
	if len(valueBytes) == 0 {
		fmt.Printf("User not found.\n")
		return
	}

	var userInfo = new(proto.UserInfo)
	if err = json.Unmarshal(valueBytes, userInfo); err != nil {
		fmt.Printf("Decode UserInfo failed: %v\n", err)
		return
	}

	key = fmt.Sprintf("#ak#%s", userInfo.AccessKey)
	if value, err = store.Get(key); err != nil {
		fmt.Printf("Load value of key %v from DB failed: %v\n", key, err)
		return
	}

	valueBytes = value.([]byte)
	if len(valueBytes) == 0 {
		fmt.Printf("AKUser not found.\n")
	}
	var akUser = new(proto.AKUser)
	if err = json.Unmarshal(valueBytes, akUser); err != nil {
		fmt.Printf("Decoder AKUser failed: %v\n", err)
		return
	}

	fmt.Printf("Current password (MD5): %v\n", akUser.Password)

	akUser.Password = encodingPassword(password)
	fmt.Printf("    New password (MD5): %v\n", akUser.Password)
	if valueBytes, err = json.Marshal(akUser); err != nil {
		fmt.Printf("Encode AKUser failed: %v\n ", err)
		return
	}

	if _, err = store.Put(key, valueBytes, true); err != nil {
		fmt.Printf("Store value of key %v to DB failed: %v\n", key, err)
		return
	}


	//snap := store.RocksDBSnapshot()
	//iter := store.Iterator(snap)
	//iter.SeekToFirst()
	//for iter.Valid() {
	//	key := iter.Key()
	//	var keyString = string(key.Data()[:key.Size()])
	//	value := iter.Value()
	//	var valueString = string(value.Data()[:value.Size()])
	//	fmt.Printf("%v -> %v\n", keyString, valueString)
	//	iter.Next()
	//}

}

func encodingPassword(s string) string {
	t := sha1.New()
	io.WriteString(t, s)
	return hex.EncodeToString(t.Sum(nil))
}