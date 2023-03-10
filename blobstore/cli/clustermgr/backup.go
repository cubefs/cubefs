// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package clustermgr

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/kvdb"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/raftdb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	volumeDBPathName = "snapshot-volumedb"
	normalDBPathName = "snapshot-normaldb"
	raftDBPathName   = "snapshot-raftdb"
	kvDBPathName     = "snapshot-kvdb"
)

var ErrArgument = errors.New("invalid argument")

type SnapshotItem struct {
	DbName string `json:"db_name"`
	CfName string `json:"cf_name"`
}

type SnapshotData struct {
	Header SnapshotItem
	Key    []byte
	Value  []byte
}

func decodeSnapshotData(reader io.Reader) (ret *SnapshotData, err error) {
	_ret := &SnapshotData{}
	dbNameSize := int32(0)
	cfNameSize := int32(0)
	keySize := int32(0)
	valueSize := int32(0)

	if err = binary.Read(reader, binary.BigEndian, &dbNameSize); err != nil {
		return
	}
	dbName := make([]byte, dbNameSize)
	if _, err = reader.Read(dbName); err != nil {
		return
	}
	_ret.Header.DbName = string(dbName)

	if err = binary.Read(reader, binary.BigEndian, &cfNameSize); err != nil {
		return
	}
	cfName := make([]byte, cfNameSize)
	if _, err = reader.Read(cfName); err != nil {
		return
	}
	_ret.Header.CfName = string(cfName)

	if err = binary.Read(reader, binary.BigEndian, &keySize); err != nil {
		return
	}
	key := make([]byte, keySize)
	if _, err = reader.Read(key); err != nil {
		return
	}
	_ret.Key = key

	if err = binary.Read(reader, binary.BigEndian, &valueSize); err != nil {
		return
	}
	value := make([]byte, valueSize)
	if _, err = reader.Read(value); err != nil {
		return
	}
	_ret.Value = value

	ret = _ret
	return
}

func addCmdSnapshot(cmd *grumble.Command) {
	command := &grumble.Command{
		Name:     "snapshot",
		Help:     "snapshot tools",
		LongHelp: "snapshot tools for clustermgr",
	}
	cmd.AddCommand(command)

	command.AddCommand(&grumble.Command{
		Name: "dump",
		Help: "dump snapshot",
		Run:  cmdDumpSnapshot,
		Args: func(a *grumble.Args) {
			a.Int("d", "save days of snapshot")
			a.String("f", "the path to save the db data")
			a.String("w", "the path of wal data")
		},
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
		},
	})
}

func cmdDumpSnapshot(c *grumble.Context) error {
	days := c.Args.Int("d")
	dbPath := c.Args.String("f")
	walPath := c.Args.String("w")
	if days <= 0 || dbPath == "" || walPath == "" {
		return ErrArgument
	}

	cmClient := newCMClient(c.Flags)
	if _, err := os.Stat(dbPath); err != nil {
		if !os.IsNotExist(err.(*os.PathError)) {
			return fmt.Errorf("stat backup directory failed: %s", err.Error())
		}
		os.MkdirAll(dbPath, 0o755)
	}

	// curl get snapshot data
	_, ctx := trace.StartSpanFromContext(common.CmdContext(), "")
	resp, err := cmClient.Get(ctx, "/snapshot/dump")
	if err != nil {
		return fmt.Errorf("snapshot dump error: %s", err.Error())
	}
	defer resp.Body.Close()

	// todo use api constant
	index, err := strconv.ParseUint(resp.Header.Get("Raft-Snapshot-Index"), 10, 64)
	if err != nil {
		log.Fatalf("parse snapshot index failed: %s", err.Error())
	}

	date := time.Now().Format("2006-01-02")
	tmpNormalDBPath := dbPath + "/" + date + "/" + normalDBPathName
	tmpVolumeDBPath := dbPath + "/" + date + "/" + volumeDBPathName
	tmpRaftDBPath := dbPath + "/" + date + "/" + raftDBPathName
	tmpKvDBPath := dbPath + "/" + date + "/" + kvDBPathName
	os.RemoveAll(tmpNormalDBPath)
	os.RemoveAll(tmpVolumeDBPath)
	os.RemoveAll(tmpRaftDBPath)
	os.RemoveAll(tmpKvDBPath)
	os.MkdirAll(tmpNormalDBPath, 0o755)
	os.MkdirAll(tmpVolumeDBPath, 0o755)
	os.MkdirAll(tmpRaftDBPath, 0o755)
	os.MkdirAll(tmpKvDBPath, 0o755)

	normalDB, err := normaldb.OpenNormalDB(tmpNormalDBPath, false, func(option *kvstore.RocksDBOption) {
		option.ReadOnly = false
	})
	if err != nil {
		return fmt.Errorf("open normal db failed: %s", err.Error())
	}
	defer normalDB.Close()
	volumeDB, err := volumedb.Open(tmpVolumeDBPath, false, func(option *kvstore.RocksDBOption) {
		option.ReadOnly = false
	})
	if err != nil {
		return fmt.Errorf("open volume db failed: %s", err.Error())
	}
	defer volumeDB.Close()
	raftDB, err := raftdb.OpenRaftDB(tmpRaftDBPath, false, func(option *kvstore.RocksDBOption) {
		option.ReadOnly = false
	})
	if err != nil {
		return fmt.Errorf("open raft db failed: %s", err.Error())
	}
	defer raftDB.Close()

	kvDB, err := kvdb.Open(tmpKvDBPath, false, func(option *kvstore.RocksDBOption) {
		option.ReadOnly = false
	})
	if err != nil {
		return fmt.Errorf("open kv db failed: %s", err.Error())
	}
	defer kvDB.Close()

	snapshotDBs := make(map[string]base.SnapshotDB)
	snapshotDBs["volume"] = volumeDB
	snapshotDBs["normal"] = normalDB
	snapshotDBs["keyValue"] = kvDB

	// write snapshot data into backup directory
	for {
		// todo use clustermgr/base decode snapshot method
		snapshotData, err := decodeSnapshotData(resp.Body)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decode snapshot data failed: %s", err.Error())
		}
		dbName := snapshotData.Header.DbName
		cfName := snapshotData.Header.CfName

		if snapshotData.Header.CfName != "" {
			err = snapshotDBs[dbName].Table(cfName).Put(kvstore.KV{Key: snapshotData.Key, Value: snapshotData.Value})
		} else {
			err = snapshotDBs[dbName].Put(kvstore.KV{Key: snapshotData.Key, Value: snapshotData.Value})
		}
		if err != nil {
			return fmt.Errorf("put snapshot data failed: %s", err.Error())
		}
	}
	indexValue := make([]byte, 8)
	binary.BigEndian.PutUint64(indexValue, index)
	err = raftDB.Put(base.ApplyIndexKey, indexValue)
	if err != nil {
		return fmt.Errorf("put raft index failed: %s", err.Error())
	}

	// check if meet the max backup days and delete the oldest backup
	backupDirFiless, err := os.ReadDir(dbPath)
	if err != nil {
		return fmt.Errorf("read backup dir failed: %s", err.Error())
	}
	backups := make([]string, 0)
	for i := range backupDirFiless {
		if _, err := time.Parse("2006-01-02", backupDirFiless[i].Name()); err == nil {
			log.Infof("name: %s", backupDirFiless[i].Name())
			backups = append(backups, backupDirFiless[i].Name())
		}
	}

	if len(backups) > days {
		// delete the oldest backup
		oldestT := time.Now()
		oldestBackup := ""
		for i := range backups {
			t, err := time.Parse("2006-01-02", backups[i])
			if err == nil && t.Before(oldestT) {
				oldestT = t
				oldestBackup = backups[i]
			}
		}
		if oldestBackup != "" {
			raftDB, err := raftdb.OpenRaftDB(dbPath+"/"+oldestBackup+"/"+raftDBPathName, false, func(option *kvstore.RocksDBOption) {
				option.ReadOnly = false
			})
			if err != nil {
				return fmt.Errorf("open oldest raft db failed: %s", err.Error())
			}
			rawOldestIndex, err := raftDB.Get(base.ApplyIndexKey)
			if err != nil {
				return fmt.Errorf("read apply index from oldest raft db failed: %s", err.Error())
			}
			oldestIndex := binary.BigEndian.Uint64(rawOldestIndex)

			// clean db
			raftDB.Close()
			os.RemoveAll(dbPath + "/" + oldestBackup)

			// clean useless wal backup file
			walDirFiles, err := os.ReadDir(walPath)
			if err != nil {
				return fmt.Errorf("read wal dir failed: %s", err.Error())
			}

			sort.Slice(walDirFiles, func(i, j int) bool {
				raw1 := strings.Split(walDirFiles[i].Name(), "-")
				if len(raw1) != 2 {
					return false
				}
				raw2 := strings.Split(walDirFiles[j].Name(), "-")
				if len(raw2) != 2 {
					return false
				}
				seq1, err := strconv.ParseUint(raw1[0], 16, 64)
				if err != nil {
					return false
				}
				seq2, err := strconv.ParseUint(raw2[0], 16, 64)
				if err != nil {
					return false
				}
				return seq1 < seq2
			})
			cleanWals := make([]string, 0)
			for i := 1; i < len(walDirFiles); i++ {
				if raw := strings.Split(walDirFiles[i].Name(), "-"); len(raw) == 2 {
					parsedIndex, err := strconv.ParseUint(strings.Replace(raw[1], ".log", "", 1), 16, 64)
					if err != nil {
						log.Errorf("parse wal log file index failed: %s", err.Error())
						continue
					}
					log.Infof("parsedIndex: %d, oldestIndex:%d", parsedIndex, oldestIndex)
					// if i's index less than oldestIndex, which means that the previous one can be deleted
					if parsedIndex < oldestIndex {
						cleanWals = append(cleanWals, walDirFiles[i-1].Name())
					}
				}
			}
			log.Println("clean wal files: ", cleanWals)
			for i := range cleanWals {
				os.RemoveAll(walPath + "/" + cleanWals[i])
			}
		}
	}
	return nil
}
