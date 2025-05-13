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
	"encoding/json"
	"errors"
	"time"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/kvdb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
)

type volInfo struct {
	volumedb.VolumeRecord
	units []volumedb.VolumeUnitRecord
	token volumedb.TokenRecord
}

type serviceNode struct {
	ClusterID uint64    `json:"cluster_id"`
	Name      string    `json:"moduleName"`
	Host      string    `json:"host"`
	Idc       string    `json:"idc"`
	Timeout   int       `json:"timeout"`
	Expires   time.Time `json:"expires"`
}

func addCmdListAllDB(cmd *grumble.Command) {
	command := &grumble.Command{
		Name:     "listAllDB",
		Help:     "list all db tools",
		LongHelp: "list all db tools for clustermgr",
		Run:      cmdListAllDB,
		Args: func(a *grumble.Args) {
			a.String("volumeDBPath", "volume db path")
			a.String("normalDBPath", "normal db path")
			a.String("kvDBPath", "kv db path")
		},
	}
	cmd.AddCommand(command)
}

func cmdListAllDB(c *grumble.Context) error {
	volumeDBPath := c.Args.String("volumeDBPath")
	normalDBPath := c.Args.String("normalDBPath")
	kvDBPath := c.Args.String("kvDBPath")
	if volumeDBPath == "" || normalDBPath == "" || kvDBPath == "" {
		return errors.New("invalid command arguments")
	}

	volumeDB, err := openVolumeDB(volumeDBPath, false)
	if err != nil {
		return err
	}
	defer volumeDB.Close()
	normalDB, err := openNormalDB(normalDBPath, false)
	if err != nil {
		return err
	}
	defer normalDB.Close()
	kvDB, err := openKvDB(kvDBPath, false)
	if err != nil {
		return err
	}
	defer kvDB.Close()

	volumeTbl, err := volumedb.OpenVolumeTable(volumeDB)
	if err != nil {
		return err
	}
	fmt.Println("list volumes: ")
	err = listAllVolumes(volumeTbl)
	if err != nil {
		return err
	}
	fmt.Println()

	fmt.Println("list volumeUnits: ")
	err = listAllVolumeUnits(volumeTbl)
	if err != nil {
		return err
	}
	fmt.Println()

	diskTbl, err := normaldb.OpenBlobNodeDiskTable(normalDB, true)
	if err != nil {
		return err
	}
	fmt.Println("list disk: ")
	err = listAllDisks(diskTbl)
	if err != nil {
		return err
	}
	fmt.Println()

	dropTbl, err := normaldb.OpenBlobNodeDroppedDiskTable(normalDB)
	if err != nil {
		return err
	}
	fmt.Println("list dropping disk: ")
	err = listAllDroppingDisks(dropTbl)
	if err != nil {
		return err
	}
	fmt.Println()

	scopeTbl, err := normaldb.OpenScopeTable(normalDB)
	if err != nil {
		return err
	}
	fmt.Println("list scope: ")
	err = listAllScopes(scopeTbl)
	if err != nil {
		return err
	}
	fmt.Println()

	serviceTbl := normaldb.OpenServiceTable(normalDB)
	fmt.Println("list service: ")
	err = listAllServices(serviceTbl)
	if err != nil {
		return err
	}
	fmt.Println()

	kvTbl, err := kvdb.OpenKvTable(kvDB)
	if err != nil {
		return err
	}
	fmt.Println("list kv: ")
	err = listAllKvs(kvTbl)
	if err != nil {
		return err
	}
	fmt.Println()

	return nil
}

func openNormalDB(path string, readonly bool) (*normaldb.NormalDB, error) {
	db, err := normaldb.OpenNormalDB(path, kvstore.WithReadonly(readonly))
	if err != nil {
		return nil, fmt.Errorf("open db failed, err: %s", err.Error())
	}
	return db, nil
}

func listAllVolumes(volumeTbl *volumedb.VolumeTable) error {
	return volumeTbl.RangeVolumeRecord(func(volRecord *volumedb.VolumeRecord) error {
		volInfo := volInfo{
			VolumeRecord: *volRecord,
			units:        make([]volumedb.VolumeUnitRecord, 0),
		}

		for _, vuidPrefix := range volRecord.VuidPrefixs {
			unitRecord, err := volumeTbl.GetVolumeUnit(vuidPrefix)
			if err != nil {
				return fmt.Errorf("get volume unit failed: %s", err.Error())
			}
			volInfo.units = append(volInfo.units, *unitRecord)
		}

		token, _ := volumeTbl.GetToken(volRecord.Vid)
		if token != nil {
			volInfo.token = *token
		}

		data, err := json.Marshal(volInfo)
		if err != nil {
			return fmt.Errorf("json marshal failed, err: %s", err.Error())
		}
		fmt.Println(string(data))
		return nil
	})
}

func listAllVolumeUnits(volumeTbl *volumedb.VolumeTable) error {
	return volumeTbl.RangeVolumeUnits(func(unitRecord *volumedb.VolumeUnitRecord) error {
		data, err := json.Marshal(*unitRecord)
		if err != nil {
			return fmt.Errorf("json marshal: %s", err.Error())
		}
		fmt.Println(string(data))
		return nil
	})
}

func listAllDisks(tbl *normaldb.BlobNodeDiskTable) error {
	list, err := tbl.GetAllDisks()
	if err != nil {
		return fmt.Errorf("list disk failed, err: %s", err.Error())
	}
	for i := range list {
		data, err := json.Marshal(list[i])
		if err != nil {
			return fmt.Errorf("json marshal failed, err: %s", err.Error())
		}
		fmt.Println(string(data))
	}
	return nil
}

func listAllDroppingDisks(tbl *normaldb.DroppedDiskTable) error {
	list, err := tbl.GetAllDroppingDisk()
	if err != nil {
		return fmt.Errorf("list dropping disk failed, err: %s", err.Error())
	}
	fmt.Println(list)
	return nil
}

func listAllScopes(tbl *normaldb.ScopeTable) error {
	scopes, err := tbl.Load()
	if err != nil {
		return fmt.Errorf("list scopes failed, err: %s", err.Error())
	}
	fmt.Println(scopes)
	return nil
}

func listAllServices(tbl *normaldb.ServiceTable) error {
	var retErr error
	err := tbl.Range(func(key, val []byte) bool {
		node := &serviceNode{}
		if err := json.Unmarshal(val, &node); err != nil {
			retErr = fmt.Errorf("json unmarshal one service from db failed, err: %s", err.Error())
			return false
		}
		fmt.Println(string(val))
		return true
	})
	if err != nil {
		retErr = err
	}
	return retErr
}

func listAllKvs(tbl *kvdb.KvTable) error {
	count := 200
	args := &clustermgr.ListKvOpts{Count: count}
	for {
		kvs, err := tbl.List(args)
		if err != nil {
			return fmt.Errorf("list kv failed, err: %s", err.Error())
		}
		for i := range kvs {
			fmt.Println(kvs[i])
		}
		if len(kvs) < count {
			break
		}
		args.Marker = kvs[len(kvs)-1].Key
	}
	return nil
}
