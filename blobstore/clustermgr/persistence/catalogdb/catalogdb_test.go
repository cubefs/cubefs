// Copyright 2024 The CubeFS Authors.
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

package catalogdb

import (
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/stretchr/testify/require"
)

var (
	catalogDB     *CatalogDB
	catalogDBPath = path.Join(os.TempDir(), "catalogdb")
)

func initCatalogDB() {
	catalogDBPath += strconv.Itoa(rand.Intn(20000))

	var err error
	catalogDB, err = Open(catalogDBPath)
	if err != nil {
		log.Error("open db error")
	}

	catalogTable, err = OpenCatalogTable(catalogDB)
	if err != nil {
		log.Error("open catalog table error")
	}

	transitedTable, err = OpenTransitedTable(catalogDB)
	if err != nil {
		log.Error("open transited table error")
	}
}

func closeCatalogDB() {
	catalogDB.Close()
	os.RemoveAll(catalogDBPath)
}

func TestCatalogDB(t *testing.T) {
	catalogDBPath = ""
	_, err := Open(catalogDBPath)
	require.Error(t, err)
}

func TestCatalogDB_GetAllCfNames(t *testing.T) {
	catalogDBPath = ""
	db, err := Open(catalogDBPath)
	require.Error(t, err)
	db.GetAllCfNames()

	_, err = OpenCatalogTable(nil)
	require.Error(t, err)
}
