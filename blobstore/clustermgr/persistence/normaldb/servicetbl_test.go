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

package normaldb

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServiceTbl(t *testing.T) {
	tmpDBPath := "/tmp/tmpservicenormaldb" + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath, false)
	assert.NoError(t, err)
	defer db.Close()

	serviceTbl := OpenServiceTable(db)
	testServiceName := "testService"
	otherTestServiceName := "otherTestService"
	testHostPrefix := "testHost-"

	{
		for i := 1; i <= 10; i++ {
			host := testHostPrefix + strconv.Itoa(i)
			err = serviceTbl.Put(testServiceName, host, []byte(testServiceName+host))
			assert.NoError(t, err)
			err = serviceTbl.Put(otherTestServiceName, host, []byte(testServiceName+host))
			assert.NoError(t, err)
		}

		list, err := serviceTbl.Get(testServiceName)
		assert.NoError(t, err)
		assert.Equal(t, 10, len(list))

		err = serviceTbl.Delete(testServiceName, testHostPrefix+strconv.Itoa(1))
		assert.NoError(t, err)

		list, err = serviceTbl.Get(testServiceName)
		assert.NoError(t, err)
		assert.Equal(t, 9, len(list))

		count := 0
		serviceTbl.Range(func(key []byte, val []byte) bool {
			count += 1
			return true
		})
		assert.Equal(t, 19, count)
	}
}
