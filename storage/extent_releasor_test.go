package storage

import (
	"context"
	"path"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/util/testutil"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/stretchr/testify/assert"
)

func TestRecordFileName_parse(t *testing.T) {
	type Case struct {
		raw       string
		isValid   bool
		isCurrent bool
		isArchive bool
		archiveNo int
	}
	var cases = []Case{
		{
			raw:       "DeletionRecord.2023010101",
			isValid:   false,
			isCurrent: false,
			isArchive: false,
			archiveNo: -1,
		},
		{
			raw:       "DeletionRecord.2023010101.0004",
			isValid:   true,
			isCurrent: false,
			isArchive: true,
			archiveNo: 4,
		},
		{
			raw:       "DeletionRecord.2023010101.04",
			isValid:   false,
			isCurrent: false,
			isArchive: false,
			archiveNo: -1,
		},
		{
			raw:       "DeletionRecord.2023010125.0004",
			isValid:   false,
			isCurrent: false,
			isArchive: false,
			archiveNo: -1,
		},
		{
			raw:       "DeletionRecord.2023010101.current",
			isValid:   true,
			isCurrent: true,
			isArchive: false,
			archiveNo: -1,
		},
	}
	for i, c := range cases {
		t.Run("case-"+strconv.Itoa(i), func(t *testing.T) {
			rfn, _ := parseRecordFileName(c.raw)
			assertEqual(t, c.isValid, rfn.valid())
			assertEqual(t, c.isCurrent, rfn.isCurrent())
			assertEqual(t, c.isArchive, rfn.isArchived())
			assertEqual(t, c.archiveNo, rfn.archivedNo())
		})
	}
}

func TestRecordFIleName_toArchive(t *testing.T) {
	type Case struct {
		raw       string
		input     int
		resultRaw string
		resultNo  int
	}
	var cases = []Case{
		{
			raw:       "DeletionRecord.2023090111.0000",
			input:     15,
			resultRaw: "DeletionRecord.2023090111.0015",
			resultNo:  15,
		},
		{
			raw:       "DeletionRecord.2023091510.current",
			input:     1,
			resultRaw: "DeletionRecord.2023091510.0001",
			resultNo:  1,
		},
	}
	for i, c := range cases {
		t.Run("case-"+strconv.Itoa(i), func(t *testing.T) {
			rfn, _ := parseRecordFileName(c.raw)
			rfn = rfn.toArchive(c.input)
			assertEqual(t, c.resultRaw, string(rfn))
			assertEqual(t, c.resultNo, rfn.archivedNo())
		})
	}
}

func TestExtentReleasor(t *testing.T) {
	ttp := testutil.InitTempTestPath(t)
	defer ttp.Cleanup()

	var err error
	var store *ExtentStore
	store, err = NewExtentStore(ttp.Path(), 1, 120*unit.MB, 1, nil, false, nil)
	assert.Nil(t, err)
	assert.NotNil(t, store)
	var releasorParh = path.Join(ttp.Path(), "releasor")
	var releasor *ExtentReleasor
	releasor, err = NewExtentReleasor(releasorParh, store, false, nil)
	assert.Nil(t, err)
	assert.NotNil(t, releasor)

	for i := 1; i <= 1024; i++ {
		err = releasor.MarkDelete(context.Background(), 0, uint64(i)+1024, 0, 0)
		assert.Nil(t, err)
		if i%256 == 0 {
			err = releasor.Rotate()
			assert.Nil(t, err)
		}
	}
	err = releasor.FlushDelete(10)
	assert.Nil(t, err)
	releasor.Stop()
}
