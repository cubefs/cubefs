package storage

import (
	"container/list"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRecordFilename_parse(t *testing.T) {
	type Case struct {
		raw     string
		isValid bool
		seq     uint64
	}
	var cases = []Case{
		{
			raw:     "REC.0000000000000000000",
			isValid: false,
			seq:     0,
		},
		{
			raw:     "REC.00000000000000000000",
			isValid: true,
			seq:     0,
		},
		{
			raw:     "REC.00000000000000000001",
			isValid: true,
			seq:     1,
		},
		{
			raw:     "REC.00000000000000010010",
			isValid: true,
			seq:     10010,
		},
	}
	for i, c := range cases {
		t.Run("case-"+strconv.Itoa(i), func(t *testing.T) {
			rfn, _ := parseRecordFilename(c.raw)
			assertEqual(t, c.isValid, rfn.valid())
			assertEqual(t, c.seq, rfn.seq())
		})
	}
}

func TestExtentQueue(t *testing.T) {
	var testpath = testutil.InitTempTestPath(t)
	defer testpath.Cleanup()

	const (
		maxFilesize = 1024 * 100 // 100KB
		retainFiles = 1
	)

	type __item struct {
		ino       uint64
		extent    uint64
		offset    int64
		size      int64
		timestamp int64
	}

	var (
		gen     uint64 = 0
		genitem        = func() *__item {
			item := &__item{
				ino:       gen + 1,
				extent:    gen + 1024,
				offset:    0,
				size:      0,
				timestamp: time.Now().Unix(),
			}
			gen++
			return item
		}

		memqueue   = list.New()
		memproduce = func(item *__item) {
			memqueue.PushBack(item)
		}
		memconsume = func() *__item {
			if front := memqueue.Front(); front != nil {
				memqueue.Remove(front)
				return front.Value.(*__item)
			}
			return nil
		}
	)

	var err error
	var queue *ExtentQueue
	queue, err = OpenExtentQueue(testpath.Path(), maxFilesize, retainFiles)
	assert.Nil(t, err)

	// 测试队列里没有新数据时的消费情况以及尝试消费后的元信息
	err = queue.Consume(func(ino, extent uint64, offset, size, timestamp int64) (goon bool, err error) {
		assert.Fail(t, "no record should be consume")
		return true, nil
	})
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), queue.mf.getConsumed().seq)
	assert.Equal(t, uint64(0), queue.mf.getConsumed().off)

	// 测试队列单挑生产5000条记录
	for i := 0; i < 5000; i++ {
		item := genitem()
		err = queue.Produce(item.ino, item.extent, item.offset, item.size, item.timestamp)
		assert.Nil(t, err)
		memproduce(item)
	}

	// 检查队列Remain结果
	assert.Equal(t, 5000, queue.Remain())

	// 测试队列批量生产5000条记录，其中分10个Batch，每个Batch包含500条记录
	for i := 0; i < 10; i++ {
		var producer *BatchProducer
		producer, err = queue.BatchProduce(100)
		assert.Nil(t, err)
		for j := 0; j < 500; j++ {
			item := genitem()
			producer.Add(item.ino, item.extent, item.offset, item.size, item.timestamp)
			memproduce(item)
		}
		err = producer.Submit()
		assert.Nil(t, err)
	}

	// 检查队列Remain结果
	assert.Equal(t, 10000, queue.Remain())

	// 检查数据目录数据文件是否按预期自动结转
	validRecordFileList(t, testpath.Path(), []string{
		"REC.00000000000000000000",
		"REC.00000000000000000001",
		"REC.00000000000000000002",
		"REC.00000000000000000003",
		"REC.00000000000000000004",
	})

	// 消费1000条数据, 验证消费顺序及数据内存是否与生产顺序一致
	var counter int
	err = queue.Consume(func(ino, extent uint64, offset, size, timestamp int64) (goon bool, err error) {
		item := memconsume()
		assert.NotNil(t, item)
		assert.Equal(t, item.ino, ino)
		assert.Equal(t, item.extent, extent)
		assert.Equal(t, item.offset, offset)
		assert.Equal(t, item.size, size)
		assert.Equal(t, item.timestamp, timestamp)
		counter++
		if counter == 1000 {
			return false, nil
		}
		return true, nil
	})
	assert.Nil(t, err)
	assert.Equal(t, 1000, counter)

	// 检查队列Remain结果
	assert.Equal(t, 9000, queue.Remain())

	// 关闭队列实例后再打开, 以验证队列数据及状态恢复情况
	queue.Close()

	queue, err = OpenExtentQueue(testpath.Path(), maxFilesize, retainFiles)
	assert.Nil(t, err)

	// 消费完队列中剩余的9000条数据
	counter = 0
	err = queue.Consume(func(ino, extent uint64, offset, size, timestamp int64) (goon bool, err error) {
		item := memconsume()
		assert.NotNil(t, item)
		assert.Equal(t, item.ino, ino)
		assert.Equal(t, item.extent, extent)
		assert.Equal(t, item.offset, offset)
		assert.Equal(t, item.size, size)
		assert.Equal(t, item.timestamp, timestamp)
		counter++
		return true, nil
	})
	assert.Nil(t, err)
	assert.Equal(t, 9000, counter)

	// 检查队列Remain结果
	assert.Equal(t, 0, queue.Remain())

	// 当前队列已无未消费消息, 再次尝试消费, 此时应不会有任何消息被Consume方法消费
	err = queue.Consume(func(ino, extent uint64, offset, size, timestamp int64) (goon bool, err error) {
		assert.Fail(t, "no record should be consume")
		return true, nil
	})
	assert.Nil(t, err)

	// 检查数据目录数据文件是否按预期在消费后移除并保留至少一个文件.
	validRecordFileList(t, testpath.Path(), []string{
		"REC.00000000000000000003",
		"REC.00000000000000000004",
	})

	queue.Close()
}

func validRecordFileList(t *testing.T, path string, expected []string) {
	var err error
	var dirFp *os.File
	dirFp, err = os.Open(path)
	assert.Nil(t, err)
	var names []string
	names, err = dirFp.Readdirnames(-1)
	assert.Nil(t, err)
	_ = dirFp.Close()
	sort.Strings(names)
	var actual []string
	for _, name := range names {
		if _, is := parseRecordFilename(name); is {
			actual = append(actual, name)
		}
	}
	assert.Equal(t, expected, actual)
}
