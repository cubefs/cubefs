package sink

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"strings"

	"github.com/tiglabs/raft/storage/wal"

	"github.com/chubaofs/chubaofs/raftstore/walreader/common"
	"github.com/chubaofs/chubaofs/raftstore/walreader/decode"
	"github.com/tiglabs/raft/proto"
)

const (
	columnWidthIndex   = 8
	columnWidthTerm    = 4
	columnWidthType    = 10
	columnWidthDefault = 0
)

type RecordFilter uint8

func (f RecordFilter) String() string {
	switch f {
	case RecordFilter_All:
		return "All"
	case RecordFilter_Normal:
		return "Normal"
	case RecordFilter_ConfChange:
		return "ConfChange"
	default:
	}
	return "Unknown"
}

const (
	RecordFilter_All RecordFilter = iota
	RecordFilter_Normal
	RecordFilter_ConfChange
)

type Option struct {
	File    string
	Start   uint64
	Count   uint64
	Filter  RecordFilter
	Keyword string
}

type Sinker struct {
	logDir  string
	decoder decode.LogCommandDecoder
	writer  io.Writer

	opt Option
}

func (s *Sinker) Run() (err error) {
	var ws *wal.Storage
	if ws, err = wal.NewStorage(s.logDir, &wal.Config{}); err != nil {
		err = fmt.Errorf("open log dir failed: %v", err)
		return
	}
	defer ws.Close()

	var (
		fi, li         uint64
		readSingleFile = len(s.opt.File) > 0
	)
	if readSingleFile {
		fi, err = ws.FirstIndexOfFile(s.opt.File)
	} else {
		fi, err = ws.FirstIndexForWalreader()
	}
	if err != nil {
		err = fmt.Errorf("read first index failed: %v", err)
		return
	}
	if readSingleFile {
		li, err = ws.LastIndexOfFile(s.opt.File)
	} else {
		li, err = ws.LastIndex()
	}
	if err != nil {
		err = fmt.Errorf("read last index failed: %v", err)
		return
	}
	if err = s.writef("First index: %v\n", fi); err != nil {
		err = fmt.Errorf("output failed: %v", err)
		return
	}
	if err = s.writef("Last index : %v\n", li); err != nil {
		err = fmt.Errorf("ouput failed: %v", err)
		return
	}

	var hs proto.HardState
	if hs, err = ws.InitialState(); err != nil {
		err = fmt.Errorf("read hard state failed: %v", err)
		return
	}
	if err = s.writef("Hard state: commit %v, term %v, vote %v\n", hs.Commit, hs.Term, hs.Vote); err != nil {
		err = fmt.Errorf("output failed: %v", err)
		return
	}
	var lo = fi
	if s.opt.Start > lo {
		lo = s.opt.Start
	}

	headerRowText := s.buildHeaderRowText()
	if _, err = s.writer.Write([]byte(headerRowText + "\n")); err != nil {
		err = fmt.Errorf("output failed: %v", err)
		return
	}

	var (
		entries []*proto.Entry
		count   uint64
	)
	for {
		if lo > li || (s.opt.Count > 0 && count >= s.opt.Count) {
			break
		}
		// the following ws.Entries functions read [lo,li)
		if readSingleFile {
			entries, err = ws.EntriesOfFile(s.opt.File, lo, li+1)
		} else {
			entries, _, err = ws.EntriesForWalreader(lo, li+1, 4*1024*1024)
		}
		if err != nil {
			err = fmt.Errorf("read entries [lo %v, hi %v] failed: %v\n", lo, li, err)
			return
		}
		if len(entries) == 0 {
			break
		}
		lo = entries[len(entries)-1].Index + 1
		for _, entry := range entries {
			if s.opt.Count > 0 && count >= s.opt.Count {
				break
			}
			var recordRawText string
			var skip bool
			if len(entry.Data) == 0 {
				fmt.Println(entry)
				continue
			}
			if recordRawText, skip, err = s.buildRecordRowText(entry); err != nil {
				err = fmt.Errorf("output record failed: %v", err)
				return
			}
			if skip {
				continue
			}
			if _, err = s.writer.Write([]byte(recordRawText + "\n")); err != nil {
				err = fmt.Errorf("output failed: %v", err)
				return
			}
			count++
		}
	}
	return
}

func (s *Sinker) writef(format string, vals ...interface{}) (err error) {
	_, err = s.writer.Write([]byte(fmt.Sprintf(format, vals...)))
	return
}

func (s *Sinker) buildHeaderRowText() string {
	var values = common.NewColumnValues(
		common.ColumnValue{Value: "INDEX", Width: columnWidthIndex},
		common.ColumnValue{Value: "TERM", Width: columnWidthTerm},
		common.ColumnValue{Value: "TYPE", Width: columnWidthType},
	)
	values.Add(s.decoder.Header()...)
	return values.BuildColumnText()
}

func (s *Sinker) buildRecordRowText(entry *proto.Entry) (text string, skip bool, err error) {

	var values = common.NewColumnValues(
		common.ColumnValue{Value: entry.Index, Width: columnWidthIndex},
		common.ColumnValue{Value: entry.Term, Width: columnWidthTerm},
		common.ColumnValue{Value: s.formatEntryType(entry.Type), Width: columnWidthType},
	)
	switch entry.Type {
	case proto.EntryNormal:
		if s.opt.Filter != RecordFilter_All && s.opt.Filter != RecordFilter_Normal {
			skip = true
			return
		}
		var childValues common.ColumnValues
		if childValues, err = s.decoder.DecodeCommand(entry.Data); err != nil {
			return
		}
		values.Add(childValues...)
	case proto.EntryConfChange:
		if s.opt.Filter != RecordFilter_All && s.opt.Filter != RecordFilter_ConfChange {
			skip = true
			return
		}
		cc := new(proto.ConfChange)
		cc.Decode(entry.Data)
		content := fmt.Sprintf("%v(%v)", cc.Type, cc.Peer)
		values.Add(common.ColumnValue{Value: content, Width: columnWidthDefault})
	}
	text = values.BuildColumnText()

	if len(s.opt.Keyword) > 0 && !strings.Contains(text, s.opt.Keyword) {
		skip = true
	}
	return
}

var footerMagic = []byte{'\xf9', '\xbf', '\x3e', '\x0a', '\xd3', '\xc5', '\xcc', '\x3f'}
const indexItemSize = 8 + 8 + 4

type indexItem struct {
	logindex uint64 // 日志的index
	logterm  uint64 // 日志的term
	offset   uint32 // 日志在文件中的偏移
}

type logEntryIndex []indexItem

func decodeLogIndex(data []byte) (logEntryIndex, error) {
	offset := 0
	var err error

	nItems := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	calcItems := uint32((len(data) - 4 -29 -4) / 20)
	if calcItems < nItems {
		err = fmt.Errorf("!!! log expect %d recs, but now have %d recs", nItems, calcItems)
		nItems = calcItems
	}
	li := make([]indexItem, nItems)

	for i := 0; i < int(nItems); i++ {
		li[i].logindex = binary.BigEndian.Uint64(data[offset:])
		offset += 8
		li[i].logterm = binary.BigEndian.Uint64(data[offset:])
		offset += 8
		li[i].offset = binary.BigEndian.Uint32(data[offset:])
		offset += 4
	}
	return li, err
}

func (s *Sinker) getRaftLogRecordsIndexItem(data []byte) (logEntryIndex, error) {
	index := make([]indexItem, 0)
	footer := data[len(data) - 29 :]
	if footer[0] != 3 {
		return index, fmt.Errorf("record file has no index item")
	}

	if binary.BigEndian.Uint64(footer[1:9]) != 16 {
		return index, fmt.Errorf("record file footer data len error")
	}

	logIndexOffset := int(binary.BigEndian.Uint64(footer[9:17]))
	if !bytes.Equal(footer[17:25], footerMagic) {
		return index, fmt.Errorf("record file footer magic error\n")
	}

	logIndexBuff := data[logIndexOffset : ]

	return decodeLogIndex(logIndexBuff[9:])
}

func (s *Sinker)parseLogEntry(data []byte, offset int, lastTerm, lastIndex *uint64) (int, error) {
	if offset >= len(data) {
		return offset, fmt.Errorf("parese finished, offset[%d] is beyond data buff[%d]", offset, len(data))
	}
	logType := data[offset]
	offset += 1
	if logType != 1 {
		return offset, fmt.Errorf("parese finished, logtype is not log entry")
	}

	size := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	if offset + int(size) > len(data) {
		return offset - 9, fmt.Errorf("parese log failed: data size err")
	}

	entryBuff := data[offset: offset + int(size)]
	newCrc := common.NewCRC(entryBuff)
	offset += int(size)
	crc := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if crc != newCrc.Value() {
		fmt.Printf("parese log failed, crc err, expect[%d], but now[%d], next item maybe not right\n", crc, newCrc.Value())
	}

	entry := &proto.Entry{}
	entry.Decode(entryBuff)

	if *lastTerm == 0 {
		*lastTerm = entry.Term
	}
	if *lastIndex == 0 {
		*lastIndex = entry.Index
	}

	if *lastTerm != entry.Term {
		fmt.Printf("leader changed, term:%d-->%d, index:%d-->%d\n", *lastTerm, entry.Term, *lastIndex, entry.Index)
	} else {
		if *lastIndex != (entry.Index - 1) {
			fmt.Printf("index skipped, term:%d, index:%d-->%d\n", entry.Term, *lastIndex, entry.Index)
		}
	}
	value, _, _ :=s.buildRecordRowText(entry)
	fmt.Printf("%v\n", value)
	*lastTerm = entry.Term
	*lastIndex = entry.Index
	return offset, nil
}

func (s *Sinker) ForceParseRaftLog() {

	fileNames, _ := common.ListLogEntryFiles(s.logDir)
	lastIndex := uint64(0)
	lastTerm  := uint64(0)
	fmt.Printf("force parse \ntotal files:%d \n%s\n", len(fileNames), s.buildHeaderRowText())
	for index, logFile := range fileNames {
		fileAbsName := path.Join(s.logDir, logFile.String())
		data, err := ioutil.ReadFile(fileAbsName)
		if err != nil {
			fmt.Printf("read %d file[%s] failed:%s\n", index, err.Error())
			continue
		}

		//get index recs
		indexRecs, err:= s.getRaftLogRecordsIndexItem(data)
		if err != nil {
			fmt.Printf("get log records failed:%s\n", err.Error())
		}
		offset := 0

		// read data as log entry
		for {
			offset , err = s.parseLogEntry(data, offset, &lastTerm, &lastIndex)
			if err != nil {
				fmt.Printf("parse %d file[%s] err:%s\n", index, logFile.String(), err.Error())
				break
			}
		}
		fmt.Printf("parse %d file[%s] seq read record finished: offset:%d\n", index, logFile.String(), offset)
		//read data by recs offset
		lastOffset := 0
		for _, rec := range indexRecs {
			lastOffset = int(rec.offset)
			if int(rec.offset) < offset {
				continue
			}

			_, err = s.parseLogEntry(data, int(rec.offset), &lastTerm, &lastIndex)
			if err != nil {
				fmt.Printf("parse %d file[%s] err:%s\n", index, fileAbsName, err.Error())
			}
		}
		fmt.Printf("\n*****************************\nparse %d file[%s] seq read record finished: offset:%d, last rec offset:%d, data len:%d \n*****************************\n",
			index, logFile.String(), offset, lastOffset, len(data))
	}
}

func (s *Sinker) formatEntryType(entryType proto.EntryType) string {
	switch entryType {
	case proto.EntryNormal:
		return "Normal"
	case proto.EntryConfChange:
		return "ConfChange"
	}
	return "Unknown"
}

func NewLogEntrySinker(logDir string, decoder decode.LogCommandDecoder, writer io.Writer, opt Option) *Sinker {
	return &Sinker{
		logDir:  logDir,
		decoder: decoder,
		writer:  writer,
		opt:     opt,
	}
}
