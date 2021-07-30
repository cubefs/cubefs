package sink

import (
	"fmt"
	"io"
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
		fi, li uint64
	)
	if fi, err = ws.FirstIndex(); err != nil {
		err = fmt.Errorf("read first index failed: %v", err)
		return
	}
	if li, err = ws.LastIndex(); err != nil {
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

	var entries []*proto.Entry

	headerRowText := s.buildHeaderRowText()
	if _, err = s.writer.Write([]byte(headerRowText + "\n")); err != nil {
		err = fmt.Errorf("output failed: %v", err)
		return
	}

	var count uint64
	for {
		if lo > li || (s.opt.Count > 0 && count >= s.opt.Count) {
			break
		}
		if entries, _, err = ws.Entries(lo, li, 4*1024*1024); err != nil {
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
