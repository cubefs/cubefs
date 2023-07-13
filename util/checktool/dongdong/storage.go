package dongdong

import (
	"bufio"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	LogDateForMat = "20060102"
	DataDir       = "/export/home/tomcat/DongDongAlarm/"
)

type FileStorage struct {
	path          string
	offsetFD      *os.File
	rfd           *os.File
	wfd           *os.File
	readOffset    int64
	readFileName  string
	writeFileName string
	api           *DDAPI
	lock          sync.Mutex // protect the store function
}

func newStorage(app string, api *DDAPI) (sto *FileStorage, err error) {
	sto = new(FileStorage)
	sto.api = api
	sto.path = path.Join(DataDir, app)
	err = os.MkdirAll(sto.path, 0644)
	if err != nil {
		err = fmt.Errorf("[newStorage], failed to mkdir: %v, err: %v", sto.path, err.Error())
		return
	}
	err = sto.loadOffset()
	if err != nil {
		err = fmt.Errorf("[newStorage], failed to load offset, err: %v", err.Error())
		return
	}
	sto.writeFileName = time.Now().Format(LogDateForMat)
	log.LogInfof("last file: %v, offset: %v", sto.readFileName, sto.readOffset)
	go sto.monitorFileScheduler()
	return
}

func (s *FileStorage) loadOffset() (err error) {
	offsetFile := path.Join(s.path, "offset")
	s.offsetFD, err = os.OpenFile(offsetFile, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		err = fmt.Errorf("[loadOffset], failed to open path: %v, err: %v", offsetFile, err.Error())
		return
	}
	data := make([]byte, 128)
	var readSize int
	readSize, err = s.offsetFD.Read(data)
	if err != nil && err != io.EOF {
		err = fmt.Errorf("[loadOffset], failed to read path: %v, err: %v", offsetFile, err.Error())
		return
	}
	_, err = s.offsetFD.Seek(0, io.SeekStart)
	if err != nil {
		err = fmt.Errorf("[loadOffset], failed to seek path: %v, err: %v", offsetFile, err.Error())
		return
	}

	realData := data[0:readSize]
	if readSize > 0 {
		s.readFileName, s.readOffset, err = parseOffset(string(realData))
		if err != nil {
			err = fmt.Errorf("[loadOffset], path: %v, Data: %v, err: %v", offsetFile, string(realData), err.Error())
			return
		}
	}
	return
}

func (s *FileStorage) monitorFileScheduler() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			files, err := ioutil.ReadDir(s.path)
			if err != nil {
				continue
			}
			for _, f := range files {
				if strings.HasPrefix(f.Name(), "20") {
					if f.Name() != s.writeFileName {
						name := path.Join(s.path, f.Name())
						err = os.Remove(name)
						if err != nil {
							err = fmt.Errorf("failed to rm expired file: %v, err: %v", name, err.Error())
						}
						log.LogWarnf("cleanup the expired message file: %v", name)
						continue
					}
					if f.Name() != s.readFileName {
						s.readOffset = 0
						s.readFileName = f.Name()
						s.persistOffset(f.Name(), s.readOffset)
					}
					err = s.sendMsgFromFile()
					if err != nil {
						log.LogErrorf(err.Error())
					}
				}
			}
		}
	}
}

func (s *FileStorage) sendMsgFromFile() (err error) {
	var readFD *os.File
	path := path.Join(s.path, s.readFileName)
	defer func() {
		if err != nil {
			err = fmt.Errorf("[sendMsgFromFile], file: %v, err: %v", path, err.Error())
		}
		if readFD != nil {
			readFD.Close()
		}
	}()
	log.LogInfof("[sendMsgFromFile], path: %v, offset: %v", path, s.readOffset)
	readFD, err = os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		err = fmt.Errorf("[sendMsgFromFile], path: %v, err: %v", path, err.Error())
		return
	}

	_, err = readFD.Seek(s.readOffset, io.SeekStart)
	if err != nil {
		err = fmt.Errorf("failed to seek, err: %v", err.Error())
		return
	}
	var (
		info os.FileInfo
	)
	for {
		time.Sleep(5 * time.Second)
		info, err = readFD.Stat()
		if info.Size() == s.readOffset && s.readFileName != s.writeFileName {
			break
		}

		if info.Size() > s.readOffset {
			_, err = readFD.Seek(s.readOffset, io.SeekStart)
			if err != nil {
				err = fmt.Errorf("failed to seek, err: %v", err.Error())
				return
			}
			s.sendMsg(bufio.NewReader(readFD))
		}
	}
	return
}

func (s *FileStorage) sendMsg(reader *bufio.Reader) {
	for {
		line, e := reader.ReadString('\n')
		if e != nil && e != io.EOF {
			log.LogWarnf("failed to read, err: %v", e.Error())
			break
		}
		if e == io.EOF {
			break
		}

		msg, e := unmarshalMsg([]byte(line[0 : len(line)-1]))
		if e != nil {
			s.readOffset += int64(len(line))
			s.persistOffset(s.readFileName, s.readOffset)
			continue
		}
		if msg.isExpires() {
			log.LogWarnf("msg: %v is expire", msg)
			s.readOffset += int64(len(line))
			s.persistOffset(s.readFileName, s.readOffset)
			continue
		}

		for {
			if msg.isExpires() {
				log.LogWarnf("msg: %v is expire", msg)
				break
			}
			switch msg.MType {
			case MsgTypeERP:
				e = s.api.SendMsgToERP(msg.ERP, msg.Data)
			case MsgTypeGroup:
				e = s.api.SendMsgToGroup(msg.Gid, msg.Data)
			default:
				break
			}
			if e != nil {
				log.LogErrorf("[sendMsgFromFile], err: %v", e.Error())
				time.Sleep(5 * time.Second)
				continue
			}
			break
		}
		s.readOffset += int64(len(line))
		s.persistOffset(s.readFileName, s.readOffset)
	}
	return
}

func (s *FileStorage) persistOffset(fileName string, offset int64) (err error) {
	err = s.offsetFD.Truncate(0)
	if err != nil {
		return
	}
	_, err = s.offsetFD.Seek(0, io.SeekStart)
	if err != nil {
		return
	}
	_, err = s.offsetFD.WriteString(fmt.Sprintf("%v,%v", fileName, offset))
	if err != nil {
		return
	}
	err = s.offsetFD.Sync()
	if err != nil {
		return
	}
	return
}

func (s *FileStorage) createFile() (err error) {
	todayStr := time.Now().Format(LogDateForMat)
	s.writeFileName = todayStr
	fpath := path.Join(s.path, todayStr)
	_, err = os.Stat(fpath)
	exist := true
	if err != nil && os.IsNotExist(err) {
		exist = false
	}
	s.wfd, err = os.OpenFile(fpath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		err = fmt.Errorf("failed to create file: %v, err: %v", fpath, err.Error())
		return
	}

	// if the check tool is restart at the same day, the new log is write to next line
	if exist {
		_, err = s.wfd.Write([]byte("\n"))
		if err != nil {
			return
		}
	}
	return
}

func (s *FileStorage) rotate() (err error) {
	if s.wfd == nil {
		err = s.createFile()
		if err != nil {
			return
		}
		return
	}

	todayStr := time.Now().Format(LogDateForMat)
	if todayStr == s.writeFileName {
		return
	}

	err = s.wfd.Close()
	if err != nil {
		err = fmt.Errorf("[rotate], err: %v", err.Error())
		return
	}
	return s.createFile()
}

func (s *FileStorage) store(msg *Msg) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	err = s.rotate()
	if err != nil {
		return
	}

	var data []byte
	data, err = msg.marshal()
	if err != nil {
		return
	}
	_, err = s.wfd.Write(data)
	if err != nil {
		return
	}
	_, err = s.wfd.Write([]byte("\n"))
	if err != nil {
		return
	}

	return
}

func parseOffset(data string) (date string, offset int64, err error) {
	fields := strings.Split(data, ",")
	if len(fields) != 2 {
		err = fmt.Errorf("invalid offset Data: %v", data)
		return
	}
	date = fields[0]
	offset, err = strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid offset: %v, err: %v", fields[1], err.Error())
		return
	}

	return
}
