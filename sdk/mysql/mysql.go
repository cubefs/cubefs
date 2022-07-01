package mysql

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var db *sql.DB

func InitMysqlClient(mc *config.MysqlConfig) (err error) {
	if db != nil {
		log.LogInfof("mysql client has been initialized")
		return nil
	}
	mysqlPort := strconv.Itoa(mc.Port)
	dataSource := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&loc=Local", mc.Username, mc.Password, mc.Url, mysqlPort, mc.Database)
	db, err = sql.Open("mysql", dataSource)
	if err != nil {
		log.LogErrorf("[InitMysqlClient] init mysql client failed, url(%v), username(%v), password(%v), err(%v)", mc.Url, mc.Username, mc.Password, err)
		return
	}

	db.SetMaxIdleConns(mc.MaxIdleConns)
	db.SetMaxOpenConns(mc.MaxOpenConns)
	if err := db.Ping(); err != nil {
		log.LogErrorf("[InitMysqlClient] check mysql client connection failed, url(%v), username(%v), password(%v), err(%v)", mc.Url, mc.Username, mc.Password, err)
	}
	log.LogInfof("mysql client initialize success")
	return
}

func Close() {
	if db != nil {
		_ = db.Close()
	}
}

func Transaction(sqlCmd string, args []interface{}) (rs sql.Result, err error) {
	var (
		tx   *sql.Tx
		stmt *sql.Stmt
	)
	tx, err = db.Begin()
	if err != nil {
		return
	}

	stmt, err = tx.Prepare(sqlCmd)
	if err != nil {
		return
	}
	defer func() {
		if stmt != nil {
			_ = stmt.Close()
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if rs, err = stmt.Exec(args...); err != nil {
		return
	}
	if err = tx.Commit(); err != nil {
		return
	}
	return
}

func Retry(f func() error, times int) (err error) {
	for i := 1; i <= times; i++ {
		if err = f(); err == nil {
			return
		}
		log.LogWarnf("execute function in retry failed, function(%v), times(%v), err(%v)", "functionName", i, err)
	}
	log.LogErrorf("execute function failed after retry %v times, function(%v), err(%v)", times, "functionName", err)
	return
}

func FormatTime(sourceStr string) (t time.Time, err error) {
	return time.ParseInLocation("2006-01-02 15:04:05", sourceStr, time.Local)
}

func ParsePassword(filePath, confKey string) (password string, err error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	defer file.Close()

	if err != nil {
		return
	}
	var line string
	srcReader := bufio.NewReader(file)
	for {
		line, err = srcReader.ReadString('\n')
		if (len(line) == 0 && err == io.EOF) || (err != nil && err != io.EOF) {
			return
		}
		if len(line) == 0 || line == "\n" {
			continue
		}
		log.LogDebugf("[ParsePassword] origin string: %v", line)
		line = strings.ReplaceAll(line, " ", "")
		line = strings.ReplaceAll(line, "\n", "")
		index := strings.Index(line, "=")
		name := util.SubString(line, 0, index)
		value := util.SubString(line, index+1, len(line))
		if name == confKey {
			log.LogDebugf("[ParsePassword] parsed value: %v", value)
			return value, nil
		}
		if err == io.EOF {
			return
		}
	}
}
