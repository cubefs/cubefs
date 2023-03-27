package convertnode

import (
	"encoding/json"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"strings"
)

type DBConfig struct {
	Path         string `json:"path"`
	Config       string `json:"config"`
	Dbname       string `json:"dbName"`
	Username     string `json:"userName"`
	Password     string `json:"password"`
	MaxIdleConns int    `json:"maxIdleConns"`
	MaxOpenConns int    `json:"maxOpenConns"`
	LogMode      bool   `json:"logMode"`
	LogZap       string `json:"logZap"`
}

func (d *DBConfig) DataSourceName() string {
	return d.Username + ":" + d.Password + "@tcp(" + d.Path + ")/" + d.Dbname + "?" + d.Config
}

//todo: state maxRetryCnt
type DBInfo struct {
	dbHandle  *gorm.DB
	state     uint8
	maxTryCnt uint8
	Config    DBConfig
}

func (db *DBInfo) parseMysqlDBConfig(cfg *config.Config) (err error) {
	sqlConfBytes := cfg.GetJsonObjectBytes(ConfigMySQLDB)
	if err = json.Unmarshal(sqlConfBytes, &db.Config); err != nil {
		log.LogErrorf("[parseMysqlDBConfig] unmarshall failed:%v", err)
		return
	}
	return
}

func (db *DBInfo) Open() (err error) {
	mysqlConfig := mysql.Config{
		DSN:                       db.Config.DataSourceName(), // data source name
		DefaultStringSize:         191,                        // string 类型字段的默认长度
		DisableDatetimePrecision:  true,                       // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,                       // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,                       // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false,                      // 根据版本自动配置
	}

	if db.dbHandle, err = gorm.Open(mysql.New(mysqlConfig), gormConfig(db.Config.LogZap, db.Config.LogMode)); err != nil {
		return
	}
	sqlDB, _ := db.dbHandle.DB()
	sqlDB.SetMaxIdleConns(db.Config.MaxIdleConns)
	sqlDB.SetMaxOpenConns(db.Config.MaxOpenConns)
	return
}

func gormConfig(logZap string, mod bool) *gorm.Config {
	var gormConf = &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true}
	switch logZap {
	case "silent", "Silent":
		gormConf.Logger = logger.Default.LogMode(logger.Silent)
	case "error", "Error":
		gormConf.Logger = logger.Default.LogMode(logger.Error)
	case "warn", "Warn":
		gormConf.Logger = logger.Default.LogMode(logger.Warn)
	case "info", "Info":
		gormConf.Logger = logger.Default.LogMode(logger.Info)
	case "zap", "Zap":
		gormConf.Logger = logger.Default.LogMode(logger.Info)
	default:
		if mod {
			gormConf.Logger = logger.Default.LogMode(logger.Info)
		} else {
			gormConf.Logger = logger.Default.LogMode(logger.Silent)
		}
	}
	return gormConf
}

func (db *DBInfo) ReleaseDBHandle() {
	sqlDB, _ := db.dbHandle.DB()
	sqlDB.Close()
	return
}

func (db *DBInfo) getServerAddr() string {
	return db.Config.Path
}

type ClusterInfoTable struct {
	ID          int    `gorm:"column:id;"`
	ClusterName string `gorm:"column:cluster_name;"`
	MasterAddr  string `gorm:"column:addr;"`
}

func (ClusterInfoTable) TableName() string {
	return "cluster_info"
}

func (db *DBInfo) PutClusterInfoToDB(clusterName string, mastersAddr []string) error {
	return db.dbHandle.Create(&ClusterInfoTable{
		ClusterName: clusterName,
		MasterAddr:  strings.Join(mastersAddr, ","),
	}).Error
}

func (db *DBInfo) DeleteClusterInfoFromDB(clusterName string) error {
	return db.dbHandle.Where("cluster_name = ?", clusterName).Delete(&ClusterInfoTable{}).Error
}

func (db *DBInfo) UpdateClusterInfoToDB(clusterName string, mastersAddr []string) error {
	addr := strings.Join(mastersAddr, ",")
	return db.dbHandle.Model(&ClusterInfoTable{}).Where("cluster_name = ?", clusterName).
		Update("addr", addr).Error
}

func (db *DBInfo) LoadAllClusterInfoFromDB() (clustersInfo []*ClusterInfoTable, err error) {
	clustersInfo = make([]*ClusterInfoTable, 0)
	err = db.dbHandle.Table(ClusterInfoTable{}.TableName()).Find(&clustersInfo).Error
	return
}

//todo:struct member
type TaskInfo struct {
	ID                   uint64 `gorm:"column:id;"`
	ClusterName          string `gorm:"column:cluster_name;"`
	VolumeName           string `gorm:"column:volume_name;"`
	SelectedMP           string `gorm:"column:selected_mp;"` //
	FinishedMP           string `gorm:"column:finished_mp;"` //
	RunningMP            uint64 `gorm:"column:running_mp;"`
	Layout               string `gorm:"column:layout;"`        //
	Process              uint32 `gorm:"-"`                     //
	TaskStage            uint32 `gorm:"column:task_stage;"`    //
	MPConvertStage       uint32 `gorm:"column:convert_stage;"` //
	SelectedReplNodeAddr string `gorm:"column:selected_replace_node_addr"`
	OldNodeAddr          string `gorm:"column:old_node_addr"`
}

func (TaskInfo) TableName() string {
	return "task_info"
}

//if task exist, update task info, else create task info to db
func (db *DBInfo) PutTaskInfoToDB(task *TaskInfo) error {
	taskRec := new(TaskInfo)
	if err := db.dbHandle.Table(TaskInfo{}.TableName()).Where("cluster_name = ? and volume_name = ?",
		task.ClusterName, task.VolumeName).First(taskRec).Error; err == gorm.ErrRecordNotFound {
		//create
		return db.dbHandle.Table(TaskInfo{}.TableName()).Create(task).Error
	}
	//update all fields
	task.ID = taskRec.ID
	return db.dbHandle.Table(TaskInfo{}.TableName()).Save(task).Error
}

func (db *DBInfo) DeleteTaskInfoFromDB(clusterName, volumeName string) error {
	return db.dbHandle.Table(TaskInfo{}.TableName()).Where("cluster_name = ? and volume_name = ?",
		clusterName, volumeName).Delete(&TaskInfo{}).Error
}

func (db *DBInfo) LoadAllTaskInfoFromDB() (tasks []*TaskInfo, err error) {
	tasks = make([]*TaskInfo, 0)
	err = db.dbHandle.Table(TaskInfo{}.TableName()).Scan(&tasks).Error
	return
}

type SingletonCheckTable struct {
	ID     int    `gorm:"column:id;"`
	IpAddr string `gorm:"column:ip_addr;"`
}

func (SingletonCheckTable) TableName() string {
	return "singleton_check"
}

func (db *DBInfo) LoadIpAddrFromDataBase() (records []*SingletonCheckTable, err error) {
	if err = db.dbHandle.Table(SingletonCheckTable{}.TableName()).Scan(&records).Error; err != nil {
		return
	}
	return
}

func (db *DBInfo) PutIpAddrToDataBase(ipAddr string) (err error) {
	return db.dbHandle.Table(SingletonCheckTable{}.TableName()).Create(&SingletonCheckTable{
		IpAddr: ipAddr,
	}).Error
}

func (db *DBInfo) ClearIpAddrInDataBase(addr string) (err error) {
	return db.dbHandle.Model(&SingletonCheckTable{}).Where("ip_addr = ?", addr).Delete(&SingletonCheckTable{}).Error
}
