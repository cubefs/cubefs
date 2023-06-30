package rebalance

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"time"
)

func (rw *ReBalanceWorker) DataSourceName() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&loc=Local", rw.MysqlConfig.Username, rw.MysqlConfig.Password, rw.MysqlConfig.Url, rw.MysqlConfig.Port, rw.MysqlConfig.Database)
}

func (rw *ReBalanceWorker) OpenSql() (err error) {
	mysqlConfig := mysql.Config{
		DSN:                       rw.DataSourceName(), // data source name
		DefaultStringSize:         191,                 // string 类型字段的默认长度
		DisableDatetimePrecision:  true,                // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,                // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,                // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false,               // 根据版本自动配置
	}

	if rw.dbHandle, err = gorm.Open(mysql.New(mysqlConfig)); err != nil {
		return
	}
	sqlDB, _ := rw.dbHandle.DB()
	sqlDB.SetMaxIdleConns(rw.MysqlConfig.MaxIdleConns)
	sqlDB.SetMaxOpenConns(rw.MysqlConfig.MaxOpenConns)
	err = rw.dbHandle.AutoMigrate(MigrateRecordTable{})
	if err != nil {
		return
	}
	return
}

type MigrateRecordTable struct {
	ID int `gorm:"column:id;"`

	ClusterName  string  `gorm:"column:cluster_name;"`
	ZoneName     string  `gorm:"column:zone_name;"`
	VolName      string  `gorm:"column:vol_name;"`
	PartitionID  uint64  `gorm:"column:partition_id;"`
	SrcAddr      string  `gorm:"column:src_addr;"`
	SrcDisk      string  `gorm:"column:src_disk"`
	DstAddr      string  `gorm:"column:dst_addr;"`
	OldUsage     float64 `gorm:"column:old_usage"`
	NewUsage     float64 `gorm:"column:new_usage"`
	OldDiskUsage float64 `gorm:"column:old_disk_usage"`
	NewDiskUsage float64 `gorm:"column:new_disk_usage"`

	CreatedAt time.Time `gorm:"column:created_at"`
}

func (MigrateRecordTable) TableName() string {
	return "migrate_record"
}

func (rw *ReBalanceWorker) PutMigrateInfoToDB(info *MigrateRecordTable) error {
	return rw.dbHandle.Create(info).Error
}

func (rw *ReBalanceWorker) GetMigrateRecordsByZoneName(clusterName, zoneName string) (migrateRecords []*MigrateRecordTable, err error) {
	migrateRecords = make([]*MigrateRecordTable, 0)
	err = rw.dbHandle.Table(MigrateRecordTable{}.TableName()).
		Where("cluster_name = ? AND zone_name = ?", clusterName, zoneName).
		Find(&migrateRecords).Error
	return
}

func (rw *ReBalanceWorker) GetMigrateRecordsBySrcNode(host string) (migrateRecords []*MigrateRecordTable, err error) {
	migrateRecords = make([]*MigrateRecordTable, 0)
	err = rw.dbHandle.Table(MigrateRecordTable{}.TableName()).
		Where("src_addr like ?", fmt.Sprintf("%%%s%%", host)).
		Find(&migrateRecords).Error
	return
}
