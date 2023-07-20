package migrate

import (
	"encoding/json"
	"errors"
	"github.com/cubefs/cubefs/console/backend/model"
	"github.com/cubefs/cubefs/console/backend/service/auth"
	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/gorm"

	"github.com/cubefs/cubefs/console/backend/model/mysql"
)

var migrations = make([]*gormigrate.Migration, 0)

func Init() error {
	migrations = append(migrations, &gormigrate.Migration{
		ID: "202304200_create_cluster",
		Migrate: func(tx *gorm.DB) error {
			if tx.Migrator().HasTable(&model.Cluster{}) {
				return nil
			}
			return tx.Migrator().AutoMigrate(&model.Cluster{})
		},
		Rollback: func(db *gorm.DB) error {
			return nil
		},
	}, &gormigrate.Migration{
		ID: "202304200_create_node_config",
		Migrate: func(tx *gorm.DB) error {
			if !tx.Migrator().HasTable(&model.NodeConfig{}) {
				err := tx.Migrator().AutoMigrate(&model.NodeConfig{})
				if err != nil {
					return err
				}
			}
			if !tx.Migrator().HasTable(&model.NodeConfigFailure{}) {
				err := tx.Migrator().AutoMigrate(&model.NodeConfigFailure{})
				if err != nil {
					return err
				}
			}
			return nil
		},
		Rollback: func(db *gorm.DB) error {
			return nil
		},
	}, &gormigrate.Migration{
		ID: "202304230_create_op_type",
		Migrate: func(tx *gorm.DB) error {
			if tx.Migrator().HasTable(&model.OpType{}) {
				return nil
			}
			err := tx.Migrator().AutoMigrate(&model.OpType{})
			if err != nil {
				return err
			}
			return model.InitOpTypeData(tx)
		},
		Rollback: func(db *gorm.DB) error {
			return nil
		},
	}, &gormigrate.Migration{
		ID: "202304240_create_operation_log",
		Migrate: func(tx *gorm.DB) error {
			if tx.Migrator().HasTable(&model.OperationLog{}) {
				return nil
			}
			return tx.Migrator().AutoMigrate(&model.OperationLog{})
		},
		Rollback: func(db *gorm.DB) error {
			return nil
		},
	}, &gormigrate.Migration{
		ID: "202305060_create_user",
		Migrate: func(tx *gorm.DB) error {
			if tx.Migrator().HasTable(&model.User{}) {
				return nil
			}
			return tx.Migrator().AutoMigrate(&model.User{})
		},
		Rollback: func(db *gorm.DB) error {
			return nil
		},
	}, &gormigrate.Migration{
		ID: "202305060_create_vol",
		Migrate: func(tx *gorm.DB) error {
			if tx.Migrator().HasTable(&model.Vol{}) {
				return nil
			}
			return tx.Migrator().AutoMigrate(&model.Vol{})
		},
		Rollback: func(db *gorm.DB) error {
			return nil
		},
	}, &gormigrate.Migration{
		ID: "202305100_create_auth",
		Migrate: func(tx *gorm.DB) error {
			errorList := auth.CreateDb()
			if len(errorList) != 0 {
				errJson, _ := json.Marshal(errorList)
				return errors.New(string(errJson))
			}
			return auth.InitAuth()
		},
		Rollback: func(db *gorm.DB) error {
			return nil
		},
	})
	m := gormigrate.New(mysql.GetDB(), gormigrate.DefaultOptions, migrations)
	return m.Migrate()
}
