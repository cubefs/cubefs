package model

import (
	"time"

	"github.com/cubefs/cubefs/console/backend/helper/types"
	"github.com/cubefs/cubefs/console/backend/model/mysql"
)

type User struct {
	Id         uint64           `gorm:"primaryKey" json:"id"`
	Name       string           `gorm:"type:varchar(50);not null;default:'';uniqueIndex" json:"name"`
	Role       int              `gorm:"type:tinyint(4);not null;default:3" json:"role"`
	AccessKey  types.EncryptStr `gorm:"type:varchar(500);not null;default:''" json:"access_key"`
	SecretKey  types.EncryptStr `gorm:"type:varchar(500);not null;default:''" json:"secret_key"`
	CreatorId  int              `gorm:"not null;default:0;index" json:"creator_id"`
	CreateTime time.Time        `gorm:"not null;default:CURRENT_TIMESTAMP(3)" json:"create_time"`
}

func (u *User) Create() error {
	return mysql.GetDB().Create(u).Error
}
