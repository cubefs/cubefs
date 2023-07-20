package model

import (
	"time"

	"gorm.io/gorm"

	"github.com/cubefs/cubefs/console/backend/helper/types"
	"github.com/cubefs/cubefs/console/backend/model/mysql"
)

type NodeConfig struct {
	Id            int64        `gorm:"primaryKey" json:"id"`
	Node          string       `gorm:"type:varchar(255);index;not null;default:''" json:"node"`
	Cluster       string       `gorm:"type:varchar(255);not null;default:''" json:"cluster"`
	Configuration types.MapStr `gorm:"type:varchar(2048);not null;default:'{}'" json:"configuration"`
	UpdatedAt     time.Time    `json:"updated_at"`
}

func (e *NodeConfig) Upsert(node, cluster, key, value string) error {
	db := mysql.GetDB()
	nodeConf := &NodeConfig{Node: node, Cluster: cluster, Configuration: map[string]string{}}
	err := db.Where("node = ? and cluster = ? ", node, cluster).First(nodeConf).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return err
	}
	if err != nil && err == gorm.ErrRecordNotFound {
		nodeConf.Configuration = map[string]string{}
	}
	nodeConf.Configuration[key] = value
	return db.Save(nodeConf).Error
}

func (e *NodeConfig) One(node, cluster string) error {
	selector := map[string]string{
		"node":    node,
		"cluster": cluster,
	}
	return mysql.GetDB().Where(selector).Last(e).Error
}

type NodeConfigFailure struct {
	Id           int64     `gorm:"primaryKey" json:"id"`
	Node         string    `gorm:"type:varchar(255);not null;default:''" json:"node"`
	Cluster      string    `gorm:"type:varchar(255);not null;default:''" json:"cluster"`
	Key          string    `gorm:"type:varchar(255);not null;default:''" json:"key"`
	Value        string    `gorm:"type:varchar(255);not null;default:''" json:"value"`
	FailedReason string    `gorm:"type:varchar(255);not null;default:''"  json:"failed_reason"`
	CreatedAt    time.Time `json:"created_at"`
}

func (e *NodeConfigFailure) Insert() error {
	return mysql.GetDB().Create(e).Error
}

func (e *NodeConfigFailure) One(node, cluster string) error {
	selector := map[string]string{
		"node":    node,
		"cluster": cluster,
	}
	return mysql.GetDB().Where(selector).Last(e).Error
}
