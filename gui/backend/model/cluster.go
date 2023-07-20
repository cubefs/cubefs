package model

import (
	"errors"
	"time"

	"github.com/cubefs/cubefs/console/backend/helper/enums"
	"github.com/cubefs/cubefs/console/backend/helper/types"
	"github.com/cubefs/cubefs/console/backend/model/mysql"
)

type Cluster struct {
	Id         int64          `gorm:"primaryKey;auto_increment" json:"id"`
	Name       string         `gorm:"type:varchar(100);not null;default:'';index" json:"name"`
	MasterAddr types.StrSlice `gorm:"type:varchar(1024);not null;default:'[]'" json:"master_addr"`
	IDC        string         `gorm:"column:idc;type:varchar(255);not null;default:''" json:"idc"`
	Cli        string         `gorm:"type:varchar(255);not null;default:''" json:"cli"`
	Domain     string         `gorm:"type:varchar(255);not null;default:''" json:"domain"`
	ConsulAddr string         `gorm:"type:varchar(255);not null;default:''" json:"consul_addr"`
	Tag        string         `gorm:"type:varchar(255);not null;default:'';index" json:"tag"`
	S3Endpoint string         `gorm:"column:s3_endpoint;type:varchar(255);not null;default:''" json:"s3_endpoint"`
	VolType    enums.VolType  `gorm:"type:tinyint(1);not null;default:0" json:"vol_type"`
	CreateTime time.Time      `gorm:"create_time" json:"create_time"`
	UpdateTime time.Time      `gorm:"update_time" json:"update_time"`
}

func (c *Cluster) Create() error {
	c.CreateTime = time.Now()
	c.UpdateTime = time.Now()
	return mysql.GetDB().Create(c).Error
}

func (c *Cluster) Update(id int64, set map[string]interface{}) error {
	if id == 0 {
		return errors.New("id is required")
	}
	if set == nil {
		return nil
	}
	return mysql.GetDB().Model(&Cluster{}).Where("id = ?", id).Updates(set).Error
}

func (c *Cluster) FindId(id int64) error {
	if id == 0 {
		return errors.New("id is required")
	}
	return mysql.GetDB().Where("id = ?", id).First(c).Error
}

// FindName find cluster by name. Does this need to be cached ?
func (c *Cluster) FindName(name string) (*Cluster, error) {
	if name == "" {
		return nil, errors.New("name is required")
	}
	clusters := &Cluster{}
	err := mysql.GetDB().Where("name = ?", name).First(clusters).Error
	return clusters, err
}

func (c *Cluster) FindTag(tag string) (*Cluster, error)  {
	if tag == "" {
		return nil, errors.New("tag is required")
	}
	clusters := &Cluster{}
	err := mysql.GetDB().Where("tag = ?", tag).First(clusters).Error
	return clusters, err
}

type FindClusterParam struct {
	Page    int    `form:"page"`
	PerPage int    `form:"per_page"`
	Name    string `form:"name"`
	VolType *int   `form:"vol_type"`
}

func (c *Cluster) Find(param FindClusterParam) ([]Cluster, int64, error) {
	db := mysql.GetDB().Model(&Cluster{})
	if param.Name != "" {
		db = db.Where("name = ?", param.Name)
	}
	if param.VolType != nil {
		db = db.Where("vol_type = ?", param.VolType)
	}
	var count int64
	if err := db.Count(&count).Error; err != nil {
		return nil, 0, err
	}
	clusters := make([]Cluster, 0)
	err := db.Scopes(mysql.Paginate(param.PerPage, param.Page)).Find(&clusters).Error
	return clusters, count, err
}

func (c *Cluster) FindAll(name string) ([]Cluster, error) {
	db := mysql.GetDB().Model(&Cluster{})
	if name != "" {
		db = db.Where("name = ?", name)
	}
	clusters := make([]Cluster, 0)
	err := db.Find(&clusters).Error
	return clusters, err
}
