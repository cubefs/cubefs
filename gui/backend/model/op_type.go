package model

import (
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/cubefs/cubefs/console/backend/config"
	"github.com/cubefs/cubefs/console/backend/model/mysql"
)

type OpType struct {
	Id         int       `gorm:"primaryKey" json:"id"`
	NameEN     string    `gorm:"type:varchar(100);not null;default:''" json:"name_en"`
	NameCN     string    `gorm:"type:varchar(100);not null;default:''" json:"name_cn"`
	URI        string    `gorm:"type:varchar(200);uniqueIndex:idx_uri_method;not null;default:''" json:"uri"`
	Method     string    `gorm:"type:varchar(10);uniqueIndex:idx_uri_method;not null;default:''" json:"method"`
	Record     bool      `gorm:"not null;default:0" json:"record"`
	CreateTime time.Time `gorm:"not null;default:CURRENT_TIMESTAMP(3)" json:"create_time"`
	UpdateTime time.Time `gorm:"not null;default:CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)" json:"update_time"`
}

func (o *OpType) Create() error {
	return mysql.GetDB().Create(o).Error
}

type OpTypeUpdateParam struct {
	Id     int    `gorm:"-" json:"id" binding:"required"`
	NameEN string `json:"name_en"`
	NameCN string `json:"name_cn"`
	Record *bool  `json:"record"`
}

func (o *OpType) UpdateId(param *OpTypeUpdateParam) error {
	if param == nil {
		return nil
	}
	if param.Id == 0 {
		return errors.New("id is required")
	}
	set := map[string]interface{}{}
	if param.Record != nil {
		set["record"] = *param.Record
	}
	if param.NameEN != "" {
		set["name_en"] = param.NameEN
	}
	if param.NameCN != "" {
		set["name_cn"] = param.NameCN
	}
	if len(set) == 0 {
		return nil
	}
	return mysql.GetDB().Model(&OpType{}).Where("id = ?", param.Id).Updates(set).Error
}

func (o *OpType) Find(record *bool) ([]OpType, error) {
	optypes := make([]OpType, 0)
	db := mysql.GetDB().Model(&OpType{})
	if record != nil {
		db = db.Where("record = ?", *record)
	}
	err := db.Find(&optypes).Error
	return optypes, err
}

func (o *OpType) FindByUniqKey(uri, method string) (*OpType, error)  {
	if uri == "" || method == "" {
		return nil, errors.New("uri and method are required")
	}
	optype := new(OpType)
	db := mysql.GetDB().Model(&OpType{})
	err := db.Where("uri = ? and method = ?", uri, method).Find(optype).Error
	return optype, err
}

func InitOpTypeData(db *gorm.DB) error {
	optypes := getDefaultOpTypes()
	return db.CreateInBatches(optypes, len(optypes)).Error
}

func getDefaultOpTypes() []OpType {
	prefix := config.Conf.Prefix.Api + "/console"
	optypes := []OpType{
		// clusters
		{Method: "POST", URI: prefix + "/clusters/create", NameEN: "create cluster", NameCN: "创建集群", Record: true},
		{Method: "PUT", URI: prefix + "/clusters/update", NameEN: "update cluster", NameCN: "更新集群", Record: true},
		{Method: "GET", URI: prefix + "/clusters/list", NameEN: "list cluster", NameCN: "获取集群列表"},

		// blobstore.clusters
		{Method: "GET", URI: prefix + "/blobstore/:cluster/clusters/list", NameEN: "list clusters", NameCN: "获取cluster列表", Record: false},

		// blobstore
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/leadership/transfer", NameEN: "change leader", NameCN: "切主", Record: true},
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/member/remove", NameEN: "remove member", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/stat", NameEN: "get cluster stat info", NameCN: ""},

		// blobstore.nodes
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/nodes/access", NameEN: "access node", NameCN: ""},
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/nodes/drop", NameEN: "drop node", NameCN: "", Record: true},
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/nodes/offline", NameEN: "offline node", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/nodes/list", NameEN: "list node", NameCN: ""},
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/nodes/config/reload", NameEN: "reload node configuration", NameCN: ""},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/nodes/config/info", NameEN: "get node configuration", NameCN: ""},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/nodes/config/failures", NameEN: "get failed configuration", NameCN: ""},

		// blobstore.volumes
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/volumes/list", NameEN: "list volumes", NameCN: ""},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/volumes/writing/list", NameEN: "list writing volumes", NameCN: ""},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/volumes/v2/list", NameEN: "list volumes through v2", NameCN: ""},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/volumes/allocated/list", NameEN: "list allocated volumes", NameCN: ""},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/volumes/get", NameEN: "get volumes", NameCN: ""},

		// blobstore.disks
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/disks/access", NameEN: "access disk", NameCN: ""},
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/disks/set", NameEN: "set disk", NameCN: "", Record: true},
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/disks/drop", NameEN: "disk drop", NameCN: "", Record: true},
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/disks/probe", NameEN: "probe disk", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/disks/list", NameEN: "list disk", NameCN: ""},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/disks/info", NameEN: "get disk info", NameCN: ""},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/disks/dropping/list", NameEN: "get disk dropping list", NameCN: ""},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/disks/stats/migrating", NameEN: "get disk migrating status", NameCN: ""},

		// blobstore.config
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/config/set", NameEN: "set config", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/config/list", NameEN: "list config", NameCN: ""},

		// blobstore.services
		{Method: "POST", URI: prefix + "/blobstore/:cluster/:id/services/offline", NameEN: "offline service", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/services/list", NameEN: "list services", NameCN: ""},
		{Method: "GET", URI: prefix + "/blobstore/:cluster/:id/services/get", NameEN: "get service", NameCN: ""},

		// cfs.users
		{Method: "POST", URI: prefix + "/cfs/:cluster/users/create", NameEN: "create user", NameCN: "", Record: true},
		{Method: "PUT", URI: prefix + " /cfs/:cluster/users/policies", NameEN: "update user policies", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/cfs/:cluster/users/list", NameEN: "list users", NameCN: ""},
		{Method: "GET", URI: prefix + "/cfs/:cluster/users/names", NameEN: "get users' names", NameCN: ""},

		// cfs.vols
		{Method: "POST", URI: prefix + "/cfs/:cluster/vols/create", NameEN: "create vol", NameCN: "", Record: true},
		{Method: "PUT", URI: prefix + "/cfs/:cluster/vols/update", NameEN: "update vol", NameCN: "", Record: true},
		{Method: "PUT", URI: prefix + "/cfs/:cluster/vols/expand", NameEN: "expand vol", NameCN: "", Record: true},
		{Method: "PUT", URI: prefix + "/cfs/:cluster/vols/shrink", NameEN: "shrink vol", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/cfs/:cluster/vols/list", NameEN: "list vols", NameCN: ""},
		{Method: "GET", URI: prefix + "/cfs/:cluster/vols/info", NameEN: "get vol info", NameCN: ""},

		// cfs.domains
		{Method: "GET", URI: prefix + "/cfs/:cluster/domains/status", NameEN: "get domain status", NameCN: ""},
		{Method: "GET", URI: prefix + "/cfs/:cluster/domains/info", NameEN: "get domain info", NameCN: ""},

		// cfs.dataNode
		{Method: "POST", URI: prefix + "/cfs/:cluster/dataNode/add", NameEN: "add dataNode", NameCN: "", Record: true},
		{Method: "POST", URI: prefix + "/cfs/:cluster/dataNode/decommission", NameEN: "decommission dataNode", NameCN: "", Record: true},
		{Method: "POST", URI: prefix + "/cfs/:cluster/dataNode/migrate", NameEN: "migrate dataNode", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/cfs/:cluster/dataNode/list", NameEN: "list dataNode", NameCN: ""},
		{Method: "GET", URI: prefix + "/cfs/:cluster/dataNode/partitions", NameEN: "get dataNode partitions", NameCN: ""},

		// cfs.metaNode
		{Method: "POST", URI: prefix + "/cfs/:cluster/metaNode/add", NameEN: "add metaNode", NameCN: "", Record: true},
		{Method: "POST", URI: prefix + "/cfs/:cluster/metaNode/decommission", NameEN: "decommission metaNode", NameCN: "", Record: true},
		{Method: "POST", URI: prefix + "/cfs/:cluster/metaNode/migrate", NameEN: "migrate metaNode", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/cfs/:cluster/metaNode/list", NameEN: "list metaNode", NameCN: ""},
		{Method: "GET", URI: prefix + "/cfs/:cluster/metaNode/partitions", NameEN: "get metaNode partitions", NameCN: ""},

		// cfs.dataPartition
		{Method: "POST", URI: prefix + "/cfs/:cluster/dataPartition/create", NameEN: "create dataPartition", NameCN: "", Record: true},
		{Method: "POST", URI: prefix + "/cfs/:cluster/dataPartition/decommission", NameEN: "decommission dataPartition", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/cfs/:cluster/dataPartition/load", NameEN: "load dataPartition", NameCN: ""},
		{Method: "GET", URI: prefix + "/cfs/:cluster/dataPartition/list", NameEN: "list dataPartition", NameCN: ""},
		{Method: "GET", URI: prefix + "/cfs/:cluster/dataPartition/diagnosis", NameEN: "get dataPartition diagnosis", NameCN: ""},

		// cfs.metaPartition
		{Method: "POST", URI: prefix + "/cfs/:cluster/metaPartition/create", NameEN: "create metaPartition", NameCN: "", Record: true},
		{Method: "POST", URI: prefix + "/cfs/:cluster/metaPartition/decommission", NameEN: "decommission metaPartition", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/cfs/:cluster/metaPartition/load", NameEN: "load metaPartition", NameCN: ""},
		{Method: "GET", URI: prefix + "/cfs/:cluster/metaPartition/list", NameEN: "list metaPartition", NameCN: ""},
		{Method: "GET", URI: prefix + "/cfs/:cluster/metaPartition/diagnosis", NameEN: "get metaPartition diagnosis", NameCN: ""},

		// cfs.disks
		{Method: "POST", URI: prefix + "/cfs/:cluster/disks/decommission", NameEN: "decommission disk", NameCN: "", Record: true},
		{Method: "GET", URI: prefix + "/cfs/:cluster/disks/list", NameEN: "list disk", NameCN: ""},

		// auth
		{Method: "POST", URI: prefix + "/auth/login", NameEN: "user login", NameCN: "登录"},
		{Method: "POST", URI: prefix + "/auth/logout", NameEN: "user logout", NameCN: "登出"},

		// auth.user
		{Method: "POST", URI: prefix + "/auth/user/create", NameEN: "create user", NameCN: "创建用户", Record: false},
		{Method: "PUT", URI: prefix + "/auth/user/update", NameEN: "update user", NameCN: "更新用户", Record: true},
		{Method: "DELETE", URI: prefix + "/auth/user/delete", NameEN: "delete user", NameCN: "删除用户", Record: true},
		{Method: "GET", URI: prefix + "/auth/user/list", NameEN: "list user", NameCN: "获取用户列表"},
		{Method: "PUT", URI: prefix + "/auth/user/password/update", NameEN: "change user password", NameCN: "更新用户密码"},

		// auth.role
		{Method: "POST", URI: prefix + "/auth/role/create", NameEN: "create role", NameCN: "创建角色", Record: true},
		{Method: "PUT", URI: prefix + "/auth/role/update", NameEN: "update role", NameCN: "更新角色", Record: true},
		{Method: "DELETE", URI: prefix + "/auth/role/delete", NameEN: "delete role", NameCN: "删除角色", Record: true},
		{Method: "GET", URI: prefix + "/auth/role/list", NameEN: "list role", NameCN: "获取角色列表"},

		// auth.permission
		{Method: "POST", URI: prefix + "/auth/permission/create", NameEN: "create permission", NameCN: "创建权限", Record: true},
		{Method: "PUT", URI: prefix + "/auth/permission/update", NameEN: "update permission", NameCN: "更新权限", Record: true},
		{Method: "DELETE", URI: prefix + "/auth/permission/delete", NameEN: "delete permission", NameCN: "删除权限", Record: true},
		{Method: "GET", URI: prefix + "/auth/permission/list", NameEN: "list permission", NameCN: "获取权限列表"},

		// op_types
		{Method: "POST", URI: prefix + "/optypes/create", NameEN: "create operation types", NameCN: "创建日志类型", Record: true},
		{Method: "PUT", URI: prefix + "/optypes/update", NameEN: "update operation types", NameCN: "更新日志类型", Record: true},
		{Method: "GET", URI: prefix + "/optypes/list", NameEN: "list operation types", NameCN: "获取日志类型列表", Record: false},

		// op_logs
		{Method: "GET", URI: prefix + "/oplogs/list", NameEN: "list operation logs", NameCN: "获取日志列表"},
	}
	return optypes
}
