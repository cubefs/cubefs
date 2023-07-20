package auth

import (
	"github.com/cubefs/cubefs/console/backend/model"
)

var (
	DefaultRole = []model.AuthRole{
		{RoleCode: "Root", RoleName: "root_user", MaxVolCount: -1, MaxVolSize: -1},
		{RoleCode: "Ops", RoleName: "ops_user", MaxVolCount: -1, MaxVolSize: -1},
		{RoleCode: "Normal", RoleName: "normal_user", MaxVolCount: 1, MaxVolSize: 2},
	}
	DefaultRolePermission = map[string][]string{
		"Root": []string{},
		"Ops": []string{
			"CLUSTER_CREATE", "CLUSTER_UPDATE", "CLUSTER_LIST",
			"BLOBSTORE_CLUSTER_LIST",
			"BLOBSTORE_STAT", "BLOBSTORE_LEADERSHIP", "BLOBSTORE_MEMBER_REMOVE",
			"BLOBSTORE_NODES_LIST", "BLOBSTORE_NODES_ACCESS", "BLOBSTORE_NODES_DROP", "BLOBSTORE_NODES_OFFLINE", "BLOBSTORE_NODES_CONFIG_RELOAD", "BLOBSTORE_NODES_CONFIG_INFO", "BLOBSTORE_NODES_CONFIG_FAIL",
			"BLOBSTORE_VOLUMES_LIST", "BLOBSTORE_VOLUMES_WRITING_LIST", "BLOBSTORE_VOLUMES_V2_LIST", "BLOBSTORE_VOLUMES_ALLOCATED_LIST", "BLOBSTORE_VOLUMES_GET",
			"BLOBSTORE_DISKS_LIST", "BLOBSTORE_DISKS_INFO", "BLOBSTORE_DISKS_DROPPING_LIST", "BLOBSTORE_DISKS_ACCESS", "BLOBSTORE_DISKS_SET", "BLOBSTORE_DISKS_DROP", "BLOBSTORE_DISKS_PROBE", "BLOBSTORE_DISKS_STATS_MIGRATING",
			"BLOBSTORE_CONFIG_LIST", "BLOBSTORE_CONFIG_SET",
			"BLOBSTORE_SERVICES_LIST", "BLOBSTORE_SERVICES_GET", "BLOBSTORE_SERVICES_OFFLINE",
			"CFS_USERS_CREATE", "CFS_USERS_LIST", "CFS_USERS_NAMES", "CFS_USERS_POLICIES",
			"CFS_VOLS_CREATE", "CFS_VOLS_LIST", "CFS_VOLS_INFO", "CFS_VOLS_UPDATE", "CFS_VOLS_EXPAND", "CFS_VOLS_SHRINK",
			"CFS_DOMAINS_STATUS", "CFS_DOMAINS_INFO",
			"CFS_DATANODE_ADD", "CFS_DATANODE_LIST", "CFS_DATANODE_PARTITIONS", "CFS_DATANODE_DECOMMISSION", "CFS_DATANODE_MIGRATE",
			"CFS_METANODE_ADD", "CFS_METANODE_LIST", "CFS_METANODE_PARTITIONS", "CFS_METANODE_DECOMMISSION", "CFS_METANODE_MIGRATE",
			"CFS_DATAPARTITION_CREATE", "CFS_DATAPARTITION_LOAD", "CFS_DATAPARTITION_LIST", "CFS_DATAPARTITION_DECOMMISSION", "CFS_DATAPARTITION_DIAGNOSIS",
			"CFS_METAPARTITION_CREATE", "CFS_METAPARTITION_LOAD", "CFS_METAPARTITION_LIST", "CFS_METAPARTITION_DECOMMISSION", "CFS_METAPARTITION_DIAGNOSIS",
			"CFS_DISKS_LIST", "CFS_DISKS_DECOMMISSION",
			"CFS_S3_FILES_LIST", "CFS_S3_FILES_DOWNLOAD_SIGNEDURL", "CFS_S3_FILES_UPLOAD_SIGNEDURL", "CFS_S3_FILES_UPLOAD_MULTIPART_SIGNEDURL", "CFS_S3_FILES_UPLOAD_MULTIPART_COMPLETE", "CFS_S3_DIRS_CREATE",
			"OPTYPES_CREATE", "OPTYPES_UPDATE", "OPTYPES_LIST", "OPLOGS_LIST",
		},
		"Normal": []string{
			"CLUSTER_LIST",
			"BLOBSTORE_CLUSTER_LIST", "BLOBSTORE_STAT",
			"BLOBSTORE_NODES_LIST", "BLOBSTORE_NODES_ACCESS", "BLOBSTORE_NODES_CONFIG_INFO", "BLOBSTORE_NODES_CONFIG_FAIL",
			"BLOBSTORE_VOLUMES_LIST", "BLOBSTORE_VOLUMES_WRITING_LIST", "BLOBSTORE_VOLUMES_V2_LIST", "BLOBSTORE_VOLUMES_ALLOCATED_LIST", "BLOBSTORE_VOLUMES_GET",
			"BLOBSTORE_DISKS_LIST", "BLOBSTORE_DISKS_INFO", "BLOBSTORE_DISKS_DROPPING_LIST", "BLOBSTORE_DISKS_ACCESS", "BLOBSTORE_DISKS_SET", "BLOBSTORE_DISKS_PROBE",
			"BLOBSTORE_CONFIG_LIST", "BLOBSTORE_CONFIG_SET",
			"BLOBSTORE_SERVICES_LIST", "BLOBSTORE_SERVICES_GET",
			"CFS_USERS_LIST", "CFS_USERS_NAMES", "CFS_USERS_POLICIES",
			"CFS_VOLS_CREATE", "CFS_VOLS_LIST", "CFS_VOLS_INFO", "CFS_VOLS_UPDATE", "CFS_VOLS_EXPAND", "CFS_VOLS_SHRINK",
			"CFS_DOMAINS_STATUS", "CFS_DOMAINS_INFO",
			"CFS_DATANODE_LIST", "CFS_DATANODE_PARTITIONS",
			"CFS_METANODE_LIST", "CFS_METANODE_PARTITIONS",
			"CFS_DATAPARTITION_LIST", "CFS_DATAPARTITION_DECOMMISSION", "CFS_DATAPARTITION_DIAGNOSIS",
			"CFS_METAPARTITION_LIST", "CFS_METAPARTITION_DIAGNOSIS",
			"CFS_DISKS_LIST",
			"CFS_S3_FILES_LIST", "CFS_S3_FILES_DOWNLOAD_SIGNEDURL", "CFS_S3_FILES_UPLOAD_SIGNEDURL", "CFS_S3_FILES_UPLOAD_MULTIPART_SIGNEDURL", "CFS_S3_FILES_UPLOAD_MULTIPART_COMPLETE", "CFS_S3_DIRS_CREATE",
		},
	}
	DefaultRoot = model.AuthUser{
		UserName: "admin",
		Email:    "admin@cubefs.com",
		Phone:    "12345678900",
		Password: "Vcjjk8PdWcsJYvz7S7O0dg==",
	}
	backend           = 0
	front             = 1
	prefix            = "/api/cubefs/console"
	DefaultPermission = []model.AuthPermission{
		// clusters
		{AuthCode: "CLUSTER_CREATE", AuthName: "create cluster", AuthType: &backend, URI: prefix + "/clusters/crete", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CLUSTER_UPDATE", AuthName: "update cluster", AuthType: &backend, URI: prefix + "/clusters/update", Method: "PUT", IsLogin: true, IsCheck: true},
		{AuthCode: "CLUSTER_LIST", AuthName: "list cluster", AuthType: &backend, URI: prefix + "/clusters/list", Method: "GET", IsLogin: true, IsCheck: true},

		// blobstore.clusters
		{AuthCode: "BLOBSTORE_CLUSTER_LIST", AuthName: "list blobstore cluster", AuthType: &backend, URI: prefix + "/blobstore/:clusters/cluster/list", Method: "GET", IsLogin: true, IsCheck: true},

		// blobstore
		{AuthCode: "BLOBSTORE_LEADERSHIP", AuthName: "change leader", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/leadership/transfer", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_MEMBER_REMOVE", AuthName: "remove member", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/member/remove", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_STAT", AuthName: "get cluster stat info", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/stat", Method: "GET", IsLogin: true, IsCheck: true},

		// blobstore.nodes
		{AuthCode: "BLOBSTORE_NODES_ACCESS", AuthName: "access node", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/nodes/access", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_NODES_DROP", AuthName: "drop node", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/nodes/drop", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_NODES_OFFLINE", AuthName: "offline node", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/nodes/offline", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_NODES_LIST", AuthName: "list node", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/nodes/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_NODES_CONFIG_RELOAD", AuthName: "reload node configuration", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/nodes/config/reload", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_NODES_CONFIG_INFO", AuthName: "get node configuration", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/nodes/config/info", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_NODES_CONFIG_FAIL", AuthName: "get failed configuration", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/nodes/config/failures", Method: "GET", IsLogin: true, IsCheck: true},

		// blobstore.volumes
		{AuthCode: "BLOBSTORE_VOLUMES_LIST", AuthName: "list volumes", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/volumes/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_VOLUMES_WRITING_LIST", AuthName: "list writing volumes", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/volumes/writing/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_VOLUMES_V2_LIST", AuthName: "list volumes through v2", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/volumes/v2/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_VOLUMES_ALLOCATED_LIST", AuthName: "list allcoated volumes", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/volumes/allocated/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_VOLUMES_GET", AuthName: "get volumes", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/volumes/get", Method: "GET", IsLogin: true, IsCheck: true},

		// blobstore.disks
		{AuthCode: "BLOBSTORE_DISKS_ACCESS", AuthName: "access disk", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/disks/access", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_DISKS_SET", AuthName: "set disk", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/disks/set", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_DISKS_DROP", AuthName: "drop disk", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/disks/drop", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_DISKS_PROBE", AuthName: "probe disk", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/disks/probe", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_DISKS_LIST", AuthName: "list disk", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/disks/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_DISKS_INFO", AuthName: "get disk info", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/disks/info", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_DISKS_DROPPING_LIST", AuthName: "get disk dropping list", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/disks/dropping/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_DISKS_STATS_MIGRATING", AuthName: "get disk migrating status", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/disks/stats/migrating", Method: "GET", IsLogin: true, IsCheck: true},

		// blobstore.config
		{AuthCode: "BLOBSTORE_CONFIG_SET", AuthName: "set config", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/config/set", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_CONFIG_LIST", AuthName: "list config", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/config/list", Method: "GET", IsLogin: true, IsCheck: true},

		// blobstore.services
		{AuthCode: "BLOBSTORE_SERVICES_OFFLINE", AuthName: "offline service", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/services/offline", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_SERVICES_LIST", AuthName: "list service", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/services/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "BLOBSTORE_SERVICES_GET", AuthName: "get service", AuthType: &backend, URI: prefix + "/blobstore/:clusters/:id/services/get", Method: "GET", IsLogin: true, IsCheck: true},

		// cfs.users
		{AuthCode: "CFS_USERS_CREATE", AuthName: "create cfs user", AuthType: &backend, URI: prefix + "/cfs/:cluster/users/create", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_USERS_POLICIES", AuthName: "update user policies", AuthType: &backend, URI: prefix + "/cfs/:cluster/users/policies", Method: "PUT", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_USERS_LIST", AuthName: "list cfs user", AuthType: &backend, URI: prefix + "/cfs/:cluster/users/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_USERS_NAMES", AuthName: "get user names", AuthType: &backend, URI: prefix + "/cfs/:cluster/users/names", Method: "GET", IsLogin: true, IsCheck: true},

		// cfs.vols
		{AuthCode: "CFS_VOLS_CREATE", AuthName: "create cfs vol", AuthType: &backend, URI: prefix + "/cfs/:cluster/vols/create", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_VOLS_UPDATE", AuthName: "update cfs vol", AuthType: &backend, URI: prefix + "/cfs/:cluster/vols/update", Method: "PUT", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_VOLS_EXPAND", AuthName: "expand cfs vol", AuthType: &backend, URI: prefix + "/cfs/:cluster/vols/expand", Method: "PUT", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_VOLS_SHRINK", AuthName: "shrink cfs vol", AuthType: &backend, URI: prefix + "/cfs/:cluster/vols/shrink", Method: "PUT", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_VOLS_LIST", AuthName: "list cfs vol", AuthType: &backend, URI: prefix + "/cfs/:cluster/vols/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_VOLS_INFO", AuthName: "get cfs vol info", AuthType: &backend, URI: prefix + "/cfs/:cluster/vols/info", Method: "GET", IsLogin: true, IsCheck: true},

		// cfs.domains
		{AuthCode: "CFS_DOMAINS_STATUS", AuthName: "get domain status", AuthType: &backend, URI: prefix + "/cfs/:cluster/domains/status", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_DOMAINS_INFO", AuthName: "get domain info", AuthType: &backend, URI: prefix + "/cfs/:cluster/domains/info", Method: "GET", IsLogin: true, IsCheck: true},

		// cfs.dataNode
		{AuthCode: "CFS_DATANODE_ADD", AuthName: "add dataNode", AuthType: &backend, URI: prefix + "/cfs/:cluster/dataNode/add", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_DATANODE_DECOMMISSION", AuthName: "decommission dataNode", AuthType: &backend, URI: prefix + "/cfs/:cluster/dataNode/decommission", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_DATANODE_MIGRATE", AuthName: "migrate dataNode", AuthType: &backend, URI: prefix + "/cfs/:cluster/dataNode/migrate", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_DATANODE_LIST", AuthName: "list dataNode", AuthType: &backend, URI: prefix + "/cfs/:cluster/dataNode/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_DATANODE_PARTITIONS", AuthName: "get dataNode partitions", AuthType: &backend, URI: prefix + "/cfs/:cluster/dataNode/partitions", Method: "GET", IsLogin: true, IsCheck: true},

		// cfs.metaNode
		{AuthCode: "CFS_METANODE_ADD", AuthName: "add metaNode", AuthType: &backend, URI: prefix + "/cfs/:cluster/metaNode/add", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_METANODE_DECOMMISSION", AuthName: "decommission metaNode", AuthType: &backend, URI: prefix + "/cfs/:cluster/metaNode/decommission", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_METANODE_MIGRATE", AuthName: "migrate metaNode", AuthType: &backend, URI: prefix + "/cfs/:cluster/metaNode/migrate", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_METANODE_LIST", AuthName: "list metaNode", AuthType: &backend, URI: prefix + "/cfs/:cluster/metaNode/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_METANODE_PARTITIONS", AuthName: "get metaNode partitions", AuthType: &backend, URI: prefix + "/cfs/:cluster/metaNode/partitions", Method: "GET", IsLogin: true, IsCheck: true},

		// cfs.dataPartition
		{AuthCode: "CFS_DATAPARTITION_CREATE", AuthName: "create dataPartition", AuthType: &backend, URI: prefix + "/cfs/:cluster/dataPartition/create", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_DATAPARTITION_DECOMMISSION", AuthName: "decommission dataPartition", AuthType: &backend, URI: prefix + "/cfs/:cluster/dataPartition/decommission", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_DATAPARTITION_LOAD", AuthName: "load dataPartition", AuthType: &backend, URI: prefix + "/cfs/:cluster/dataPartition/load", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_DATAPARTITION_LIST", AuthName: "list dataPartition", AuthType: &backend, URI: prefix + "/cfs/:cluster/dataPartition/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_DATAPARTITION_DIAGNOSIS", AuthName: "get dataPartition diagnosis", AuthType: &backend, URI: prefix + "/cfs/:cluster/dataPartition/diagnosis", Method: "GET", IsLogin: true, IsCheck: true},

		// cfs.metaPartition
		{AuthCode: "CFS_METAPARTITION_CREATE", AuthName: "create metaPartition", AuthType: &backend, URI: prefix + "/cfs/:cluster/metaPartition/create", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_METAPARTITION_DECOMMISSION", AuthName: "decommission metaPartition", AuthType: &backend, URI: prefix + "/cfs/:cluster/metaPartition/decommission", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_METAPARTITION_LOAD", AuthName: "load metaPartition", AuthType: &backend, URI: prefix + "/cfs/:cluster/metaPartition/load", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_METAPARTITION_LIST", AuthName: "list metaPartition", AuthType: &backend, URI: prefix + "/cfs/:cluster/metaPartition/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_METAPARTITION_DIAGNOSIS", AuthName: "get metaPartition diagnosis", AuthType: &backend, URI: prefix + "/cfs/:cluster/metaPartition/diagnosis", Method: "GET", IsLogin: true, IsCheck: true},

		// cfs.disks
		{AuthCode: "CFS_DISKS_DECOMMISSION", AuthName: "decommission disk", AuthType: &backend, URI: prefix + "/cfs/:cluster/disks/decommission", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_DISKS_LIST", AuthName: "list disk", AuthType: &backend, URI: prefix + "/cfs/:cluster/disks/list", Method: "GET", IsLogin: true, IsCheck: true},

		// cfs.s3
		{AuthCode: "CFS_S3_FILES_LIST", AuthName: "list s3 files", AuthType: &backend, URI: prefix + "/cfs/:cluster/s3/files/list", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_S3_FILES_DOWNLOAD_SIGNEDURL", AuthName: "download s3 files signedUrl", AuthType: &backend, URI: prefix + "/cfs/:cluster/s3/files/download/signedUrl", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_S3_FILES_UPLOAD_SIGNEDURL", AuthName: "upload s3 files signedUrl", AuthType: &backend, URI: prefix + "/cfs/:cluster/s3/files/upload/signedUrl", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_S3_FILES_UPLOAD_MULTIPART_SIGNEDURL", AuthName: "upload s3 files multipart signedUrl", AuthType: &backend, URI: prefix + "/cfs/:cluster/s3/files/upload/multipart/signedUrl", Method: "GET", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_S3_FILES_UPLOAD_MULTIPART_COMPLETE", AuthName: "upload s3 files multipart complete", AuthType: &backend, URI: prefix + "/cfs/:cluster/s3/files/upload/multipart/complete", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "CFS_S3_DIRS_CREATE", AuthName: "create s3 dirs", AuthType: &backend, URI: prefix + "/cfs/:cluster/s3/dirs/create", Method: "POST", IsLogin: true, IsCheck: true},

		// auth
		{AuthCode: "AUTH_LOGIN", AuthName: "user login", AuthType: &backend, URI: prefix + "/auth/login", Method: "POST", IsLogin: false, IsCheck: false},
		{AuthCode: "AUTH_LOGOUT", AuthName: "user logout", AuthType: &backend, URI: prefix + "/auth/logout", Method: "POST", IsLogin: true, IsCheck: false},

		// auth user
		{AuthCode: "AUTH_USER_LIST", AuthName: "list user", AuthType: &backend, URI: prefix + "/auth/user/list", Method: "GET", IsLogin: true, IsCheck: false},
		{AuthCode: "AUTH_USER_CREATE", AuthName: "create user", AuthType: &backend, URI: prefix + "/auth/user/create", Method: "POST", IsLogin: false, IsCheck: false},
		{AuthCode: "AUTH_USER_UPDATE", AuthName: "update user", AuthType: &backend, URI: prefix + "/auth/user/update", Method: "PUT", IsLogin: true, IsCheck: true},
		{AuthCode: "AUTH_USER_SELF_UPDATE", AuthName: "update user(self)", AuthType: &backend, URI: prefix + "/auth/user/self/update", Method: "PUT", IsLogin: true, IsCheck: false},
		{AuthCode: "AUTH_USER_DELETE", AuthName: "delete user", AuthType: &backend, URI: prefix + "/auth/user/delete", Method: "DELETE", IsLogin: true, IsCheck: true},
		{AuthCode: "AUTH_USER_PASSWORD_UPDATE", AuthName: "update user(admin)", AuthType: &backend, URI: prefix + "/auth/user/password/update", Method: "PUT", IsLogin: true, IsCheck: true},
		{AuthCode: "AUTH_USER_SELF_PASSWORD_UPDATE", AuthName: "change user(no login)", AuthType: &backend, URI: prefix + "/auth/user/password/self/update", Method: "PUT", IsLogin: false, IsCheck: false},
		{AuthCode: "AUTH_USER_PERMISSION", AuthName: "list user permission", AuthType: &backend, URI: prefix + "/auth/user/permission", Method: "GET", IsLogin: true, IsCheck: false},

		// auth role
		{AuthCode: "AUTH_ROLE_LIST", AuthName: "list role", AuthType: &backend, URI: prefix + "/auth/role/list", Method: "GET", IsLogin: true, IsCheck: false},
		{AuthCode: "AUTH_ROLE_CREATE", AuthName: "create role", AuthType: &backend, URI: prefix + "/auth/role/create", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "AUTH_ROLE_UPDATE", AuthName: "update role", AuthType: &backend, URI: prefix + "/auth/role/update", Method: "PUT", IsLogin: true, IsCheck: true},
		{AuthCode: "AUTH_ROLE_DELETE", AuthName: "delete role", AuthType: &backend, URI: prefix + "/auth/role/delete", Method: "DELETE", IsLogin: true, IsCheck: true},

		// auth permission
		{AuthCode: "AUTH_PERMISSION_LIST", AuthName: "list permission", AuthType: &backend, URI: prefix + "/auth/permission/list", Method: "GET", IsLogin: true, IsCheck: false},
		{AuthCode: "AUTH_PERMISSION_CREATE", AuthName: "create permission", AuthType: &backend, URI: prefix + "/auth/permission/create", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "AUTH_PERMISSION_UPDATE", AuthName: "update permission", AuthType: &backend, URI: prefix + "/auth/permission/update", Method: "PUT", IsLogin: true, IsCheck: true},
		{AuthCode: "AUTH_PERMISSION_DELETE", AuthName: "delete permission", AuthType: &backend, URI: prefix + "/auth/permission/delete", Method: "DELETE", IsLogin: true, IsCheck: true},

		// op_types
		{AuthCode: "OPTYPES_CREATE", AuthName: "create operation types", AuthType: &backend, URI: prefix + "/optypes/create", Method: "POST", IsLogin: true, IsCheck: true},
		{AuthCode: "OPTYPES_UPDATE", AuthName: "update operation types", AuthType: &backend, URI: prefix + "/optypes/update", Method: "PUT", IsLogin: true, IsCheck: true},
		{AuthCode: "OPTYPES_LIST", AuthName: "list operation types", AuthType: &backend, URI: prefix + "/optypes/list", Method: "GET", IsLogin: true, IsCheck: true},

		// op_logs
		{AuthCode: "OPLOGS_LIST", AuthName: "list operation logs", AuthType: &backend, URI: prefix + "/oplogs/list", Method: "GET", IsLogin: true, IsCheck: true},
	}
)
