# BlobNode 配置

`BlobNode` 是单机存储引擎模块，主要负责组织数据落盘、磁盘读取数据、磁盘删除数据以及后台任务的执行等。

BlobNode的配置是基于[公有配置](./base.md)，以下配置说明主要针对于BlobNode的私有配置。

## 配置说明

### 关键配置

| 配置项              | 说明                                        | 必需  |
|:-----------------|:------------------------------------------|:----|
| 公有配置             | 如服务端口、运行日志以及审计日志等，参考[基础服务配置](./base.md)章节 | 是   |
| disks            | 要注册的磁盘路径列表                                | 是   |
| disable_sync     | 是否关闭磁盘sync，值为true表示关闭sync，可以提高写性能         | 否   |
| rack             | 所在机架编号，clustermgr打开机架隔离时需要此字段             | 否   |
| host             | 本机的blobnode服务地址，需要注册到clustermgr           | 是   |
| must_mount_point | 校验注册路径是否是挂载路径，生产环境建议打开                    | 否   |
| data_qos         | 数据qos分层限流，生产环境建议打开                        | 否   |
| meta_config      | 元数据相关配置，包含rocksdb的cache大小等                | 否   |
| clustermgr       | clustermgr的服务地址信息等                        | 是   |

### 全部配置
```json
{
	"cluster_id": "集群id",
	"idc": "机房",
	"rack": "机架",
	"host": "服务地址ip:端口",
	"dropped_bid_record": {
		"dir": "下线的bid记录目录",
		"chunkbits": "n: 单个文件大小，2的n次方字节"
	},
	"disks": [
		{
			"auto_format": "是否自动创建目录",
			"disable_sync": "是否关闭磁盘sync",
			"path": "数据存放目录",
			"max_chunks": "单盘最大的chunk数量限制"
		},
		{
			"auto_format": "同上",
			"disable_sync": "同上",
			"path": "同上:每个磁盘都需要配置，会注册到clustermgr",
			"max_chunks": "同上"
		}
	],
	"disk_config": {
		"disk_reserved_space_B": "单盘保留大小,可写空间会减去该值,默认60GB",
		"compact_reserved_space_B": "留给压缩的单盘保留大小,默认20GB",
		"chunk_protection_M": "release chunk的保护周期,最后修改时间到此刻没有超过保护周期则不允许release",
		"chunk_compact_interval_S": "compact的定时任务周期",
		"chunk_clean_interval_S": "chunk清理的定时任务周期",
		"chunk_gc_create_time_protection_M": "清理chunk时判断创建时间的保护周期",
		"chunk_gc_modify_time_protection_M": "清理chunk时判断修改时间的保护周期",
		"disk_usage_interval_S": "更新磁盘空间使用情况的定时任务周期",
		"disk_clean_trash_interval_S": "磁盘清理垃圾数据的定时任务周期",
		"disk_trash_protection_M": "磁盘垃圾数据的保护周期",
		"allow_clean_trash": "是否允许清理垃圾",
		"disable_modify_in_compacting": "压缩中是否不允许修改chunk",
		"compact_min_size_threshold": "chunk大小满足压缩条件的最小值",
		"compact_trigger_threshold": "chunk size达到该值触发压缩",
		"compact_empty_rate_threshold": "chunk空洞率达到该值允许压缩",
		"need_compact_check": "压缩完成后,是否巡检压缩前后的blob,确保压缩前后数据一致",
		"allow_force_compact": "是否允许接口强制进行压缩,跳过压缩条件",
		"compact_batch_size": "执行压缩时每一批次的bid数量",
		"must_mount_point": "数据存放目录是否强制是挂载点",
		"metric_report_interval_S": "metric上报的定时任务周期",
		"data_qos": {
			"disk_bandwidth_MBPS": "单盘整体带宽阈值,带宽达到该值会调整每个level的限流带宽为bandwidth_MBPS*factor",
			"disk_iops": "单盘整体iops阈值,iops达到该值会调整每个level的限流iops为level配置的iops*factor",
			"flow_conf": {
				"level0": {
					"bandwidth_MBPS": "(level0是用户读写io)限流带宽MB/s",
					"iops": "限流IOPS",
					"factor": "限流因子"
				},
				"level1": {
					"bandwidth_MBPS": "(level1是shard repair io)限流带宽MB/s",
					"iops": "同上",
					"factor": "同上"
				},
				"level2": {
					"bandwidth_MBPS": "(level2是disk repair, delete, compact io)限流带宽MB/s",
					"iops": "同上",
					"factor": "同上"
				},
				"level3": {
					"bandwidth_MBPS": "(level3是balance, drop, manual migrate io)限流带宽MB/s",
					"iops": "同上",
					"factor": "同上"
				},
				"level4": {
					"bandwidth_MBPS": "(level4是inspect io)限流带宽MB/s",
					"iops": "同上",
					"factor": "同上"
				}
			}
		}
	},
	"meta_config": {
		"meta_root_prefix": "配置统一的meta数据存放目录,可以配置到ssd盘提高元数据读写速度,默认不配置",
		"batch_process_count": "元数据批量处理请求的个数,包含删除和写",
		"support_inline": "是否开启小文件内联写到元数据存rocksdb",
		"tinyfile_threshold_B": "小文件阈值,可以配置小于等于128k",
		"write_pri_ratio": "元数据批量处理写请求的比例,具体个数为batch_process_count*write_pri_ratio",
		"sync": "是否开启磁盘sync",
		"rocksdb_option": {
			"lrucache": "缓存大小",
			"write_buffer_size": "rocksdb写buffer大小"
		},
		"meta_qos": {
			"level0": {
				"iops": "level0是用户读写io的iops限制,元数据限流配置"
			},
			"level1": {
				"iops": "level1是shard repair io的iops限制"
			},
			"level2": {
				"iops": "level2是disk repair, delete, compact io的iops限制"
			},
			"level3": {
				"iops": "level3是balance, drop, manual migrate io的iops限制"
			},
			"level4": {
				"iops": "level4是inspect io的iops限制"
			}
		}
	},
	"clustermgr": {
		"hosts": "clustermgr服务地址"
	},
	"blobnode": {
		"client_timeout_ms": "后台任务用到的blobnode client的超时时间"
	},
	"scheduler": {
		"host_sync_interval_ms": "后台任务用到的scheduler client的后端节点同步时间"
	},
	"chunk_protection_period_S": "过期epoch chunk判断创建时间的保护周期",
	"put_qps_limit_per_disk": "单盘写并发数控制",
	"get_qps_limit_per_disk": "单盘读并发数控制",
	"get_qps_limit_per_key": "单个shard的读并发数控制",
	"delete_qps_limit_per_disk": "单盘删除的并发数控制",
	"shard_repair_concurrency": "后台任务shard repair的并发数控制",
	"flock_filename": "进程文件锁路径"
}
```

### 示例配置
```json
{    
    "bind_addr": ":8889",    
    "log": {		
        "level": 2    
    },    
    "cluster_id": 10001,    
    "idc": "bjht",    
    "rack": "HT02-B11-F4-402-0406",    
    "host": "http://10.39.34.185:8889",                                             
    "dropped_bid_record": {       
        "dir": "/home/service/ebs-blobnode/_package/dropped_bids/",       
        "chunkbits": 29     
    },    
    "disks": [      
        {"auto_format": true,"disable_sync": true,"path": "/home/service/var/data1"},
        {"auto_format": true,"disable_sync": true,"path": "/home/service/var/data2"}
    ],    
    "disk_config": {        
        "chunk_clean_interval_S": 60,        
        "chunk_protection_M": 30,        
        "disk_clean_trash_interval_S": 60,        
        "disk_trash_protection_M": 1440,        
        "metric_report_interval_S": 300,        
        "need_compact_check": true,        
        "allow_force_compact": true,        
        "allow_clean_trash": true,        
        "must_mount_point": true,        
        "data_qos": {            
            "disk_bandwidth_MBPS": 200,            
            "disk_iops": 8000,            
            "flow_conf": {                                                                                             
                "level0": { 					
                    "bandwidth_MBPS": 200, 
                    "iops": 4000, 
                    "factor": 1
                },                
                "level1": {							
                    "bandwidth_MBPS": 40, 
                    "iops": 2000, 
                    "factor": 0.5
                },                
                "level2": {							
                    "bandwidth_MBPS": 40, 
                    "iops": 2000, 
                    "factor": 0.5
                },                
                "level3": {							
                    "bandwidth_MBPS": 40, 
                    "iops": 2000, 
                    "factor": 0.5
                },                
                "level4": {							
                    "bandwidth_MBPS": 20, 
                    "iops": 1000, 
                    "factor": 0.5
                }            
            }        
        }    
    },    
    "meta_config": {        
        "sync": false,        
        "rocksdb_option": {            
            "lrucache": 268435456,            
            "write_buffer_size": 52428800        
        },        
        "meta_qos": {             
            "level0": { 
                "iops": 8000 
            },             
            "level1": { 
                "iops": 8000 
            },             
            "level2": { 
                "iops": 8000 
            },             
            "level3": { 
                "iops": 8000 
            },             
            "level4": { 
                "iops": 8000 
            }        
        }    
    },    
    "clustermgr": {        
        "hosts": [      
            "http://10.39.30.78:9998",      
            "http://10.39.32.224:9998",      
            "http://10.39.32.234:9998"
        ],        
        "transport_config": {         	
            "max_conns_per_host": 4,            
            "auth": {                
                "enable_auth": false,                
                "secret": "b2e5e2ed-6fca-47ce-bfbc-5e8f0650603b"            
            }        
        }    
    },    
    "blobnode": {    	
        "client_timeout_ms": 5000    
    },    
    "scheduler": {    	
        "host_sync_interval_ms": 3600000    
    },    
    "chunk_protection_period_S": 600,     
    "put_qps_limit_per_disk": 1024,    
    "get_qps_limit_per_disk": 1024,    
    "get_qps_limit_per_key": 1024,    
    "delete_qps_limit_per_disk": 64,      
    "shard_repair_concurrency": 100,    
    "flock_filename": "/home/service/ebs-blobnode/_package/run/blobnode.0.flock",    
    "auditlog": {        
        "logdir": "/home/service/ebs-blobnode/_package/run/auditlog/ebs-blobnode",        
        "chunkbits": 29,        
        "log_file_suffix": ".log",        
        "backup": 10,        
        "keywords_filter": [
            "list",
            "metrics",
            "/shard/get/"
        ],        
        "metric_config": {            
            "idc": "bjht",            
            "service": "BLOBNODE",            
            "team": "ocs",            
            "enable_http_method": true,            
            "enable_req_length_cnt": true,            
            "enable_resp_length_cnt": true,            
            "enable_resp_duration": true,            
            "max_api_level": 3,            
            "size_buckets": [
                0, 
                65536, 
                131072, 
                262144, 
                524288, 
                1048576, 
                4194304, 
                8388608, 
                16777216, 
                33554432
            ]        
        }    
    }
}
```