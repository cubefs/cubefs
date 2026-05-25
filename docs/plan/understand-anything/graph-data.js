window.__UA_GRAPH_SUMMARY__ = {
  "project": {
    "name": "CubeFS",
    "languages": [
      "c",
      "dockerfile",
      "go",
      "java",
      "json",
      "makefile",
      "markdown",
      "protobuf",
      "python",
      "shell",
      "xml",
      "yaml"
    ],
    "frameworks": [
      "Docker",
      "GitHub Actions",
      "GitLab CI"
    ],
    "description": "CubeFS 是一个 CNCF 毕业的开源云原生分布式文件与对象存储系统，支持 POSIX、HDFS、S3 与 REST API 访问，面向数据中心文件系统、数据湖以及公有、私有和混合云存储场景。注意：该项目包含超过 100 个源文件，如需更快分析，建议按子目录缩小范围。",
    "analyzedAt": "2026-05-25T03:44:10.727Z",
    "gitCommitHash": "f2be19ece01ae2c6535678938a4120112ef7a55b"
  },
  "meta": {
    "lastAnalyzedAt": "2026-05-25T03:45:05.745Z",
    "gitCommitHash": "f2be19ece01ae2c6535678938a4120112ef7a55b",
    "version": "1.0.0",
    "analyzedFiles": 2320
  },
  "totals": {
    "nodes": 13387,
    "edges": 54440,
    "modules": 27,
    "files": 2173,
    "functions": 9989,
    "classes": 934,
    "layers": 8,
    "tours": 8,
    "crossModuleEdges": 101
  },
  "nodeTypes": [
    {
      "key": "function",
      "label": "function",
      "count": 9989
    },
    {
      "key": "file",
      "label": "file",
      "count": 2173
    },
    {
      "key": "class",
      "label": "class",
      "count": 934
    },
    {
      "key": "pipeline",
      "label": "pipeline",
      "count": 131
    },
    {
      "key": "config",
      "label": "config",
      "count": 78
    },
    {
      "key": "document",
      "label": "document",
      "count": 49
    },
    {
      "key": "service",
      "label": "service",
      "count": 19
    },
    {
      "key": "schema",
      "label": "schema",
      "count": 14
    }
  ],
  "layers": [
    {
      "id": "layer:project-entry-and-governance",
      "name": "项目入口与治理层",
      "description": "仓库根目录中的 README、治理文档、顶层模块声明与全局入口文件，帮助理解项目目标、构建边界和整体结构。",
      "nodeCount": 19,
      "moduleCount": 1,
      "topModules": [
        {
          "key": "(root)",
          "label": "仓库根目录",
          "count": 19
        }
      ],
      "primaryModuleCount": 0
    },
    {
      "id": "layer:control-plane",
      "name": "控制面与管理服务层",
      "description": "master、authnode 与 console 相关文件，负责集群管理、认证鉴权、调度决策和运维入口。",
      "nodeCount": 100,
      "moduleCount": 3,
      "topModules": [
        {
          "key": "master",
          "label": "Master",
          "count": 81
        },
        {
          "key": "authnode",
          "label": "authnode",
          "count": 12
        },
        {
          "key": "console",
          "label": "console",
          "count": 7
        }
      ],
      "primaryModuleCount": 3
    },
    {
      "id": "layer:storage-and-data-plane",
      "name": "存储与数据平面层",
      "description": "datanode、metanode、lcnode、raftstore、blobstore 与 remotecache 相关文件，负责元数据、数据分区、一致性与存储执行链路。",
      "nodeCount": 578,
      "moduleCount": 6,
      "topModules": [
        {
          "key": "blobstore",
          "label": "BlobStore",
          "count": 454
        },
        {
          "key": "metanode",
          "label": "metanode",
          "count": 51
        },
        {
          "key": "remotecache",
          "label": "remotecache",
          "count": 31
        },
        {
          "key": "datanode",
          "label": "datanode",
          "count": 29
        },
        {
          "key": "lcnode",
          "label": "lcnode",
          "count": 7
        },
        {
          "key": "raftstore",
          "label": "raftstore",
          "count": 6
        }
      ],
      "primaryModuleCount": 6
    },
    {
      "id": "layer:gateway-and-sync",
      "name": "对象网关与同步层",
      "description": "objectnode 与 syncnode 相关文件，负责对象协议适配、同步任务与对外访问网关。",
      "nodeCount": 144,
      "moduleCount": 2,
      "topModules": [
        {
          "key": "syncnode",
          "label": "SyncNode",
          "count": 79
        },
        {
          "key": "objectnode",
          "label": "ObjectNode",
          "count": 65
        }
      ],
      "primaryModuleCount": 1
    },
    {
      "id": "layer:client-and-access",
      "name": "客户端与接入层",
      "description": "client、sdk、cli、cmd、java 与 shell 相关文件，负责命令行、SDK、客户端文件系统和外部接入体验。",
      "nodeCount": 224,
      "moduleCount": 6,
      "topModules": [
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 91
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 57
        },
        {
          "key": "cli",
          "label": "CLI",
          "count": 30
        },
        {
          "key": "client",
          "label": "client",
          "count": 29
        },
        {
          "key": "shell",
          "label": "shell",
          "count": 11
        },
        {
          "key": "java",
          "label": "java",
          "count": 6
        }
      ],
      "primaryModuleCount": 6
    },
    {
      "id": "layer:protocol-and-foundation",
      "name": "协议与基础库层",
      "description": "proto、util、depends 与 security 相关文件，提供协议模型、公共工具、兼容依赖与底层支撑能力。",
      "nodeCount": 439,
      "moduleCount": 4,
      "topModules": [
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 282
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 117
        },
        {
          "key": "proto",
          "label": "proto",
          "count": 37
        },
        {
          "key": "security",
          "label": "security",
          "count": 3
        }
      ],
      "primaryModuleCount": 4
    },
    {
      "id": "layer:engineering-and-ops",
      "name": "工程化与部署层",
      "description": "docker、deploy、tool、CI/CD 与运行脚本相关文件，负责构建、测试、发布、部署和工程自动化。",
      "nodeCount": 310,
      "moduleCount": 9,
      "topModules": [
        {
          "key": "docker",
          "label": "docker",
          "count": 117
        },
        {
          "key": "(root)",
          "label": "仓库根目录",
          "count": 59
        },
        {
          "key": "tool",
          "label": "Tooling",
          "count": 50
        },
        {
          "key": "blobstore",
          "label": "BlobStore",
          "count": 29
        },
        {
          "key": ".github",
          "label": "GitHub 工作流",
          "count": 21
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 16
        }
      ],
      "primaryModuleCount": 5
    },
    {
      "id": "layer:test-and-validation",
      "name": "测试与验证层",
      "description": "各子系统中的测试文件与验证样例，用于保障协议、状态机、存储路径和工具链行为的正确性。",
      "nodeCount": 650,
      "moduleCount": 20,
      "topModules": [
        {
          "key": "blobstore",
          "label": "BlobStore",
          "count": 254
        },
        {
          "key": "syncnode",
          "label": "SyncNode",
          "count": 108
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 67
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 54
        },
        {
          "key": "master",
          "label": "Master",
          "count": 32
        },
        {
          "key": "objectnode",
          "label": "ObjectNode",
          "count": 31
        }
      ],
      "primaryModuleCount": 2
    }
  ],
  "tours": [
    {
      "order": 1,
      "title": "从 README 与启动入口开始",
      "description": "先阅读 README、模块声明和顶层启动代码，建立对 CubeFS 定位、能力边界与整体可执行入口的第一印象。这一步会把文档叙事和真实启动路径对齐起来。",
      "nodeIds": [
        "document:README.md",
        "config:go.mod",
        "file:cubefs.go",
        "file:cmd/cmd.go"
      ],
      "languageLesson": "Go 项目通常通过 package main、go.mod 和 cmd 目录组织可执行入口；先看入口文件，再顺着 imports 往下走，能最快建立阅读路径。"
    },
    {
      "order": 2,
      "title": "控制面：集群管理与认证",
      "description": "这一站聚焦 master 与 authnode，理解集群元信息、认证鉴权和控制面服务如何协同工作。阅读这些文件可以帮助你把管理请求、调度决策和权限模型串成一条线。",
      "nodeIds": [
        "file:master/server.go",
        "file:master/cluster.go",
        "file:authnode/server.go"
      ]
    },
    {
      "order": 3,
      "title": "数据面：元数据与数据分区",
      "description": "接着进入 datanode 和 metanode，观察数据分区、元数据分区以及节点服务如何承接控制面下发的任务。这一层基本定义了 CubeFS 的核心存储执行路径。",
      "nodeIds": [
        "file:datanode/server.go",
        "file:datanode/partition.go",
        "file:metanode/server.go",
        "file:metanode/partition.go"
      ],
      "languageLesson": "在 Go 中，围绕同一子系统按 package 聚合 server、partition、manager 等文件，是很典型的拆分方式；顺着 package 内部的 contains 和 imports 边阅读，比按单个函数跳读更稳。"
    },
    {
      "order": 4,
      "title": "BlobStore 子系统",
      "description": "BlobStore 是 CubeFS 内部非常重要的一组存储服务，这一步把入口、cluster manager、proxy 和 scheduler 放在一起看。它能帮助你理解更细粒度对象块存储和后台调度是如何组织的。",
      "nodeIds": [
        "file:blobstore/blobstore.go",
        "file:blobstore/api/clustermgr/service.go",
        "file:blobstore/proxy/service.go",
        "file:blobstore/scheduler/service.go"
      ]
    },
    {
      "order": 5,
      "title": "对象网关与同步能力",
      "description": "随后阅读 objectnode 与 syncnode，理解 CubeFS 如何把对象访问协议和同步任务暴露为独立服务。这里能看到从路由、服务启动到任务执行的另一条访问链路。",
      "nodeIds": [
        "file:objectnode/server.go",
        "file:objectnode/router.go",
        "file:syncnode/server.go"
      ]
    },
    {
      "order": 6,
      "title": "客户端、SDK 与 CLI 接入",
      "description": "这一站从 CLI、FUSE 客户端和 master SDK 切入，理解外部用户是如何与集群交互的。它把命令入口、客户端文件系统语义和 API 封装连接到一起。",
      "nodeIds": [
        "file:cli/cli.go",
        "file:client/fuse.go",
        "file:client/fs/super.go",
        "file:sdk/master/client.go"
      ]
    },
    {
      "order": 7,
      "title": "协议、共识与公共基础能力",
      "description": "proto、raftstore 和 util 是贯穿全局的基础设施层。阅读这些文件可以理解请求协议、状态复制以及日志与公共工具是如何在各子系统之间复用的。",
      "nodeIds": [
        "file:proto/admin_proto.go",
        "file:proto/fs_proto.go",
        "file:raftstore/raftstore.go",
        "file:util/log/log.go"
      ],
      "languageLesson": "这一步能同时看到 hand-written Go 代码和 protobuf 生成代码在同一仓库中的协作方式；识别 generated code 与业务封装代码的边界，会显著提升阅读效率。"
    },
    {
      "order": 8,
      "title": "部署、构建与工程化流程",
      "description": "最后查看 Dockerfile、运行脚本和 CI 流水线文件，补齐 CubeFS 从源码到交付的工程化路径。这一步适合在理解系统结构后，再回头看构建和发布约束。",
      "nodeIds": [
        "service:Dockerfile",
        "file:docker/run_docker.sh",
        "pipeline:.github/workflows/ci.yml",
        "pipeline:.gitlab-ci.yml"
      ]
    }
  ],
  "modules": [
    {
      "module": "blobstore",
      "label": "BlobStore",
      "summary": "BlobStore 共有 705 个文件节点、2395 个函数节点和 288 个类型节点，主要落在“存储与数据平面层”。",
      "primaryLayerId": "layer:storage-and-data-plane",
      "primaryLayerName": "存储与数据平面层",
      "nodeCount": 3420,
      "fileCount": 705,
      "functionCount": 2395,
      "classCount": 288,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 2395
        },
        {
          "key": "file",
          "label": "file",
          "count": 705
        },
        {
          "key": "class",
          "label": "class",
          "count": 288
        },
        {
          "key": "pipeline",
          "label": "pipeline",
          "count": 26
        },
        {
          "key": "schema",
          "label": "schema",
          "count": 3
        },
        {
          "key": "service",
          "label": "service",
          "count": 3
        }
      ],
      "topDependencies": [],
      "topDependents": [
        {
          "key": "objectnode",
          "label": "ObjectNode",
          "count": 113
        },
        {
          "key": "client",
          "label": "client",
          "count": 47
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 43
        },
        {
          "key": "datanode",
          "label": "datanode",
          "count": 14
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 10
        }
      ],
      "sampleFiles": [
        {
          "id": "file:blobstore/common/proto/scheduler_test.go",
          "path": "blobstore/common/proto/scheduler_test.go",
          "name": "scheduler_test.go",
          "summary": "BlobStore 存储子系统中的测试文件，负责覆盖 scheduler_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 264,
          "importsOut": 13
        },
        {
          "id": "file:blobstore/common/proto/basic_test.go",
          "path": "blobstore/common/proto/basic_test.go",
          "name": "basic_test.go",
          "summary": "BlobStore 存储子系统中的测试文件，负责覆盖 basic_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 264,
          "importsOut": 10
        },
        {
          "id": "file:blobstore/common/proto/const_test.go",
          "path": "blobstore/common/proto/const_test.go",
          "name": "const_test.go",
          "summary": "BlobStore 存储子系统中的测试文件，负责覆盖 const_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 264,
          "importsOut": 10
        },
        {
          "id": "file:blobstore/common/proto/scheduler.go",
          "path": "blobstore/common/proto/scheduler.go",
          "name": "scheduler.go",
          "summary": "BlobStore 存储子系统中的代码文件，主要承担 后台任务与调度流程，是该子系统实现链路的一部分。 该文件提取到 19 个函数和 10 个类型/类定义。",
          "importsIn": 264,
          "importsOut": 7
        },
        {
          "id": "file:blobstore/common/proto/proxy.go",
          "path": "blobstore/common/proto/proxy.go",
          "name": "proxy.go",
          "summary": "BlobStore 存储子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 6 个函数和 3 个类型/类定义。",
          "importsIn": 264,
          "importsOut": 4
        }
      ],
      "fileImportInbound": 15700,
      "fileImportOutbound": 15457
    },
    {
      "module": "depends",
      "label": "第三方依赖镜像",
      "summary": "第三方依赖镜像 共有 309 个文件节点、1114 个函数节点和 167 个类型节点，主要落在“协议与基础库层”。",
      "primaryLayerId": "layer:protocol-and-foundation",
      "primaryLayerName": "协议与基础库层",
      "nodeCount": 1633,
      "fileCount": 309,
      "functionCount": 1114,
      "classCount": 167,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 1114
        },
        {
          "key": "file",
          "label": "file",
          "count": 309
        },
        {
          "key": "class",
          "label": "class",
          "count": 167
        },
        {
          "key": "document",
          "label": "document",
          "count": 21
        },
        {
          "key": "pipeline",
          "label": "pipeline",
          "count": 11
        },
        {
          "key": "config",
          "label": "config",
          "count": 9
        }
      ],
      "topDependencies": [
        {
          "key": "util",
          "label": "通用工具库",
          "count": 32
        },
        {
          "key": "proto",
          "label": "proto",
          "count": 31
        }
      ],
      "topDependents": [
        {
          "key": "client",
          "label": "client",
          "count": 322
        },
        {
          "key": "datanode",
          "label": "datanode",
          "count": 211
        },
        {
          "key": "raftstore",
          "label": "raftstore",
          "count": 129
        },
        {
          "key": "metanode",
          "label": "metanode",
          "count": 93
        },
        {
          "key": "master",
          "label": "Master",
          "count": 48
        }
      ],
      "sampleFiles": [
        {
          "id": "file:depends/bazil.org/fuse/fs/serve.go",
          "path": "depends/bazil.org/fuse/fs/serve.go",
          "name": "serve.go",
          "summary": "第三方依赖与兼容层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 59 个函数和 57 个类型/类定义。",
          "importsIn": 17,
          "importsOut": 91
        },
        {
          "id": "file:depends/bazil.org/fuse/fs/serve_test.go",
          "path": "depends/bazil.org/fuse/fs/serve_test.go",
          "name": "serve_test.go",
          "summary": "第三方依赖与兼容层中的测试文件，负责覆盖 serve_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 17,
          "importsOut": 60
        },
        {
          "id": "file:depends/bazil.org/fuse/options_daemon_timeout_test.go",
          "path": "depends/bazil.org/fuse/options_daemon_timeout_test.go",
          "name": "options_daemon_timeout_test.go",
          "summary": "第三方依赖与兼容层中的测试文件，负责覆盖 options_daemon_timeout_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 28,
          "importsOut": 47
        },
        {
          "id": "file:depends/bazil.org/fuse/options_test.go",
          "path": "depends/bazil.org/fuse/options_test.go",
          "name": "options_test.go",
          "summary": "第三方依赖与兼容层中的测试文件，负责覆盖 options_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 28,
          "importsOut": 47
        },
        {
          "id": "file:depends/bazil.org/fuse/options_nocomma_test.go",
          "path": "depends/bazil.org/fuse/options_nocomma_test.go",
          "name": "options_nocomma_test.go",
          "summary": "第三方依赖与兼容层中的测试文件，负责覆盖 options_nocomma_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 28,
          "importsOut": 42
        }
      ],
      "fileImportInbound": 2327,
      "fileImportOutbound": 1414
    },
    {
      "module": "syncnode",
      "label": "SyncNode",
      "summary": "SyncNode 共有 186 个文件节点、994 个函数节点和 100 个类型节点，主要落在“测试与验证层”。",
      "primaryLayerId": "layer:test-and-validation",
      "primaryLayerName": "测试与验证层",
      "nodeCount": 1281,
      "fileCount": 186,
      "functionCount": 994,
      "classCount": 100,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 994
        },
        {
          "key": "file",
          "label": "file",
          "count": 186
        },
        {
          "key": "class",
          "label": "class",
          "count": 100
        },
        {
          "key": "document",
          "label": "document",
          "count": 1
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 403
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 123
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 72
        },
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 8
        }
      ],
      "topDependents": [
        {
          "key": "master",
          "label": "Master",
          "count": 56
        },
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 18
        }
      ],
      "sampleFiles": [
        {
          "id": "file:syncnode/server.go",
          "path": "syncnode/server.go",
          "name": "server.go",
          "summary": "同步节点子系统中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 22 个函数和 1 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 158
        },
        {
          "id": "file:syncnode/snapshot.go",
          "path": "syncnode/snapshot.go",
          "name": "snapshot.go",
          "summary": "同步节点子系统中的代码文件，主要承担 快照、持久化与恢复，是该子系统实现链路的一部分。 该文件提取到 5 个函数和 1 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 104
        },
        {
          "id": "file:syncnode/snapshot_test.go",
          "path": "syncnode/snapshot_test.go",
          "name": "snapshot_test.go",
          "summary": "同步节点子系统中的测试文件，负责覆盖 snapshot_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 1,
          "importsOut": 97
        },
        {
          "id": "file:syncnode/tasks/runner.go",
          "path": "syncnode/tasks/runner.go",
          "name": "runner.go",
          "summary": "同步节点子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 28 个函数和 4 个类型/类定义。",
          "importsIn": 9,
          "importsOut": 82
        },
        {
          "id": "file:syncnode/tasks/degrade_hook_test.go",
          "path": "syncnode/tasks/degrade_hook_test.go",
          "name": "degrade_hook_test.go",
          "summary": "同步节点子系统中的测试文件，负责覆盖 degrade_hook_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 9,
          "importsOut": 78
        }
      ],
      "fileImportInbound": 1745,
      "fileImportOutbound": 2277
    },
    {
      "module": "util",
      "label": "通用工具库",
      "summary": "通用工具库 共有 183 个文件节点、626 个函数节点和 39 个类型节点，主要落在“协议与基础库层”。",
      "primaryLayerId": "layer:protocol-and-foundation",
      "primaryLayerName": "协议与基础库层",
      "nodeCount": 850,
      "fileCount": 183,
      "functionCount": 626,
      "classCount": 39,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 626
        },
        {
          "key": "file",
          "label": "file",
          "count": 183
        },
        {
          "key": "class",
          "label": "class",
          "count": 39
        },
        {
          "key": "config",
          "label": "config",
          "count": 1
        },
        {
          "key": "document",
          "label": "document",
          "count": 1
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 248
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 15
        },
        {
          "key": "blobstore",
          "label": "BlobStore",
          "count": 10
        },
        {
          "key": "raftstore",
          "label": "raftstore",
          "count": 6
        }
      ],
      "topDependents": [
        {
          "key": "master",
          "label": "Master",
          "count": 1254
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 980
        },
        {
          "key": "metanode",
          "label": "metanode",
          "count": 787
        },
        {
          "key": "datanode",
          "label": "datanode",
          "count": 780
        },
        {
          "key": "remotecache",
          "label": "remotecache",
          "count": 672
        }
      ],
      "sampleFiles": [
        {
          "id": "file:util/log/log.go",
          "path": "util/log/log.go",
          "name": "log.go",
          "summary": "公共工具层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 55 个函数和 3 个类型/类定义。",
          "importsIn": 310,
          "importsOut": 4
        },
        {
          "id": "file:util/log/log_get.go",
          "path": "util/log/log_get.go",
          "name": "log_get.go",
          "summary": "公共工具层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 4 个函数和 1 个类型/类定义。",
          "importsIn": 310,
          "importsOut": 0
        },
        {
          "id": "file:util/log/log_test.go",
          "path": "util/log/log_test.go",
          "name": "log_test.go",
          "summary": "公共工具层中的测试文件，负责覆盖 log_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 310,
          "importsOut": 0
        },
        {
          "id": "file:util/log/rotate.go",
          "path": "util/log/rotate.go",
          "name": "rotate.go",
          "summary": "公共工具层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 3 个函数和 1 个类型/类定义。",
          "importsIn": 310,
          "importsOut": 0
        },
        {
          "id": "file:util/recycle_timer_test.go",
          "path": "util/recycle_timer_test.go",
          "name": "recycle_timer_test.go",
          "summary": "公共工具层中的测试文件，负责覆盖 recycle_timer_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 147,
          "importsOut": 32
        }
      ],
      "fileImportInbound": 6880,
      "fileImportOutbound": 1131
    },
    {
      "module": "master",
      "label": "Master",
      "summary": "Master 共有 113 个文件节点、1335 个函数节点和 66 个类型节点，主要落在“控制面与管理服务层”。",
      "primaryLayerId": "layer:control-plane",
      "primaryLayerName": "控制面与管理服务层",
      "nodeCount": 1514,
      "fileCount": 113,
      "functionCount": 1335,
      "classCount": 66,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 1335
        },
        {
          "key": "file",
          "label": "file",
          "count": 113
        },
        {
          "key": "class",
          "label": "class",
          "count": 66
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 2573
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 1254
        },
        {
          "key": "remotecache",
          "label": "remotecache",
          "count": 120
        },
        {
          "key": "syncnode",
          "label": "SyncNode",
          "count": 56
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 48
        }
      ],
      "topDependents": [
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 108
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 108
        }
      ],
      "sampleFiles": [
        {
          "id": "file:master/cluster.go",
          "path": "master/cluster.go",
          "name": "cluster.go",
          "summary": "主控与调度子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 256 个函数和 13 个类型/类定义。",
          "importsIn": 2,
          "importsOut": 122
        },
        {
          "id": "file:master/flash_node.go",
          "path": "master/flash_node.go",
          "name": "flash_node.go",
          "summary": "主控与调度子系统中的代码文件，主要承担 节点管理与节点行为，是该子系统实现链路的一部分。 该文件提取到 30 个函数和 0 个类型/类定义。",
          "importsIn": 2,
          "importsOut": 96
        },
        {
          "id": "file:master/api_service.go",
          "path": "master/api_service.go",
          "name": "api_service.go",
          "summary": "主控与调度子系统中的代码文件，主要承担 接口处理与协议适配，是该子系统实现链路的一部分。 该文件提取到 245 个函数和 6 个类型/类定义。",
          "importsIn": 2,
          "importsOut": 90
        },
        {
          "id": "file:master/data_node.go",
          "path": "master/data_node.go",
          "name": "data_node.go",
          "summary": "主控与调度子系统中的代码文件，主要承担 节点管理与节点行为，是该子系统实现链路的一部分。 该文件提取到 62 个函数和 1 个类型/类定义。",
          "importsIn": 2,
          "importsOut": 80
        },
        {
          "id": "file:master/vol.go",
          "path": "master/vol.go",
          "name": "vol.go",
          "summary": "主控与调度子系统中的代码文件，主要承担 卷管理与容量操作，是该子系统实现链路的一部分。 该文件提取到 80 个函数和 8 个类型/类定义。",
          "importsIn": 2,
          "importsOut": 80
        }
      ],
      "fileImportInbound": 236,
      "fileImportOutbound": 4185
    },
    {
      "module": "objectnode",
      "label": "ObjectNode",
      "summary": "ObjectNode 共有 96 个文件节点、459 个函数节点和 29 个类型节点，主要落在“对象网关与同步层”。",
      "primaryLayerId": "layer:gateway-and-sync",
      "primaryLayerName": "对象网关与同步层",
      "nodeCount": 584,
      "fileCount": 96,
      "functionCount": 459,
      "classCount": 29,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 459
        },
        {
          "key": "file",
          "label": "file",
          "count": 96
        },
        {
          "key": "class",
          "label": "class",
          "count": 29
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 775
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 285
        },
        {
          "key": "metanode",
          "label": "metanode",
          "count": 140
        },
        {
          "key": "blobstore",
          "label": "BlobStore",
          "count": 113
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 66
        }
      ],
      "topDependents": [
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 96
        },
        {
          "key": "console",
          "label": "console",
          "count": 96
        }
      ],
      "sampleFiles": [
        {
          "id": "file:objectnode/fs_volume.go",
          "path": "objectnode/fs_volume.go",
          "name": "fs_volume.go",
          "summary": "对象访问网关子系统中的代码文件，主要承担 卷管理与容量操作，是该子系统实现链路的一部分。 该文件提取到 66 个函数和 7 个类型/类定义。",
          "importsIn": 2,
          "importsOut": 190
        },
        {
          "id": "file:objectnode/server.go",
          "path": "objectnode/server.go",
          "name": "server.go",
          "summary": "对象访问网关子系统中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 13 个函数和 1 个类型/类定义。",
          "importsIn": 2,
          "importsOut": 123
        },
        {
          "id": "file:objectnode/meta_cache.go",
          "path": "objectnode/meta_cache.go",
          "name": "meta_cache.go",
          "summary": "对象访问网关子系统中的代码文件，主要承担 缓存与热点数据处理，是该子系统实现链路的一部分。 该文件提取到 33 个函数和 6 个类型/类定义。",
          "importsIn": 2,
          "importsOut": 105
        },
        {
          "id": "file:objectnode/api_middleware.go",
          "path": "objectnode/api_middleware.go",
          "name": "api_middleware.go",
          "summary": "对象访问网关子系统中的代码文件，主要承担 接口处理与协议适配，是该子系统实现链路的一部分。 该文件提取到 12 个函数和 0 个类型/类定义。",
          "importsIn": 2,
          "importsOut": 56
        },
        {
          "id": "file:objectnode/policy_action.go",
          "path": "objectnode/policy_action.go",
          "name": "policy_action.go",
          "summary": "对象访问网关子系统中的代码文件，主要承担 认证、鉴权与策略控制，是该子系统实现链路的一部分。 该文件提取到 3 个函数和 0 个类型/类定义。",
          "importsIn": 2,
          "importsOut": 54
        }
      ],
      "fileImportInbound": 192,
      "fileImportOutbound": 1391
    },
    {
      "module": "cmd",
      "label": "命令入口",
      "summary": "命令入口 共有 86 个文件节点、52 个函数节点和 0 个类型节点，主要落在“客户端与接入层”。",
      "primaryLayerId": "layer:client-and-access",
      "primaryLayerName": "客户端与接入层",
      "nodeCount": 145,
      "fileCount": 86,
      "functionCount": 52,
      "classCount": 0,
      "nodeTypes": [
        {
          "key": "file",
          "label": "file",
          "count": 86
        },
        {
          "key": "function",
          "label": "function",
          "count": 52
        },
        {
          "key": "config",
          "label": "config",
          "count": 7
        }
      ],
      "topDependencies": [
        {
          "key": "master",
          "label": "Master",
          "count": 108
        },
        {
          "key": "objectnode",
          "label": "ObjectNode",
          "count": 96
        },
        {
          "key": "metanode",
          "label": "metanode",
          "count": 70
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 50
        },
        {
          "key": "remotecache",
          "label": "remotecache",
          "count": 35
        }
      ],
      "topDependents": [
        {
          "key": "metanode",
          "label": "metanode",
          "count": 20
        },
        {
          "key": "master",
          "label": "Master",
          "count": 12
        },
        {
          "key": "client",
          "label": "client",
          "count": 8
        },
        {
          "key": "datanode",
          "label": "datanode",
          "count": 8
        },
        {
          "key": "remotecache",
          "label": "remotecache",
          "count": 8
        }
      ],
      "sampleFiles": [
        {
          "id": "file:cmd/cmd.go",
          "path": "cmd/cmd.go",
          "name": "cmd.go",
          "summary": "进程启动与命令装配层中的代码文件，主要承担 进程入口与命令装配，是该子系统实现链路的一部分。 该文件提取到 8 个函数和 0 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 438
        },
        {
          "id": "file:cmd/common/argument.go",
          "path": "cmd/common/argument.go",
          "name": "argument.go",
          "summary": "进程启动与命令装配层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 8 个函数和 1 个类型/类定义。",
          "importsIn": 19,
          "importsOut": 23
        },
        {
          "id": "file:cmd/cubefs-mcp/main.go",
          "path": "cmd/cubefs-mcp/main.go",
          "name": "main.go",
          "summary": "进程启动与命令装配层中的代码文件，主要承担 进程入口与命令装配，是该子系统实现链路的一部分。 该文件提取到 2 个函数和 0 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 38
        },
        {
          "id": "file:cmd/cubefs-mcp/internal/masterclient/masterclient.go",
          "path": "cmd/cubefs-mcp/internal/masterclient/masterclient.go",
          "name": "masterclient.go",
          "summary": "进程启动与命令装配层中的代码文件，主要承担 客户端调用与远端访问，是该子系统实现链路的一部分。 该文件提取到 7 个函数和 3 个类型/类定义。",
          "importsIn": 36,
          "importsOut": 0
        },
        {
          "id": "file:cmd/common/server.go",
          "path": "cmd/common/server.go",
          "name": "server.go",
          "summary": "进程启动与命令装配层中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 3 个函数和 2 个类型/类定义。",
          "importsIn": 19,
          "importsOut": 1
        }
      ],
      "fileImportInbound": 149,
      "fileImportOutbound": 535
    },
    {
      "module": "metanode",
      "label": "metanode",
      "summary": "metanode 共有 70 个文件节点、642 个函数节点和 25 个类型节点，主要落在“存储与数据平面层”。",
      "primaryLayerId": "layer:storage-and-data-plane",
      "primaryLayerName": "存储与数据平面层",
      "nodeCount": 737,
      "fileCount": 70,
      "functionCount": 642,
      "classCount": 25,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 642
        },
        {
          "key": "file",
          "label": "file",
          "count": 70
        },
        {
          "key": "class",
          "label": "class",
          "count": 25
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 1581
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 787
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 93
        },
        {
          "key": "datanode",
          "label": "datanode",
          "count": 60
        },
        {
          "key": "raftstore",
          "label": "raftstore",
          "count": 24
        }
      ],
      "topDependents": [
        {
          "key": "cli",
          "label": "CLI",
          "count": 140
        },
        {
          "key": "objectnode",
          "label": "ObjectNode",
          "count": 140
        },
        {
          "key": "tool",
          "label": "Tooling",
          "count": 140
        },
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 70
        }
      ],
      "sampleFiles": [
        {
          "id": "file:metanode/manager.go",
          "path": "metanode/manager.go",
          "name": "manager.go",
          "summary": "元数据节点子系统中的代码文件，主要承担 管理器状态编排，是该子系统实现链路的一部分。 该文件提取到 35 个函数和 4 个类型/类定义。",
          "importsIn": 7,
          "importsOut": 117
        },
        {
          "id": "file:metanode/partition.go",
          "path": "metanode/partition.go",
          "name": "partition.go",
          "summary": "元数据节点子系统中的代码文件，主要承担 分区元数据与分片操作，是该子系统实现链路的一部分。 该文件提取到 87 个函数和 16 个类型/类定义。",
          "importsIn": 7,
          "importsOut": 114
        },
        {
          "id": "file:metanode/metanode.go",
          "path": "metanode/metanode.go",
          "name": "metanode.go",
          "summary": "元数据节点子系统中的代码文件，主要承担 节点管理与节点行为，是该子系统实现链路的一部分。 该文件提取到 19 个函数和 1 个类型/类定义。",
          "importsIn": 7,
          "importsOut": 96
        },
        {
          "id": "file:metanode/partition_free_list.go",
          "path": "metanode/partition_free_list.go",
          "name": "partition_free_list.go",
          "summary": "元数据节点子系统中的代码文件，主要承担 分区元数据与分片操作，是该子系统实现链路的一部分。 该文件提取到 20 个函数和 0 个类型/类定义。",
          "importsIn": 7,
          "importsOut": 80
        },
        {
          "id": "file:metanode/partition_fsm.go",
          "path": "metanode/partition_fsm.go",
          "name": "partition_fsm.go",
          "summary": "元数据节点子系统中的代码文件，主要承担 状态复制与一致性处理，是该子系统实现链路的一部分。 该文件提取到 12 个函数和 0 个类型/类定义。",
          "importsIn": 7,
          "importsOut": 80
        }
      ],
      "fileImportInbound": 490,
      "fileImportOutbound": 2597
    },
    {
      "module": "sdk",
      "label": "SDK",
      "summary": "SDK 共有 68 个文件节点、641 个函数节点和 37 个类型节点，主要落在“客户端与接入层”。",
      "primaryLayerId": "layer:client-and-access",
      "primaryLayerName": "客户端与接入层",
      "nodeCount": 746,
      "fileCount": 68,
      "functionCount": 641,
      "classCount": 37,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 641
        },
        {
          "key": "file",
          "label": "file",
          "count": 68
        },
        {
          "key": "class",
          "label": "class",
          "count": 37
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 1829
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 980
        },
        {
          "key": "master",
          "label": "Master",
          "count": 108
        },
        {
          "key": "blobstore",
          "label": "BlobStore",
          "count": 43
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 33
        }
      ],
      "topDependents": [
        {
          "key": "client",
          "label": "client",
          "count": 201
        },
        {
          "key": "cli",
          "label": "CLI",
          "count": 174
        },
        {
          "key": "tool",
          "label": "Tooling",
          "count": 164
        },
        {
          "key": "remotecache",
          "label": "remotecache",
          "count": 105
        },
        {
          "key": "lcnode",
          "label": "lcnode",
          "count": 85
        }
      ],
      "sampleFiles": [
        {
          "id": "file:sdk/data/stream/extent_client.go",
          "path": "sdk/data/stream/extent_client.go",
          "name": "extent_client.go",
          "summary": "SDK 接入层中的代码文件，主要承担 客户端调用与远端访问，是该子系统实现链路的一部分。 该文件提取到 54 个函数和 3 个类型/类定义。",
          "importsIn": 18,
          "importsOut": 136
        },
        {
          "id": "file:sdk/graphql/general.go",
          "path": "sdk/graphql/general.go",
          "name": "general.go",
          "summary": "SDK 接入层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 22 个函数和 3 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 139
        },
        {
          "id": "file:sdk/data/blobstore/reader.go",
          "path": "sdk/data/blobstore/reader.go",
          "name": "reader.go",
          "summary": "SDK 接入层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 12 个函数和 3 个类型/类定义。",
          "importsIn": 10,
          "importsOut": 109
        },
        {
          "id": "file:sdk/data/stream/stream_reader.go",
          "path": "sdk/data/stream/stream_reader.go",
          "name": "stream_reader.go",
          "summary": "SDK 接入层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 27 个函数和 2 个类型/类定义。",
          "importsIn": 18,
          "importsOut": 96
        },
        {
          "id": "file:sdk/master/api_admin.go",
          "path": "sdk/master/api_admin.go",
          "name": "api_admin.go",
          "summary": "SDK 接入层中的代码文件，主要承担 接口处理与协议适配，是该子系统实现链路的一部分。 该文件提取到 110 个函数和 1 个类型/类定义。",
          "importsIn": 54,
          "importsOut": 58
        }
      ],
      "fileImportInbound": 1230,
      "fileImportOutbound": 3312
    },
    {
      "module": "tool",
      "label": "Tooling",
      "summary": "Tooling 共有 52 个文件节点、253 个函数节点和 19 个类型节点，主要落在“工程化与部署层”。",
      "primaryLayerId": "layer:engineering-and-ops",
      "primaryLayerName": "工程化与部署层",
      "nodeCount": 330,
      "fileCount": 52,
      "functionCount": 253,
      "classCount": 19,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 253
        },
        {
          "key": "file",
          "label": "file",
          "count": 52
        },
        {
          "key": "class",
          "label": "class",
          "count": 19
        },
        {
          "key": "document",
          "label": "document",
          "count": 4
        },
        {
          "key": "config",
          "label": "config",
          "count": 2
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 589
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 164
        },
        {
          "key": "metanode",
          "label": "metanode",
          "count": 140
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 136
        },
        {
          "key": "datanode",
          "label": "datanode",
          "count": 10
        }
      ],
      "topDependents": [],
      "sampleFiles": [
        {
          "id": "file:tool/fsck/cmd/gc.go",
          "path": "tool/fsck/cmd/gc.go",
          "name": "gc.go",
          "summary": "开发工具层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 47 个函数和 4 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 155
        },
        {
          "id": "file:tool/fsck/cmd/check.go",
          "path": "tool/fsck/cmd/check.go",
          "name": "check.go",
          "summary": "开发工具层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 22 个函数和 1 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 112
        },
        {
          "id": "file:tool/cfs-sync/storage/cfs_linux.go",
          "path": "tool/cfs-sync/storage/cfs_linux.go",
          "name": "cfs_linux.go",
          "summary": "开发工具层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 15 个函数和 3 个类型/类定义。",
          "importsIn": 4,
          "importsOut": 71
        },
        {
          "id": "file:tool/cfs-sync/client_linux.go",
          "path": "tool/cfs-sync/client_linux.go",
          "name": "client_linux.go",
          "summary": "开发工具层中的代码文件，主要承担 客户端调用与远端访问，是该子系统实现链路的一部分。 该文件提取到 12 个函数和 2 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 74
        },
        {
          "id": "file:tool/remotecache-benchmark/storage/localfs.go",
          "path": "tool/remotecache-benchmark/storage/localfs.go",
          "name": "localfs.go",
          "summary": "开发工具层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 12 个函数和 2 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 63
        }
      ],
      "fileImportInbound": 56,
      "fileImportOutbound": 1103
    },
    {
      "module": "docker",
      "label": "docker",
      "summary": "docker 共有 44 个文件节点、12 个函数节点和 15 个类型节点，主要落在“工程化与部署层”。",
      "primaryLayerId": "layer:engineering-and-ops",
      "primaryLayerName": "工程化与部署层",
      "nodeCount": 158,
      "fileCount": 44,
      "functionCount": 12,
      "classCount": 15,
      "nodeTypes": [
        {
          "key": "file",
          "label": "file",
          "count": 44
        },
        {
          "key": "config",
          "label": "config",
          "count": 39
        },
        {
          "key": "pipeline",
          "label": "pipeline",
          "count": 37
        },
        {
          "key": "class",
          "label": "class",
          "count": 15
        },
        {
          "key": "function",
          "label": "function",
          "count": 12
        },
        {
          "key": "service",
          "label": "service",
          "count": 10
        }
      ],
      "topDependencies": [],
      "topDependents": [],
      "sampleFiles": [
        {
          "id": "file:docker/s3tests/base.py",
          "path": "docker/s3tests/base.py",
          "name": "base.py",
          "summary": "容器化与运行脚本层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 8 个函数和 1 个类型/类定义。",
          "importsIn": 14,
          "importsOut": 1
        },
        {
          "id": "file:docker/s3tests/env.py",
          "path": "docker/s3tests/env.py",
          "name": "env.py",
          "summary": "容器化与运行脚本层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。",
          "importsIn": 15,
          "importsOut": 0
        },
        {
          "id": "file:docker/s3tests/test_acl.py",
          "path": "docker/s3tests/test_acl.py",
          "name": "test_acl.py",
          "summary": "容器化与运行脚本层中的测试文件，负责覆盖 test_acl.py 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 0,
          "importsOut": 2
        },
        {
          "id": "file:docker/s3tests/test_bucket_policy.py",
          "path": "docker/s3tests/test_bucket_policy.py",
          "name": "test_bucket_policy.py",
          "summary": "容器化与运行脚本层中的测试文件，负责覆盖 test_bucket_policy.py 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 0,
          "importsOut": 2
        },
        {
          "id": "file:docker/s3tests/test_bucket.py",
          "path": "docker/s3tests/test_bucket.py",
          "name": "test_bucket.py",
          "summary": "容器化与运行脚本层中的测试文件，负责覆盖 test_bucket.py 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 0,
          "importsOut": 2
        }
      ],
      "fileImportInbound": 29,
      "fileImportOutbound": 29
    },
    {
      "module": "remotecache",
      "label": "remotecache",
      "summary": "remotecache 共有 43 个文件节点、323 个函数节点和 15 个类型节点，主要落在“存储与数据平面层”。",
      "primaryLayerId": "layer:storage-and-data-plane",
      "primaryLayerName": "存储与数据平面层",
      "nodeCount": 381,
      "fileCount": 43,
      "functionCount": 323,
      "classCount": 15,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 323
        },
        {
          "key": "file",
          "label": "file",
          "count": 43
        },
        {
          "key": "class",
          "label": "class",
          "count": 15
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 1023
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 672
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 105
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 41
        },
        {
          "key": "raftstore",
          "label": "raftstore",
          "count": 30
        }
      ],
      "topDependents": [
        {
          "key": "master",
          "label": "Master",
          "count": 120
        },
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 35
        },
        {
          "key": "tool",
          "label": "Tooling",
          "count": 8
        }
      ],
      "sampleFiles": [
        {
          "id": "file:remotecache/flashnode/flashnode_op.go",
          "path": "remotecache/flashnode/flashnode_op.go",
          "name": "flashnode_op.go",
          "summary": "远程缓存子系统中的代码文件，主要承担 节点管理与节点行为，是该子系统实现链路的一部分。 该文件提取到 26 个函数和 0 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 113
        },
        {
          "id": "file:remotecache/flashnode/cachengine/engine.go",
          "path": "remotecache/flashnode/cachengine/engine.go",
          "name": "engine.go",
          "summary": "远程缓存子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 51 个函数和 7 个类型/类定义。",
          "importsIn": 8,
          "importsOut": 91
        },
        {
          "id": "file:remotecache/flashnode/flashnode.go",
          "path": "remotecache/flashnode/flashnode.go",
          "name": "flashnode.go",
          "summary": "远程缓存子系统中的代码文件，主要承担 节点管理与节点行为，是该子系统实现链路的一部分。 该文件提取到 23 个函数和 1 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 95
        },
        {
          "id": "file:remotecache/flashgroupmanager/api_service.go",
          "path": "remotecache/flashgroupmanager/api_service.go",
          "name": "api_service.go",
          "summary": "远程缓存子系统中的代码文件，主要承担 接口处理与协议适配，是该子系统实现链路的一部分。 该文件提取到 37 个函数和 0 个类型/类定义。",
          "importsIn": 7,
          "importsOut": 81
        },
        {
          "id": "file:remotecache/flashnode/extent_reader.go",
          "path": "remotecache/flashnode/extent_reader.go",
          "name": "extent_reader.go",
          "summary": "远程缓存子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 10 个函数和 0 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 84
        }
      ],
      "fileImportInbound": 219,
      "fileImportOutbound": 1935
    },
    {
      "module": "datanode",
      "label": "datanode",
      "summary": "datanode 共有 37 个文件节点、390 个函数节点和 13 个类型节点，主要落在“存储与数据平面层”。",
      "primaryLayerId": "layer:storage-and-data-plane",
      "primaryLayerName": "存储与数据平面层",
      "nodeCount": 440,
      "fileCount": 37,
      "functionCount": 390,
      "classCount": 13,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 390
        },
        {
          "key": "file",
          "label": "file",
          "count": 37
        },
        {
          "key": "class",
          "label": "class",
          "count": 13
        }
      ],
      "topDependencies": [
        {
          "key": "util",
          "label": "通用工具库",
          "count": 780
        },
        {
          "key": "proto",
          "label": "proto",
          "count": 713
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 211
        },
        {
          "key": "raftstore",
          "label": "raftstore",
          "count": 24
        },
        {
          "key": "blobstore",
          "label": "BlobStore",
          "count": 14
        }
      ],
      "topDependents": [
        {
          "key": "metanode",
          "label": "metanode",
          "count": 60
        },
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 24
        },
        {
          "key": "master",
          "label": "Master",
          "count": 10
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 10
        },
        {
          "key": "tool",
          "label": "Tooling",
          "count": 10
        }
      ],
      "sampleFiles": [
        {
          "id": "file:datanode/wrap_operator.go",
          "path": "datanode/wrap_operator.go",
          "name": "wrap_operator.go",
          "summary": "数据节点子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 52 个函数和 0 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 124
        },
        {
          "id": "file:datanode/repl/packet.go",
          "path": "datanode/repl/packet.go",
          "name": "packet.go",
          "summary": "数据节点子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 65 个函数和 2 个类型/类定义。",
          "importsIn": 10,
          "importsOut": 114
        },
        {
          "id": "file:datanode/server.go",
          "path": "datanode/server.go",
          "name": "server.go",
          "summary": "数据节点子系统中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 39 个函数和 3 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 120
        },
        {
          "id": "file:datanode/disk.go",
          "path": "datanode/disk.go",
          "name": "disk.go",
          "summary": "数据节点子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 61 个函数和 2 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 115
        },
        {
          "id": "file:datanode/server_handler.go",
          "path": "datanode/server_handler.go",
          "name": "server_handler.go",
          "summary": "数据节点子系统中的代码文件，主要承担 接口处理与协议适配，是该子系统实现链路的一部分。 该文件提取到 34 个函数和 3 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 105
        }
      ],
      "fileImportInbound": 294,
      "fileImportOutbound": 1937
    },
    {
      "module": "client",
      "label": "client",
      "summary": "client 共有 33 个文件节点、230 个函数节点和 9 个类型节点，主要落在“客户端与接入层”。",
      "primaryLayerId": "layer:client-and-access",
      "primaryLayerName": "客户端与接入层",
      "nodeCount": 272,
      "fileCount": 33,
      "functionCount": 230,
      "classCount": 9,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 230
        },
        {
          "key": "file",
          "label": "file",
          "count": 33
        },
        {
          "key": "class",
          "label": "class",
          "count": 9
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 465
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 322
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 319
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 201
        },
        {
          "key": "blobstore",
          "label": "BlobStore",
          "count": 47
        }
      ],
      "topDependents": [
        {
          "key": "sdk",
          "label": "SDK",
          "count": 32
        },
        {
          "key": "objectnode",
          "label": "ObjectNode",
          "count": 8
        }
      ],
      "sampleFiles": [
        {
          "id": "file:client/fuse.go",
          "path": "client/fuse.go",
          "name": "fuse.go",
          "summary": "客户端文件系统层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 24 个函数和 0 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 188
        },
        {
          "id": "file:client/fs/super.go",
          "path": "client/fs/super.go",
          "name": "super.go",
          "summary": "客户端文件系统层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 32 个函数和 1 个类型/类定义。",
          "importsIn": 3,
          "importsOut": 160
        },
        {
          "id": "file:client/libsdk/libsdk.go",
          "path": "client/libsdk/libsdk.go",
          "name": "libsdk.go",
          "summary": "客户端文件系统层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 62 个函数和 4 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 140
        },
        {
          "id": "file:client/gosdk/cfs_client.go",
          "path": "client/gosdk/cfs_client.go",
          "name": "cfs_client.go",
          "summary": "客户端文件系统层中的代码文件，主要承担 客户端调用与远端访问，是该子系统实现链路的一部分。 该文件提取到 44 个函数和 1 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 137
        },
        {
          "id": "file:client/fs/dir.go",
          "path": "client/fs/dir.go",
          "name": "dir.go",
          "summary": "客户端文件系统层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 32 个函数和 3 个类型/类定义。",
          "importsIn": 3,
          "importsOut": 102
        }
      ],
      "fileImportInbound": 128,
      "fileImportOutbound": 1450
    },
    {
      "module": "proto",
      "label": "proto",
      "summary": "proto 共有 31 个文件节点、166 个函数节点和 83 个类型节点，主要落在“协议与基础库层”。",
      "primaryLayerId": "layer:protocol-and-foundation",
      "primaryLayerName": "协议与基础库层",
      "nodeCount": 291,
      "fileCount": 31,
      "functionCount": 166,
      "classCount": 83,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 166
        },
        {
          "key": "class",
          "label": "class",
          "count": 83
        },
        {
          "key": "file",
          "label": "file",
          "count": 31
        },
        {
          "key": "schema",
          "label": "schema",
          "count": 11
        }
      ],
      "topDependencies": [
        {
          "key": "util",
          "label": "通用工具库",
          "count": 166
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 32
        }
      ],
      "topDependents": [
        {
          "key": "master",
          "label": "Master",
          "count": 2573
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 1829
        },
        {
          "key": "metanode",
          "label": "metanode",
          "count": 1581
        },
        {
          "key": "remotecache",
          "label": "remotecache",
          "count": 1023
        },
        {
          "key": "objectnode",
          "label": "ObjectNode",
          "count": 775
        }
      ],
      "sampleFiles": [
        {
          "id": "file:proto/admin_proto.go",
          "path": "proto/admin_proto.go",
          "name": "admin_proto.go",
          "summary": "协议定义层中的代码文件，主要承担 协议结构与序列化定义，是该子系统实现链路的一部分。 该文件提取到 36 个函数和 108 个类型/类定义。",
          "importsIn": 378,
          "importsOut": 59
        },
        {
          "id": "file:proto/extent_key_test.go",
          "path": "proto/extent_key_test.go",
          "name": "extent_key_test.go",
          "summary": "协议定义层中的测试文件，负责覆盖 extent_key_test.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 378,
          "importsOut": 39
        },
        {
          "id": "file:proto/extent_key.go",
          "path": "proto/extent_key.go",
          "name": "extent_key.go",
          "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 28 个函数和 5 个类型/类定义。",
          "importsIn": 378,
          "importsOut": 38
        },
        {
          "id": "file:proto/packet.go",
          "path": "proto/packet.go",
          "name": "packet.go",
          "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 57 个函数和 1 个类型/类定义。",
          "importsIn": 378,
          "importsOut": 36
        },
        {
          "id": "file:proto/meta_proto.go",
          "path": "proto/meta_proto.go",
          "name": "meta_proto.go",
          "summary": "协议定义层中的代码文件，主要承担 协议结构与序列化定义，是该子系统实现链路的一部分。 该文件提取到 7 个函数和 16 个类型/类定义。",
          "importsIn": 378,
          "importsOut": 23
        }
      ],
      "fileImportInbound": 11718,
      "fileImportOutbound": 229
    },
    {
      "module": "cli",
      "label": "CLI",
      "summary": "CLI 共有 30 个文件节点、181 个函数节点和 1 个类型节点，主要落在“客户端与接入层”。",
      "primaryLayerId": "layer:client-and-access",
      "primaryLayerName": "客户端与接入层",
      "nodeCount": 214,
      "fileCount": 30,
      "functionCount": 181,
      "classCount": 1,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 181
        },
        {
          "key": "file",
          "label": "file",
          "count": 30
        },
        {
          "key": "config",
          "label": "config",
          "count": 2
        },
        {
          "key": "class",
          "label": "class",
          "count": 1
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 744
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 174
        },
        {
          "key": "metanode",
          "label": "metanode",
          "count": 140
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 131
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 8
        }
      ],
      "topDependents": [],
      "sampleFiles": [
        {
          "id": "file:cli/api/metaapi.go",
          "path": "cli/api/metaapi.go",
          "name": "metaapi.go",
          "summary": "CLI 接入层中的代码文件，主要承担 接口处理与协议适配，是该子系统实现链路的一部分。 该文件提取到 12 个函数和 4 个类型/类定义。",
          "importsIn": 2,
          "importsOut": 105
        },
        {
          "id": "file:cli/cmd/compatibility.go",
          "path": "cli/cmd/compatibility.go",
          "name": "compatibility.go",
          "summary": "CLI 接入层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 4 个函数和 0 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 102
        },
        {
          "id": "file:cli/cmd/vol.go",
          "path": "cli/cmd/vol.go",
          "name": "vol.go",
          "summary": "CLI 接入层中的代码文件，主要承担 卷管理与容量操作，是该子系统实现链路的一部分。 该文件提取到 20 个函数和 0 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 76
        },
        {
          "id": "file:cli/cli.go",
          "path": "cli/cli.go",
          "name": "cli.go",
          "summary": "CLI 接入层中的代码文件，主要承担 进程入口与命令装配，是该子系统实现链路的一部分。 该文件提取到 3 个函数和 0 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 70
        },
        {
          "id": "file:cli/cmd/acl.go",
          "path": "cli/cmd/acl.go",
          "name": "acl.go",
          "summary": "CLI 接入层中的代码文件，主要承担 认证、鉴权与策略控制，是该子系统实现链路的一部分。 该文件提取到 5 个函数和 0 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 61
        }
      ],
      "fileImportInbound": 30,
      "fileImportOutbound": 1229
    },
    {
      "module": "deploy",
      "label": "deploy",
      "summary": "deploy 共有 15 个文件节点、24 个函数节点和 4 个类型节点，主要落在“工程化与部署层”。",
      "primaryLayerId": "layer:engineering-and-ops",
      "primaryLayerName": "工程化与部署层",
      "nodeCount": 44,
      "fileCount": 15,
      "functionCount": 24,
      "classCount": 4,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 24
        },
        {
          "key": "file",
          "label": "file",
          "count": 15
        },
        {
          "key": "class",
          "label": "class",
          "count": 4
        },
        {
          "key": "config",
          "label": "config",
          "count": 1
        }
      ],
      "topDependencies": [
        {
          "key": "util",
          "label": "通用工具库",
          "count": 4
        }
      ],
      "topDependents": [],
      "sampleFiles": [
        {
          "id": "file:deploy/deploy_cli.go",
          "path": "deploy/deploy_cli.go",
          "name": "deploy_cli.go",
          "summary": "部署编排层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 2 个函数和 0 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 18
        },
        {
          "id": "file:deploy/cmd/cluster.go",
          "path": "deploy/cmd/cluster.go",
          "name": "cluster.go",
          "summary": "部署编排层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 7 个函数和 0 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 0
        },
        {
          "id": "file:deploy/cmd/config.go",
          "path": "deploy/cmd/config.go",
          "name": "config.go",
          "summary": "部署编排层中的代码文件，主要承担 配置与常量定义，是该子系统实现链路的一部分。 该文件提取到 2 个函数和 7 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 0
        },
        {
          "id": "file:deploy/cmd/datanode.go",
          "path": "deploy/cmd/datanode.go",
          "name": "datanode.go",
          "summary": "部署编排层中的代码文件，主要承担 节点管理与节点行为，是该子系统实现链路的一部分。 该文件提取到 6 个函数和 1 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 0
        },
        {
          "id": "file:deploy/cmd/docker.go",
          "path": "deploy/cmd/docker.go",
          "name": "docker.go",
          "summary": "部署编排层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 12 个函数和 0 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 0
        }
      ],
      "fileImportInbound": 14,
      "fileImportOutbound": 18
    },
    {
      "module": "authnode",
      "label": "authnode",
      "summary": "authnode 共有 12 个文件节点、37 个函数节点和 3 个类型节点，主要落在“控制面与管理服务层”。",
      "primaryLayerId": "layer:control-plane",
      "primaryLayerName": "控制面与管理服务层",
      "nodeCount": 52,
      "fileCount": 12,
      "functionCount": 37,
      "classCount": 3,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 37
        },
        {
          "key": "file",
          "label": "file",
          "count": 12
        },
        {
          "key": "class",
          "label": "class",
          "count": 3
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 186
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 130
        },
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 44
        },
        {
          "key": "raftstore",
          "label": "raftstore",
          "count": 22
        }
      ],
      "topDependents": [
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 11
        }
      ],
      "sampleFiles": [
        {
          "id": "file:authnode/server.go",
          "path": "authnode/server.go",
          "name": "server.go",
          "summary": "认证与访问控制子系统中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 8 个函数和 2 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 84
        },
        {
          "id": "file:authnode/cluster.go",
          "path": "authnode/cluster.go",
          "name": "cluster.go",
          "summary": "认证与访问控制子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 10 个函数和 2 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 74
        },
        {
          "id": "file:authnode/api_service.go",
          "path": "authnode/api_service.go",
          "name": "api_service.go",
          "summary": "认证与访问控制子系统中的代码文件，主要承担 接口处理与协议适配，是该子系统实现链路的一部分。 该文件提取到 29 个函数和 0 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 48
        },
        {
          "id": "file:authnode/keystore_fsm.go",
          "path": "authnode/keystore_fsm.go",
          "name": "keystore_fsm.go",
          "summary": "认证与访问控制子系统中的代码文件，主要承担 状态复制与一致性处理，是该子系统实现链路的一部分。 该文件提取到 17 个函数和 1 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 43
        },
        {
          "id": "file:authnode/http_server.go",
          "path": "authnode/http_server.go",
          "name": "http_server.go",
          "summary": "认证与访问控制子系统中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 6 个函数和 0 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 38
        }
      ],
      "fileImportInbound": 11,
      "fileImportOutbound": 382
    },
    {
      "module": "lcnode",
      "label": "lcnode",
      "summary": "lcnode 共有 11 个文件节点、43 个函数节点和 4 个类型节点，主要落在“存储与数据平面层”。",
      "primaryLayerId": "layer:storage-and-data-plane",
      "primaryLayerName": "存储与数据平面层",
      "nodeCount": 58,
      "fileCount": 11,
      "functionCount": 43,
      "classCount": 4,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 43
        },
        {
          "key": "file",
          "label": "file",
          "count": 11
        },
        {
          "key": "class",
          "label": "class",
          "count": 4
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 341
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 109
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 85
        },
        {
          "key": "blobstore",
          "label": "BlobStore",
          "count": 5
        },
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 4
        }
      ],
      "topDependents": [
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 11
        }
      ],
      "sampleFiles": [
        {
          "id": "file:lcnode/server.go",
          "path": "lcnode/server.go",
          "name": "server.go",
          "summary": "生命周期节点子系统中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 17 个函数和 1 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 113
        },
        {
          "id": "file:lcnode/lc_scanner.go",
          "path": "lcnode/lc_scanner.go",
          "name": "lc_scanner.go",
          "summary": "生命周期节点子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 17 个函数和 1 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 85
        },
        {
          "id": "file:lcnode/lc_transition.go",
          "path": "lcnode/lc_transition.go",
          "name": "lc_transition.go",
          "summary": "生命周期节点子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 3 个函数和 3 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 58
        },
        {
          "id": "file:lcnode/snapshot_scanner.go",
          "path": "lcnode/snapshot_scanner.go",
          "name": "snapshot_scanner.go",
          "summary": "生命周期节点子系统中的代码文件，主要承担 快照、持久化与恢复，是该子系统实现链路的一部分。 该文件提取到 12 个函数和 1 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 53
        },
        {
          "id": "file:lcnode/lc_op.go",
          "path": "lcnode/lc_op.go",
          "name": "lc_op.go",
          "summary": "生命周期节点子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 5 个函数和 0 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 37
        }
      ],
      "fileImportInbound": 11,
      "fileImportOutbound": 544
    },
    {
      "module": "shell",
      "label": "shell",
      "summary": "shell 共有 11 个文件节点、0 个函数节点和 0 个类型节点，主要落在“客户端与接入层”。",
      "primaryLayerId": "layer:client-and-access",
      "primaryLayerName": "客户端与接入层",
      "nodeCount": 11,
      "fileCount": 11,
      "functionCount": 0,
      "classCount": 0,
      "nodeTypes": [
        {
          "key": "file",
          "label": "file",
          "count": 11
        }
      ],
      "topDependencies": [],
      "topDependents": [],
      "sampleFiles": [
        {
          "id": "file:shell/deploy_client.sh",
          "path": "shell/deploy_client.sh",
          "name": "deploy_client.sh",
          "summary": "shell 子目录中的脚本文件，负责 客户端调用与远端访问，用于自动化执行环境准备、构建或运维动作。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:shell/deploy_object.sh",
          "path": "shell/deploy_object.sh",
          "name": "deploy_object.sh",
          "summary": "shell 子目录中的脚本文件，负责 核心实现逻辑，用于自动化执行环境准备、构建或运维动作。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:shell/deploy.sh",
          "path": "shell/deploy.sh",
          "name": "deploy.sh",
          "summary": "shell 子目录中的脚本文件，负责 核心实现逻辑，用于自动化执行环境准备、构建或运维动作。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:shell/genConf.sh",
          "path": "shell/genConf.sh",
          "name": "genConf.sh",
          "summary": "shell 子目录中的脚本文件，负责 核心实现逻辑，用于自动化执行环境准备、构建或运维动作。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:shell/genIp.sh",
          "path": "shell/genIp.sh",
          "name": "genIp.sh",
          "summary": "shell 子目录中的脚本文件，负责 核心实现逻辑，用于自动化执行环境准备、构建或运维动作。",
          "importsIn": 0,
          "importsOut": 0
        }
      ],
      "fileImportInbound": 0,
      "fileImportOutbound": 0
    },
    {
      "module": "test",
      "label": "test",
      "summary": "test 共有 10 个文件节点、2 个函数节点和 0 个类型节点，主要落在“测试与验证层”。",
      "primaryLayerId": "layer:test-and-validation",
      "primaryLayerName": "测试与验证层",
      "nodeCount": 13,
      "fileCount": 10,
      "functionCount": 2,
      "classCount": 0,
      "nodeTypes": [
        {
          "key": "file",
          "label": "file",
          "count": 10
        },
        {
          "key": "function",
          "label": "function",
          "count": 2
        },
        {
          "key": "document",
          "label": "document",
          "count": 1
        }
      ],
      "topDependencies": [],
      "topDependents": [],
      "sampleFiles": [
        {
          "id": "file:test/regression/idempotent/main.go",
          "path": "test/regression/idempotent/main.go",
          "name": "main.go",
          "summary": "测试与验证层中的测试文件，负责覆盖 main.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:test/regression/overlapping/main.go",
          "path": "test/regression/overlapping/main.go",
          "name": "main.go",
          "summary": "测试与验证层中的测试文件，负责覆盖 main.go 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:test/userTest.sh",
          "path": "test/userTest.sh",
          "name": "userTest.sh",
          "summary": "测试与验证层中的测试文件，负责覆盖 userTest.sh 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:test/volumeTest/deleteVol.sh",
          "path": "test/volumeTest/deleteVol.sh",
          "name": "deleteVol.sh",
          "summary": "测试与验证层中的测试文件，负责覆盖 deleteVol.sh 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:test/volumeTest/getVol.sh",
          "path": "test/volumeTest/getVol.sh",
          "name": "getVol.sh",
          "summary": "测试与验证层中的测试文件，负责覆盖 getVol.sh 关联逻辑并验证关键行为与边界条件。",
          "importsIn": 0,
          "importsOut": 0
        }
      ],
      "fileImportInbound": 0,
      "fileImportOutbound": 0
    },
    {
      "module": "console",
      "label": "console",
      "summary": "console 共有 9 个文件节点、34 个函数节点和 6 个类型节点，主要落在“控制面与管理服务层”。",
      "primaryLayerId": "layer:control-plane",
      "primaryLayerName": "控制面与管理服务层",
      "nodeCount": 49,
      "fileCount": 9,
      "functionCount": 34,
      "classCount": 6,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 34
        },
        {
          "key": "file",
          "label": "file",
          "count": 9
        },
        {
          "key": "class",
          "label": "class",
          "count": 6
        }
      ],
      "topDependencies": [
        {
          "key": "proto",
          "label": "proto",
          "count": 155
        },
        {
          "key": "objectnode",
          "label": "ObjectNode",
          "count": 96
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 13
        },
        {
          "key": "sdk",
          "label": "SDK",
          "count": 10
        }
      ],
      "topDependents": [
        {
          "key": "cmd",
          "label": "命令入口",
          "count": 4
        }
      ],
      "sampleFiles": [
        {
          "id": "file:console/service/file.go",
          "path": "console/service/file.go",
          "name": "file.go",
          "summary": "控制台服务层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 15 个函数和 3 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 136
        },
        {
          "id": "file:console/server.go",
          "path": "console/server.go",
          "name": "server.go",
          "summary": "控制台服务层中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 9 个函数和 1 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 42
        },
        {
          "id": "file:console/cutil/handler.go",
          "path": "console/cutil/handler.go",
          "name": "handler.go",
          "summary": "控制台服务层中的代码文件，主要承担 接口处理与协议适配，是该子系统实现链路的一部分。 该文件提取到 8 个函数和 4 个类型/类定义。",
          "importsIn": 3,
          "importsOut": 36
        },
        {
          "id": "file:console/service/login.go",
          "path": "console/service/login.go",
          "name": "login.go",
          "summary": "控制台服务层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 4 个函数和 2 个类型/类定义。",
          "importsIn": 1,
          "importsOut": 36
        },
        {
          "id": "file:console/cutil/cache.go",
          "path": "console/cutil/cache.go",
          "name": "cache.go",
          "summary": "控制台服务层中的代码文件，主要承担 缓存与热点数据处理，是该子系统实现链路的一部分。 该文件提取到 2 个函数和 1 个类型/类定义。",
          "importsIn": 3,
          "importsOut": 33
        }
      ],
      "fileImportInbound": 13,
      "fileImportOutbound": 283
    },
    {
      "module": "raftstore",
      "label": "raftstore",
      "summary": "raftstore 共有 8 个文件节点、28 个函数节点和 8 个类型节点，主要落在“存储与数据平面层”。",
      "primaryLayerId": "layer:storage-and-data-plane",
      "primaryLayerName": "存储与数据平面层",
      "nodeCount": 44,
      "fileCount": 8,
      "functionCount": 28,
      "classCount": 8,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 28
        },
        {
          "key": "class",
          "label": "class",
          "count": 8
        },
        {
          "key": "file",
          "label": "file",
          "count": 8
        }
      ],
      "topDependencies": [
        {
          "key": "depends",
          "label": "第三方依赖镜像",
          "count": 129
        },
        {
          "key": "util",
          "label": "通用工具库",
          "count": 57
        }
      ],
      "topDependents": [
        {
          "key": "master",
          "label": "Master",
          "count": 46
        },
        {
          "key": "remotecache",
          "label": "remotecache",
          "count": 30
        },
        {
          "key": "datanode",
          "label": "datanode",
          "count": 24
        },
        {
          "key": "metanode",
          "label": "metanode",
          "count": 24
        },
        {
          "key": "authnode",
          "label": "authnode",
          "count": 22
        }
      ],
      "sampleFiles": [
        {
          "id": "file:raftstore/raftstore.go",
          "path": "raftstore/raftstore.go",
          "name": "raftstore.go",
          "summary": "Raft 共识存储层中的代码文件，主要承担 状态复制与一致性处理，是该子系统实现链路的一部分。 该文件提取到 11 个函数和 2 个类型/类定义。",
          "importsIn": 22,
          "importsOut": 57
        },
        {
          "id": "file:raftstore/resolver.go",
          "path": "raftstore/resolver.go",
          "name": "resolver.go",
          "summary": "Raft 共识存储层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 5 个函数和 4 个类型/类定义。",
          "importsIn": 22,
          "importsOut": 36
        },
        {
          "id": "file:raftstore/partition.go",
          "path": "raftstore/partition.go",
          "name": "partition.go",
          "summary": "Raft 共识存储层中的代码文件，主要承担 分区元数据与分片操作，是该子系统实现链路的一部分。 该文件提取到 15 个函数和 2 个类型/类定义。",
          "importsIn": 22,
          "importsOut": 35
        },
        {
          "id": "file:raftstore/raftstore_db/store_rocksdb.go",
          "path": "raftstore/raftstore_db/store_rocksdb.go",
          "name": "store_rocksdb.go",
          "summary": "Raft 共识存储层中的代码文件，主要承担 存储访问与数据读写，是该子系统实现链路的一部分。 该文件提取到 23 个函数和 1 个类型/类定义。",
          "importsIn": 11,
          "importsOut": 37
        },
        {
          "id": "file:raftstore/monitor.go",
          "path": "raftstore/monitor.go",
          "name": "monitor.go",
          "summary": "Raft 共识存储层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 6 个函数和 3 个类型/类定义。",
          "importsIn": 22,
          "importsOut": 18
        }
      ],
      "fileImportInbound": 154,
      "fileImportOutbound": 188
    },
    {
      "module": "(root)",
      "label": "仓库根目录",
      "summary": "仓库根目录 共有 5 个文件节点、1 个函数节点和 0 个类型节点，主要落在“工程化与部署层”。",
      "primaryLayerId": "layer:engineering-and-ops",
      "primaryLayerName": "工程化与部署层",
      "nodeCount": 79,
      "fileCount": 5,
      "functionCount": 1,
      "classCount": 0,
      "nodeTypes": [
        {
          "key": "pipeline",
          "label": "pipeline",
          "count": 48
        },
        {
          "key": "document",
          "label": "document",
          "count": 15
        },
        {
          "key": "config",
          "label": "config",
          "count": 6
        },
        {
          "key": "file",
          "label": "file",
          "count": 5
        },
        {
          "key": "service",
          "label": "service",
          "count": 4
        },
        {
          "key": "function",
          "label": "function",
          "count": 1
        }
      ],
      "topDependencies": [],
      "topDependents": [],
      "sampleFiles": [
        {
          "id": "file:.gitattributes",
          "path": ".gitattributes",
          "name": ".gitattributes",
          "summary": "项目根目录中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:.semgrepignore",
          "path": ".semgrepignore",
          "name": ".semgrepignore",
          "summary": "项目根目录中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:build.sh",
          "path": "build.sh",
          "name": "build.sh",
          "summary": "项目根目录中的脚本文件，负责 核心实现逻辑，用于自动化执行环境准备、构建或运维动作。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:cubefs.go",
          "path": "cubefs.go",
          "name": "cubefs.go",
          "summary": "项目根目录中的代码文件，主要承担 进程入口与命令装配，是该子系统实现链路的一部分。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:NOTICE",
          "path": "NOTICE",
          "name": "NOTICE",
          "summary": "项目根目录中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。",
          "importsIn": 0,
          "importsOut": 0
        }
      ],
      "fileImportInbound": 0,
      "fileImportOutbound": 0
    },
    {
      "module": "java",
      "label": "java",
      "summary": "java 共有 5 个文件节点、7 个函数节点和 3 个类型节点，主要落在“客户端与接入层”。",
      "primaryLayerId": "layer:client-and-access",
      "primaryLayerName": "客户端与接入层",
      "nodeCount": 17,
      "fileCount": 5,
      "functionCount": 7,
      "classCount": 3,
      "nodeTypes": [
        {
          "key": "function",
          "label": "function",
          "count": 7
        },
        {
          "key": "file",
          "label": "file",
          "count": 5
        },
        {
          "key": "class",
          "label": "class",
          "count": 3
        },
        {
          "key": "config",
          "label": "config",
          "count": 1
        },
        {
          "key": "document",
          "label": "document",
          "count": 1
        }
      ],
      "topDependencies": [],
      "topDependents": [],
      "sampleFiles": [
        {
          "id": "file:java/build.sh",
          "path": "java/build.sh",
          "name": "build.sh",
          "summary": "Java 客户端层中的脚本文件，负责 核心实现逻辑，用于自动化执行环境准备、构建或运维动作。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:java/src/main/java/io/cubefs/fs/CfsLibrary.java",
          "path": "java/src/main/java/io/cubefs/fs/CfsLibrary.java",
          "name": "CfsLibrary.java",
          "summary": "Java 客户端层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 0 个函数和 1 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:java/src/main/java/io/cubefs/fs/CfsMount.java",
          "path": "java/src/main/java/io/cubefs/fs/CfsMount.java",
          "name": "CfsMount.java",
          "summary": "Java 客户端层中的代码文件，主要承担 状态复制与一致性处理，是该子系统实现链路的一部分。 该文件提取到 22 个函数和 1 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:java/src/main/java/io/cubefs/fs/TestCfsClient.java",
          "path": "java/src/main/java/io/cubefs/fs/TestCfsClient.java",
          "name": "TestCfsClient.java",
          "summary": "Java 客户端层中的代码文件，主要承担 客户端调用与远端访问，是该子系统实现链路的一部分。 该文件提取到 9 个函数和 1 个类型/类定义。",
          "importsIn": 0,
          "importsOut": 0
        },
        {
          "id": "file:java/src/main/resources/.gitkeep",
          "path": "java/src/main/resources/.gitkeep",
          "name": ".gitkeep",
          "summary": "Java 客户端层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。",
          "importsIn": 0,
          "importsOut": 0
        }
      ],
      "fileImportInbound": 0,
      "fileImportOutbound": 0
    },
    {
      "module": ".github",
      "label": "GitHub 工作流",
      "summary": "GitHub 工作流 共有 1 个文件节点、0 个函数节点和 0 个类型节点，主要落在“工程化与部署层”。",
      "primaryLayerId": "layer:engineering-and-ops",
      "primaryLayerName": "工程化与部署层",
      "nodeCount": 21,
      "fileCount": 1,
      "functionCount": 0,
      "classCount": 0,
      "nodeTypes": [
        {
          "key": "config",
          "label": "config",
          "count": 10
        },
        {
          "key": "pipeline",
          "label": "pipeline",
          "count": 9
        },
        {
          "key": "document",
          "label": "document",
          "count": 1
        },
        {
          "key": "file",
          "label": "file",
          "count": 1
        }
      ],
      "topDependencies": [],
      "topDependents": [],
      "sampleFiles": [
        {
          "id": "file:.github/CODEOWNERS",
          "path": ".github/CODEOWNERS",
          "name": "CODEOWNERS",
          "summary": ".github 子目录中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。",
          "importsIn": 0,
          "importsOut": 0
        }
      ],
      "fileImportInbound": 0,
      "fileImportOutbound": 0
    },
    {
      "module": "security",
      "label": "security",
      "summary": "security 共有 0 个文件节点、0 个函数节点和 0 个类型节点，主要落在“协议与基础库层”。",
      "primaryLayerId": "layer:protocol-and-foundation",
      "primaryLayerName": "协议与基础库层",
      "nodeCount": 3,
      "fileCount": 0,
      "functionCount": 0,
      "classCount": 0,
      "nodeTypes": [
        {
          "key": "document",
          "label": "document",
          "count": 3
        }
      ],
      "topDependencies": [],
      "topDependents": [],
      "sampleFiles": [],
      "fileImportInbound": 0,
      "fileImportOutbound": 0
    }
  ],
  "moduleGraph": {
    "nodes": [
      {
        "module": "blobstore",
        "label": "BlobStore",
        "primaryLayerId": "layer:storage-and-data-plane",
        "primaryLayerName": "存储与数据平面层",
        "nodeCount": 3420,
        "fileCount": 705,
        "functionCount": 2395,
        "classCount": 288
      },
      {
        "module": "depends",
        "label": "第三方依赖镜像",
        "primaryLayerId": "layer:protocol-and-foundation",
        "primaryLayerName": "协议与基础库层",
        "nodeCount": 1633,
        "fileCount": 309,
        "functionCount": 1114,
        "classCount": 167
      },
      {
        "module": "syncnode",
        "label": "SyncNode",
        "primaryLayerId": "layer:test-and-validation",
        "primaryLayerName": "测试与验证层",
        "nodeCount": 1281,
        "fileCount": 186,
        "functionCount": 994,
        "classCount": 100
      },
      {
        "module": "util",
        "label": "通用工具库",
        "primaryLayerId": "layer:protocol-and-foundation",
        "primaryLayerName": "协议与基础库层",
        "nodeCount": 850,
        "fileCount": 183,
        "functionCount": 626,
        "classCount": 39
      },
      {
        "module": "master",
        "label": "Master",
        "primaryLayerId": "layer:control-plane",
        "primaryLayerName": "控制面与管理服务层",
        "nodeCount": 1514,
        "fileCount": 113,
        "functionCount": 1335,
        "classCount": 66
      },
      {
        "module": "objectnode",
        "label": "ObjectNode",
        "primaryLayerId": "layer:gateway-and-sync",
        "primaryLayerName": "对象网关与同步层",
        "nodeCount": 584,
        "fileCount": 96,
        "functionCount": 459,
        "classCount": 29
      },
      {
        "module": "cmd",
        "label": "命令入口",
        "primaryLayerId": "layer:client-and-access",
        "primaryLayerName": "客户端与接入层",
        "nodeCount": 145,
        "fileCount": 86,
        "functionCount": 52,
        "classCount": 0
      },
      {
        "module": "metanode",
        "label": "metanode",
        "primaryLayerId": "layer:storage-and-data-plane",
        "primaryLayerName": "存储与数据平面层",
        "nodeCount": 737,
        "fileCount": 70,
        "functionCount": 642,
        "classCount": 25
      },
      {
        "module": "sdk",
        "label": "SDK",
        "primaryLayerId": "layer:client-and-access",
        "primaryLayerName": "客户端与接入层",
        "nodeCount": 746,
        "fileCount": 68,
        "functionCount": 641,
        "classCount": 37
      },
      {
        "module": "tool",
        "label": "Tooling",
        "primaryLayerId": "layer:engineering-and-ops",
        "primaryLayerName": "工程化与部署层",
        "nodeCount": 330,
        "fileCount": 52,
        "functionCount": 253,
        "classCount": 19
      },
      {
        "module": "docker",
        "label": "docker",
        "primaryLayerId": "layer:engineering-and-ops",
        "primaryLayerName": "工程化与部署层",
        "nodeCount": 158,
        "fileCount": 44,
        "functionCount": 12,
        "classCount": 15
      },
      {
        "module": "remotecache",
        "label": "remotecache",
        "primaryLayerId": "layer:storage-and-data-plane",
        "primaryLayerName": "存储与数据平面层",
        "nodeCount": 381,
        "fileCount": 43,
        "functionCount": 323,
        "classCount": 15
      },
      {
        "module": "datanode",
        "label": "datanode",
        "primaryLayerId": "layer:storage-and-data-plane",
        "primaryLayerName": "存储与数据平面层",
        "nodeCount": 440,
        "fileCount": 37,
        "functionCount": 390,
        "classCount": 13
      },
      {
        "module": "client",
        "label": "client",
        "primaryLayerId": "layer:client-and-access",
        "primaryLayerName": "客户端与接入层",
        "nodeCount": 272,
        "fileCount": 33,
        "functionCount": 230,
        "classCount": 9
      },
      {
        "module": "proto",
        "label": "proto",
        "primaryLayerId": "layer:protocol-and-foundation",
        "primaryLayerName": "协议与基础库层",
        "nodeCount": 291,
        "fileCount": 31,
        "functionCount": 166,
        "classCount": 83
      },
      {
        "module": "cli",
        "label": "CLI",
        "primaryLayerId": "layer:client-and-access",
        "primaryLayerName": "客户端与接入层",
        "nodeCount": 214,
        "fileCount": 30,
        "functionCount": 181,
        "classCount": 1
      },
      {
        "module": "deploy",
        "label": "deploy",
        "primaryLayerId": "layer:engineering-and-ops",
        "primaryLayerName": "工程化与部署层",
        "nodeCount": 44,
        "fileCount": 15,
        "functionCount": 24,
        "classCount": 4
      },
      {
        "module": "authnode",
        "label": "authnode",
        "primaryLayerId": "layer:control-plane",
        "primaryLayerName": "控制面与管理服务层",
        "nodeCount": 52,
        "fileCount": 12,
        "functionCount": 37,
        "classCount": 3
      },
      {
        "module": "lcnode",
        "label": "lcnode",
        "primaryLayerId": "layer:storage-and-data-plane",
        "primaryLayerName": "存储与数据平面层",
        "nodeCount": 58,
        "fileCount": 11,
        "functionCount": 43,
        "classCount": 4
      },
      {
        "module": "shell",
        "label": "shell",
        "primaryLayerId": "layer:client-and-access",
        "primaryLayerName": "客户端与接入层",
        "nodeCount": 11,
        "fileCount": 11,
        "functionCount": 0,
        "classCount": 0
      },
      {
        "module": "test",
        "label": "test",
        "primaryLayerId": "layer:test-and-validation",
        "primaryLayerName": "测试与验证层",
        "nodeCount": 13,
        "fileCount": 10,
        "functionCount": 2,
        "classCount": 0
      },
      {
        "module": "console",
        "label": "console",
        "primaryLayerId": "layer:control-plane",
        "primaryLayerName": "控制面与管理服务层",
        "nodeCount": 49,
        "fileCount": 9,
        "functionCount": 34,
        "classCount": 6
      },
      {
        "module": "raftstore",
        "label": "raftstore",
        "primaryLayerId": "layer:storage-and-data-plane",
        "primaryLayerName": "存储与数据平面层",
        "nodeCount": 44,
        "fileCount": 8,
        "functionCount": 28,
        "classCount": 8
      },
      {
        "module": "(root)",
        "label": "仓库根目录",
        "primaryLayerId": "layer:engineering-and-ops",
        "primaryLayerName": "工程化与部署层",
        "nodeCount": 79,
        "fileCount": 5,
        "functionCount": 1,
        "classCount": 0
      },
      {
        "module": "java",
        "label": "java",
        "primaryLayerId": "layer:client-and-access",
        "primaryLayerName": "客户端与接入层",
        "nodeCount": 17,
        "fileCount": 5,
        "functionCount": 7,
        "classCount": 3
      },
      {
        "module": ".github",
        "label": "GitHub 工作流",
        "primaryLayerId": "layer:engineering-and-ops",
        "primaryLayerName": "工程化与部署层",
        "nodeCount": 21,
        "fileCount": 1,
        "functionCount": 0,
        "classCount": 0
      },
      {
        "module": "security",
        "label": "security",
        "primaryLayerId": "layer:protocol-and-foundation",
        "primaryLayerName": "协议与基础库层",
        "nodeCount": 3,
        "fileCount": 0,
        "functionCount": 0,
        "classCount": 0
      }
    ],
    "edges": [
      {
        "source": "master",
        "sourceLabel": "Master",
        "target": "proto",
        "targetLabel": "proto",
        "count": 2573,
        "weight": 1
      },
      {
        "source": "sdk",
        "sourceLabel": "SDK",
        "target": "proto",
        "targetLabel": "proto",
        "count": 1829,
        "weight": 0.7108
      },
      {
        "source": "metanode",
        "sourceLabel": "metanode",
        "target": "proto",
        "targetLabel": "proto",
        "count": 1581,
        "weight": 0.6145
      },
      {
        "source": "master",
        "sourceLabel": "Master",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 1254,
        "weight": 0.4874
      },
      {
        "source": "remotecache",
        "sourceLabel": "remotecache",
        "target": "proto",
        "targetLabel": "proto",
        "count": 1023,
        "weight": 0.3976
      },
      {
        "source": "sdk",
        "sourceLabel": "SDK",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 980,
        "weight": 0.3809
      },
      {
        "source": "metanode",
        "sourceLabel": "metanode",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 787,
        "weight": 0.3059
      },
      {
        "source": "datanode",
        "sourceLabel": "datanode",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 780,
        "weight": 0.3031
      },
      {
        "source": "objectnode",
        "sourceLabel": "ObjectNode",
        "target": "proto",
        "targetLabel": "proto",
        "count": 775,
        "weight": 0.3012
      },
      {
        "source": "cli",
        "sourceLabel": "CLI",
        "target": "proto",
        "targetLabel": "proto",
        "count": 744,
        "weight": 0.2892
      },
      {
        "source": "datanode",
        "sourceLabel": "datanode",
        "target": "proto",
        "targetLabel": "proto",
        "count": 713,
        "weight": 0.2771
      },
      {
        "source": "remotecache",
        "sourceLabel": "remotecache",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 672,
        "weight": 0.2612
      },
      {
        "source": "tool",
        "sourceLabel": "Tooling",
        "target": "proto",
        "targetLabel": "proto",
        "count": 589,
        "weight": 0.2289
      },
      {
        "source": "client",
        "sourceLabel": "client",
        "target": "proto",
        "targetLabel": "proto",
        "count": 465,
        "weight": 0.1807
      },
      {
        "source": "syncnode",
        "sourceLabel": "SyncNode",
        "target": "proto",
        "targetLabel": "proto",
        "count": 403,
        "weight": 0.1566
      },
      {
        "source": "lcnode",
        "sourceLabel": "lcnode",
        "target": "proto",
        "targetLabel": "proto",
        "count": 341,
        "weight": 0.1325
      },
      {
        "source": "client",
        "sourceLabel": "client",
        "target": "depends",
        "targetLabel": "第三方依赖镜像",
        "count": 322,
        "weight": 0.1251
      },
      {
        "source": "client",
        "sourceLabel": "client",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 319,
        "weight": 0.124
      },
      {
        "source": "objectnode",
        "sourceLabel": "ObjectNode",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 285,
        "weight": 0.1108
      },
      {
        "source": "util",
        "sourceLabel": "通用工具库",
        "target": "proto",
        "targetLabel": "proto",
        "count": 248,
        "weight": 0.0964
      },
      {
        "source": "datanode",
        "sourceLabel": "datanode",
        "target": "depends",
        "targetLabel": "第三方依赖镜像",
        "count": 211,
        "weight": 0.082
      },
      {
        "source": "client",
        "sourceLabel": "client",
        "target": "sdk",
        "targetLabel": "SDK",
        "count": 201,
        "weight": 0.0781
      },
      {
        "source": "authnode",
        "sourceLabel": "authnode",
        "target": "proto",
        "targetLabel": "proto",
        "count": 186,
        "weight": 0.0723
      },
      {
        "source": "cli",
        "sourceLabel": "CLI",
        "target": "sdk",
        "targetLabel": "SDK",
        "count": 174,
        "weight": 0.0676
      },
      {
        "source": "proto",
        "sourceLabel": "proto",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 166,
        "weight": 0.0645
      },
      {
        "source": "tool",
        "sourceLabel": "Tooling",
        "target": "sdk",
        "targetLabel": "SDK",
        "count": 164,
        "weight": 0.0637
      },
      {
        "source": "console",
        "sourceLabel": "console",
        "target": "proto",
        "targetLabel": "proto",
        "count": 155,
        "weight": 0.0602
      },
      {
        "source": "cli",
        "sourceLabel": "CLI",
        "target": "metanode",
        "targetLabel": "metanode",
        "count": 140,
        "weight": 0.0544
      },
      {
        "source": "objectnode",
        "sourceLabel": "ObjectNode",
        "target": "metanode",
        "targetLabel": "metanode",
        "count": 140,
        "weight": 0.0544
      },
      {
        "source": "tool",
        "sourceLabel": "Tooling",
        "target": "metanode",
        "targetLabel": "metanode",
        "count": 140,
        "weight": 0.0544
      },
      {
        "source": "tool",
        "sourceLabel": "Tooling",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 136,
        "weight": 0.0529
      },
      {
        "source": "cli",
        "sourceLabel": "CLI",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 131,
        "weight": 0.0509
      },
      {
        "source": "authnode",
        "sourceLabel": "authnode",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 130,
        "weight": 0.0505
      },
      {
        "source": "raftstore",
        "sourceLabel": "raftstore",
        "target": "depends",
        "targetLabel": "第三方依赖镜像",
        "count": 129,
        "weight": 0.0501
      },
      {
        "source": "syncnode",
        "sourceLabel": "SyncNode",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 123,
        "weight": 0.0478
      },
      {
        "source": "master",
        "sourceLabel": "Master",
        "target": "remotecache",
        "targetLabel": "remotecache",
        "count": 120,
        "weight": 0.0466
      },
      {
        "source": "objectnode",
        "sourceLabel": "ObjectNode",
        "target": "blobstore",
        "targetLabel": "BlobStore",
        "count": 113,
        "weight": 0.0439
      },
      {
        "source": "lcnode",
        "sourceLabel": "lcnode",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 109,
        "weight": 0.0424
      },
      {
        "source": "cmd",
        "sourceLabel": "命令入口",
        "target": "master",
        "targetLabel": "Master",
        "count": 108,
        "weight": 0.042
      },
      {
        "source": "sdk",
        "sourceLabel": "SDK",
        "target": "master",
        "targetLabel": "Master",
        "count": 108,
        "weight": 0.042
      },
      {
        "source": "remotecache",
        "sourceLabel": "remotecache",
        "target": "sdk",
        "targetLabel": "SDK",
        "count": 105,
        "weight": 0.0408
      },
      {
        "source": "cmd",
        "sourceLabel": "命令入口",
        "target": "objectnode",
        "targetLabel": "ObjectNode",
        "count": 96,
        "weight": 0.0373
      },
      {
        "source": "console",
        "sourceLabel": "console",
        "target": "objectnode",
        "targetLabel": "ObjectNode",
        "count": 96,
        "weight": 0.0373
      },
      {
        "source": "metanode",
        "sourceLabel": "metanode",
        "target": "depends",
        "targetLabel": "第三方依赖镜像",
        "count": 93,
        "weight": 0.0361
      },
      {
        "source": "lcnode",
        "sourceLabel": "lcnode",
        "target": "sdk",
        "targetLabel": "SDK",
        "count": 85,
        "weight": 0.033
      },
      {
        "source": "syncnode",
        "sourceLabel": "SyncNode",
        "target": "sdk",
        "targetLabel": "SDK",
        "count": 72,
        "weight": 0.028
      },
      {
        "source": "cmd",
        "sourceLabel": "命令入口",
        "target": "metanode",
        "targetLabel": "metanode",
        "count": 70,
        "weight": 0.0272
      },
      {
        "source": "objectnode",
        "sourceLabel": "ObjectNode",
        "target": "sdk",
        "targetLabel": "SDK",
        "count": 66,
        "weight": 0.0257
      },
      {
        "source": "metanode",
        "sourceLabel": "metanode",
        "target": "datanode",
        "targetLabel": "datanode",
        "count": 60,
        "weight": 0.0233
      },
      {
        "source": "raftstore",
        "sourceLabel": "raftstore",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 57,
        "weight": 0.0222
      },
      {
        "source": "master",
        "sourceLabel": "Master",
        "target": "syncnode",
        "targetLabel": "SyncNode",
        "count": 56,
        "weight": 0.0218
      },
      {
        "source": "cmd",
        "sourceLabel": "命令入口",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 50,
        "weight": 0.0194
      },
      {
        "source": "master",
        "sourceLabel": "Master",
        "target": "depends",
        "targetLabel": "第三方依赖镜像",
        "count": 48,
        "weight": 0.0187
      },
      {
        "source": "client",
        "sourceLabel": "client",
        "target": "blobstore",
        "targetLabel": "BlobStore",
        "count": 47,
        "weight": 0.0183
      },
      {
        "source": "master",
        "sourceLabel": "Master",
        "target": "raftstore",
        "targetLabel": "raftstore",
        "count": 46,
        "weight": 0.0179
      },
      {
        "source": "master",
        "sourceLabel": "Master",
        "target": "sdk",
        "targetLabel": "SDK",
        "count": 46,
        "weight": 0.0179
      },
      {
        "source": "authnode",
        "sourceLabel": "authnode",
        "target": "depends",
        "targetLabel": "第三方依赖镜像",
        "count": 44,
        "weight": 0.0171
      },
      {
        "source": "sdk",
        "sourceLabel": "SDK",
        "target": "blobstore",
        "targetLabel": "BlobStore",
        "count": 43,
        "weight": 0.0167
      },
      {
        "source": "remotecache",
        "sourceLabel": "remotecache",
        "target": "depends",
        "targetLabel": "第三方依赖镜像",
        "count": 41,
        "weight": 0.0159
      },
      {
        "source": "cmd",
        "sourceLabel": "命令入口",
        "target": "remotecache",
        "targetLabel": "remotecache",
        "count": 35,
        "weight": 0.0136
      },
      {
        "source": "sdk",
        "sourceLabel": "SDK",
        "target": "depends",
        "targetLabel": "第三方依赖镜像",
        "count": 33,
        "weight": 0.0128
      },
      {
        "source": "depends",
        "sourceLabel": "第三方依赖镜像",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 32,
        "weight": 0.0124
      },
      {
        "source": "proto",
        "sourceLabel": "proto",
        "target": "depends",
        "targetLabel": "第三方依赖镜像",
        "count": 32,
        "weight": 0.0124
      },
      {
        "source": "sdk",
        "sourceLabel": "SDK",
        "target": "client",
        "targetLabel": "client",
        "count": 32,
        "weight": 0.0124
      },
      {
        "source": "cmd",
        "sourceLabel": "命令入口",
        "target": "proto",
        "targetLabel": "proto",
        "count": 31,
        "weight": 0.012
      },
      {
        "source": "depends",
        "sourceLabel": "第三方依赖镜像",
        "target": "proto",
        "targetLabel": "proto",
        "count": 31,
        "weight": 0.012
      },
      {
        "source": "remotecache",
        "sourceLabel": "remotecache",
        "target": "raftstore",
        "targetLabel": "raftstore",
        "count": 30,
        "weight": 0.0117
      },
      {
        "source": "cmd",
        "sourceLabel": "命令入口",
        "target": "datanode",
        "targetLabel": "datanode",
        "count": 24,
        "weight": 0.0093
      },
      {
        "source": "datanode",
        "sourceLabel": "datanode",
        "target": "raftstore",
        "targetLabel": "raftstore",
        "count": 24,
        "weight": 0.0093
      },
      {
        "source": "metanode",
        "sourceLabel": "metanode",
        "target": "raftstore",
        "targetLabel": "raftstore",
        "count": 24,
        "weight": 0.0093
      },
      {
        "source": "metanode",
        "sourceLabel": "metanode",
        "target": "sdk",
        "targetLabel": "SDK",
        "count": 23,
        "weight": 0.0089
      },
      {
        "source": "authnode",
        "sourceLabel": "authnode",
        "target": "raftstore",
        "targetLabel": "raftstore",
        "count": 22,
        "weight": 0.0086
      },
      {
        "source": "metanode",
        "sourceLabel": "metanode",
        "target": "cmd",
        "targetLabel": "命令入口",
        "count": 20,
        "weight": 0.0078
      },
      {
        "source": "cmd",
        "sourceLabel": "命令入口",
        "target": "syncnode",
        "targetLabel": "SyncNode",
        "count": 18,
        "weight": 0.007
      },
      {
        "source": "util",
        "sourceLabel": "通用工具库",
        "target": "depends",
        "targetLabel": "第三方依赖镜像",
        "count": 15,
        "weight": 0.0058
      },
      {
        "source": "datanode",
        "sourceLabel": "datanode",
        "target": "blobstore",
        "targetLabel": "BlobStore",
        "count": 14,
        "weight": 0.0054
      },
      {
        "source": "console",
        "sourceLabel": "console",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 13,
        "weight": 0.0051
      },
      {
        "source": "master",
        "sourceLabel": "Master",
        "target": "cmd",
        "targetLabel": "命令入口",
        "count": 12,
        "weight": 0.0047
      },
      {
        "source": "cmd",
        "sourceLabel": "命令入口",
        "target": "authnode",
        "targetLabel": "authnode",
        "count": 11,
        "weight": 0.0043
      },
      {
        "source": "cmd",
        "sourceLabel": "命令入口",
        "target": "lcnode",
        "targetLabel": "lcnode",
        "count": 11,
        "weight": 0.0043
      },
      {
        "source": "console",
        "sourceLabel": "console",
        "target": "sdk",
        "targetLabel": "SDK",
        "count": 10,
        "weight": 0.0039
      },
      {
        "source": "master",
        "sourceLabel": "Master",
        "target": "datanode",
        "targetLabel": "datanode",
        "count": 10,
        "weight": 0.0039
      },
      {
        "source": "sdk",
        "sourceLabel": "SDK",
        "target": "datanode",
        "targetLabel": "datanode",
        "count": 10,
        "weight": 0.0039
      },
      {
        "source": "tool",
        "sourceLabel": "Tooling",
        "target": "datanode",
        "targetLabel": "datanode",
        "count": 10,
        "weight": 0.0039
      },
      {
        "source": "util",
        "sourceLabel": "通用工具库",
        "target": "blobstore",
        "targetLabel": "BlobStore",
        "count": 10,
        "weight": 0.0039
      },
      {
        "source": "metanode",
        "sourceLabel": "metanode",
        "target": "blobstore",
        "targetLabel": "BlobStore",
        "count": 9,
        "weight": 0.0035
      },
      {
        "source": "cli",
        "sourceLabel": "CLI",
        "target": "depends",
        "targetLabel": "第三方依赖镜像",
        "count": 8,
        "weight": 0.0031
      },
      {
        "source": "client",
        "sourceLabel": "client",
        "target": "cmd",
        "targetLabel": "命令入口",
        "count": 8,
        "weight": 0.0031
      },
      {
        "source": "datanode",
        "sourceLabel": "datanode",
        "target": "cmd",
        "targetLabel": "命令入口",
        "count": 8,
        "weight": 0.0031
      },
      {
        "source": "objectnode",
        "sourceLabel": "ObjectNode",
        "target": "client",
        "targetLabel": "client",
        "count": 8,
        "weight": 0.0031
      },
      {
        "source": "remotecache",
        "sourceLabel": "remotecache",
        "target": "cmd",
        "targetLabel": "命令入口",
        "count": 8,
        "weight": 0.0031
      },
      {
        "source": "syncnode",
        "sourceLabel": "SyncNode",
        "target": "cmd",
        "targetLabel": "命令入口",
        "count": 8,
        "weight": 0.0031
      },
      {
        "source": "tool",
        "sourceLabel": "Tooling",
        "target": "remotecache",
        "targetLabel": "remotecache",
        "count": 8,
        "weight": 0.0031
      },
      {
        "source": "datanode",
        "sourceLabel": "datanode",
        "target": "sdk",
        "targetLabel": "SDK",
        "count": 7,
        "weight": 0.0027
      },
      {
        "source": "util",
        "sourceLabel": "通用工具库",
        "target": "raftstore",
        "targetLabel": "raftstore",
        "count": 6,
        "weight": 0.0023
      },
      {
        "source": "lcnode",
        "sourceLabel": "lcnode",
        "target": "blobstore",
        "targetLabel": "BlobStore",
        "count": 5,
        "weight": 0.0019
      },
      {
        "source": "cmd",
        "sourceLabel": "命令入口",
        "target": "console",
        "targetLabel": "console",
        "count": 4,
        "weight": 0.0016
      },
      {
        "source": "deploy",
        "sourceLabel": "deploy",
        "target": "util",
        "targetLabel": "通用工具库",
        "count": 4,
        "weight": 0.0016
      },
      {
        "source": "lcnode",
        "sourceLabel": "lcnode",
        "target": "cmd",
        "targetLabel": "命令入口",
        "count": 4,
        "weight": 0.0016
      },
      {
        "source": "objectnode",
        "sourceLabel": "ObjectNode",
        "target": "cmd",
        "targetLabel": "命令入口",
        "count": 4,
        "weight": 0.0016
      },
      {
        "source": "cli",
        "sourceLabel": "CLI",
        "target": "blobstore",
        "targetLabel": "BlobStore",
        "count": 2,
        "weight": 0.0008
      }
    ]
  },
  "topImportedFiles": [
    {
      "id": "file:proto/admin_proto_test.go",
      "path": "proto/admin_proto_test.go",
      "name": "admin_proto_test.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的测试文件，负责覆盖 admin_proto_test.go 关联逻辑并验证关键行为与边界条件。"
    },
    {
      "id": "file:proto/admin_proto.go",
      "path": "proto/admin_proto.go",
      "name": "admin_proto.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 协议结构与序列化定义，是该子系统实现链路的一部分。 该文件提取到 36 个函数和 108 个类型/类定义。"
    },
    {
      "id": "file:proto/admin_task.go",
      "path": "proto/admin_task.go",
      "name": "admin_task.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 后台任务与调度流程，是该子系统实现链路的一部分。 该文件提取到 14 个函数和 1 个类型/类定义。"
    },
    {
      "id": "file:proto/auth_proto.go",
      "path": "proto/auth_proto.go",
      "name": "auth_proto.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 认证、鉴权与策略控制，是该子系统实现链路的一部分。 该文件提取到 22 个函数和 11 个类型/类定义。"
    },
    {
      "id": "file:proto/cluster_balance.go",
      "path": "proto/cluster_balance.go",
      "name": "cluster_balance.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 0 个函数和 6 个类型/类定义。"
    },
    {
      "id": "file:proto/distributed_cache_test.go",
      "path": "proto/distributed_cache_test.go",
      "name": "distributed_cache_test.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的测试文件，负责覆盖 distributed_cache_test.go 关联逻辑并验证关键行为与边界条件。"
    },
    {
      "id": "file:proto/distributed_cache.go",
      "path": "proto/distributed_cache.go",
      "name": "distributed_cache.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 缓存与热点数据处理，是该子系统实现链路的一部分。 该文件提取到 43 个函数和 22 个类型/类定义。"
    },
    {
      "id": "file:proto/distributed_cache.pb.go",
      "path": "proto/distributed_cache.pb.go",
      "name": "distributed_cache.pb.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 缓存与热点数据处理，是该子系统实现链路的一部分。 该文件提取到 176 个函数和 10 个类型/类定义。"
    },
    {
      "id": "file:proto/errors.go",
      "path": "proto/errors.go",
      "name": "errors.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 2 个函数和 1 个类型/类定义。"
    },
    {
      "id": "file:proto/extent_key_test.go",
      "path": "proto/extent_key_test.go",
      "name": "extent_key_test.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的测试文件，负责覆盖 extent_key_test.go 关联逻辑并验证关键行为与边界条件。"
    },
    {
      "id": "file:proto/extent_key.go",
      "path": "proto/extent_key.go",
      "name": "extent_key.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 28 个函数和 5 个类型/类定义。"
    },
    {
      "id": "file:proto/fs_proto.go",
      "path": "proto/fs_proto.go",
      "name": "fs_proto.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 协议结构与序列化定义，是该子系统实现链路的一部分。 该文件提取到 25 个函数和 130 个类型/类定义。"
    },
    {
      "id": "file:proto/header.go",
      "path": "proto/header.go",
      "name": "header.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。"
    },
    {
      "id": "file:proto/hybridcloud_obj_extent_key.go",
      "path": "proto/hybridcloud_obj_extent_key.go",
      "name": "hybridcloud_obj_extent_key.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 0 个函数和 1 个类型/类定义。"
    },
    {
      "id": "file:proto/lifecycle.go",
      "path": "proto/lifecycle.go",
      "name": "lifecycle.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 10 个函数和 10 个类型/类定义。"
    },
    {
      "id": "file:proto/meta_proto.go",
      "path": "proto/meta_proto.go",
      "name": "meta_proto.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 协议结构与序列化定义，是该子系统实现链路的一部分。 该文件提取到 7 个函数和 16 个类型/类定义。"
    },
    {
      "id": "file:proto/model.go",
      "path": "proto/model.go",
      "name": "model.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 5 个函数和 53 个类型/类定义。"
    },
    {
      "id": "file:proto/mount_options.go",
      "path": "proto/mount_options.go",
      "name": "mount_options.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 9 个函数和 2 个类型/类定义。"
    },
    {
      "id": "file:proto/obj_extent_key.go",
      "path": "proto/obj_extent_key.go",
      "name": "obj_extent_key.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 6 个函数和 2 个类型/类定义。"
    },
    {
      "id": "file:proto/packet.go",
      "path": "proto/packet.go",
      "name": "packet.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 57 个函数和 1 个类型/类定义。"
    },
    {
      "id": "file:proto/perm_action.go",
      "path": "proto/perm_action.go",
      "name": "perm_action.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 16 个函数和 0 个类型/类定义。"
    },
    {
      "id": "file:proto/s3qos.go",
      "path": "proto/s3qos.go",
      "name": "s3qos.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 1 个函数和 3 个类型/类定义。"
    },
    {
      "id": "file:proto/snapshot.go",
      "path": "proto/snapshot.go",
      "name": "snapshot.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 快照、持久化与恢复，是该子系统实现链路的一部分。 该文件提取到 0 个函数和 4 个类型/类定义。"
    },
    {
      "id": "file:proto/status.go",
      "path": "proto/status.go",
      "name": "status.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。"
    },
    {
      "id": "file:proto/sync_prefix.go",
      "path": "proto/sync_prefix.go",
      "name": "sync_prefix.go",
      "module": "proto",
      "moduleLabel": "proto",
      "count": 378,
      "summary": "协议定义层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 0 个函数和 2 个类型/类定义。"
    }
  ],
  "topImportingFiles": [
    {
      "id": "file:cmd/cmd.go",
      "path": "cmd/cmd.go",
      "name": "cmd.go",
      "module": "cmd",
      "moduleLabel": "命令入口",
      "count": 438,
      "summary": "进程启动与命令装配层中的代码文件，主要承担 进程入口与命令装配，是该子系统实现链路的一部分。 该文件提取到 8 个函数和 0 个类型/类定义。"
    },
    {
      "id": "file:objectnode/fs_volume.go",
      "path": "objectnode/fs_volume.go",
      "name": "fs_volume.go",
      "module": "objectnode",
      "moduleLabel": "ObjectNode",
      "count": 190,
      "summary": "对象访问网关子系统中的代码文件，主要承担 卷管理与容量操作，是该子系统实现链路的一部分。 该文件提取到 66 个函数和 7 个类型/类定义。"
    },
    {
      "id": "file:client/fuse.go",
      "path": "client/fuse.go",
      "name": "fuse.go",
      "module": "client",
      "moduleLabel": "client",
      "count": 188,
      "summary": "客户端文件系统层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 24 个函数和 0 个类型/类定义。"
    },
    {
      "id": "file:blobstore/clustermgr/svr.go",
      "path": "blobstore/clustermgr/svr.go",
      "name": "svr.go",
      "module": "blobstore",
      "moduleLabel": "BlobStore",
      "count": 183,
      "summary": "BlobStore 存储子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 16 个函数和 3 个类型/类定义。"
    },
    {
      "id": "file:client/fs/super.go",
      "path": "client/fs/super.go",
      "name": "super.go",
      "module": "client",
      "moduleLabel": "client",
      "count": 160,
      "summary": "客户端文件系统层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 32 个函数和 1 个类型/类定义。"
    },
    {
      "id": "file:syncnode/server.go",
      "path": "syncnode/server.go",
      "name": "server.go",
      "module": "syncnode",
      "moduleLabel": "SyncNode",
      "count": 158,
      "summary": "同步节点子系统中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 22 个函数和 1 个类型/类定义。"
    },
    {
      "id": "file:tool/fsck/cmd/gc.go",
      "path": "tool/fsck/cmd/gc.go",
      "name": "gc.go",
      "module": "tool",
      "moduleLabel": "Tooling",
      "count": 155,
      "summary": "开发工具层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 47 个函数和 4 个类型/类定义。"
    },
    {
      "id": "file:client/libsdk/libsdk.go",
      "path": "client/libsdk/libsdk.go",
      "name": "libsdk.go",
      "module": "client",
      "moduleLabel": "client",
      "count": 140,
      "summary": "客户端文件系统层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 62 个函数和 4 个类型/类定义。"
    },
    {
      "id": "file:sdk/graphql/general.go",
      "path": "sdk/graphql/general.go",
      "name": "general.go",
      "module": "sdk",
      "moduleLabel": "SDK",
      "count": 139,
      "summary": "SDK 接入层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 22 个函数和 3 个类型/类定义。"
    },
    {
      "id": "file:client/gosdk/cfs_client.go",
      "path": "client/gosdk/cfs_client.go",
      "name": "cfs_client.go",
      "module": "client",
      "moduleLabel": "client",
      "count": 137,
      "summary": "客户端文件系统层中的代码文件，主要承担 客户端调用与远端访问，是该子系统实现链路的一部分。 该文件提取到 44 个函数和 1 个类型/类定义。"
    },
    {
      "id": "file:console/service/file.go",
      "path": "console/service/file.go",
      "name": "file.go",
      "module": "console",
      "moduleLabel": "console",
      "count": 136,
      "summary": "控制台服务层中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 15 个函数和 3 个类型/类定义。"
    },
    {
      "id": "file:sdk/data/stream/extent_client.go",
      "path": "sdk/data/stream/extent_client.go",
      "name": "extent_client.go",
      "module": "sdk",
      "moduleLabel": "SDK",
      "count": 136,
      "summary": "SDK 接入层中的代码文件，主要承担 客户端调用与远端访问，是该子系统实现链路的一部分。 该文件提取到 54 个函数和 3 个类型/类定义。"
    },
    {
      "id": "file:blobstore/blobnode/startup.go",
      "path": "blobstore/blobnode/startup.go",
      "name": "startup.go",
      "module": "blobstore",
      "moduleLabel": "BlobStore",
      "count": 124,
      "summary": "BlobStore 存储子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 10 个函数和 0 个类型/类定义。"
    },
    {
      "id": "file:datanode/wrap_operator.go",
      "path": "datanode/wrap_operator.go",
      "name": "wrap_operator.go",
      "module": "datanode",
      "moduleLabel": "datanode",
      "count": 124,
      "summary": "数据节点子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 52 个函数和 0 个类型/类定义。"
    },
    {
      "id": "file:objectnode/server.go",
      "path": "objectnode/server.go",
      "name": "server.go",
      "module": "objectnode",
      "moduleLabel": "ObjectNode",
      "count": 123,
      "summary": "对象访问网关子系统中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 13 个函数和 1 个类型/类定义。"
    },
    {
      "id": "file:master/cluster.go",
      "path": "master/cluster.go",
      "name": "cluster.go",
      "module": "master",
      "moduleLabel": "Master",
      "count": 122,
      "summary": "主控与调度子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 256 个函数和 13 个类型/类定义。"
    },
    {
      "id": "file:blobstore/access/server.go",
      "path": "blobstore/access/server.go",
      "name": "server.go",
      "module": "blobstore",
      "moduleLabel": "BlobStore",
      "count": 121,
      "summary": "BlobStore 存储子系统中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 14 个函数和 3 个类型/类定义。"
    },
    {
      "id": "file:datanode/server.go",
      "path": "datanode/server.go",
      "name": "server.go",
      "module": "datanode",
      "moduleLabel": "datanode",
      "count": 120,
      "summary": "数据节点子系统中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 39 个函数和 3 个类型/类定义。"
    },
    {
      "id": "file:blobstore/clustermgr/volumemgr/volumemgr_test.go",
      "path": "blobstore/clustermgr/volumemgr/volumemgr_test.go",
      "name": "volumemgr_test.go",
      "module": "blobstore",
      "moduleLabel": "BlobStore",
      "count": 117,
      "summary": "BlobStore 存储子系统中的测试文件，负责覆盖 volumemgr_test.go 关联逻辑并验证关键行为与边界条件。"
    },
    {
      "id": "file:metanode/manager.go",
      "path": "metanode/manager.go",
      "name": "manager.go",
      "module": "metanode",
      "moduleLabel": "metanode",
      "count": 117,
      "summary": "元数据节点子系统中的代码文件，主要承担 管理器状态编排，是该子系统实现链路的一部分。 该文件提取到 35 个函数和 4 个类型/类定义。"
    },
    {
      "id": "file:datanode/disk.go",
      "path": "datanode/disk.go",
      "name": "disk.go",
      "module": "datanode",
      "moduleLabel": "datanode",
      "count": 115,
      "summary": "数据节点子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 61 个函数和 2 个类型/类定义。"
    },
    {
      "id": "file:datanode/repl/packet.go",
      "path": "datanode/repl/packet.go",
      "name": "packet.go",
      "module": "datanode",
      "moduleLabel": "datanode",
      "count": 114,
      "summary": "数据节点子系统中的代码文件，主要承担 核心实现逻辑，是该子系统实现链路的一部分。 该文件提取到 65 个函数和 2 个类型/类定义。"
    },
    {
      "id": "file:metanode/partition.go",
      "path": "metanode/partition.go",
      "name": "partition.go",
      "module": "metanode",
      "moduleLabel": "metanode",
      "count": 114,
      "summary": "元数据节点子系统中的代码文件，主要承担 分区元数据与分片操作，是该子系统实现链路的一部分。 该文件提取到 87 个函数和 16 个类型/类定义。"
    },
    {
      "id": "file:blobstore/blobnode/svr_test.go",
      "path": "blobstore/blobnode/svr_test.go",
      "name": "svr_test.go",
      "module": "blobstore",
      "moduleLabel": "BlobStore",
      "count": 113,
      "summary": "BlobStore 存储子系统中的测试文件，负责覆盖 svr_test.go 关联逻辑并验证关键行为与边界条件。"
    },
    {
      "id": "file:lcnode/server.go",
      "path": "lcnode/server.go",
      "name": "server.go",
      "module": "lcnode",
      "moduleLabel": "lcnode",
      "count": 113,
      "summary": "生命周期节点子系统中的代码文件，主要承担 服务启动与对外暴露，是该子系统实现链路的一部分。 该文件提取到 17 个函数和 1 个类型/类定义。"
    }
  ],
  "crossModuleEdges": [
    {
      "source": "master",
      "sourceLabel": "Master",
      "target": "proto",
      "targetLabel": "proto",
      "count": 2573,
      "weight": 1
    },
    {
      "source": "sdk",
      "sourceLabel": "SDK",
      "target": "proto",
      "targetLabel": "proto",
      "count": 1829,
      "weight": 0.7108
    },
    {
      "source": "metanode",
      "sourceLabel": "metanode",
      "target": "proto",
      "targetLabel": "proto",
      "count": 1581,
      "weight": 0.6145
    },
    {
      "source": "master",
      "sourceLabel": "Master",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 1254,
      "weight": 0.4874
    },
    {
      "source": "remotecache",
      "sourceLabel": "remotecache",
      "target": "proto",
      "targetLabel": "proto",
      "count": 1023,
      "weight": 0.3976
    },
    {
      "source": "sdk",
      "sourceLabel": "SDK",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 980,
      "weight": 0.3809
    },
    {
      "source": "metanode",
      "sourceLabel": "metanode",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 787,
      "weight": 0.3059
    },
    {
      "source": "datanode",
      "sourceLabel": "datanode",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 780,
      "weight": 0.3031
    },
    {
      "source": "objectnode",
      "sourceLabel": "ObjectNode",
      "target": "proto",
      "targetLabel": "proto",
      "count": 775,
      "weight": 0.3012
    },
    {
      "source": "cli",
      "sourceLabel": "CLI",
      "target": "proto",
      "targetLabel": "proto",
      "count": 744,
      "weight": 0.2892
    },
    {
      "source": "datanode",
      "sourceLabel": "datanode",
      "target": "proto",
      "targetLabel": "proto",
      "count": 713,
      "weight": 0.2771
    },
    {
      "source": "remotecache",
      "sourceLabel": "remotecache",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 672,
      "weight": 0.2612
    },
    {
      "source": "tool",
      "sourceLabel": "Tooling",
      "target": "proto",
      "targetLabel": "proto",
      "count": 589,
      "weight": 0.2289
    },
    {
      "source": "client",
      "sourceLabel": "client",
      "target": "proto",
      "targetLabel": "proto",
      "count": 465,
      "weight": 0.1807
    },
    {
      "source": "syncnode",
      "sourceLabel": "SyncNode",
      "target": "proto",
      "targetLabel": "proto",
      "count": 403,
      "weight": 0.1566
    },
    {
      "source": "lcnode",
      "sourceLabel": "lcnode",
      "target": "proto",
      "targetLabel": "proto",
      "count": 341,
      "weight": 0.1325
    },
    {
      "source": "client",
      "sourceLabel": "client",
      "target": "depends",
      "targetLabel": "第三方依赖镜像",
      "count": 322,
      "weight": 0.1251
    },
    {
      "source": "client",
      "sourceLabel": "client",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 319,
      "weight": 0.124
    },
    {
      "source": "objectnode",
      "sourceLabel": "ObjectNode",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 285,
      "weight": 0.1108
    },
    {
      "source": "util",
      "sourceLabel": "通用工具库",
      "target": "proto",
      "targetLabel": "proto",
      "count": 248,
      "weight": 0.0964
    },
    {
      "source": "datanode",
      "sourceLabel": "datanode",
      "target": "depends",
      "targetLabel": "第三方依赖镜像",
      "count": 211,
      "weight": 0.082
    },
    {
      "source": "client",
      "sourceLabel": "client",
      "target": "sdk",
      "targetLabel": "SDK",
      "count": 201,
      "weight": 0.0781
    },
    {
      "source": "authnode",
      "sourceLabel": "authnode",
      "target": "proto",
      "targetLabel": "proto",
      "count": 186,
      "weight": 0.0723
    },
    {
      "source": "cli",
      "sourceLabel": "CLI",
      "target": "sdk",
      "targetLabel": "SDK",
      "count": 174,
      "weight": 0.0676
    },
    {
      "source": "proto",
      "sourceLabel": "proto",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 166,
      "weight": 0.0645
    },
    {
      "source": "tool",
      "sourceLabel": "Tooling",
      "target": "sdk",
      "targetLabel": "SDK",
      "count": 164,
      "weight": 0.0637
    },
    {
      "source": "console",
      "sourceLabel": "console",
      "target": "proto",
      "targetLabel": "proto",
      "count": 155,
      "weight": 0.0602
    },
    {
      "source": "cli",
      "sourceLabel": "CLI",
      "target": "metanode",
      "targetLabel": "metanode",
      "count": 140,
      "weight": 0.0544
    },
    {
      "source": "objectnode",
      "sourceLabel": "ObjectNode",
      "target": "metanode",
      "targetLabel": "metanode",
      "count": 140,
      "weight": 0.0544
    },
    {
      "source": "tool",
      "sourceLabel": "Tooling",
      "target": "metanode",
      "targetLabel": "metanode",
      "count": 140,
      "weight": 0.0544
    },
    {
      "source": "tool",
      "sourceLabel": "Tooling",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 136,
      "weight": 0.0529
    },
    {
      "source": "cli",
      "sourceLabel": "CLI",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 131,
      "weight": 0.0509
    },
    {
      "source": "authnode",
      "sourceLabel": "authnode",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 130,
      "weight": 0.0505
    },
    {
      "source": "raftstore",
      "sourceLabel": "raftstore",
      "target": "depends",
      "targetLabel": "第三方依赖镜像",
      "count": 129,
      "weight": 0.0501
    },
    {
      "source": "syncnode",
      "sourceLabel": "SyncNode",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 123,
      "weight": 0.0478
    },
    {
      "source": "master",
      "sourceLabel": "Master",
      "target": "remotecache",
      "targetLabel": "remotecache",
      "count": 120,
      "weight": 0.0466
    },
    {
      "source": "objectnode",
      "sourceLabel": "ObjectNode",
      "target": "blobstore",
      "targetLabel": "BlobStore",
      "count": 113,
      "weight": 0.0439
    },
    {
      "source": "lcnode",
      "sourceLabel": "lcnode",
      "target": "util",
      "targetLabel": "通用工具库",
      "count": 109,
      "weight": 0.0424
    },
    {
      "source": "cmd",
      "sourceLabel": "命令入口",
      "target": "master",
      "targetLabel": "Master",
      "count": 108,
      "weight": 0.042
    },
    {
      "source": "sdk",
      "sourceLabel": "SDK",
      "target": "master",
      "targetLabel": "Master",
      "count": 108,
      "weight": 0.042
    }
  ]
};
