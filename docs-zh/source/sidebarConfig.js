module.exports = [
    {
        text: '概览',
        children: [
            'overview/introduction.md',
            'overview/architecture.md',
            'overview/plan.md',
        ]
    },
    {
        text: '快速入门',
        children: [
            'quick-start/requirement.md',
            'quick-start/node.md',
            'quick-start/manual-deploy.md',
            'quick-start/verify.md',
        ]
    },
    {
        text: '集群部署',
        children: [
            'deploy/env.md',
            'deploy/yum.md',
            'deploy/k8s.md',
            {
                text: '可视化监控',
                children: [
                    'deploy/metrics/metrics.md',
                    'deploy/metrics/collect.md',
                    'deploy/metrics/view.md',
                ]
            },
            'deploy/upgrade.md',
        ]
    },
    {
        text: '使用指南',
        children: [
            'user-guide/volume.md',
            'user-guide/file.md',
            'user-guide/objectnode.md',
            'user-guide/blobstore.md',
            'user-guide/hadoop.md',
            'user-guide/k8s.md',
            'user-guide/atomicity.md',
            'user-guide/quota.md',
            'user-guide/qos.md',
            'user-guide/trash.md',
            {
                text: '优化',
                children: [
                    'user-guide/optimization/fuse.md',
                    'user-guide/optimization/cache.md',
                    'user-guide/optimization/autofs.md',
                ]
            },
            'user-guide/authnode.md',
        ]
    },
    {
        text: '运维指南',
        children: [
            'maintenance/capacity.md',
            'maintenance/zone.md',
            'maintenance/log.md',
            'maintenance/code.md',
	        'maintenance/security_practice.md',
            {
                text: '服务管理',
                children: [
                    'maintenance/admin-api/master/cluster.md',
                    'maintenance/admin-api/master/metanode.md',
                    'maintenance/admin-api/master/datanode.md',
                    'maintenance/admin-api/master/volume.md',
                    'maintenance/admin-api/master/meta-partition.md',
                    'maintenance/admin-api/master/data-partition.md',
                    'maintenance/admin-api/master/management.md',
                    'maintenance/admin-api/master/user.md',
                    'maintenance/admin-api/master/failureDomain.md',
                    'maintenance/admin-api/metanode/partition.md',
                    'maintenance/admin-api/metanode/inode.md',
                    'maintenance/admin-api/metanode/dentry.md',
                    'maintenance/admin-api/blobstore/base.md',
                    'maintenance/admin-api/blobstore/cm.md',
                    'maintenance/admin-api/blobstore/blobnode.md',
                    'maintenance/admin-api/blobstore/access.md',
                    'maintenance/admin-api/blobstore/scheduler.md',
                ]
            },
            {
                text: '配置管理',
                children: [
                    'maintenance/configs/master.md',
                    'maintenance/configs/metanode.md',
                    'maintenance/configs/datanode.md',
                    'maintenance/configs/objectnode.md',
                    'maintenance/configs/client.md',
                    'maintenance/configs/blobstore/base.md',
                    'maintenance/configs/blobstore/rpc.md',
                    'maintenance/configs/blobstore/cm.md',
                    'maintenance/configs/blobstore/access.md',
                    'maintenance/configs/blobstore/proxy.md',
                    'maintenance/configs/blobstore/blobnode.md',
                    'maintenance/configs/blobstore/scheduler.md',
                    'maintenance/configs/config.md',
                ]
            },
            {
                text: '问题排查',
                children: [
                    'maintenance/troubleshoot/strategy.md',
                    'maintenance/troubleshoot/case.md',
                ]
            },
        ]
    },
    {
        text: '管理工具',
        children: [
            'tools/gui.md',
            {
                text: 'cfs-cli工具使用',
                children: [
                    'tools/cfs-cli/overview.md',
                    'tools/cfs-cli/cluster.md',
                    'tools/cfs-cli/metanode.md',
                    'tools/cfs-cli/datanode.md',
                    'tools/cfs-cli/metapartition.md',
                    'tools/cfs-cli/datapartition.md',
                    'tools/cfs-cli/config.md',
                    'tools/cfs-cli/volume.md',
                    'tools/cfs-cli/user.md',
                    'tools/cfs-cli/nodeset.md',
                    'tools/cfs-cli/quota.md',
                ]
            },
            'tools/blobstore-cli.md',
        ]
    },
    {
        text: '测试评估',
        children: [
            'evaluation/env.md',
            'evaluation/tiny.md',
            'evaluation/io.md',
            'evaluation/meta.md',
        ]
    },
    {
        text: '设计文档',
        children: [
            'design/master.md',
            'design/metanode.md',
            'design/datanode.md',
            'design/blobstore.md',
            'design/objectnode.md',
            'design/client.md',
            'design/authnode.md',
        ]
    },
    {
        text: '社区',
        children: [
            'community/overview.md',
            'community/article.md',
        ]
    },
    {
        text: 'FAQ',
        children: [
            'faq/development.md',
            'faq/build.md',
            'faq/fuse.md',
            'faq/kafka.md',
        ]
    }
]
