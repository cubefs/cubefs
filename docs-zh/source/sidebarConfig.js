module.exports = [
    {
        text: '概览',
        children: [
            'overview/introduction.md',
            'overview/architecture.md',
        ]
    },
    {
        text: '快速上手',
        children: [
            'quickstart/requirement.md',
            'quickstart/single-deploy.md',
            'quickstart/cluster-deploy.md',
            'quickstart/verify.md',
        ]
    },
    {
        text: '部署',
        children: [
            'deploy/env.md',
            'deploy/yum.md',
            'deploy/k8s.md',
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
            'user-guide/flashnode.md',
            'user-guide/authnode.md',
            'user-guide/gui.md',
            {
                text: 'CLI 使用',
                children: [
                    'user-guide/cli/overview.md',
                    'user-guide/cli/cluster.md',
                    'user-guide/cli/metanode.md',
                    'user-guide/cli/datanode.md',
                    'user-guide/cli/metapartition.md',
                    'user-guide/cli/datapartition.md',
                    'user-guide/cli/flashnode.md',
                    'user-guide/cli/config.md',
                    'user-guide/cli/volume.md',
                    'user-guide/cli/user.md',
                    'user-guide/cli/nodeset.md',
                    'user-guide/cli/quota.md',
                    'user-guide/cli/blobstore-cli.md',
                ]
            },
        ]
    },
    {
        text: '模块设计',
        children: [
            'design/master.md',
            'design/metanode.md',
            'design/datanode.md',
            'design/blobstore.md',
            'design/objectnode.md',
            'design/client.md',
            'design/lcnode.md',
            'design/authnode.md',
            'design/kernelclient.md',
        ]
    },
    {
        text: '重要特性',
        children: [
            'feature/cache.md',
            'feature/flash.md',
            'feature/qos.md',
            'feature/quota.md',
            'feature/trash.md',
            'feature/autofs.md',
            'feature/hybridcloud.md',
        ]
    },
    {
        text: '性能测试&调优',
        children: [
            'evaluation/env.md',
            'evaluation/tiny.md',
            'evaluation/io.md',
            'evaluation/meta.md',
            'evaluation/fuse.md',
        ]
    },
    {
        text: '运维',
        children: [
            'ops/capacity.md',
            'ops/zone.md',
            'ops/log.md',
            {
                text: '配置管理',
                children: [
                    'ops/configs/master.md',
                    'ops/configs/metanode.md',
                    'ops/configs/datanode.md',
                    'ops/configs/objectnode.md',
                    'ops/configs/client.md',
                    'ops/configs/lcnode.md',
                    'ops/configs/flashnode.md',
                    'ops/configs/blobstore/base.md',
                    'ops/configs/blobstore/rpc.md',
                    'ops/configs/blobstore/cm.md',
                    'ops/configs/blobstore/access.md',
                    'ops/configs/blobstore/proxy.md',
                    'ops/configs/blobstore/blobnode.md',
                    'ops/configs/blobstore/scheduler.md',
                    'ops/configs/config.md',
                ]
            },
            'ops/auto-ops.md',
        ]
    },
    {
        text: '安全',
        children: [
            'security/security_practice.md',
        ]
    },
    {
        text: '生态对接',
        children: [
            'ecology/hadoop.md',
            'ecology/k8s.md',
            'ecology/prometheus.md',
            'ecology/grafana.md',

        ]
    },
    {
        text: '开发指南',
        children: [
            'dev-guide/code.md',
            {
                text: '管理 API',
                children: [
                    'dev-guide/admin-api/master/cluster.md',
                    'dev-guide/admin-api/master/metanode.md',
                    'dev-guide/admin-api/master/datanode.md',
                    'dev-guide/admin-api/master/volume.md',
                    'dev-guide/admin-api/master/meta-partition.md',
                    'dev-guide/admin-api/master/data-partition.md',
                    'dev-guide/admin-api/master/management.md',
                    'dev-guide/admin-api/master/user.md',
                    'dev-guide/admin-api/master/failureDomain.md',
                    'dev-guide/admin-api/metanode/partition.md',
                    'dev-guide/admin-api/metanode/inode.md',
                    'dev-guide/admin-api/metanode/dentry.md',
                    'dev-guide/admin-api/blobstore/base.md',
                    'dev-guide/admin-api/blobstore/cm.md',
                    'dev-guide/admin-api/blobstore/blobnode.md',
                    'dev-guide/admin-api/blobstore/access.md',
                    'dev-guide/admin-api/blobstore/scheduler.md',
                ]
            },
        ]
    },
    {
        text: '常见问题',
        children: [
            'faq/development.md',
            'faq/build.md',
            'faq/fuse.md',
            'faq/kafka.md',
            'faq/troubleshoot/strategy.md',
            'faq/troubleshoot/case.md',
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
        text: '版本更新',
        children: [
            'version/plan.md',
        ]
    }
]
