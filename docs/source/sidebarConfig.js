module.exports = [
    {
        text: 'Overview',
        children: [
            'overview/introduction.md',
            'overview/architecture.md',
        ]
    },
    {
        text: 'Quick Start',
        children: [
            'quickstart/requirement.md',
            'quickstart/single-deploy.md',
            'quickstart/cluster-deploy.md',
            'quickstart/verify.md',
        ]
    },
    {
        text: 'Deployment',
        children: [
            'deploy/env.md',
            'deploy/yum.md',
            'deploy/k8s.md',
            'deploy/upgrade.md',
        ]
    },
    {
        text: 'User Guide',
        children: [
            'user-guide/volume.md',
            'user-guide/file.md',
            'user-guide/objectnode.md',
            'user-guide/blobstore.md',
            'user-guide/flashnode.md',
            'user-guide/authnode.md',
            'user-guide/gui.md',
            {
                text: 'Cli Guide',
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
        text: 'Design',
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
        text: 'Feature',
        children: [
            'feature/cache.md',
            'feature/flash.md',
            'feature/qos.md',
            'feature/quota.md',
            'feature/trash.md',
            'feature/autofs.md',
            "feature/hybridcloud.md",
        ]
    },
    {
        text: 'Test and Evaluation',
        children: [
            'evaluation/env.md',
            'evaluation/tiny.md',
            'evaluation/io.md',
            'evaluation/meta.md',
            'evaluation/fuse.md',
        ]
    },
    {
        text: 'Ops',
        children: [
            'ops/capacity.md',
            'ops/zone.md',
            'ops/log.md',
            {
                text: 'Config Manage',
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
        text: 'Security',
        children: [
            'security/security_practice.md',
        ]
    },
    {
        text: 'Ecology',
        children: [
            'ecology/hadoop.md',
            'ecology/k8s.md',
	    'ecology/prometheus.md',
	    'ecology/grafana.md',
        ]
    },
    {
        text: 'Dev Guide',
        children: [
            'dev-guide/code.md',
            {
                text: 'Manage API',
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
        text: 'FAQ',
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
        text: 'Community',
        children: [
            'community/overview.md',
            'community/article.md',
        ]
    },
    {
        text: 'Version',
        children: [
            'version/plan.md',
        ]
    }
]
