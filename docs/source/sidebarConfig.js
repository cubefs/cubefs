module.exports = [
    {
        text: 'Overview',
        children: [
            'overview/introduction.md',
            'overview/architecture.md',
            'overview/plan.md',
        ]
    },
    {
        text: 'Quick Start',
        children: [
            'quick-start/requirement.md',
            'quick-start/node.md',
            'quick-start/manual-deploy.md',
            'quick-start/verify.md',
        ]
    },
    {
        text: 'Cluster Deployment',
        children: [
            'deploy/env.md',
            'deploy/yum.md',
            'deploy/k8s.md',
            {
                text: 'Monitoring and Visualization',
                children: [
                    'maintenance/metrics/metrics.md',
                    'maintenance/metrics/collect.md',
                    'maintenance/metrics/view.md',
                ]
            },
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
            'user-guide/hadoop.md',
            'user-guide/k8s.md',
            'user-guide/fuse.md',
            'user-guide/atomicity.md',
            'user-guide/quota.md',
            'user-guide/qos.md',
            {
                text: 'Performance Optimization',
                children: [
                    'user-guide/optimization/fuse.md',
                    'user-guide/optimization/cache.md',
                ]
            },
        ]
    },
    {
        text: 'Operation Guide',
        children: [
            'maintenance/capacity.md',
            'maintenance/zone.md',
            'maintenance/log.md',
            'maintenance/code.md',
            {
                text: 'Service Management',
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
                text: 'Configuration Management',
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
                text: 'Troubleshooting',
                children: [
                    'maintenance/troubleshoot/strategy.md',
                    'maintenance/troubleshoot/case.md',
                ]
            },
        ]
    },
    {
        text: 'Tools',
        children: [
            'tools/gui.md',
            'tools/cli.md',
        ]
    },
    {
        text: 'Test and Evaluation',
        children: [
            'evaluation/env.md',
            'evaluation/tiny.md',
            'evaluation/io.md',
            'evaluation/meta.md',
        ]
    },
    {
        text: 'Design Document',
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
        text: 'Community',
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