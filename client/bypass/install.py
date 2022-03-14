#!/usr/bin/python
import os
# for python < 2.7
os.system("wget --quiet -O argparse.py http://storage.jd.local/dpgimage/libcfs_mysql/argparse.py")
import argparse

parser = argparse.ArgumentParser(prog = 'install', description = 
'''
This script will install kernel bypass client for ChubaoFS(CFS).
CFS is virtually mounted at mountPoint. LD_PRELOAD is utilized to 
hook libc wrapper functions of file system calls. All functons with 
paths or file descriptors belong to the mount point are distributed 
to CFS.

Usage:
Starting a process with environment variable LD_PRELOAD=libcfsclient.so 
CFS_CONFIG_PATH={configDir}/cfs_client.ini will enable CFS support.
(e.g. Surpose mountPoint=/cfs_test, LD_PRELOAD= CFS_CONFIG_PATH= 
cp -r someLocalDir /cfs_test)

Notice:
Some shell features are not supported, e.g. cd, file globbing, mkdir -p.
And mv is not supported for coreutils under version 8.31.
''')
parser.add_argument('--configDir', default='/export/servers/cfs', help='directory for client config file (default: /export/servers/cfs)')
parser.add_argument('--mountPoint', default='/export/data/mysql', help='mount point (default: /export/data/mysql)')
parser.add_argument('--ignorePath', help='add ignore path at mountPoint, separated by commas')
parser.add_argument('--masterAddr', default='cn.elasticdb.jd.local')
parser.add_argument('--volName', required=True)
parser.add_argument('--owner', required=True)
parser.add_argument('--followerRead', default='false', help='enable to read from follower nodes (default: false)')
parser.add_argument('--logDir', default='/export/data/mysql/log', help='log dir (default: /export/data/mysql/log)')
parser.add_argument('--logLevel', default='warn', help='log level, debug|info|warn|error (default: warn)')
parser.add_argument('--app', default='', help='mysql | coraldb | mysql_8')
parser.add_argument('--profPort', default='10094,10095', help='port for profiling (default: 10094,10095 10095 is for xtrabackup on the same host of mysqld)')
#parser.add_argument('--tracingSamplerType', default='probabilistic', help='(default: probabilistic)')
#parser.add_argument('--tracingSamplerParam', default='0.1', help='(default: 0.1)')
parser.add_argument('--tracingSamplerType', default='const', help='(default: const)')
parser.add_argument('--tracingSamplerParam', default='1', help='(default: 1)')
parser.add_argument('--tracingReportAddr', default='jaegermysqlcfs.jd.local:6831', help='(default: jaegermysqlcfs.jd.local:6831)')
parser.add_argument('--libDir', default='/usr/lib64', help='(default: /usr/lib64)')
args = parser.parse_args()
if args.ignorePath is None:
    args.ignorePath = ''

os.system('mkdir -p {0}'.format(args.configDir))
with open('{0}/cfs_client.ini'.format(args.configDir), 'w') as f:
    f.write('mountPoint={0}\nignorePath={1}\nmasterAddr={2}\nvolName={3}\nowner={4}\nfollowerRead={5}\nlogDir={6}\nlogLevel={7}\napp={8}\nprofPort={9}\ntracingSamplerType={10}\ntracingSamplerParam={11}\ntracingReportAddr={12}\n'.format(args.mountPoint, args.ignorePath, args.masterAddr, args.volName, args.owner, args.followerRead, args.logDir, args.logLevel, args.app, args.profPort, args.tracingSamplerType, args.tracingSamplerParam, args.tracingReportAddr))

os.system('wget --quiet -O libcfs.tar.gz http://storage.jd.local/dpgimage/libcfs_mysql/libcfs.tar.gz && tar xzf libcfs.tar.gz && chmod 755 libcfssdk.so libcfsclient.so && mv libcfssdk.so libcfsclient.so {0} && rm -f libcfs.tar.gz argparse.py*'.format(args.libDir))
