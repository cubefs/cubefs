#!/bin/sh

salt "perftest-client*" cmd.run "yum install -y fio fuse"
salt "perftest-client*" cmd.script  salt://perftest/install/install-rsh.sh
salt "perftest-client*" cp.get_file salt://perftest/pkg/mdtest-bin.tgz /tmp/mdtest-bin.tgz
salt "perftest-client*" cmd.script salt://perftest/install/install-mdtest.sh
salt "perftest-client*" cp.get_file salt://perftest/pkg/openmpi-1.10.7.tgz /tmp/openmpi-1.10.7.tgz
salt "perftest-client*" cmd.script  salt://perftest/install/install-mpi.sh
