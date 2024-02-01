
#yum install -y  fio

salt -N 'cfs-perftest-client' cmd.run "yum install -y fio"
