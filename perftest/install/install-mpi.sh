#!/bin/sh

yum install -y openmpi openmpi-devel libevent libevent-devel opal opal-devel

mkdir -p /root/tools ; 
cd /root/tools; 
tar xf /tmp/openmpi-1.10.7.tgz ; 
cd /root/tools/openmpi-1.10.7 ; 
make clean ;
./configure --prefix=/usr/local/openmpi/ ; 
make ; make install

cat > /etc/profile.d/openmpi.sh <<EOF
export PATH=\$PATH:/usr/local/openmpi/bin/:/usr/local/ior/bin/
export LD_LIBRARY_PATH=/usr/local/openmpi/lib:\${LD_LIBRARY_PATH}
export MPI_CC=mpicc
EOF

source /etc/profile.d/openmpi.sh

