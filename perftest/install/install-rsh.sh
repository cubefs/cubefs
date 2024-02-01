#!/bin/sh

yum -y install rsh rsh-server
systemctl restart rsh.socket
systemctl restart rlogin.socket
systemctl restart rexec.socket

cat > /root/.rhosts <<EOF
192.168.0.210 root
192.168.0.211 root
192.168.0.212 root
192.168.0.213 root
192.168.0.214 root
192.168.0.215 root
192.168.0.216 root
192.168.0.217 root
EOF

sed -i "/rsh/d" /etc/securetty
sed -i "/rexec/d" /etc/securetty
sed -i "/rlogin/d" /etc/securetty
cat >> /etc/securetty <<EOF
rsh
rexec
rlogin
EOF

# salt -N 'cfs-perftest-client' cmd.script  salt://script/install-rsh.sh
