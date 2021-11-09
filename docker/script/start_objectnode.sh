#!/bin/bash

set_root_user() {
    local masters=("192.168.0.11" "192.168.0.12" "192.168.0.13")
    local port=17011
    local found=0

    mkdir -m 700 /root/.ssh
    cat /root/id_rsa > /root/.ssh/id_rsa
    chown root:root /root/.ssh/id_rsa
    chmod 600 /root/.ssh/id_rsa

    for retry in {1..10}; do
        for addr in ${masters[@]} ; do
            echo "Access master $addr ... "
            local values=`ssh -o StrictHostKeyChecking=no root@$addr \
                "curl -s localhost:$port/admin/getRoot" | \
                jq -r '.code,.data.access_key,.data.secret_key'`
            local code=`echo $values | awk '{print $1}'`
            local ak=`echo $values | awk '{print $2}'`
            local sk=`echo $values | awk '{print $3}'`
            if [ $code -ne 0 ] || [ x"$ak" = x"null" ] || [ x"$ak" = x"" ] || \
               [ x"$sk" = x"null" ] || [ x"$sk" = x"" ]; then
                continue
            fi
            echo "Get AccessKey and SecretKey at $addr"
            local found=1
            break
        done
        if [ $found -eq 1 ]; then
            break
        fi
        sleep 0.5
    done

    if [ $found -eq 0 ]; then
        echo -e "Setting root user ... \033[31mfail\033[0m"
        exit 1
    fi

    cp /root/objectnode.json /cfs/conf/objectnode.json
    sed -i "s/root's access key value/$ak/" /cfs/conf/objectnode.json
    sed -i "s/root's secret key value/$sk/" /cfs/conf/objectnode.json
    cp /cfs/conf/objectnode.json /cfs/log/
    echo -e "Setting root user ... \033[32mdone\033[0m"
}

echo "start objectnode"
mkdir -p /cfs/bin /cfs/log /cfs/conf
set_root_user
/cfs/bin/cfs-server -f -c /cfs/conf/objectnode.json

