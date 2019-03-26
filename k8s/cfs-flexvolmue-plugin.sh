#!/bin/bash
#/usr/libexec/kubernetes/kubelet-plugins/volume/exec/jdos.com~cfs/cfs 
cd $(dirname $0)
usage() {
    err "Invalid usage. Usage: "
    err "\t$0 init"
    err "\t$0 mount <mount dir> <json params>"
    err "\t$0 unmount <mount dir>"
    exit 1
}

err() {
    echo -ne $* 1>&2
}

log() {
    echo -ne $* >&1
}

ismounted() {
    INODE=$(stat ${MNTPATH} 2>/dev/null | grep 'Inode' 2>/dev/null | awk '{print $4}')
    if [ x"$INODE" == x"1"  ]; then
        echo "1"
    else
        echo "0"
    fi
}
putPidToCgroup() {
    cat > cgroup.sh  <<"EOF"
#!/bin/bash
PID=$1
POD_UID=$2
for i in `seq 1 10`;do
   PAUSE_ID=$(docker ps | grep $POD_UID | grep pause  | awk '{print $1}')
   if [ x"$PAUSE_ID" == x ];then
        sleep 10
   else
        CGROUP_PARENT=$(docker inspect $PAUSE_ID 2>/dev/null | jq -r .[0].HostConfig.CgroupParent )
        if [ X"${CGROUP_PARENT}" != X"" ];then
            echo $PID >> "/sys/fs/cgroup/cpu"$CGROUP_PARENT/tasks
            echo $PID >> "/sys/fs/cgroup/memory"$CGROUP_PARENT/tasks
            break
        fi
   fi
done
EOF
    chmod +x cgroup.sh
    nohup ./cgroup.sh $1 $2 &>/dev/null &
}


domount() {
    MNTPATH=$1

    CFS_UUID=$(echo $2 | jq -r '.uuid')
    CFS_VOLMGR=$(echo $2 | jq -r '.volmgr')
    CFS_WRITEBUFFER=$(echo $2 | jq -r '.writebuffer')
    POD_UID=$(echo $1 |  awk -F '/' '{print $5}')


    if [ $(ismounted) -eq 1 ] ; then
        log '{"status": "Success"}'
        exit 0
    fi

    mkdir -p ${MNTPATH} &> /dev/null

    LOGPATH="/export/Logs/baudstorage" #change this
    JSON=$(jq -n -c -M --arg m "$MNTPATH" --arg v "$CFS_UUID" --arg a "$CFS_VOLMGR"  --arg l "$LOGPATH" '{"mountpoint": $m,"volname": $v,"master": $a,"logpath": $l,"loglvl": "info","profport": "10094"}')
    echo $JSON > /tmp/"${POD_UID}"-cfs.json
    nohup chubao-client -c /tmp/"${POD_UID}"-cfs.json &>/dev/null &
    PID=$!
    sleep 5
    if [ $(ismounted) -ne 1 ]; then
        umount "${MNTPATH}" 2>/dev/null
        err "{ \"status\": \"Failure\", \"message\": \"Failed to mount ${CFS_VOLMGR}:${CFS_UUID} at ${MNTPATH}\"}"
        exit 1
    fi
    putPidToCgroup $PID $POD_UID
    log '{"status": "Success"}'
    exit 0
}

unmount() {
    MNTPATH=$1
    POD_UID=$(echo $1 |  awk -F '/' '{print $5}')
    if [ $(ismounted) -eq 0 ] ; then
        log '{"status": "Success"}'
        exit 0
    fi

    umount ${MNTPATH} &> /dev/null
    if [ $? -ne 0 ]; then
        err "{ \"status\": \"Failed\", \"message\": \"Failed to unmount volume at ${MNTPATH}\"}"
        exit 1
    fi
    rm -f /tmp/"${POD_UID}"-cfs.json
    log '{"status": "Success"}'
    exit 0
}

op=$1

if ! command -v jq >/dev/null 2>&1; then
    err "{ \"status\": \"Failure\", \"message\": \"'jq' binary not found. Please install jq package before using this driver\"}"
    exit 1
fi

if ! command -v chubao-client >/dev/null 2>&1; then
    err "{ \"status\": \"Failure\", \"message\": \"'chubao-client' binary not found. Please install chubao-client before using this driver\"}"
    exit 1
fi

if [ "$op" = "init" ]; then
    log '{"status": "Success", "capabilities": {"attach": false}}'
    exit 0
fi

if [ $# -lt 1 ]; then
    usage
fi

shift

case "$op" in
    mount)
        domount $*
        ;;
    unmount)
        unmount $*
        ;;
    *)
        log '{"status": "Not supported"}'
        exit 0
esac

exit 1
