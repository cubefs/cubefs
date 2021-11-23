#!/bin/bash

#master address
masterAddr="http://192.168.0.11:17010"
mastersAddr="192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010"

if [ x"$1" == x"" ] || [ x"$2" == x"" ]; then
    echo "Usage: $0 <root's access key> <root's secret key>"
    exit 1
fi

declare -A users
users["root"]="$1,$2"

function gen_sign()
{
    local uid=$1
    local val=${users["$uid"]}
    local ak=`echo $val | cut -d ',' -f 1`
    local sk=`echo $val | cut -d ',' -f 2`
    local path=`echo $2 | cut -d '?' -f 1`
    local sign=""

    if [ x"$val" != x"" ]; then
        local sign=`$cfs_authtool sign --user $uid --ak $ak --sk $sk --path $path`
    fi

    echo $sign
}

function add_dict_user()
{
    local userID=$1
    local sign=`gen_sign root /user/info`
    local operation="/user/info?user=$userID&signature=[$sign]"
    curl -g -s "${masterAddr}${operation}" | jq > return_Info.json

    local accesskey=`jq -r '.data.access_key' return_Info.json`
    local secretkey=`jq -r '.data.secret_key' return_Info.json`

    users["$userID"]=$accesskey","$secretkey

    #for key in "${!users[@]}"; do
    #    echo $key ${users[$key]}
    #done
}

#1.according to the input operation, send the corresponding curl request
#2.store curl result in return_Info.json
function test_operation()
{
    local operation=$1
    local user=$2
    if [ x"$user" != x"" ]; then
        local sign=`gen_sign $user $operation`
        local url="${masterAddr}${operation}&signature=[$sign]"
    else
        local url="${masterAddr}${operation}"
    fi

    curl -g -s $url | jq > return_Info.json
}

#check if msg exists in return_Info.json
#  $1: msg need to be checked, if exist, it means succ
#  $2: testcase number
#  $3: stage number of testcase
function check_msg()
{
    local message=$1
    local test=$2
    local stage=$3
    grep "$message" return_Info.json > /dev/null
    if [ $? -eq 0 ];
    then
        echo "test $test stage $stage SUCCESS"
    else
        echo "test $test stage $stage FAILED"
	cat return_Info.json
	exit 1
    fi
}

#check if msg does not exist in return_Info.json
#  $1: msg need to be checked, if not exist, it means succ
#  $2: testcase number
#  $3: stage number of testcase
function check_no_msg()
{
    local message=$1
    local test=$2
    local stage=$3
    grep "$message" return_Info.json > /dev/null
    if [ $? -ne 0 ];
    then
        echo "test $test stage $stage SUCCESS"
    else
        echo "test $test stage $stage FAILED"
	cat return_Info.json
	exit 1
    fi
}

#global variable store accesskey & secretkey
accesskey=0
secretkey=0
#get user keys by user id
function get_keys_by_ID()
{
    local userID=$1
    test_operation "/user/info?user=$userID" root
    accesskey=`jq -r '.data.access_key' return_Info.json`
    secretkey=`jq -r '.data.secret_key' return_Info.json`
}

#global variable to store md5
md5=0
#calculate the MD5 value of the corresponding string
function cal_md5()
{
    local owner=$1
    md5=`echo -n $owner | md5sum | awk '{print $1}'`
}

#prepare ltptest user
function prepare_ltptest_user()
{
    get_keys_by_ID ltptest
    local msg=`jq -r '.msg' return_Info.json`
    if [ x"$msg" != x"success" ]; then
        local pwd=ChubaoFSUser
        local ak=39bEF4RrAQgMj6RV
        local sk=TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd
        local sign=`gen_sign root /user/create`
        curl -g -s -H "Content-Type:application/json" -X POST \
             --data '{"id":"ltptest","pwd":"'$pwd'","ak":"'$ak'","sk":"'$sk'","type":3}' \
             "$masterAddr/user/create?signature=[$sign]" | jq > return_Info.json
        local msg=`jq -r '.msg' return_Info.json`
        if [ x"$msg" != x"success" ]; then
            echo "Failed to create ltptest user"
            exit 1
        fi
    fi
    add_dict_user ltptest
}
