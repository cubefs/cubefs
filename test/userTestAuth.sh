#!/bin/bash
#testcases for user interfaces of CFS resource management with simple auth enabled

. ./lib/libTestAuth.sh

cp ../build/bin/cfs-authtool ./
cfs_authtool=./cfs-authtool

#create user
function test_create()
{
    local sign=`gen_sign $4 /user/create`
    curl -g -s -H "Content-Type:application/json" -X POST \
         --data '{"id":"'$1'","pwd":"'$2'","type":'$3'}' \
         "$masterAddr/user/create?signature=[$sign]" | jq > return_Info.json
    grep "success" return_Info.json > /dev/null
}

#update user
function test_update()
{
    local user_id=$1
    local access_key=$2
    local secret_key=$3
    local type=$4
    local sign=`gen_sign $5 /user/update`
    curl -g -s -H "Content-Type:application/json" -X POST \
         --data '{"user_id":'\"$user_id\"',"access_key":'\"$access_key\"',"secret_key":'\"$secret_key\"',"type":'$4'}' \
         "$masterAddr/user/update?signature=[$sign]" | jq > return_Info.json
}

#remove policy
function test_rmpolicy()
{
    local user_id=$1
    local vol_name=$2
    local sign=`gen_sign $3 /user/removePolicy`
    curl -g -s -H "Content-Type:application/json" -X POST \
         --data '{"user_id":'\"$user_id\"',"volume":'\"$vol_name\"'}' \
         "$masterAddr/user/removePolicy?signature=[$sign]" | jq > return_Info.json
}

#transfer vol
function test_transfer()
{
    local vol_name=$1
    local user_src=$2
    local user_dst=$3
    local bool=$4
    local sign=`gen_sign $5 /user/transferVol`
    curl -g -s -H "Content-Type:application/json" -X POST --data '{"volume":'\"$vol_name\"',"user_src":'\"$user_src\"',"user_dst":'\"$user_dst\"',"force":'$bool'}' "$masterAddr/user/transferVol?signature=[$sign]" |jq >return_Info.json
}

#grant vol permission to a user
function test_perm()
{
    local user_id=$1
    local volume=$2
    local policy=$3
    local sign=`gen_sign $4 /user/updatePolicy`
    curl -g -s -H "Content-Type:application/json" -X POST \
         --data '{"user_id":'\"$user_id\"',"volume":'\"$volume\"',"policy":['\"$policy\"']}' \
         "$masterAddr/user/updatePolicy?signature=[$sign]" | jq > return_Info.json
}

prepare_ltptest_user

#test01 create a correct user
test_create "userc" "123" "3" ltptest
check_msg "no permission" 1 1
test_create "userc" "123" "3" root
check_msg "success" 1 2
add_dict_user userc

#test02 seek user by id
test_operation "/user/info?user=userc"
check_msg "no permission" 2 1
test_operation "/user/info?user=userc" ltptest
check_msg "success" 2 2
check_msg "\*" 2 3
test_operation "/user/info?user=userc" userc
check_msg "success" 2 4
check_no_msg "\*" 2 5
test_operation "/user/info?user=userc" root
check_msg "success" 2 6
check_no_msg "\*" 2 7

#test03 delete the user
test_operation "/user/delete?user=userc" ltptest
check_msg "no permission" 3 1
test_operation "/user/delete?user=userc" userc
check_msg "no permission" 3 2
test_operation "/user/delete?user=userc" root
check_msg "success" 3 3

#test04 creat vol
test_create "user1" "123" "3" root
check_msg "success" 4 1
test_operation "/user/info?user=user1" root
check_msg "success" 4 2
add_dict_user user1
test_operation "/admin/createVol?name=test1&capacity=1&owner=user1&mpCount=3" ltptest
check_msg "no permission" 4 3
test_operation "/admin/createVol?name=test1&capacity=1&owner=user1&mpCount=3" root
check_msg "success" 4 4

#test05 seek vol
cal_md5 "ltptest"
test_operation "/client/vol?name=test1&authKey=$md5"
check_msg "client and server auth key do not match" 5 1
cal_md5 "user1"
test_operation "/client/vol?name=test1&authKey=$md5"
check_msg "success" 5 2

#test06 del vol
cal_md5 "user1"
test_operation "/vol/delete?name=test1&authKey=$md5" ltptest
check_msg "no permission" 6 1
test_operation "/vol/delete?name=test1&authKey=$md5" root
check_msg "success" 6 2

#test07 query the user by correct ak
get_keys_by_ID "user1"
test_operation "/user/akInfo?ak=$accesskey" user1
check_msg "success" 7 1
check_msg "user1" 7 2
check_msg "$secretkey" 7 3
test_operation "/user/akInfo?ak=$accesskey" ltptest
check_msg "success" 7 4
check_msg "user1"  7 5
check_msg "\*" 7 6

#test08 query the user list by keyword=user1
get_keys_by_ID "user1"
test_operation "/user/list?keywords=user" root
check_msg "success" 8 1
check_msg "user1" 8 2
check_msg "$secretkey" 8 3
check_no_msg "\*" 8 4
test_operation "/user/list?keywords=user" ltptest
check_msg "success" 8 5
check_msg "user1" 8 6
check_msg "\*" 8 7
test_operation "/user/list?keywords=user" user1
check_msg "success" 8 8
check_msg "user1" 8 9
check_msg "$secretkey" 8 10
test_operation "/user/list?keywords=user"
check_msg "no permission" 8 11

#test09 update exsists user
test_update "user1" "KkiiVYCFcvu0c6Rd" "222wlCchJeeuGSnmFW72J2oDqLlSqvA5" "2" user1
check_msg "no permission" 9 1
test_update "user1" "KkiiVYCFcvu0c6Rd" "222wlCchJeeuGSnmFW72J2oDqLlSqvA5" "2" root
check_msg "success" 9 2
test_operation "/user/info?user=user1" root
check_msg "success" 9 3
check_msg "\"user_id\": \"user1\"," 9 4
check_msg "\"access_key\": \"KkiiVYCFcvu0c6Rd\"," 9 5
check_msg "\"secret_key\": \"222wlCchJeeuGSnmFW72J2oDqLlSqvA5\"," 9 6
check_msg "\"user_type\": 2," 9 7
add_dict_user user1

#test10 authorize a user with ReadOnly policy
test_operation "/admin/createVol?name=test10&capacity=1&owner=ltptest&mpCount=3" root
check_msg "success" 10 1
test_perm "user1" "test10" "perm:builtin:ReadOnly" ltptest
check_msg "no permission" 10 2
test_perm "user1" "test10" "perm:builtin:ReadOnly" root
check_msg "success" 10 3
check_msg "\"test10\": \[" 10 4
check_msg "\"perm:builtin:ReadOnly\"" 10 5

#test11 transfer
test_transfer "test10" "ltptest" "user1" "false" user1
check_msg "no permission" 11 1
test_transfer "test10" "ltptest" "user1" "false" user1
check_msg "no permission" 11 2
test_transfer "test10" "ltptest" "user1" "false" root
check_msg "success" 11 3
check_msg "user1" 11 4
check_msg "test10" 11 5
test_operation "/user/info?user=ltptest" ltptest
check_msg "success" 11 6
check_msg "\"own_vols\": \[\]," 11 7

# cleanup
cal_md5 'user1'
test_operation "/vol/delete?name=test10&authKey=$md5" root
check_msg "success" 12 1
test_operation "/user/delete?user=user1" root
check_msg "success" 12 2

rm return_Info.json
rm $cfs_authtool
