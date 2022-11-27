#!/bin/bash
#function:This script is used to test the user interface of CFS resource management

#master address
masterAddr="http://192.168.0.11:17010"
mastersAddr="192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010"


#cfs-client path
cd ../client
./build.sh
cp cfs-client ../'test'/
cd ../'test'
cfs_client=./cfs-client


#test_arry initial with 1,mark the result of each case 
#1 means succeed, 0 means failed
for loop in {1..73}
do
    test_array[$loop]=1
done


#function: 1.according to the input operation, send the corresponding curl request 
#          2.store curl result in return_Info.json 
function test_operation(){
    local operation=$1
    local url="${masterAddr}${operation}"
    curl -s  $url |jq >return_Info.json
}


#function:checking message in return_Info ,if failed turn test_array[testnum] to 0
#$1:msg need to be checked $2:test number 
function check_msg(){
    local message=$1
    local test_num=$2
    local stage=$3
    grep "$message" ./return_Info.json > /dev/null
    if [ $? -ne 0 ];
    then
        echo "test[$test_num] stage $stage"
        test_array[$test_num]=0
    fi
}

#function: check whether the return_Info contains the results we don't want to produce
#$1:not expect msg need wo be checked $2:test number
function check_wrong_msg(){
    local wrong_msg=$1
    local test_num=$2
    local stage=$3
    grep "$wrong_msg" ./return_Info.json > /dev/null
    if [ $? -eq 0 ];
    then
        test_array[$test_num]=0
    fi
}


#global variable store accesskey 
accesskey=0
#get user accesskey by user id
function get_accesskey_by_ID(){
    local userID=$1
    test_operation "/user/info?user=$userID"
    grep 'access_key' return_Info.json >accesskey.file
    read accesskey <accesskey.file
    accesskey=${accesskey:15:16}
    rm accesskey.file
}


#global variable store secretkey
secretkey=0
#get user secretkey by user id
function get_secretkey_by_ID(){
    local userID=$1
    test_operation "/user/info?user=$userID"
    grep 'secret_key' return_Info.json >secretkey.file
    read secretkey <secretkey.file
    secretkey=${secretkey:15:32}
    rm secretkey.file
}


#function:execute a curl request to create user
#$1:userId $2:pwd $3:userType 
function test_create(){
    curl  -s  -H "Content-Type:application/json" -X POST --data '{"id":"'$1'","pwd":"'$2'","type":'$3'}' "$masterAddr/user/create" |jq > return_Info.json
    grep "success" ./return_Info.json > /dev/null 
}



#function:execute a curl request to update user
function test_update(){
    local user_id=$1
    local access_key=$2
    local secret_key=$3
    local type=$4
    curl -s -H "Content-Type:application/json" -X POST --data '{"user_id":'\"$user_id\"',"access_key":'\"$access_key\"',"secret_key":'\"$secret_key\"',"type":'$4'}' "$masterAddr/user/update" |jq > return_Info.json
}


#function:execute a curl request to remove policy
function test_rmpolicy(){
    local user_id=$1
    local vol_name=$2
    curl -s -H "Content-Type:application/json" -X POST --data '{"user_id":'\"$user_id\"',"volume":'\"$vol_name\"'}' "$masterAddr/user/removePolicy"|jq >return_Info.json
}


#function:excute a curl request to transfer vol
function test_transfer(){
    local vol_name=$1
    local user_src=$2
    local user_dst=$3
    local bool=$4
    curl -s -H "Content-Type:application/json" -X POST --data '{"volume":'\"$vol_name\"',"user_src":'\"$user_src\"',"user_dst":'\"$user_dst\"',"force":'$bool'}' "$masterAddr/user/transferVol" |jq >return_Info.json
}


#function:execute  a curl request to grant vol permission to a user
function test_perm(){
    local user_id=$1
    local volume=$2
    local policy=$3
    curl -s -H "Content-Type:application/json" -X POST --data '{"user_id":'\"$user_id\"',"volume":'\"$volume\"',"policy":['\"$policy\"']}' "$masterAddr/user/updatePolicy" |jq >return_Info.json
}


#global variable to store md5
md5=0
#function: calculate the MD5 value of the corresponding string
#$1:input string
function cal_md5(){
    local owner=$1
    echo -n "$owner"|md5sum >md5.file
    read md5 <md5.file
    md5=${md5:0:32}
    rm md5.file
}


#function:mount subdir
function mount_subdir(){
    mkdir -p /cfs/mnt
    mkdir -p /cfs/log
    local owner=$1
    local vol_name=$2
    local subdir=$3
    get_accesskey_by_ID "$owner"
    get_secretkey_by_ID "$owner"
    $cfs_client --masterAddr=$mastersAddr --mountPoint=/cfs/mnt --volName=$vol_name --owner=$owner --logDir=/cfs/log --logLevel=info --accessKey=$accesskey --secretKey=$secretkey --subdir=$subdir>/dev/null 2>&1
}


#function:mount volume
function mount_dir(){
     sleep  10s
     mkdir -p /cfs/mnt
     mkdir -p /cfs/log
     local owner=$1
     local vol_name=$2
     get_accesskey_by_ID "$owner"
     get_secretkey_by_ID "$owner"
     $cfs_client --masterAddr=$mastersAddr --mountPoint=/cfs/mnt --volName=$vol_name --owner=$owner --logDir=/cfs/log --logLevel=info --accessKey=$accesskey --secretKey=$secretkey >/dev/null 2>&1
}


#function:umount
function umount_dir(){
    umount /cfs/mnt >/dev/null 2>&1
    sleep 10s 
    rm -rf /cfs
}




#--------------------------------------
#             test create 
#--------------------------------------
#test01 create a correct user
test_create "userc" "123" "3" 
check_msg "success" 1


#test02 seek user by id
test_operation "/user/info?user=userc" 
check_msg "success" 2


#test03 delete the user
test_operation "/user/delete?user=userc"
check_msg "success" 3


#test04 creat a user that id=""
test_create "" "123" "3"
check_msg "invalid user ID" 4
test_operation "/user/info?user="
check_msg "parameter user not found" 4


#test05 creat user that id length>20
test_create "abcdefghijklmnopqrstuvwxyz" "123" "3"
check_msg "invalid user ID" 5
test_operation "/user/info?user=abcdefghijklmnopqrstuvwxyz"
check_msg "user not exists" 5


#test06 creat a user that id="cfs_versioni1"
test_create "cfs_version_1" "123" "3"
check_msg "success" 6
test_operation "/user/info?user=cfs_version_1"
check_msg "cfs_version_1" 6
test_operation "/user/delete?user=cfs_version_1"
check_msg "success" 6


#test07 create a user that id="cfs?123"
test_create "cfs?123" "123" "3"
check_msg "invalid user ID" 7 
test_operation "/user/info?user=cfs?123"
check_msg "user not exists" 7 


#test08 create a user that type =4
test_create "test_type1" "123" "4"
check_msg "invalid user type" 8
test_operation "/user/info?user=test_type1"
check_msg "user not exists" 8


#test09 create a user that type=1
test_create "test_type2" "123" "1"
check_msg "invalid user type" 9
test_operation "/user/info?user=test_type2"
check_msg "user not exists" 9


#test10 create a user that type=2 
test_create "test_type3" "123" "2"
check_msg "success" 10
test_operation "/user/info?user=test_type3"
check_msg "test_type3" 10
test_operation "/user/delete?user=test_type3"
check_msg "success" 10


#test11 create a user that length of pwd is 34
test_create "test_pwd" "1234567890abcdefg0)jklmnop_rstuv*xyz" "3"
check_msg "success" 11
test_operation "/user/info?user=test_pwd"
check_msg "test_pwd" 11
test_operation "/user/delete?user=test_pwd"
check_msg "success" 11


#test12 creat 2w user:user1 user2 user3..user20000
for i in {1..20000}
do
    test_create "user$i" "12345" "3"
    check_msg "success" 12 $i
done




#--------------------------------------
#             test delete
#--------------------------------------
#test13 delete a user taht type=2 and don't have vol
test_create "user20001" "12345" "2"
check_msg "success" 13  
test_operation "/user/info?user=user20001"
check_msg "user20001" 13 
test_operation "/user/delete?user=user20001"
check_msg "success" 13 


#test14 delete a user taht type=3 and don't have vol
test_create "user20001" "12345" "3"
check_msg "success" 14
test_operation "/user/info?user=user20001"
check_msg "user20001" 14
test_operation "/user/delete?user=user20001"
check_msg "success" 14 


#test15 delete a non-exist user
test_operation "/user/info?user=20001"
check_msg "user not exists" 15
test_operation "/user/delete?user=20001"
check_msg "user not exists" 15

#test16 delete a user that own one vol 
test_create "cfsuser" "12345" "2"
check_msg "success" 16 1
  #creat a vol owned by this cfsuser
test_operation "/admin/createVol?name=test&capacity=1&owner=cfsuser&mpCount=3"
check_msg "success" 16 2
  #seek this vol
cal_md5 "cfsuser"
test_operation "/client/vol?name=test&authKey=$md5"
check_msg "success" 16 3
  #check cfsuser own "test"vol
test_operation "/user/info?user=cfsuser"
check_msg "test" 16
  #delete this cfsuser
test_operation "/user/delete?user=cfsuser"
check_msg "own vols not empty"
 #restore test environment
test_operation "/vol/delete?name=test&authKey=$md5"
check_msg "success" 16 4
test_operation "/user/delete?user=cfsuser"
check_msg "success" 16 5


##test17 delete a user that owgn three vols
test_create "cfsuser" "12345" "3"
check_msg "success" 17
 #creat three vols owned by this cfsuser
test_operation "/admin/createVol?name=test1&capacity=1&owner=cfsuser&mpCount=3"
check_msg "success" 17
test_operation "/admin/createVol?name=test2&capacity=1&owner=cfsuser&mpCount=3"
check_msg "success" 17
test_operation "/admin/createVol?name=test3&capacity=1&owner=cfsuser&mpCount=3"
check_msg "success" 17
  #seek these vol
cal_md5 "cfsuser"
test_operation "/client/vol?name=test1&authKey=$md5"
check_msg "success" 17
test_operation "/client/vol?name=test2&authKey=$md5"
check_msg "success" 17
test_operation "/client/vol?name=test3&authKey=$md5"
check_msg "success" 17
  #check cfsuser own "test"vol
test_operation "/user/info?user=cfsuser"
check_msg "test1" 17
  #delete a user that own three vols
test_operation "/user/delete?user=cfsuser"
check_msg "own vols not empty"
  #restore test environment
test_operation "/vol/delete?name=test1&authKey=$md5"
check_msg "success" 17
test_operation "/vol/delete?name=test2&authKey=$md5"
check_msg "success" 17
test_operation "/vol/delete?name=test3&authKey=$md5"
check_msg "success" 17
test_operation "/user/delete?user=cfsuser"
check_msg "success" 17




#--------------------------------------
#             test query by ID 
#--------------------------------------
#test18 query exists users by ID
start=$(date +%s)
test_operation "/user/info?user=user1000"
check_msg "user1000" 18
check_msg "create_time" 18
check_msg "access_key" 18
end=$(date +%s)
take=$(( end - start ))
echo Query  user by id from 2w users takes ${take}s.


#test19 query user who's id="" 
test_operation "/user/info?user="""
check_msg "parameter user not found" 19


#test20 query ID for users with special symbols
test_operation "/user/info?user=+1234myx"
check_msg "user not exists" 20


#test21 query the user,ID that meets the rules but does not exist
test_operation "/user/info?user=user20001"
check_msg "user not exists" 21




#--------------------------------------
#             test query by AK
#--------------------------------------
#test22 query the user by correct ak
get_accesskey_by_ID "user1"
test_operation "/user/akInfo?ak=$accesskey"
check_msg "success"
check_msg "user1" 22
check_msg "create_time" 22
check_msg "access_key" 22


#test23 query the user by correct ak+"abc"
get_accesskey_by_ID "user12345"
test_operation "/user/akInfo?ak=$accesskey'123'"
check_msg "accesskey can only be number and letters" 23


#test24 query the user by wrong accesskey which length is 16
test_operation "/user/akInfo?ak=abcdefghijklmnop"
check_msg "access key not exists" 24


#test25 query the user by first fifteen characters of the accesskey
get_accesskey_by_ID "user98"
temp=${accesskey:0:15}
test_operation "/user/akInfo?ak=$temp"
check_msg "accesskey can only be number and letters" 25


#test26 query the user by wrong accesskey which length is 32
test_operation "/user/akInfo?ak=1234567890zxcvbnmasd123456789012"
check_msg "accesskey can only be number and letters" 26




#--------------------------------------
#             test list user
#--------------------------------------
#test27 query the user list by keyword=user1500
start=$(date +%s)
test_operation "/user/list?keywords=user15000"
end=$(date +%s)
take=$(( end - start ))
echo Query user list  from 2w users takes ${take}s.
check_msg "success" 27
check_msg "user15000" 27
check_msg "user15001" 27
check_msg "user15002" 27
check_msg "user15003" 27
check_msg "user15004" 27
check_msg "user15005" 27
check_msg "user15006" 27
check_msg "user15007" 27
check_msg "user15008" 27
check_msg "user15009" 27


#test28 query the user list ,keyword not exist
test_operation "/user/list?keywords=cfs"
check_msg "success" 28
check_msg "\"data\":\ \[\]" 28


#test29 query the user list,keyword are contains  special punctution
test_operation "/user/list?keywords=cfs?+"
check_msg "success" 29
check_msg "\"data\":\ \[\]" 29




#--------------------------------------
#             test update user
#--------------------------------------
#test30 update exsists user
test_update "user1" "KkiiVYCFcvu0c6Rd" "222wlCchJeeuGSnmFW72J2oDqLlSqvA5" "2"
check_msg "success" 30 
test_operation "/user/info?user=user1"
check_msg "success" 30 
check_msg "\"user_id\": \"user1\"," 30 
check_msg "\"access_key\": \"KkiiVYCFcvu0c6Rd\"," 30 
check_msg "\"secret_key\": \"222wlCchJeeuGSnmFW72J2oDqLlSqvA5\"," 30 
check_msg "\"user_type\": 2," 30 
test_update "user1" "KkiiVYCFcvu0c7Rd" "222wlCchJeeuGSnmFW72J2oDqLlSqvA5" "2"


#test31 update with no parameter
test_update "user2" "" "" ""
check_msg "invalid character '\}' looking for beginning of value" 31 


#test32 update user sk
curl -s -H "Content-Type:application/json" -X POST --data '{"user_id":"user3","secret_key":"111wlCchJeeuGSnmFW72J234qLlSqvA5"}' "http://192.168.0.11:17010/user/update" |jq > return_Info.json
check_msg "success" 32 1
test_operation "/user/info?user=user3"
check_msg "success" 32 2
check_msg "\"secret_key\": \"111wlCchJeeuGSnmFW72J234qLlSqvA5\"," 32 3


#test33 update user ak
curl -s -H "Content-Type:application/json" -X POST --data '{"user_id":"user4","access_key":"Kkixxxxxcvu0c6Rd"}' "http://192.168.0.11:17010/user/update" |jq > return_Info.json
check_msg "success" 33 
test_operation "/user/info?user=user4"
check_msg "success" 33 
check_msg "\"access_key\": \"Kkixxxxxcvu0c6Rd\"," 33 
curl -s -H "Content-Type:application/json" -X POST --data '{"user_id":"user4","access_key":"KkiiVYCFcvu0c7Rg"}' "http://192.168.0.11:17010/user/update" |jq > return_Info.json
check_msg "success" 33 


#test34  update user type
curl -s -H "Content-Type:application/json" -X POST --data '{"user_id":"user5","type":3}' "http://192.168.0.11:17010/user/update" |jq > return_Info.json
check_msg "success" 34 
check_msg "\"user_type\": 3," 34 
curl -s -H "Content-Type:application/json" -X POST --data '{"user_id":"user5","type":2}' "http://192.168.0.11:17010/user/update" |jq > return_Info.json
check_msg "success" 34 


#test35 update a non exists user
test_operation "/user/info?user=userupdate"
check_msg "user not exists" 35
test_update "userupdate" "KkiiVYCFcvu0c666" "222wlCchJeeuGSnmFW72J2oDqLlSqvA5" "2"
check_msg "user not exists" 35


#test36 update a user with Unqualified sk
test_update "user36" "KkiiVYCFcvu0c777" "222wlCchJeeuGSnmFW72J2oDqLlSqvA511" "2"
#cat return_Info.json
check_msg "\"msg\": \"invalid secret key\"," 36 1


#test37 update a user with unqualified ak
test_update "user7" "KkiiVYCFcvu0c6678" "222wlCchJeeuGSnmFW72J2oDqLlSqvA5" "2"
check_msg "invalid access key" 37


#test38 update a user with unqualified ak sk
test_update "user8" "KkiiVYCFcvu0c6678" "222wlCchJeeuGSnmFW72J2oDqLlSqvA52233" "2"
check_msg "invalid access key" 38


#test39 update root 
test_update "root" "KkiiVYCFcvu0c666" "222wlCchJeeuGSnmFW72J2oDqLlSqvA5" "2"
check_msg "no permission" 39


#test40 update a user with type 9
curl -s -H "Content-Type:application/json" -X POST --data '{"user_id":"user9","type":9}' "http://192.168.0.11:17010/user/update" |jq > return_Info.json
#cat return_Info.json
check_msg "invalid user type" 40 1  

#test41 update a user with type x
curl -s -H "Content-Type:application/json" -X POST --data '{"user_id":"user9","type":x}' "http://192.168.0.11:17010/user/update" |jq > return_Info.json
check_msg "invalid character 'x' looking for beginning of value" 41




#--------------------------------------
#             test authorize
#--------------------------------------
#test42 authorize user a volume that does not exist
test_perm "user1" "testvol" "perm:builtin:ReadOnly"
check_msg "vol not exists" 42


#test43 authorize a not exists user
test_operation "/admin/createVol?name=test4&capacity=1&owner=user7&mpCount=3"
check_msg "success" 43
test_perm "usert" "test4" "perm:builtin:ReadOnly"
check_msg "user not exists" 43
cal_md5 'user7'
test_operation "/vol/delete?name=test4&authKey=$md5"
check_msg "success" 43


#test44 authorize a user with wrong policy
test_operation "/admin/createVol?name=test5&capacity=1&owner=user8&mpCount=3"
check_msg "success" 44 1
test_perm "user9" "test5" "nopermission"
#cat return_Info.json
check_msg "unknown policy" 44 2
cal_md5 'user8'
test_operation "/vol/delete?name=test5&authKey=$md5"
check_msg "success" 44 3


#test72 authorize a user that already has permission on this vol  with wrong policy
test_operation "/admin/createVol?name=test72&capacity=1&owner=user72&mpCount=3"
check_msg "success" 72 1
test_perm "user720" "test72" "perm:builtin:ReadOnly\",\"perm:builtin:Writable"
check_msg "success" 72 2
test_perm "user720" "test72" "perm:builtin:Writ"
#cat return_Info.json
check_msg "unknown policy" 72 3
test_operation "/user/info?user=user720"
#cat return_Info.json
check_msg "\"perm:builtin:ReadOnly\"," 72 4
check_msg "\"perm:builtin:Writable\"" 72 5 
cal_md5 'user72'
test_operation "/vol/delete?name=test72&authKey=$md5"
check_msg "success" 72 6



#test45 authorize a user with ReadOnly policy
test_operation "/admin/createVol?name=test6&capacity=1&owner=user10&mpCount=3"
check_msg "success" 45 
#authorize user11 vol:test6 readonly permissiion
test_perm "user11" "test6" "perm:builtin:ReadOnly"
check_msg "success" 45 
check_msg "\"test6\": \[" 45 
check_msg "\"perm:builtin:ReadOnly\"" 45  
#should check by mount
mount_dir "user11" "test6"
#sleep 1m
df -h >return_Info.json
check_msg "cubefs-test6  1.0G     0  1.0G   0% /cfs/mnt" 45
#check whether the permission is ReadOnly
mkdir -p  /cfs/mnt/a 2>return_Info.json
check_msg "mkdir: cannot create directory ‘/cfs/mnt/a’: Read-only file system" 45 
umount_dir 
cal_md5 'user10'
test_operation "/vol/delete?name=test6&authKey=$md5"
check_msg "success" 45


#test46 authorize a user with perm:builtin:Writable
test_operation "/admin/createVol?name=test7&capacity=1&owner=user12&mpCount=3"
check_msg "success" 46
#authorize user13 vol:test7 Writable permission
test_perm "user13" "test7" "perm:builtin:Writable"
check_msg "success" 46
check_msg "\"test7\": \[" 46
check_msg "\"perm:builtin:Writable\"" 46
#should check by mount
mount_dir "user13" "test7"
#cat return_Info.json
sleep 10s
df -h >return_Info.json
check_msg "cubefs-test7  1.0G     0  1.0G   0% /cfs/mnt"
mkdir -p /cfs/mnt/a 
ls -a /cfs/mnt >return_Info.json
check_msg "a" 46
rm -rf /cfs/mnt/a
umount_dir
cal_md5 'user12'
test_operation "/vol/delete?name=test7&authKey=$md5"
check_msg "success" 46


#test47 authorize a user with two vol's permission
test_operation "/admin/createVol?name=test8&capacity=1&owner=user13&mpCount=3"
check_msg "success" 47 
test_operation "/admin/createVol?name=test9&capacity=1&owner=user14&mpCount=3"
check_msg "success" 47 
#authorize user15 vol:test8
test_perm "user15" "test8" "perm:builtin:Writable" 47 
check_msg "\"test8\": \[" 47 
check_msg "\"perm:builtin:Writable\"" 47 
#quthorize user15 vol:test9
test_perm "user15" "test9" "perm:builtin:ReadOnly" 47 
check_msg "\"test9\": \[" 47 
check_msg "\"perm:builtin:ReadOnly\"" 47 
#delete two vol
cal_md5 'user13'
test_operation "/vol/delete?name=test8&authKey=$md5"
check_msg "success" 47 
cal_md5 'user14'
test_operation "/vol/delete?name=test9&authKey=$md5"


#test48 authorize a user with vol subdir:/a/b ReadOnly permisson ,mount this subdir test readonly
test_operation "/admin/createVol?name=test10&capacity=1&owner=user15&mpCount=3"
check_msg "success" 48 1
  #authorize user16 vol:test10 subdir:/a/b readonly permissiion
test_perm "user16" "test10" "perm:builtin:/a/b:ReadOnly"
check_msg "success" 48 2
check_msg "\"test10\": \[" 48 3
check_msg "\"perm:builtin:/a/b:ReadOnly\"" 48 4
#create subdir /a/b on vol
mount_dir "user15" "test10"
mkdir -p /cfs/mnt/a/b
umount_dir
   #mount subdir
mount_subdir "user16" "test10" "/a/b"
df -h >return_Info.json
check_msg "cubefs-test10  1.0G     0  1.0G   0% /cfs/mnt" 48 5
#check whether the permission is ReadOnly
mkdir -p  /cfs/mnt/a 2>return_Info.json
check_msg "mkdir: cannot create directory ‘/cfs/mnt/a’: Read-only file system" 48 6
umount_dir
#clear  subdir /a/b on vol
mount_dir "user15" "test10"
rm -rf /cfs/mnt/a
umount_dir
cal_md5 'user15'
test_operation "/vol/delete?name=test10&authKey=$md5"
check_msg "success" 48 7


#test49 authorize a user with vol subd6ir:/a/b Writable permission,mount subdir /a/b/c
test_create "user49" "12345" "3"
check_msg "success" 49 21
test_operation "/admin/createVol?name=test49&capacity=1&owner=user49&mpCount=3"
check_msg "success" 49 1
#authorize user490 vol:test49 subdir:/a/b Writable permissiion
test_create "user490" "12345" "3"
check_msg "success" 49 22
test_perm "user490" "test49" "perm:builtin:/a/b:Writable"
check_msg "success" 49 2
check_msg "\"test49\": \[" 49 3
check_msg "\"perm:builtin:/a/b:Writable\"" 49 4
#create subdir /a/b on vol
mount_dir "user49" "test49"
mkdir -p /cfs/mnt/a/b
umount_dir
  #mount subdir
mount_subdir "user490" "test49" "/a/b"
df -h >return_Info.json
check_msg "cubefs-test49  1.0G     0  1.0G   0% /cfs/mnt" 49 5
mkdir -p /cfs/mnt/c
umount_dir
#mount
mount_subdir "user490" "test49" "/a/b/c" 
df -h >return_Info.json
check_msg "cubefs-test49  1.0G     0  1.0G   0% /cfs/mnt" 49 6
umount_dir
  #clear subdir on vol 
mount_dir  "user49" "test49"
rm -rf /cfs/mnt/a
umount_dir
cal_md5 'user49'
test_operation "/vol/delete?name=test49&authKey=$md5"
#cat return_Info.json
check_msg "success" 49 7
test_operation "/user/delete?user=user49" 
check_msg "success" 49 23
test_operation "/user/delete?user=user490"
check_msg "success" 49 24


#test50 authorize a user with multipul vol subdir permission,check mount
test_operation "/admin/createVol?name=test50&capacity=1&owner=user50&mpCount=3"
check_msg "success" 50 1
  #authorize user20 vol:test50 subdir:/a/b:/c/d:Writable permissiion
test_perm "user20" "test50" "perm:builtin:/e/f:/g/h:Writable"
  #cat return_Info.json
check_msg "success" 50 2
check_msg "\"test50\": \[" 50 3
check_msg "\"perm:builtin:/e/f:/g/h:Writable\"" 50 4
  #create subdir  on vol
mount_dir "user50" "test50"
df -h>return_Info.json
check_msg "cubefs-test50  1.0G     0  1.0G   0% /cfs/mnt"
mkdir -p /cfs/mnt/e/f
mkdir -p /cfs/mnt/g/h
#echo "first umount"
umount_dir
  #mount subdir
mount_subdir "user20" "test50" "/e/f"
df -h >return_Info.json
check_msg "cubefs-test50  1.0G     0  1.0G   0% /cfs/mnt" 50 5
#echo "second umount"
umount_dir
mount_subdir "user20" "test50" "/g/h"
#cat /cfs/log/client/output.log
sleep 5s
df -h >return_Info.json
check_msg "cubefs-test50  1.0G     0  1.0G   0% /cfs/mnt" 50 6
#echo "third umount"
umount_dir
 #clear subdir on vol
mount_dir "user50" "test50"
df -h>return_Info.json
check_msg "cubefs-test50  1.0G     0  1.0G   0% /cfs/mnt" 50 8
rm -rf /e
rm -rf /g
#echo "last umount"
umount_dir
cal_md5 'user50'
test_operation "/vol/delete?name=test50&authKey=$md5"
check_msg "success" 50 7


#test68 authorize a user duplicate permission
test_operation "/admin/createVol?name=test68&capacity=1&owner=user68&mpCount=3"
check_msg "success" 68 1
test_perm "user680" "test68" "perm:builtin:ReadOnly\",\"perm:builtin:ReadOnly"
#cat return_Info.json
check_wrong_msg "\"perm:builtin:ReadOnly\"," 68 2
cal_md5 'user68'
test_operation "/vol/delete?name=test68&authKey=$md5"
check_msg "success" 68 3


#test73 authorize a user duplicate permission
test_operation "/admin/createVol?name=test73&capacity=1&owner=user73&mpCount=3"
check_msg "success" 73 1
test_perm "user730" "test73" "perm:builtin:ReadOnly\",\"perm:builtin:Writable\",\"perm:builtin:ReadOnly"
#cat return_Info.json
check_wrong_msg "\"perm:builtin:Writable\"," 73 2
cal_md5 'user73'
test_operation "/vol/delete?name=test73&authKey=$md5"
check_msg "success" 73 3




#----------------------------------------------------
#              test remove policy
#----------------------------------------------------
#test51 remove a not exists user policy
test_operation "/admin/createVol?name=test51&capacity=1&owner=user51&mpCount=3"
check_msg "success" 51 
test_operation "/user/info?user=51xxx"
check_msg "user not exists" 51 
test_rmpolicy "xxx" "test51"
check_msg "user not exists" 51 
cal_md5 'user51'
test_operation "/vol/delete?name=test51&authKey=$md5"
check_msg "success" 51 


#test52 remove a exists user with none vol  policy
test_operation "/admin/createVol?name=test52&capacity=1&owner=user520&mpCount=3"
check_msg "success" 52 
test_operation "/user/info?user=user52"
check_msg "success" 52 
check_msg "\"own_vols\": \[\]," 52 
check_msg "\"authorized_vols\": {}" 52 
test_rmpolicy "user52" "test52"
check_msg "success" 52 5
check_msg "\"authorized_vols\": {}" 52 6
cal_md5 "user520"
test_operation "/vol/delete?name=test52&authKey=$md5" 
check_msg "success" 52
 

#test53 remove a not exists user with not exists vol
test_operation "/user/info?user=user53xxx"
check_msg "user not exists" 53
test_rmpolicy "user53xxx" "vol53xxx"
check_msg "vol not exists" 53 


#test54 remove a user policy and check by mount
test_operation "/admin/createVol?name=test54&capacity=1&owner=user54&mpCount=3"
check_msg "success" 54 1
test_perm "user540" "test54" "perm:builtin:Writable"
check_msg "success" 54 2
check_msg "\"test54\": \[" 54 3
check_msg "\"perm:builtin:Writable\"" 54 4
mount_dir "user540" "test54"
df -h >return_Info.json
check_msg "cubefs-test54  1.0G     0  1.0G   0% /cfs/mnt"
umount_dir
 #remove policy and check mount
test_rmpolicy "user540" "test54"
check_msg "success" 54 5
check_msg "\"authorized_vols\": {}" 54 6
mount_dir "user540" "test54"
cat /cfs/log/client/output.log >return_Info.json
check_msg "check permission failed:  no permission" 54 7
cal_md5 'user54'
test_operation "/vol/delete?name=test54&authKey=$md5" 
check_msg "success" 54 8


#test55 One user has the permissions of two volumes. after removing one, whether the use of the other will be affected
test_operation "/admin/createVol?name=test55x&capacity=1&owner=user55&mpCount=3"
check_msg "success" 55 1
test_operation "/admin/createVol?name=test55y&capacity=1&owner=user55&mpCount=3"
check_msg "success" 55 2
test_perm "user550" "test55x" "perm:builtin:Writable"
check_msg "success" 55 3
check_msg "\"perm:builtin:Writable\"" 55 4
test_perm "user550" "test55y" "perm:builtin:ReadOnly"
check_msg "success" 55 5
check_msg "perm:builtin:ReadOnly" 55 8 
  #remove one policy
test_rmpolicy "user550" "test55y"
check_msg "success" 55 9
  #mount the other one
mount_dir "user550" "test55x"
df -h >return_Info.json
check_msg "cubefs-test55x  1.0G     0  1.0G   0% /cfs/mnt"  55 10
umount_dir
 #delete two vol
cal_md5 'user55'
test_operation "/vol/delete?name=test55x&authKey=$md5"
check_msg "success" 55  6
test_operation "/vol/delete?name=test55y&authKey=$md5"
check_msg "success" 55 7




#---------------------------------------------------
#               test transfer volume
#---------------------------------------------------
#test56  transfer an non exists vol
test_transfer "nonexistsvol" "user56" "user56" "true"
check_msg "vol not exists" 56

#test57 transfer an vol with wrong user_src
test_operation "/admin/createVol?name=test57&capacity=1&owner=user57&mpCount=3"
check_msg "success" 57 1
test_transfer "test57" "user570" "user571" "false"
check_msg "no vol policy" 57 2
cal_md5 'user57'
test_operation "/vol/delete?name=test57&authKey=$md5"
check_msg "success" 57 3

#test58  force transfer a vol with wrong user_src
test_operation "/admin/createVol?name=test58&capacity=1&owner=user58&mpCount=3"
check_msg "success" 58 1 
test_transfer "test58" "user58x" "user580" "true"
check_msg "success" 58 2
check_msg "user580" 58 3
check_msg "test58"  58 4
test_operation "/user/info?user=user58"
check_msg "success" 58 5
check_msg "test58"  58 6
cal_md5 'user580'
test_operation "/vol/delete?name=test58&authKey=$md5"
check_msg "success" 58 7


#test59 tansfer a vol with wrong user_dst
test_operation "/admin/createVol?name=test59&capacity=1&owner=user59&mpCount=3"
check_msg "success" 59 1
test_operation "/user/info?user=userxxx"
check_msg "user not exists" 59 2
test_transfer "test59" "user59" "userxxx" "false"
check_msg "user not exists" 59 4
cal_md5 "user59"
test_operation "/vol/delete?name=test59&authKey=$md5"
check_msg "success" 59 3


#test60 force transfer a vol with wrong user_dst
test_operation "/admin/createVol?name=test60&capacity=1&owner=user60&mpCount=3"
check_msg "success" 60 1
test_operation "/user/info?user=userxxx"
check_msg "user not exists" 60 2
test_transfer "test60" "user60" "userxxx" "true"
check_msg "user not exists" 60 3
cal_md5 "user60"
test_operation "/vol/delete?name=test60&authKey=$md5"
check_msg "success" 60 4


#test61 transfer with correct user_src , whether the transferred volume is removed from the source user and added to the new user, and check by mount
test_operation "/admin/createVol?name=test61&capacity=1&owner=user61&mpCount=3"
check_msg "success" 61 1
test_transfer "test61" "user61" "user610" "false"
check_msg "success" 61 2
check_msg "user610" 61 3
check_msg "test61" 61 4
test_operation "/user/info?user=user61"
check_msg "success" 61 5
check_msg "\"own_vols\": \[\],"
 #check by nmount
mount_dir "user61" "test61" 
cat /cfs/log/client/output.log >return_Info.json
check_msg "check permission failed:  no permission" 61 7
mount_dir "user610" "test61"
df -h >return_Info.json
check_msg "cubefs-test61  1.0G     0  1.0G   0% /cfs/mnt" 61 9
umount_dir
cal_md5 "user610"
test_operation "/vol/delete?name=test61&authKey=$md5"
check_msg "success" 61 8


#test 62 After the forced transfer with the wrong user_src, whether the original owner user still has access to the volume
test_operation "/admin/createVol?name=test62&capacity=1&owner=user62&mpCount=3"
check_msg "success" 62 1
test_transfer "test62" "user62x" "user620" "true"
check_msg "success" 62 2
check_msg "user620" 62 3
check_msg "test62"  62 4
test_operation "/user/info?user=user62"
check_msg "success" 62 5
check_msg "test62"  62 6
 #check mount
mount_dir "user62" "test62"
df -h >return_Info.json
check_msg "cubefs-test62  1.0G     0  1.0G   0% /cfs/mnt" 62 7
umount_dir
cal_md5 'user620'
test_operation "/vol/delete?name=test62&authKey=$md5"
check_msg "success" 62 8


#test63 after force transfer and delete, whether the original owner user and the new owner user has correct information
test_operation "/admin/createVol?name=test63&capacity=1&owner=user63&mpCount=3"
check_msg "success" 63 1
test_transfer "test63" "user63x" "user630" "true"
check_msg "success" 63 2
check_msg "user630" 63 3
check_msg "test63"  63 4
test_operation "/user/info?user=user63"
check_msg "success" 63 5
check_msg "test63"  63 6
cal_md5 "user630"
test_operation "/vol/delete?name=test63&authKey=$md5"
check_msg "success" 63 7
#check by query
test_operation "/user/info?user=user63"
check_msg "success" 63 8
check_msg "\"own_vols\": \[\]," 63 9
test_operation "/user/info?user=user630" 
check_msg "success" 63 10
check_msg "\"own_vols\": \[\]," 63 11


#test64 transfer 30 volumes to the same user, check whether it is successful
 #create 30 volumes
for loop in {1..30}
do 
   test_operation "/admin/createVol?name=testx$loop&capacity=1&owner=user2$loop&mpCount=3"
   check_msg "success" 64 $loop
done
 #transfer 30 volumes to user64
for loop in {1..30}
do
    test_transfer "testx$loop" "user2$loop" "user64" "false"
    
check_msg "success" 64 ` expr $loop + 30`
done
 #query 30 volumes in user64 information
test_operation "/user/info?user=user64"
check_msg "success" 64 61
for loop in {1..30}
do
    check_msg "testx$loop" 64  `expr $loop + 61`
done
 #delete 30 volumes
cal_md5 "user64"
for loop in {1..30}
do
    test_operation "/vol/delete?name=testx$loop&authKey=$md5"
    check_msg "success" 64 `expr $loop + 91`
done


#test65 The transfer source user and target user are both the original owner of the volume
test_operation "/admin/createVol?name=test65&capacity=1&owner=user65&mpCount=3"
check_msg "success" 65 1
test_transfer "test65" "user65" "user65" "false"
check_msg "user65" 65 5
check_msg "test65" 65 4
check_msg "success" 65 2
cal_md5 "user65"
test_operation "/vol/delete?name=test65&authKey=$md5"
check_msg "success" 65 3


#test66 The transfer source user and target user are the same user, but not is the original owner of the volume
test_operation "/admin/createVol?name=test66&capacity=1&owner=user66&mpCount=3"
check_msg "success" 66 1
test_transfer "test66" "user65" "user65" "false"
check_msg "no vol policy" 66 2
cal_md5 "user66"
test_operation "/vol/delete?name=test66&authKey=$md5"
check_msg "success" 66 3




#-----------------------------------
#         get users of Vol
#-----------------------------------
#test69 get users of Vol
test_operation "/admin/createVol?name=test69&capacity=1&owner=user69&mpCount=3"
check_msg "success" 69 1
test_operation "/vol/users?name=test69"
check_msg "success" 69 2
check_msg "\"user69\"" 69 3
cal_md5 "user69"
test_operation "/vol/delete?name=test69&authKey=$md5"
check_msg "success" 69 4


#test70 get users of a vol that have authorized two users Writable policy
test_operation "/admin/createVol?name=test70&capacity=1&owner=user70&mpCount=3"
check_msg "success" 70 1
test_perm "user700" "test70" "perm:builtin:Writable"
check_msg "success" 70 2
test_perm "user701" "test70" "perm:builtin:ReadOnly"
check_msg "success" 70 3
test_operation "/vol/users?name=test70"
#cat return_Info.json
check_msg "user70" 70 4
check_msg "user700" 70 5
check_msg "user701" 70 6
cal_md5 "user70"
test_operation "/vol/delete?name=test70&authKey=$md5"
check_msg "success" 70 7


#test71 get users of a not exsits vol
test_operation "/admin/createVol?name=test71&capacity=1&owner=user71&mpCount=3"
check_msg "success" 71 1
test_operation "/vol/users?name=testnotexisit"
#cat return_Info.json
check_msg "no vol policy" 71 2
cal_md5 "user71" 
test_operation "/vol/delete?name=test71&authKey=$md5"
check_msg "success" 71 3


#test67 delete 2w user1,user2,..,user20000
for i in {1..20000}
do
    test_operation "/user/delete?user=user$i"
    check_msg "success" 67 $i
done


#rm file return_Info.json cfs-client
rm -rf ./return_Info.json
rm cfs-client

#echo test result
echo "-----------------------------"
echo "          test result        "
echo "-----------------------------"
for loop in {1..73}
do
    if [ ${test_array[$loop]} -eq 0 ]
    then
        if [ $loop -lt 10 ]
        then
            echo "          test[ $loop] ×"
        else
            echo "          test[$loop] ×"
        fi
   else
       if [ $loop -lt 10 ]
       then
           echo "          test[ $loop] √"
       else
           echo "          test[$loop] √"
       fi
    fi
done

