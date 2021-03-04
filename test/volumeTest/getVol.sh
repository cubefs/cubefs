#!/bin/bash
#author:Ulrica Zhang
#get volume test

source ./test_basic.sh

#sleep to wait for delete volume
#sleep 60s

create_num=50000

#1.name&authKey true
create_vol "test_get_1" "1" "1"
check_create "success" "1" "get" 

get_vol "test_get_1" ${authKey}
check_msg "\"msg\": \"success\"" "1" "get" 

del_vol "test_get_1" ${authKey}
check_delete "success" "1" "get" 

#2.name wrong
create_vol "test_get_2" "1" "1"
check_create "success" "2" "get" 

randomName_2=$(random_str "10")
get_vol ${randomName_2} ${authKey}
check_msg "\"msg\": \"vol not exists\"" "2" "get" 

del_vol "test_get_2" ${authKey}
check_delete "success" "2" "get" 

#3.authKey wrong
create_vol "test_get_3" "1" "1"
check_create "success" "3" "get" 

randomKey_3=$(random_str "32")
get_vol "test_get_3" ${randomKey_3}
check_msg "\"msg\": \"client and server auth key do not match\"" "3" "get" 

del_vol "test_get_3" ${authKey}
check_delete "success" "3" "get" 

#4.name&authKey wrong
create_vol "test_get_4" "1" "1"
check_create "success" "4" "get" 

randomName_4=$(random_str "10")
randomKey_4=$(random_str "32")
get_vol ${randomName_4} ${randomKey_4}
check_msg "\"msg\": \"vol not exists\"" "4" "get" 

del_vol "test_get_4" ${authKey}
check_delete "success" "4" "get" 

#5.name&authKey empty
create_vol "test_get_5" "1" "1"
check_create "success" "5" "get" 

get_vol "" ""
check_msg "parameter name not found" "5" "get" 

del_vol "test_get_5" ${authKey}
check_delete "success" "5" "get" 

#6. duration at 5w volume environment
create_vol_specific "${create_num}" "test_get_6" "1" "1"
echo "test get operation duration from ${create_num} volumes ..."
time get_vol "test_get_6_1" ${authKey}
del_vol_specific "${create_num}" "test_get_6" ${authKey}

