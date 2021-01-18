#!/bin/bash
#author:Ulrica Zhang
#list volume test

source ./test_basic.sh

#sleep to wait for delete volume
#sleep 60s

create_num=50000
create_5w=50000
create_vol_specific "${create_num}" "test_list" "1" "1"
#1.keyword true
list_vol "test_list"
check_msg "\"msg\": \"success\"" "1" "list" 

#2.keyword wrong
randomKey_2=$(random_str "10")
list_vol ${randomKey_2}
check_msg "\"msg\": \"success\"" "2" "list" 

#3.keyword empty
list_vol ""
check_msg "\"msg\": \"success\"" "3" "list" 

del_vol_specific "${create_num}" "test_list" ${authKey}

#4.list at 5w volumes environment
create_vol_specific "${create_5w}" "test_list_4" "1" "1"
echo "test list operation duration from ${create_num} volumes ..."
time list_vol "test_list_12_1" ${authKey}
check_msg "\"msg\": \"success\"" "4" "list" 
del_vol_specific "${create_num}" "test_list_5" ${authKey}


