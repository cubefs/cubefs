#!/bin/bash
#author:Ulrica Zhang
#update volume test

source ./test_basic.sh

#sleep to wait for delete volume
#sleep 60s

create_num=50000

#1.name&authKey true
create_vol "test_update_1" "1" "1"
check_create "success" "1" "update"

update_vol "test_update_1" "2" ${authKey}
cat return_record
check_msg "\"msg\": \"success\"" "1" "update"

del_vol "test_update_1" ${authKey}
check_delete "success" "1" "update" 

#2.name wrong
create_vol "test_update_2" "1" "1"
check_create "success" "2" "update" 

randomName_2=$(random_str "10")
update_vol ${randomName_2} "2" ${authKey}
check_msg "\"msg\": \"vol not exists\"" "2" "update" 

del_vol "test_update_2" ${authKey}
check_delete "success" "2" "update" 

#3.authKey wrong
create_vol "test_update_3" "1" "1"
check_create "success" "3" "update" 

randomKey_3=$(random_str "32")
update_vol "test_update_3" "2" ${randomKey_3}
check_msg "\"msg\": \"client and server auth key do not match\"" "3" "update" 

del_vol "test_update_3" ${authKey}
check_delete "success" "3" "update" 

#4.name&authKey wrong
create_vol "test_update_4" "1" "1"
check_create "success" "4" "update" 

randomName_4=$(random_str "10")
randomKey_4=$(random_str "32")
update_vol ${randomName_4} "2" ${randomKey_4}
check_msg "\"msg\": \"vol not exists\"" "4" "update" 

del_vol "test_update_4" ${authKey}
check_delete "success" "4" "update" 

#5.name&authKey empty
create_vol "test_update_5" "1" "1"
check_create "success" "5" "update" 

update_vol "" "2" ""
check_msg "parameter name not found" "5" "update" 

del_vol "test_update_5" ${authKey}
check_delete "success" "5" "update" 

#6.capacity=old
create_vol "test_update_6" "1" "1"
check_create "success" "6" "update" 

update_vol "test_update_6" "1" ${authKey}
check_msg "\"msg\": \"success\"" "6" "update" 

del_vol "test_update_6" ${authKey}
check_delete "success" "6" "update" 

#7.capacity<old
create_vol "test_update_7" "2" "1"
check_create "success" "7" "update" 

update_vol "test_update_7" "1" ${authKey}
check_msg "\"msg\": \"success\"" "7" "update" 

del_vol "test_update_7" ${authKey}
check_delete "success" "7" "update" 

#8.capacity<volUsedSpace
#preparation for following operations
create_vol "test_update_8" "10" "1"
check_create "success" "8" "stat" 
mnt_dir "test_update_8"

write_file "/cfs/mnt/test_update_file" "1024"
sleep 3m
update_vol "test_update_8" "1" ${authKey}
#cat return_record
check_msg "has to be 20 percent larger than the used space" "8" "update"

umnt_dir
del_vol "test_update_8" ${authKey}
check_delete "success" "8" "update" 

#9.zoneName=empty
create_vol "test_update_9" "2" "1"
check_create "success" "9" "update" 

update_vol "test_update_9" "1" ${authKey} "&zoneName="
check_msg "\"msg\": \"success\"" "9" "update" 

del_vol "test_update_9" ${authKey}
check_delete "success" "9" "update" 

#10.zoneName=default
create_vol "test_update_10" "2" "1"
check_create "success" "10" "update"

update_vol "test_update_10" "1" ${authKey} "&zoneName=default"
check_msg "success" "10" "update" 

del_vol "test_update_10" ${authKey}
check_delete "success" "10" "update" 

#11.zoneName=other exited zone
create_vol "test_update_11" "2" "1" "&zoneName=testUpdate11_zone"
check_create "success" "11" "update" 

update_vol "test_update_11" "1" ${authKey} "&zoneName=test_update_11_zone"
check_msg "hhhhhh" "11" "update" 

del_vol "test_update_11" ${authKey}
check_delete "success" "11" "update" 

#11.zoneName=non-exited zone
create_vol "test_update_11" "2" "1"
check_create "success" "11" "update" 

randomName_11=$(random_str "10")
update_vol "test_update_11" "1" ${authKey} "&zoneName=${randomName_11}"
check_msg "not found " "11" "update" 

del_vol "test_update_11" ${authKey}
check_delete "success" "11" "update" 

#12.update at 5w volumes environment
create_vol_specific "${create_num}" "test_update_12" "1" "1"
echo "test update operation duration from ${create_num} volumes ..."
time update_vol "test_update_12_1" "2" ${authKey}
check_msg "\"msg\": \"success\"" "12" "update" 
del_vol_specific "${create_num}" "test_update_12" ${authKey}

