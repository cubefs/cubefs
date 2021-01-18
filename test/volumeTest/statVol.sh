#!/bin/bash
#author:Ulrica Zhang
#stat volume test

source ./test_basic.sh

#sleep to wait for delete volume
#sleep 60s

#1.name true
create_vol "test_stat_1" "1" "1"
check_create "success" "1" "stat" 

stat_vol "test_stat_1"
check_msg "success" "1" "stat" 

del_vol "test_stat_1" ${authKey}
check_delete "success" "1" "stat" 

#2.name wrong
create_vol "test_stat_2" "1" "1"
check_create "success" "2" "stat" 

randomName_2=$(random_str "10")
stat_vol ${randomName_2} 
check_msg "\"msg\": \"vol not exists\"" "2" "stat" 

del_vol "test_stat_2" ${authKey}
check_delete "success" "2" "stat" 

#3.name empty
create_vol "test_stat_3" "1" "1"
check_create "success" "3" "stat" 

stat_vol ""
check_msg "parameter name not found" "3" "stat" 

del_vol "test_stat_3" ${authKey}
check_delete "success" "3" "stat" 

#preparation for following operations
create_vol "test_stat" "10" "1"
check_create "success" "4_5_6_7" "stat" 
mnt_dir "test_stat"
#4.new volume
stat_vol "test_stat" 
check_msg "\"UsedSize\": 0" "4" "stat" 

#5.used volume
cover_write_file "/cfs/mnt/test_stat_file" "1024"
sleep 1m
stat_vol "test_stat"
cat return_record
check_msg "\"UsedRatio\": \"0.10\"" "5" "stat" 

umnt_dir
del_vol "test_stat" ${authKey}
check_delete "success" "4_5_6_7" "stat" 
