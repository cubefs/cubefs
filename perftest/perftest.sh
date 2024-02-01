#!/bin/bash
root_path=$(cd $(dirname $BASH_SOURCE)/.. ; pwd)

cluster_name="cfs-perftest-client"
mpi_host=${MPI_HOST:-"192.168.0.210"}
cfs_target_base=/opt/chubaofs
perftest_base_dir=${cfs_target_base}/perftest
perftest_script_dir=${perftest_base_dir}/script
client_bin_path=perftest/bin/cfs-client

roll_batch=${CFS_ROLL_BATCH:-1}
roll_interval=${CFS_ROLL_INTERVAL:-"$((60))"}
stop_timeout=${CMD_STOP_TIMEOUT:-$((10*60))}
start_timeout=${CMD_START_TIMEOUT:-$((50*60))}
args=()
cmd=${1:-"usage"}; shift
for arg in $@ ; do
    case $arg in
    --debug) set -x ;;
    -m=*|--mpi_host=*) mpi_host=${arg##*=} ;;
    -c=*|--cluster=*) cluster_name=${arg##*=} ;;
    *) args+=("$arg");
    esac
done

client_json_path=perftest/conf/client.json
client_json_real_path=${root_path}/salt/${client_json_path}
cfs_mount_point=$(cat ${client_json_real_path} | jq -r '.mountPoint')

install() {
  salt -N "$cluster_name" cp.get_dir  salt://perftest/ ${cfs_target_base} makedirs=True
  salt -N "$cluster_name" cmd.run  "chmod a+x ${cfs_target_base}/perftest/bin/*"
}

list_jobs() {
  salt -S "${mpi_host}" saltutil.running
}

kill_job() {
  salt -N "$cluster_name" saltutil.kill_job $1
}

mount_cfs() {
  salt -N "$cluster_name" cmd.run  "${perftest_base_dir}/bin/cfs-client -c ${perftest_base_dir}/conf/client.json"
}

umount_cfs() {
  salt -N "$cluster_name" cmd.run  "umount ${cfs_mount_point}"
}

dt=$(date +%Y%m%d_%H%M%S)
task_file=/opt/perftest_task.txt

list_tasks() {
  cat $task_file
}

mdtest_op() {
  echo "mdtest op: $dt"
  echo "mdtest_op $dt" >> $task_file
  salt -S "${mpi_host}" cmd.run "sh ${perftest_script_dir}/mdtest.sh op ${cfs_mount_point} ${dt}"
}

mdtest_small() {
  echo "mdtest_small $dt"
  echo "mdtest_small: $dt" >> $task_file
  salt -S "${mpi_host}" cmd.run "sh ${perftest_script_dir}/mdtest.sh smallfile ${cfs_mount_point} ${dt}"
}

fio_test() {
  echo "fio_test $dt"
  echo "fio_test: $dt" >> $task_file

  salt -S "${mpi_host}" cmd.run "sh ${perftest_script_dir}/fiotest.sh mpi_batch ${cfs_mount_point} ${dt}"
}

print_mdtest_report() {
  last_task=$( cat $task_file | grep "mdtest" | tail -1 | awk '{print $2}')
  test_date=${1:-"$last_task"}
  salt -S "${mpi_host}" cmd.run "cat ${perftest_base_dir}/report/mdtest/*_${test_date}*.txt"
}

print_fio_report() {
  last_task=$( cat $task_file | grep "fio_test:" | tail -1 | awk '{print $2}')
  test_date=${1:-"$last_task"}
  salt -S "${mpi_host}" cmd.run "cat ${perftest_base_dir}/report/fiotest/${test_date}.txt"  
}

run_cmd() {
  salt -N "$cluster_name" cmd.run "$@"
}

usage() {
  cat <<EOF
chubaofs perftest tool
Usage:
  $0 <cluster_dir> <cmd> [args]
Cmd:
  install:        install perftest scripts
  list_jobs:       list perftest scripts
  kill_job:        kill perftest scripts
  mount_cfs:      cluster, get cluster info
  umount_cfs:      cluster, get cluster info
  update:      cluster, get cluster info
  mdtest_op:      cluster, get cluster info
  mdtest_small:      cluster, get cluster info
  fio_test:      cluster, get cluster info
EOF
}

case "$cmd" in
  "install") install "${args[@]}" ;;
  "list_jobs") list_jobs "${args[@]}" ;;
  "kill_job") kill_job "${args[@]}" ;;
  "mount_cfs") mount_cfs "${args[@]}" ;;
  "umount_cfs") umount_cfs "${args[@]}" ;;
  "run") run_cmd "${args[@]}" ;;
  "mdtest_op") mdtest_op "${args[@]}" ;;
  "mdtest_small") mdtest_small "${args[@]}"  ;;
  "fio_test") fio_test "${args[@]}"  ;;
  "print_fio_report") print_fio_report "${args[@]}" ;;
  "print_mdtest_report") print_mdtest_report "${args[@]}" ;;
  "list_tasks") list_tasks "$@"  ;;
  *) usage ;;
esac

