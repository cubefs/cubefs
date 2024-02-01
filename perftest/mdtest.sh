#!/bin/bash

ROOT_PATH=$(cd $(dirname $BASH_SOURCE)/..; pwd)

OP=${1:-"usage"}; shift
TEST_BASE=${1:-"/mnt/cfs/perftest01"}
TEST_TIME=${2:-"$(date +%Y%m%d_%H%M%S)"}

LOG_DIR=${ROOT_PATH}/log/mdtest/${test_time}
REPORT_DIR=${ROOT_PATH}/report/mdtest
mkdir -p ${LOG_DIR} ${REPORT_DIR} 

export PATH=$PATH:/usr/local/openmpi/bin/:/usr/local/ior/bin/
export LD_LIBRARY_PATH=/usr/local/openmpi/lib:${LD_LIBRARY_PATH}
export MPI_CC=mpicc

mpi_process="1 4 16 64"
mpi_clients="1 2 4 8"
small_sizes_kb="1 2 4 8 16 32 64 128"

set -e 

print_log_report() {
  log_file=${1:?"need log file"}
  echo "$log_file"
  printf  "FileOp\tMax\tMin\tMean\tStdDev\n"
  cat $log_file | sed -n '/---/,/--/p' | sed -n '1!p;N;$q;D' | awk '{printf "%s-%s\t%s\t%s\t%s\t%s\n", $1,$2,$3,$4,$5,$6}'
}

op_test() {
  for slot in $mpi_process ; do
    for nodes in $mpi_clients ; do
      log_file=${LOG_DIR}/op_${nodes}_${slot}.log
      echo "" > $log_file
      log_err=${LOG_DIR}/op_${nodes}_${slot}.err
      report_file=${REPORT_DIR}/op_${TEST_TIME}.txt
      test_dir=${TEST_BASE}/mdtest/op_${nodes}_${slot}_${TEST_TIME}
      mkdir -p ${test_dir}

      proc_num=$(($slot * $nodes))
      hosts_file=${ROOT_PATH}/hosts/hosts${slot}.txt
      mpi_args="-np ${proc_num} --hostfile ${hosts_file}"
      mdtest_cmd="mdtest -n 5000 -u -z 2 -i 3 -d ${test_dir}"

      echo "mpirun ${mpi_args} ${mdtest_cmd}"
      mpirun \
          --allow-run-as-root \
          --prefix /usr/local/openmpi \
          --mca plm_rsh_agent rsh \
          --mca plm_rsh_force_rsh 1 \
        $mpi_args \
        $mdtest_cmd | tee >> ${log_file}
      res=$?
      if [ $res -ne 0 ] ; then
        cat ${log_file} >> ${log_err}
        exit $res
      fi

      print_log_report $log_file | tee >> $report_file
      sleep 5
    done
    sleep 10
  done
}

smallfile_test() {
  slots=64
  nodes=8
  for i in $small_sizes_kb ; do
    log_file=${LOG_DIR}/small_${i}_KB.log
    log_err=${LOG_DIR}/small_${i}_KB.err
    report_file=${REPORT_DIR}/small_${TEST_TIME}.txt
    test_dir=${TEST_BASE}/mdtest/small_${i}_KB_${TEST_TIME}
    mkdir -p ${test_dir}

    file_size=$(($i*KB))
    np=$(($slots*$nodes))
    mpi_args="-np $np --hostfile ${ROOT_PATH}/hosts/hosts${slots}.txt"
    mdtest_cmd="mdtest -n 1000 -w ${file_size} -e ${file_size} -y -u -i 3 -N 1 -F -R -d ${test_dir}"
    echo
    echo "mpirun $CMD $MDTEST_CMD"
    mpirun \
          --allow-run-as-root \
          --prefix /usr/local/openmpi \
          --mca plm_rsh_agent rsh \
          --mca plm_rsh_force_rsh 1 \
      $mpi_args \
      $mdtest_cmd 2>${log_err} | tee -a ${log_file}

    print_log_report $log_file | tee -a $report_file

    sleep 5
  done
}


usage() {
  cat <<EOF
  $0 <op> <test_dir> [args]
EOF
}

case "$OP" in
  "op") op_test "$@"  ;;
  "smallfile") smallfile_test "$@"  ;;
  *) usage ;;
esac

