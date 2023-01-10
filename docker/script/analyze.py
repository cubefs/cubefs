#!/bin/env python
import os
import subprocess
import logging

# Configurations
SOURCE_CODE_BASE_DIR = "/go/src/github.com/chubaofs/chubaofs"
COV_FILE_BASE_DIR = '/cfs/coverage'
SUMMARY_COV_FILE = 'summary.cov'
SUMMARY_COV_TEXT_FILE = 'summary_coverage.txt'
SUMMARY_COV_HTML_FILE = 'summary_coverage.html'

CONVERT_MODE_TEXT="text"
CONVERT_MODE_HTML="html"

MODULE_PREFIX_SDK = 'github.com/chubaofs/chuabofs/sdk/'

LOG_LEVEL=logging.ERROR

def init_logging():
    logging.basicConfig(level=LOG_LEVEL,
                        format='%(asctime)s [%(levelname)s]: %(message)s', datefmt='%Y/%m/%d %I:%M:%S')

def analyze_coverage():
    # collect source code version related information.
    branch, commit = get_source_code_commit_info(SOURCE_CODE_BASE_DIR)

    # compact all original coverage data file into one.
    original_files = collect_original_cov_files(COV_FILE_BASE_DIR)
    summary_cov_file_path = os.path.join(COV_FILE_BASE_DIR, SUMMARY_COV_FILE)
    compact_cov_files(original_files, summary_cov_file_path)

    # convert coverage data file to human-readable format.

    # collect summary coverage
    summary_cov_text_file = os.path.join(COV_FILE_BASE_DIR, SUMMARY_COV_TEXT_FILE)
    convert_cov_file(summary_cov_file_path, summary_cov_text_file, mode=CONVERT_MODE_TEXT)
    summary_cov_html_file = os.path.join(COV_FILE_BASE_DIR, SUMMARY_COV_HTML_FILE)
    convert_cov_file(summary_cov_file_path, summary_cov_html_file, mode=CONVERT_MODE_HTML)
    _, summary_total, _ = execute_command('tail -n 1 %s | awk \'{print$3}\'' % summary_cov_text_file)

    # collect model details
    modules = [
        "sdk",
        "repl",
        "storage",
        "master",
        "metanode",
        "datanode",
        "objectnode",
        "codecnode",
        "ecnode",
        "util",
    ]
    module_details = {}
    temp_dir = '/tmp'
    for module in modules:
        module_cov_file_path = os.path.join(temp_dir, 'module_%s.cov' % module)
        module_prefix = 'github.com/chubaofs/chubaofs/%s/' % module
        filter_cov_file(summary_cov_file_path, module_cov_file_path, module_prefix)
        module_text_file = os.path.join(temp_dir, 'modfule_%s_coverage.txt' % module)
        convert_cov_file(module_cov_file_path, module_text_file, mode=CONVERT_MODE_TEXT)
        _, module_coverage, _ = execute_command('tail -n 1 %s | awk \'{print$3}\'' % module_text_file)
        module_details[module] = module_coverage.decode('utf-8').strip('\n')
        os.remove(module_cov_file_path)
        os.remove(module_text_file)

    print('----------------------------------------------------------------------')
    print('                         Coverage Report                              ')
    print('Branch    : %s' % branch.decode('utf-8').strip('\n'))
    print('Commit    : %s' % commit.decode('utf-8').strip('\n'))
    print('Total     : %s' % summary_total.decode('utf-8').strip('\n'))
    print('----------------------------------------------------------------------')
    print('%-12s  %s' % ('MODULE', 'COVERAGE'))
    for module, result in module_details.items():
        coverage = result if len(result) > 0 else 'N/A'
        print('%-12s  %s' % (module, coverage))
    print('----------------------------------------------------------------------')
    pass

def execute_command(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.wait()
    out, err = p.communicate()
    returncode = p.returncode
    if returncode == 0:
        logging.debug('execute command \'%s\' complete.\nSTDOUT:\n%s' % (command, out))
    else:
        logging.error('execute command \'%s\' failed, return %s.\nSTDOUT:\n%s\nSTDERR:\n%s' % (command, returncode, out, err))
    return p.returncode, out, err

def exclude_package_filter(content):
    exclude_packages = [
        "github.com/chubaofs/chubaofs/cli/",
        "github.com/chubaofs/chubaofs/client/fs/",
        "github.com/chubaofs/chubaofs/console/",
        "github.com/chubaofs/chubaofs/monitor/",
        "github.com/chubaofs/chubaofs/objectnode/",
        "github.com/chubaofs/chubaofs/proto/",
        "github.com/chubaofs/chubaofs/vendor/",
        "github.com/chubaofs/chubaofs/ecstorage/",
        "github.com/chubaofs/chubaofs/convertnode/",
        "github.com/chubaofs/chubaofs/schedulenode/",
        "github.com/chubaofs/chubaofs/metanode/",
        "github.com/chubaofs/chubaofs/master/",
        "github.com/chubaofs/chubaofs/ecnode/",
        "github.com/chubaofs/chubaofs/sdk/graphql/",
        "github.com/chubaofs/chubaofs/sdk/auth/",
        "github.com/chubaofs/chubaofs/sdk/monitor/",
        "github.com/chubaofs/chubaofs/sdk/mysql/",
        "github.com/chubaofs/chubaofs/sdk/hbase/",
        "github.com/chubaofs/chubaofs/sdk/master/",
        "github.com/chubaofs/chubaofs/sdk/meta/",
        "github.com/chubaofs/chubaofs/sdk/scheduler/",
        "github.com/chubaofs/chubaofs/util/ump/",
        "github.com/chubaofs/chubaofs/util/synclist/",
        "github.com/chubaofs/chubaofs/util/log/",
        "github.com/chubaofs/chubaofs/util/cpu/",
        "github.com/chubaofs/chubaofs/util/log/http/",
        "github.com/chubaofs/chubaofs/util/keystore/",
        "github.com/chubaofs/chubaofs/util/string/",
        "github.com/chubaofs/chubaofs/util/iputil/",
        "github.com/chubaofs/chubaofs/util/ec/",
        "github.com/chubaofs/chubaofs/util/cryptoutil/",
        "github.com/chubaofs/chubaofs/util/caps/",
        "github.com/chubaofs/chubaofs/util/config/",
        "github.com/chubaofs/chubaofs/util/hbase/",
        "github.com/chubaofs/chubaofs/raftstore/rafttest/",
    ]
    for package in exclude_packages:
        if content.find(package) >= 0:
            return False
    return True

def collect_original_cov_files(file_dir):
    os.chdir(file_dir)
    files = os.listdir('.')
    cov_files = []
    for file in files:
        if file.endswith('.cov'):
            cov_files.append(os.path.join(file_dir, file))
    return cov_files

def compact_cov_files(originals, target_file, filter=exclude_package_filter):
    data_dict = {}
    total_lines = 0
    for original in originals:
        with open(original) as f:
            for line in f:
                if line.startswith('mode:'):
                    continue
                total_lines += 1
                if filter is not None:
                    if not filter(line):
                        continue
                key = "#".join(line.split(" ")[:-1])
                val = int(line.split(" ")[-1])
                if key in data_dict:
                    data_dict[key] += val
                else:
                    data_dict[key] = val
    sorted(data_dict)
    logging.debug('original coverage file compact complete: %d -> %d' % (total_lines, len(data_dict)))
    with open(target_file, 'w') as f:
        f.write('mode: atomic\n')
        for key, val in data_dict.items():
            f.write("%s %s\n" % (key.replace("#", " "), val))
    return

def convert_cov_file(src_file, target_file, mode=CONVERT_MODE_TEXT):
    if mode == CONVERT_MODE_TEXT:
        command = 'GO111MODULE="off" go tool cover -func=%s -o=%s' % (src_file, target_file)
        execute_command(command)
        return
    if mode == CONVERT_MODE_HTML:
        command = 'GO111MODULE="off" go tool cover -html=%s -o=%s' % (src_file, target_file)
        execute_command(command)
        return
    return

def filter_cov_file(source, target, prefix):
    def prefix_filter(content):
        return content.find(prefix) >= 0
    compact_cov_files(originals=[source], target_file=target, filter=prefix_filter)

def get_source_code_commit_info(base_dir):
    command = 'cd %s && git rev-parse --abbrev-ref HEAD' % base_dir
    _, branch, _ = execute_command(command)
    command = 'cd %s && git rev-parse HEAD' % base_dir
    _, commit, _ = execute_command(command)
    return branch, commit

if __name__ == '__main__':
    init_logging()
    analyze_coverage()
    pass