# Copyright 2020 The ChubaoFS Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# -*- coding: utf-8 -*-
import os
import subprocess
import time

source_code_dir = "/go/src/github.com/chubaofs/chubaofs"
cover_dir = "/cfs/coverage"

summary_cover_file = "summary.cov"
summary_report_text = "summary_coverage.txt"
summary_report_html = "summary_coverage.html"

mod_sdk_cover_file = "mod_sdk.cov"
mod_sdk_report_text = "mod_sdk_coverage.txt"
grep_sdk_prefix = "github.com/chubaofs/chubaofs/sdk"

mod_master_cover_file = "mod_master.cov"
mod_master_report_text = "mod_master_coverage.txt"
grep_master_prefix = "github.com/chubaofs/chubaofs/master"

mod_meta_cover_file = "mod_meta.cov"
mod_meta_report_text = "mod_meta_coverage.txt"
grep_meta_prefix = "github.com/chubaofs/chubaofs/metanode"

mod_data_cover_file = "mod_data.cov"
mod_data_report_text = "mod_data_coverage.txt"
grep_data_prefix = "github.com/chubaofs/chubaofs/datanode"

mod_object_cover_file = "mod_object.cov"
mod_object_report_text = "mod_object_coverage.txt"
grep_object_prefix = "github.com/chubaofs/chubaofs/objectnode"

dicts = {}
dictsSDK = {}
dictsMaster = {}
dictsMeta = {}
dictsData = {}
dictsObject = {}
errCode = 1
branch_name = ""
commit_id = ""
total_coverage = 0
mod_sdk_coverage = 0
mod_master_coverage = 0
mod_meta_coverage = 0
mod_data_coverage = 0
mod_object_coverage = 0


def DealDataFile():
    os.chdir('/go/src/github.com/chubaofs/chubaofs')
    global branch_name
    branch_name = ExecCommandAndGetResultStr('git rev-parse --abbrev-ref HEAD')
    global commit_id
    commit_id = ExecCommandAndGetResultStr('git rev-parse HEAD')

    if os.path.exists(cover_dir) != True:
        print("coverage data path not exists.")
        return errCode

    os.chdir('/cfs/coverage')
    os.system(
        ' grep -h -Ev "^mode:|chubaofs/convertnode|chubaofs/console|chubaofs/sdk/graphql|chubaofs/vendor" /cfs/coverage/*.cov | sort > merge_cover_tmp ')

    SpiltFile()
    UploadToOss()


def SpiltFile():
    originFile = "merge_cover_tmp"

    with open(originFile) as covFileHandle:
        for line in covFileHandle:
            eachLine = line.strip("\n")

            if "atomic" in eachLine:
                continue

            eachKey = "#".join(eachLine.split(" ")[:-1])
            eachValue = int(eachLine.split(" ")[-1])

            if eachKey in dicts:
                dicts[eachKey] += eachValue
            else:
                dicts[eachKey] = eachValue

            SplitData(eachKey, eachValue)

    sorted(dicts)

    WriteFile()

    PrintReport()


def SplitData(key, value):
    SplitDataByPrefix(grep_sdk_prefix, dictsSDK, key, value)
    SplitDataByPrefix(grep_master_prefix, dictsMaster, key, value)
    SplitDataByPrefix(grep_meta_prefix, dictsMeta, key, value)
    SplitDataByPrefix(grep_data_prefix, dictsData, key, value)
    SplitDataByPrefix(grep_object_prefix, dictsObject, key, value)


def SplitDataByPrefix(prefix, data, key, value):
    if prefix not in key:
        return

    if key in data:
        data[key] += value
    else:
        data[key] = value


def WriteFile():
    WriteToCovFile(summary_cover_file, dicts, summary_report_text)
    WriteToCovFile(mod_sdk_cover_file, dictsSDK, mod_sdk_report_text)
    WriteToCovFile(mod_master_cover_file, dictsMaster, mod_master_report_text)
    WriteToCovFile(mod_meta_cover_file, dictsMeta, mod_meta_report_text)
    WriteToCovFile(mod_data_cover_file, dictsData, mod_data_report_text)
    WriteToCovFile(mod_object_cover_file, dictsObject, mod_object_report_text)


def WriteToCovFile(fileName, data, report_text):
    with open(fileName, mode="w") as handle:
        handle.write("mode: atomic\n")

        if len(data) == 0:
            return

        for k, v in data.items():
            handle.write("%s %s\n" % (k.replace("#", " "), v))

    command = "go tool cover -func=%s -o %s" % (fileName, report_text)
    ExecCommandAndGetResultStr(command)
    if fileName == summary_cover_file:
        command = "go tool cover -html=%s -o %s" % (fileName, summary_report_html)
        ExecCommandAndGetResultStr(command)

    command = "tail -n 1 %s | grep -i \"total\" | awk '{print  $3}'" % report_text
    result = ExecCommandAndGetResultStr(command)

    if fileName == summary_cover_file:
        global total_coverage
        total_coverage = result
    elif fileName == mod_sdk_cover_file:
        global mod_sdk_coverage
        mod_sdk_coverage = result

    elif fileName == mod_master_cover_file:
        global mod_master_coverage
        mod_master_coverage = result

    elif fileName == mod_meta_cover_file:
        global mod_meta_coverage
        mod_meta_coverage = result

    elif fileName == mod_data_cover_file:
        global mod_data_coverage
        mod_data_coverage = result

    elif fileName == mod_object_cover_file:
        global mod_object_coverage
        mod_object_coverage = result

    else:
        pass


def PrintReport():
    print("----------------------------------------------------------------------")
    print("                         Coverage Report                              ")
    print("branch    : %s" % branch_name)
    print("commit    : %s" % commit_id)
    print("sdk       : %s" % mod_sdk_coverage)
    print("master    : %s" % mod_master_coverage)
    print("metanode  : %s" % mod_meta_coverage)
    print("datanode  : %s" % mod_data_coverage)
    print("objectnode: %s" % mod_object_coverage)
    print("total     : %s" % total_coverage)
    print("----------------------------------------------------------------------")
    os.chdir("/cfs/coverage")


def ExecCommandAndGetResultStr(command):
    return subprocess.getoutput(command)


def UploadToOss():
    os.system("tar zcvf summary_coverage.tar.gz %s %s > /dev/null" % (summary_report_text, summary_report_html))

    oss_key = "chubaofs_ci_coverage_report_%s_%s.tar.gz" % (branch_name, commit_id)
    file_name = "summary_coverage.tar.gz"
    oss_key = oss_key
    oss_ip = "172.20.25.128"
    oss_domain = "storage.jd.local"
    bucket_name = "jvspub"
    access_key = "MehHGIFUpQrq4e5V"
    secret_key = "F0PtnMbqQb1kDzmYpqTepj14NtC6tq5jN4WZMJAf"

    oss_full_path = bucket_name + "/" + oss_key
    gmt_date = ExecCommandAndGetResultStr("date -u +'%a, %d %b %Y %T GMT'")
    content_md5 = ExecCommandAndGetResultStr("md5sum summary_coverage.tar.gz | cut -d ' ' -f1")
    sign_str = "PUT\n%s\napplication/octet-stream\n%s\n/%s" % (content_md5, gmt_date, oss_full_path)
    sign = ExecCommandAndGetResultStr(
        'printf  "' + sign_str + '" | openssl dgst -hmac "' + secret_key + '" -sha1 -binary | base64')

    Authorization = "Authorization: jingdong %s:%s" % (access_key, sign)
    command = 'curl -X PUT \
        -T ' + file_name + ' \
        -H "Content-Type: application/octet-stream"  \
        -H "Content-MD5: ' + content_md5 + '" \
        -H "Connection:Keep-Alive" \
        -H "Date: ' + gmt_date + '" \
        -H "' + Authorization + '" \
        -H "Host: ' + oss_domain + '" \
        http://' + oss_ip + '/' + oss_full_path + ''
    result = ExecCommandAndGetResultStr(command)

    if '0' in result:
        print("Detail report uploaded to OSS: http://%s/%s" % (oss_domain, oss_full_path))

    else:
        print("Detail report upload failed")


def main():
    if DealDataFile() == errCode:
        return


if __name__ == '__main__':
    s = time.time()
    main()
    print("analyze_coverage_total run time [%s]s" % (time.time() - s))
