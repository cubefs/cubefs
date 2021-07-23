#!/usr/bin/env bash

function upload_to_oss() {
    file_name=$1
    oss_key=$2

    oss_ip="172.20.25.128"
    oss_domain="storage.jd.local"
    bucket_name="jvspub"
    access_key="MehHGIFUpQrq4e5V"
    secret_key="F0PtnMbqQb1kDzmYpqTepj14NtC6tq5jN4WZMJAf"

    oss_full_path=${bucket_name}/${oss_key}

    gmt_date=$(date -u +'%a, %d %b %Y %T GMT')
    content_md5=$(md5sum ${file_name} | cut -d ' ' -f1)
    sign_str="PUT\n$content_md5\napplication/octet-stream\n$gmt_date\n/$oss_full_path"
    sign=`echo -ne "$sign_str" | openssl dgst -hmac "$secret_key" -sha1 -binary | base64`

    curl -X PUT \
        -T ${file_name} \
        -H "Content-Type: application/octet-stream"  \
        -H "Content-MD5: $content_md5" \
        -H 'Connection:Keep-Alive' \
        -H "Date: $gmt_date" \
        -H "Authorization:jingdong $access_key:$sign" \
        -H "Host: $oss_domain" \
        http://${oss_ip}/${oss_full_path} 2>/dev/null

    if [ $? -eq 0 ];then
            echo "http://$oss_domain/$oss_full_path"
            return 0
    else
            return 1
    fi
}

function analyze_coverage() {
    source_code_dir=/go/src/github.com/chubaofs/chubaofs
    cover_dir=/cfs/coverage

    summary_cover_file=summary.cov
    summary_report_text=summary_coverage.txt
    summary_report_html=summary_coverage.html

    mod_sdk_cover_file=mod_sdk.cov
    mod_sdk_report_text=mod_sdk_coverage.txt

    mod_master_cover_file=mod_master.cov
    mod_master_report_text=mod_master_coverage.txt

    mod_meta_cover_file=mod_meta.cov
    mod_meta_report_text=mod_meta_coverage.txt

    mod_data_cover_file=mod_data.cov
    mod_data_report_text=mod_data_coverage.txt

    mod_object_cover_file=mod_object.cov
    mod_object_report_text=mod_object_coverage.txt

    # 获取源码版本信息
    pushd ${source_code_dir} > /dev/null
    branch_name=`git rev-parse --abbrev-ref HEAD`
    commit_id=`git rev-parse HEAD`
    popd > /dev/null

    if [[ ! -d ${cover_dir} ]]; then
        echo "coverage data path not exists."
        return
    fi

    pushd ${cover_dir} > /dev/null

    # 合并拆分覆盖率源文件
    source_cov_files=`ls node_*.cov`
    touch ${summary_cover_file} && echo "mode: atomic" > ${summary_cover_file}
    for cov_file in `echo ${source_cov_files}`; do
        tail -n +2 ${cov_file} | egrep -v "chubaofs/console|chubaofs/sdk/graphql" >> ${summary_cover_file}
        echo "[INFO] Node coverage source ${cov_file} merged to ${summary_cover_file}"
    done;
    go tool cover -func=${summary_cover_file} -o ${summary_report_text}
    go tool cover -html=${summary_cover_file} -o ${summary_report_html}

    touch ${mod_sdk_cover_file} && echo "mode: atomic" > ${mod_sdk_cover_file}
    grep "github.com/chubaofs/chubaofs/sdk" ${summary_cover_file} >> ${mod_sdk_cover_file}
    go tool cover -func=${mod_sdk_cover_file} -o ${mod_sdk_report_text}

    touch ${mod_master_cover_file} && echo "mode: atomic" > ${mod_master_cover_file}
    grep "github.com/chubaofs/chubaofs/master" ${summary_cover_file} >> ${mod_master_cover_file}
    go tool cover -func=${mod_master_cover_file} -o ${mod_master_report_text}

    touch ${mod_meta_cover_file} && echo "mode: atomic" > ${mod_meta_cover_file}
    grep "github.com/chubaofs/chubaofs/metanode" ${summary_cover_file} >> ${mod_meta_cover_file}
    go tool cover -func=${mod_meta_cover_file} -o ${mod_meta_report_text}

    touch ${mod_data_cover_file} && echo "mode: atomic" > ${mod_data_cover_file}
    grep "github.com/chubaofs/chubaofs/datanode" ${summary_cover_file} >> ${mod_data_cover_file}
    go tool cover -func=${mod_data_cover_file} -o ${mod_data_report_text}

    touch ${mod_object_cover_file} && echo "mode: atomic" > ${mod_object_cover_file}
    grep "github.com/chubaofs/chubaofs/objectnode" ${summary_cover_file} >> ${mod_object_cover_file}
    go tool cover -func=${mod_object_cover_file} -o ${mod_object_report_text}

    total_coverage=`tail -n 1 ${summary_report_text} | grep -i "total" | awk '{print$3}'`
    mod_sdk_coverage=`tail -n 1 ${mod_sdk_report_text} | grep -i "total" | awk '{print$3}'`
    mod_master_coverage=`tail -n 1 ${mod_master_report_text} | grep -i "total" | awk '{print$3}'`
    mod_meta_coverage=`tail -n 1 ${mod_meta_report_text} | grep -i "total" | awk '{print$3}'`
    mod_data_coverage=`tail -n 1 ${mod_data_report_text} | grep -i "total" | awk '{print$3}'`
    mod_object_coverage=`tail -n 1 ${mod_object_report_text} | grep -i "total" | awk '{print$3}'`
    echo "----------------------------------------------------------------------"
    echo "                         Coverage Report                              "
    echo "branch    : ${branch_name}"
    echo "commit    : ${commit_id}"
    echo "sdk       : ${mod_sdk_coverage}"
    echo "master    : ${mod_master_coverage}"
    echo "metanode  : ${mod_meta_coverage}"
    echo "datanode  : ${mod_data_coverage}"
    echo "objectnode: ${mod_object_coverage}"
    echo "total     : ${total_coverage}"
    echo "----------------------------------------------------------------------"

    # 将整体报告打包压缩上传到OSS
    tar zcvf summary_coverage.tar.gz ${summary_report_text} ${summary_report_html} > /dev/null
    oss_key="chubaofs_ci_coverage_report_${branch_name}_${commit_id}.tar.gz"
    url=`upload_to_oss summary_coverage.tar.gz ${oss_key}`
    if [[ $? -eq 0 ]]; then
        echo "Detail report uploaded to OSS: ${url}"
    else
        echo "Detail report upload failed"
    fi

    popd > /dev/null
}

analyze_coverage
