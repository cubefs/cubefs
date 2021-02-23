#!/bin/bash
LANG=C

test -f /usr/bin/openssl || yum install -y openssl

function usage()
{
    cat <<HELP_END
用法：sh publish.sh -f main -o server
        --file|-f        本地文件
        --object|-o      oss存放文件名
        --help|-h        显示帮助信息
HELP_END
    exit 1
}


function ossUpload() {
    Domain="storage.jd.local"
    BucketName="storageversion"
    AccessKey="In2kc1nP2jBx9QQ9"
    SecretKey="D7vZZSC7pBdF5RZYYa639tyO8yQtZLgSs5SqsqP9"

    FileName=$1
    OssPath=$2
    BucketPath=$BucketName/$OssPath

    GMTDate=$(date -u +'%a, %d %b %Y %T GMT')
    ContentMD5=$(md5sum $FileName | cut -d ' ' -f1)
    SignStr="PUT\n$ContentMD5\napplication/octet-stream\n$GMTDate\n/$BucketPath"
    Sign=`echo -ne "$SignStr" | openssl dgst -hmac "$SecretKey" -sha1 -binary | base64`

    curl -X PUT \
        -T $FileName \
        -H "Content-Type: application/octet-stream"  \
        -H "Content-MD5: $ContentMD5" \
        -H 'Connection:Keep-Alive' \
        -H "Date: $GMTDate" \
        -H "Authorization:jingdong $AccessKey:$Sign" \
        http://$Domain/$BucketPath

    if [ $? -eq 0 ];then
            echo "download url: http://$Domain/$BucketPath"
    else
            echo "upload err"
            exit 1
    fi
}

while [ "$1" != "${1##[-+]}" ]; do
    case $1 in
    '')
        usage
        exit 0
        ;;
    --file|-f)
        FileName=$2
        shift 2
        ;;
    --object|-o)
        Object=$2
        shift 2
        ;;
    *)
        usage
        exit 0
        ;;
  esac
done

Product="chubaofs"

Date=$(date +'%Y%m%d-%H%M%S')
OssPath=$Product/$Date/${Object:-$FileName}
ossUpload $FileName $OssPath

OssPath=$Product/latest/${Object:-$FileName}
ossUpload $FileName $OssPath

exit 0 