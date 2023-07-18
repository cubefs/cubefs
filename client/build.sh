#!/usr/bin/env bash

BranchName=`git rev-parse --abbrev-ref HEAD`
CommitID=`git rev-parse HEAD`
BuildTime=`date +%Y-%m-%d\ %H:%M`

[[ "-$GOPATH" == "-" ]] && { echo "GOPATH not set"; exit 1 ; }

goflag=""
gccflag="-g"
dynamic=0
build_sdk=1
build_client=1
build_test=0
pack_libs=0

help() {
    cat <<EOF

Usage: ./build.sh [ -h | --help ] [ -g ] [ --sdk-only | --client-only ]
    -h, --help              show help info
    --lcov                  lcov coverage measurements
    -d, --dynamic           with dynamic updating feature (customized golang required)
    -s, --sdk-only          build sdk (libcfssdk.so libempty.so) only
    -c, --client-only       build client (libcfsclient.so and cfs-client) only
    -p, --pack-libs         pack libs to cfs-client-libs.tar.gz used for bypass upgrade
    test                    build in test mode
EOF
    exit 0
}

ARGS=( "$@" )
for opt in ${ARGS[*]} ; do
    case "$opt" in
        -h | --help)
            help
            ;;
        -d | --dynamic)
            dynamic=1
            ;;
        --lcov)
            gccflag="$gccflag -fprofile-arcs -ftest-coverage -lgcov"
            ;;
        -s | --sdk-only)
            build_sdk=1
            build_client=0
            ;;
	    -c | --client-only)
            build_sdk=0
            build_client=1
            ;;
        -p | --pack-libs)
            build_sdk=1
            build_client=1
            pack_libs=1
            dynamic=1
            ;;
        test)
            build_test=1
            build_sdk=1
            build_client=1
            ;;
    esac
done

dir=$(dirname $0)
bin=${dir}/bin
echo "using goflag=\"${goflag}\""
echo "using gccflag=\"${gccflag}\""

build_sdk_dynamic_impl() {
    if [ "$1" = "libcfssdk" ]; then
        libsdk_flag="-E main.main"
    fi
    go build -ldflags "-r /usr/lib64 ${goflag} ${libsdk_flag} -X main.BranchName=${BranchName} -X main.CommitID=${CommitID} -X 'main.BuildTime=${BuildTime}'" -buildmode=plugin -linkshared -o ${bin}/$1.so ${dir}/sdk/sdk_bypass.go ${dir}/sdk/sdk_fuse.go ${dir}/sdk/http_bypass.go ${dir}/sdk/http_fuse.go ${dir}/sdk/http_common.go ${dir}/sdk/ump.go ${dir}/sdk/dynamic.go
}

build_sdk_dynamic() {
    echo "building sdk (libcfssdk.so, libcfssdk_cshared.so) commit: ${CommitID} ..."
    build_sdk_dynamic_impl "libcfssdk"
    build_sdk_dynamic_impl "libcfssdk_cshared"
}

build_sdk_nodynamic() {
    echo "building sdk (libcfssdk.so) commit: ${CommitID} ..."
    go build -ldflags "${goflag} -X main.BranchName=${BranchName} -X main.CommitID=${CommitID} -X 'main.BuildTime=${BuildTime}'" -buildmode=c-shared -o ${bin}/libcfssdk.so ${dir}/sdk/sdk_bypass.go ${dir}/sdk/http_bypass.go ${dir}/sdk/http_common.go ${dir}/sdk/ump.go ${dir}/sdk/no_dynamic.go
}

build_client_dynamic() {
    echo "building client (cfs-client cfs-client-inner cfs-client-static libcfsclient.so libcfsc.so libempty.so) ..."

    # dynamic fuse client, for libc version >= 2.14
    go build -ldflags "${goflag}" -o ${bin}/cfs-client ${dir}/fuse/run_fuse_client.go ${dir}/fuse/prepare_lib.go
    go build -ldflags "${goflag}" -linkshared -o ${bin}/cfs-client-inner ${dir}/fuse/main.go ${dir}/fuse/prepare_lib.go

    # static fuse client, for libc version < 2.14
    go build -ldflags "${goflag} -X main.BranchName=${BranchName} -X main.CommitID=${CommitID} -X 'main.BuildTime=${BuildTime}'" -o ${bin}/cfs-client-static ${dir}/sdk/sdk_fuse.go ${dir}/sdk/http_fuse.go ${dir}/sdk/http_common.go

    gcc ${gccflag} -std=c99 -fPIC -shared -DDYNAMIC_UPDATE -o ${bin}/libcfsclient.so ${dir}/bypass/main.c ${dir}/bypass/libc_operation.c -ldl -lpthread -I ${dir}/bypass/include
    g++ -std=c++11 ${gccflag} -fPIC -shared -DDYNAMIC_UPDATE -DCommitID=\"${CommitID}\" -o ${bin}/libcfsc.so ${dir}/bypass/client.c ${dir}/bypass/cache.c ${dir}/bypass/packet.c ${dir}/bypass/conn_pool.c ${dir}/bypass/ini.c ${dir}/bypass/libc_operation.c -ldl -lpthread -I ${dir}/bypass/include
    go build -ldflags "${goflag} -r /usr/lib64 " -buildmode=plugin -linkshared -o ${bin}/libempty.so ${dir}/empty.go
}

build_client_nodynamic() {
    echo "building client (cfs-client libcfsclient.so libcfsc.so) ..."
    go build -ldflags "${goflag} -X main.BranchName=${BranchName} -X main.CommitID=${CommitID} -X 'main.BuildTime=${BuildTime}'" -o ${bin}/cfs-client ${dir}/sdk/sdk_fuse.go ${dir}/sdk/http_fuse.go ${dir}/sdk/http_common.go

    gcc ${gccflag} -std=c99 -fPIC -shared -o ${bin}/libcfsclient.so ${dir}/bypass/main.c ${dir}/bypass/libc_operation.c -ldl -lpthread -I ${dir}/bypass/include
    g++ -std=c++11 ${gccflag} -DCommitID=\"${CommitID}\" -fPIC -shared -o ${bin}/libcfsc.so ${dir}/bypass/client.c ${dir}/bypass/cache.c ${dir}/bypass/packet.c ${dir}/bypass/conn_pool.c ${dir}/bypass/ini.c ${dir}/bypass/libc_operation.c -ldl -lpthread -I ${dir}/bypass/include
}

if [[ ${build_sdk} -eq 1 ]]; then
    if [[ ${dynamic} -eq 1 ]]; then
        build_sdk_dynamic
    else
        build_sdk_nodynamic
    fi
    chmod a+rx ${bin}/*
fi

if [[ ${build_client} -eq 1 ]]; then
    if [[ ${dynamic} -eq 1 ]]; then
        build_client_dynamic
    else
        build_client_nodynamic
    fi
    chmod a+rx ${bin}/*
fi

if [[ ${build_test} -eq 1 ]]; then
    echo "building test (cfs-client test-bypass libcfsclient.so libempty.so) ..."
    go test -c -covermode=atomic -coverpkg="../..." -linkshared -o ${bin}/cfs-client ${dir}/fuse/main.go ${dir}/fuse/prepare_lib.go ${dir}/fuse/fuse_test.go
    gcc -std=c99 -g ${dir}/bypass/client_test.c -o ${bin}/test-bypass
fi

if [[ ${pack_libs} -eq 1 ]]; then
    libTarName=cfs-client-libs_${CommitID}.tar.gz
    fuseTarName=cfs-client-fuse.tar.gz
    kbpTarName=libcfs.tar.gz
    if [[ `arch` == "aarch64" ]] || [[ `arch` == "arm64" ]]; then
        libTarName=cfs-client-libs_arm64_${CommitID}.tar.gz
        fuseTarName=cfs-client-fuse_arm64.tar.gz
        kbpTarName=libcfs_arm64.tar.gz
    fi

    echo "pack libs, generate cfs-client-libs.tar.gz ..."
    cd ${bin}
    versionID=`./cfs-client-static -v | grep Version: | awk '{print $2}'`
    version_regex="^[0-9]+\.[0-9]+\.[0-9]+$"
    if ! [[ ${versionID} =~ ${version_regex} ]]; then
        echo "${versionID} is not a version ID"
        exit 1
    fi
    md5sum libcfssdk.so > checkfile
    md5sum libcfsc.so >> checkfile
    echo "${versionID}  Version" >> checkfile
    tar -zcvf ${libTarName} libcfssdk.so libcfsc.so checkfile

    libstd=`ldd libcfssdk.so |grep libstd.so |awk '{print $3}'`
    cp -f ${libstd} libstd.so
    md5sum libcfssdk.so > checkfile
    md5sum libstd.so >> checkfile
    md5sum cfs-client-inner >> checkfile
    md5sum cfs-client-static >> checkfile
    echo "${versionID}  Version" >> checkfile
    tar -zcvf ${fuseTarName} libcfssdk.so libstd.so cfs-client-inner cfs-client-static checkfile

    tar -zcvf ${kbpTarName} libcfsclient.so libcfssdk.so libcfsc.so libstd.so libempty.so

    cd ~-
fi
