#!/usr/bin/env bash

BranchName=`git rev-parse --abbrev-ref HEAD`
CommitID=`git rev-parse HEAD`
BuildTime=`date +%Y-%m-%d\ %H:%M`
Debug="0"

[[ "-$GOPATH" == "-" ]] && { echo "GOPATH not set"; exit 1 ; }

goflag="-s"

build_sdk=1
build_client=1
build_test=0

help() {
    cat <<EOF

Usage: ./build.sh [ -h | --help ] [ -g ] [ --sdk-only | --client-only ]
    -h, --help              show help info
    -g                      setup Debug="1" goflag="" gccflag="-g"
    --sdk-only              build sdk (libcfssdk.so libempty.so) only
    --client-only           build client (libcfsclient.so and cfs-client) only
    test                    build in test mode
EOF
    exit 0
}

ARGS=( "$@" )
for opt in ${ARGS[*]} ; do
    case "$opt" in
        -h|--help)
            help
            ;;
        -g)
            Debug="1"
            goflag=""
            gccflag="-g"
            ;;
    	--sdk-only)
    	    build_sdk=1
    	    build_client=0
	        ;;
	    --client-only)
	        build_sdk=0
	        build_client=1
	        ;;
        test)
            build_test=1
            build_sdk=1
            build_client=0
            ;;
    esac
done

dir=$(dirname $0)
bin=${dir}/bin
echo "using Debug=\"${Debug}\""
echo "using goflag=\"${goflag}\""
echo "using gccflag=\"${gccflag}\""
if [[ ${build_sdk} -eq 1 ]]; then
    echo "building sdk (libcfssdk.so, libcfssdk_cshared.so) commit: ${CommitID} ..."
    go build -ldflags "${goflag} -E main.main -X main.BranchName=${BranchName} -X main.CommitID=${CommitID} -X 'main.BuildTime=${BuildTime}' -X 'main.Debug=${Debug}'" -buildmode=plugin -linkshared -o ${bin}/libcfssdk.so ${dir}/sdk_fuse.go ${dir}/sdk_bypass.go ${dir}/http.go ${dir}/ump.go
    go build -ldflags "${goflag} -X main.CommitID=${CommitID} -X main.BranchName=${BranchName} -X 'main.BuildTime=${BuildTime}' -X 'main.Debug=${Debug}'" -buildmode=c-shared -o ${bin}/libcfssdk_cshared.so ${dir}/sdk_fuse.go ${dir}/sdk_bypass.go ${dir}/http.go ${dir}/ump.go
fi
if [[ ${build_client} -eq 1 ]]; then
    echo "building client (cfs-client libcfsclient.so libempty.so) ..."
    go build -buildmode=plugin -linkshared -o ${bin}/libempty.so  ${dir}/empty.go
    go build -ldflags "${goflag}" -linkshared -o ${bin}/cfs-client ${dir}/main_fuse.go
    g++ ${gccflag} -fPIC -shared -o ${bin}/libcfsclient.so ${dir}/main_bypass.c ${dir}/bypass/cache.c ${dir}/bypass/ini.c -ldl -lpthread -I ${dir}/bypass/include
fi
if [[ ${build_test} -eq 1 ]]; then
    echo "building test (cfs-client test-bypass libcfsclient.so libempty.so) ..."
    go test -c -covermode=atomic -coverpkg="../..." -linkshared -o ${bin}/cfs-client ${dir}/main_fuse.go ${dir}/fuse_test.go
    go build -buildmode=plugin -linkshared -o ${bin}/libempty.so  ${dir}/empty.go
    g++ ${gccflag} -fPIC -shared -o ${bin}/libcfsclient.so ${dir}/main_bypass.c ${dir}/bypass/cache.c ${dir}/bypass/ini.c -ldl -lpthread -I ${dir}/bypass/include
    gcc ${dir}/bypass/client_test.c -o ${bin}/test-bypass
fi
