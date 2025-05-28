#!/bin/bash

RootPath=$(cd $(dirname ${BASH_SOURCE[0]})/..; pwd)
BuildPath=${RootPath}/build
BuildOutPath=${BuildPath}/out
BuildBinPath=${BuildPath}/bin
BuildDependsLibPath=${BuildPath}/lib
BuildDependsIncludePath=${BuildPath}/include
VendorPath=${RootPath}/vendor
DependsPath=${RootPath}/depends
use_clang=$(echo ${CC} | grep "clang" | grep -v "grep")
cgo_ldflags="-L${BuildDependsLibPath} -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lstdc++"
if [ "${use_clang}" != "" ]; then
    cgo_ldflags="-L${BuildDependsLibPath} -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lc++"
fi
cgo_cflags="-I${BuildDependsIncludePath}"
MODFLAGS=""
gomod=${2:-"on"}

if [ "${gomod}" == "off" ]; then
    MODFLAGS="-mod=vendor"
fi

if [ ! -d "${BuildOutPath}" ]; then
    mkdir ${BuildOutPath}
fi

if [ ! -d "${BuildBinPath}" ]; then
    mkdir ${BuildBinPath}
fi

if [ ! -d "${BuildBinPath}/blobstore" ]; then
    mkdir ${BuildBinPath}/blobstore
fi

if [ ! -d "${BuildDependsLibPath}" ]; then
    mkdir ${BuildDependsLibPath}
fi

if [ ! -d "${BuildDependsIncludePath}" ]; then
    mkdir ${BuildDependsIncludePath}
fi

RM=$(find /bin /sbin /usr/bin /usr/local -name "rm" | head -1)
if [[ "-x$RM" == "-x" ]] ; then
    RM=rm
fi

Version=$(git describe --abbrev=0 --tags 2>/dev/null)
BranchName=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
CommitID=$(git rev-parse HEAD 2>/dev/null)
BuildTime=$(date +%Y-%m-%d\ %H:%M)
LDFlags="-X 'github.com/cubefs/cubefs/proto.Version=${Version}' \
    -X 'github.com/cubefs/cubefs/proto.CommitID=${CommitID}' \
    -X 'github.com/cubefs/cubefs/proto.BranchName=${BranchName}' \
    -X 'github.com/cubefs/cubefs/proto.BuildTime=${BuildTime}' \
    -X 'github.com/cubefs/cubefs/blobstore/util/version.version=${BranchName}/${CommitID}' \
    -w -s"

NPROC=$(nproc 2>/dev/null)
if [ -e /sys/fs/cgroup/cpu ] ; then
    NPROC=4
fi
NPROC=${NPROC:-"1"}

case $(uname -s | tr 'A-Z' 'a-z') in
    "linux"|"darwin")
        ;;
    *)
        echo "Current platform $(uname -s) not support";
        exit1;
        ;;
esac

CPUTYPE=${CPUTYPE} | tr 'A-Z' 'a-z'

build_zlib() {
    ZLIB_VER=1.2.13
    if [ -f "${BuildDependsLibPath}/libz.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/zlib-${ZLIB_VER} ]; then
        tar -zxf ${DependsPath}/zlib-${ZLIB_VER}.tar.gz -C ${BuildOutPath}
    fi

    echo "build zlib..."
    pushd ${BuildOutPath}/zlib-${ZLIB_VER}
    CFLAGS='-fPIC' ./configure --static
    make -j$1
    if [ $? -ne 0 ]; then
        exit 1
    fi
    cp -f libz.a ${BuildDependsLibPath}
    cp -f zlib.h zconf.h ${BuildDependsIncludePath}
    popd
}

build_bzip2() {
    BZIP2_VER=1.0.6
    if [ -f "${BuildDependsLibPath}/libbz2.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/bzip2-bzip2-${BZIP2_VER} ]; then
        tar -zxf ${DependsPath}/bzip2-bzip2-${BZIP2_VER}.tar.gz -C ${BuildOutPath}
        if [ "${use_clang}" != "" ]; then
            sed -i '18d' ${BuildOutPath}/bzip2-bzip2-${BZIP2_VER}/Makefile
        fi
    fi

    echo "build bzip2..."
    pushd ${BuildOutPath}/bzip2-bzip2-${BZIP2_VER}
    make -j$1 CFLAGS='-fPIC -O2 -g -D_FILE_OFFSET_BITS=64'
    if [ $? -ne 0 ]; then
        exit 1
    fi
    cp -f libbz2.a ${BuildDependsLibPath}
    cp -f bzlib.h bzlib_private.h ${BuildDependsIncludePath}
    popd
}

build_lz4() {
    LZ4_VER=1.8.3
    if [ -f "${BuildDependsLibPath}/liblz4.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/lz4-${LZ4_VER} ]; then
        tar -zxf ${DependsPath}/lz4-${LZ4_VER}.tar.gz -C ${BuildOutPath}
    fi

    echo "build lz4..."
    pushd ${BuildOutPath}/lz4-${LZ4_VER}/lib
    make -j$1 CFLAGS='-fPIC -O2'
    if [ $? -ne 0 ]; then
        exit 1
    fi
    cp -f liblz4.a ${BuildDependsLibPath}
    cp -f lz4frame_static.h lz4.h lz4hc.h lz4frame.h ${BuildDependsIncludePath}
    popd
}

build_zstd() {
    ZSTD_VER=1.4.0
    if [ -f "${BuildDependsLibPath}/libzstd.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/zstd-${ZSTD_VER} ]; then
        tar -zxf ${DependsPath}/zstd-${ZSTD_VER}.tar.gz -C ${BuildOutPath}
    fi

    echo "build zstd..."
    pushd ${BuildOutPath}/zstd-${ZSTD_VER}/lib
    make -j$1 CFLAGS='-fPIC -O2'
    if [ $? -ne 0 ]; then
        exit 1
    fi
    cp -f libzstd.a ${BuildDependsLibPath}
    cp -f zstd.h common/zstd_errors.h deprecated/zbuff.h dictBuilder/zdict.h ${BuildDependsIncludePath}
    popd
}


build_snappy() {
    SNAPPY_VER=1.1.7
    if [ -f "${BuildDependsLibPath}/libsnappy.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/snappy-${SNAPPY_VER} ]; then
        tar -zxf ${DependsPath}/snappy-${SNAPPY_VER}.tar.gz -C ${BuildOutPath}
    fi

    echo "build snappy..."
    mkdir ${BuildOutPath}/snappy-${SNAPPY_VER}/build
    pushd ${BuildOutPath}/snappy-${SNAPPY_VER}/build
    cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DSNAPPY_BUILD_TESTS=OFF .. && make -j$1
    if [ $? -ne 0 ]; then
        exit 1
    fi
    cp -f libsnappy.a ${BuildDependsLibPath}
    cp -f ../snappy-c.h ../snappy-sinksource.h ../snappy.h snappy-stubs-public.h ${BuildDependsIncludePath}
    popd
}

build_rocksdb() {
    ROCKSDB_VER=6.3.6
    if [ -f "${BuildDependsLibPath}/librocksdb.a" ]; then
        return 0
    fi

    if [ ! -d ${BuildOutPath}/rocksdb-${ROCKSDB_VER} ]; then
        tar -zxf ${DependsPath}/rocksdb-${ROCKSDB_VER}.tar.gz -C ${BuildOutPath}
        pushd ${BuildOutPath} > /dev/null
        sed -i '1069s/newf/\&newf/' rocksdb-${ROCKSDB_VER}/db/db_impl/db_impl_compaction_flush.cc
        sed -i '1161s/newf/\&newf/' rocksdb-${ROCKSDB_VER}/db/db_impl/db_impl_compaction_flush.cc
        sed -i '412s/pair/\&pair/' rocksdb-${ROCKSDB_VER}/options/options_parser.cc
        sed -i '63s/std::mutex/mutable std::mutex/' rocksdb-${ROCKSDB_VER}/util/channel.h
        popd
    fi

    echo "build rocksdb..."
    pushd ${BuildOutPath}/rocksdb-${ROCKSDB_VER}
    if [ "${use_clang}" != "" ]; then
        FLAGS="-Wno-error=deprecated-copy -Wno-error=pessimizing-move -Wno-error=shadow -Wno-error=unused-but-set-variable"
    else
        CCMAJOR=`gcc -dumpversion | awk -F. '{print $1}'`
        if [ ${CCMAJOR} -ge 9 ]; then
            FLAGS="-Wno-error=deprecated-copy -Wno-error=pessimizing-move"
        fi
    fi
    FLAGS="${FLAGS} -Wno-unused-variable -Wno-unused-function"
    PORTABLE=1 make EXTRA_CXXFLAGS="-fPIC ${FLAGS} -DZLIB -DBZIP2 -DSNAPPY -DLZ4 -DZSTD -I${BuildDependsIncludePath}" static_lib
    if [ $? -ne 0 ]; then
        exit 1
    fi
    make install-static INSTALL_PATH=${BuildPath}
    strip -S -x ${BuildDependsLibPath}/librocksdb.a
    popd
}

init_gopath() {
    export GO111MODULE=${gomod}
    export GOPATH=$HOME/tmp/cfs/go

    mkdir -p $GOPATH/src/github.com/cubefs
    SrcPath=$GOPATH/src/github.com/cubefs/cubefs
    if [ -L "$SrcPath" ]; then
        $RM -f $SrcPath
    fi
    if [  ! -e "$SrcPath" ] ; then
        ln -s $RootPath $SrcPath 2>/dev/null
    fi
}

pre_build() {
    build_zlib $1
    build_bzip2 $1
    build_lz4 $1
    build_zstd $1
    build_snappy $1
    build_rocksdb $1

    export CGO_CFLAGS=${cgo_cflags}
    export CGO_LDFLAGS="${cgo_ldflags}"

    init_gopath
}

run_test() {
    pushd $SrcPath >/dev/null
    export JENKINS_TEST=1
    ulimit -n 65536
    echo -n "${TPATH}"
    go test -cover -v -coverprofile=cover.output $(go list ./... | grep -v depends | grep master) | tee cubefs_unittest.output
    ret=$?
    popd >/dev/null
    exit $ret
}

run_test_cover() {
    pushd $SrcPath >/dev/null
    export JENKINS_TEST=1
    ulimit -n 65536
    echo -n "${TPATH}"
    go test -trimpath -covermode=count --coverprofile coverage.txt $(go list ./... | grep -v depends)
    ret=$?
    popd >/dev/null
    exit $ret
}

build_server() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-server   "
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-server ${SrcPath}/cmd/*.go && echo "success" || echo "failed"
    popd >/dev/null
}

build_clustermgr() {
    pushd $SrcPath/blobstore/cmd/clustermgr >/dev/null
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore .
    popd >/dev/null
}

build_blobnode() {
    pushd $SrcPath/blobstore/cmd/blobnode >/dev/null
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore .
    popd >/dev/null
}

build_access() {
    pushd $SrcPath/blobstore/cmd/access >/dev/null
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore .
    popd >/dev/null
}

build_scheduler() {
    pushd $SrcPath/blobstore/cmd/scheduler >/dev/null
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore .
    popd >/dev/null
}

build_proxy() {
    pushd $SrcPath/blobstore/cmd/proxy >/dev/null
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore .
    popd >/dev/null
}

build_blobstore_cli() {
    pushd $SrcPath/blobstore/cli/cli >/dev/null
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/blobstore/blobstore-cli .
    popd >/dev/null
}

build_blobstore() {
    pushd $SrcPath >/dev/null
    echo -n "build blobstore    "
    build_clustermgr && build_blobnode && build_access && build_scheduler && build_proxy && build_blobstore_cli && echo "success" || echo "failed"
    popd >/dev/null
}

build_client() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-client   "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-client ${SrcPath}/client/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

build_authtool() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-authtool "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-authtool ${SrcPath}/authnode/authtool/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

build_cli() {
    #cli need gorocksdb too
    pushd $SrcPath >/dev/null
    echo -n "build cfs-cli      "
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-cli ${SrcPath}/cli/*.go  && echo "success" || echo "failed"
    #sh cli/build.sh ${BuildBinPath}/cfs-cli && echo "success" || echo "failed"
    popd >/dev/null
}



build_cfs_deploy() {
    #cfs_deploy need gorocksdb too
    pushd $SrcPath >/dev/null
    echo -n "build cfs-deploy      "
    CGO_ENABLED=1 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-deploy ${SrcPath}/deploy/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

build_fsck() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-fsck      "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-fsck ${SrcPath}/tool/fsck/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

build_snapshot() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-snapshot	"
    go build $MODFLAGS -ldflags "${LDFlags}" -o ${BuildBinPath}/cfs-snapshot ${SrcPath}/tool/snapshot/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

build_libsdkpre() {
    case `uname` in
        Linux)
            TargetFile=${1:-${BuildBinPath}/libcfs.so}
            ;;
        *)
            echo "Unsupported platform"
            exit 1
            ;;
    esac
    pushd $SrcPath > /dev/null
    echo -n "build libsdk: libcfs.so"
    CGO_ENABLED=1 go build $MODFLAGS -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -buildmode c-shared -o ${TargetFile} ${SrcPath}/client/libsdk/*.go && echo "success" || echo "failed"
    popd > /dev/null
}

build_libsdk() {
    case `uname` in
        Linux)
            TargetFile=${1:-${BuildBinPath}/libcfs.so}
            ;;
        *)
            echo "Unsupported platform"
            exit 0
            ;;
    esac
    pushd $SrcPath >/dev/null
    echo -n "build libsdk: libcfs.so       "
    CGO_ENABLED=1 go build $MODFLAGS -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -buildmode c-shared -o ${TargetFile} ${SrcPath}/client/libsdk/*.go && echo "success" || echo "failed"
    popd >/dev/null

    pushd $SrcPath/java >/dev/null
    echo -n "build java libcubefs        "
    mkdir -p $SrcPath/java/src/main/resources/
    \cp  -rf ${TargetFile}  $SrcPath/java/src/main/resources/
    mvn clean package
    \cp -rf $SrcPath/java/target/*.jar ${BuildBinPath}  && echo "build java libcubefs success" || echo "build java libcubefs failed"
    popd >/dev/null
}

build_fdstore() {
    pushd $SrcPath >/dev/null
    echo -n "build fdstore "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/fdstore ${SrcPath}/client/fdstore/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

build_preload() {
    pushd $SrcPath >/dev/null
    echo -n "build cfs-preload   "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-preload ${SrcPath}/tool/preload/*.go && echo "success" || echo "failed"
}

build_bcache(){
    pushd $SrcPath >/dev/null
    echo -n "build cfs-blockcache      "
    CGO_ENABLED=0 go build ${MODFLAGS} -gcflags=all=-trimpath=${SrcPath} -asmflags=all=-trimpath=${SrcPath} -ldflags="${LDFlags}" -o ${BuildBinPath}/cfs-bcache ${SrcPath}/client/blockcache/*.go  && echo "success" || echo "failed"
    popd >/dev/null
}

clean() {
    $RM -rf ${BuildBinPath}
}

dist_clean() {
    $RM -rf ${BuildBinPath}
    $RM -rf ${BuildOutPath}
    $RM -rf ${BuildDependsLibPath}
    $RM -rf ${BuildDependsIncludePath}
}

cmd=${1:-"all"}

if [ "${cmd}" == "dist_clean" ]; then
    dist_clean
    exit 0
elif [ "${cmd}" == "clean" ]; then
    clean
    exit 0
fi

get_cpu_cores() {
    cores=`cat /proc/cpuinfo | grep processor | wc -l`
    return $cores
}

threads=0
for para in $*
do
    check=`echo $para | grep "^--threads=" | wc -l`
    if test $check -eq 1
    then
        check=`echo "$para" | grep "^--threads=[0-9]*[^0-9]\{1,\}" | wc -l`
        if test $check -eq 0
        then
            threads=`echo "$para" | grep -o "[0-9]\{1,\}"`
        fi
    fi
done

if test $threads -eq 0
then
    get_cpu_cores
    threads=`expr $? + 1`
    threads=`expr $threads / 2`
fi

pre_build $threads

case "$cmd" in
    "all")
        build_server
        build_client
        build_cli
        build_libsdk
        build_bcache
        ;;
    "test")
        run_test
        ;;
    "testcover")
        run_test_cover
        ;;
    "server")
        build_server
        ;;
    "blobstore")
        build_blobstore
        ;;
    "client")
        build_client
        ;;
    "authtool")
        build_authtool
        ;;
    "cli")
        build_cli
        ;;
    "deploy")
        build_cfs_deploy
        ;;
    "fsck")
        build_fsck
        ;;
    "snapshot")
        build_snapshot
        ;;
    "libsdkpre")
        build_libsdkpre
        ;;
    "libsdk")
        build_libsdk
        ;;
    "fdstore")
        build_fdstore
        ;;
    "preload")
        build_preload
        ;;
    "bcache")
        build_bcache
        ;;
    *)
        ;;
esac
