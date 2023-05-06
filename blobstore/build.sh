# Copyright 2022 The CubeFS Authors.
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
CURRENT_DIR=`pwd`
INSTALLDIR=${CURRENT_DIR}/.deps
mkdir -p ${INSTALLDIR}/lib
mkdir -p ${INSTALLDIR}/include

ZLIB_VER=1.2.13
BZIP2_VER=1.0.6
SNAPPY_VER=1.1.7
LZ4_VER=1.8.3
ZSTD_VER=1.4.0
ROCKSDB_VER=6.3.6
GCCMAJOR=`gcc -dumpversion`
check=--no-check-certificate

pushd ${INSTALLDIR}
if [ ! -f lib/libz.a ]; then
    rm -rf zlib-${ZLIB_VER}
    test -e zlib-${ZLIB_VER}.tar.gz || wget https://github.com/madler/zlib/archive/refs/tags/v${ZLIB_VER}.tar.gz -O ./zlib-${ZLIB_VER}.tar.gz ${check}
    if [ $? -ne 0 ]; then
        exit 1
    fi
    tar -zxf zlib-${ZLIB_VER}.tar.gz
    if [ $? -ne 0 ]; then
        exit 1
    fi
    pushd zlib-${ZLIB_VER}
    CFLAGS='-fPIC' ./configure --prefix=${INSTALLDIR} --static
    make
    cp -f libz.a ${INSTALLDIR}/lib
    cp -f zlib.h zconf.h ${INSTALLDIR}/include
    popd
fi

if [ ! -f lib/libbz2.a ]; then
    rm -rf bzip2-bzip2-${BZIP2_VER}
    test -e bzip2-bzip2-${BZIP2_VER}.tar.gz || wget https://gitlab.com/bzip2/bzip2/-/archive/bzip2-${BZIP2_VER}/bzip2-bzip2-${BZIP2_VER}.tar.gz
    if [ $? -ne 0 ]; then
        exit 1
    fi
    tar -zxf bzip2-bzip2-${BZIP2_VER}.tar.gz
    if [ $? -ne 0 ]; then
        exit 1
    fi
    pushd bzip2-bzip2-${BZIP2_VER}
    make CFLAGS='-fPIC -O2 -g -D_FILE_OFFSET_BITS=64'
    cp -f libbz2.a ${INSTALLDIR}/lib/libbz2.a
    cp -f bzlib.h bzlib_private.h ${INSTALLDIR}/include
    popd
fi

if [ ! -f lib/libzstd.a ]; then
    rm -rf zstd-${ZSTD_VER}
    test -e zstd-${ZSTD_VER}.tar.gz || wget https://github.com/facebook/zstd/archive/v${ZSTD_VER}.tar.gz -O zstd-${ZSTD_VER}.tar.gz ${check}
    if [ $? -ne 0 ]; then
        exit 1
    fi
    tar -zxf zstd-${ZSTD_VER}.tar.gz
    if [ $? -ne 0 ]; then
        exit 1
    fi
    pushd zstd-${ZSTD_VER}/lib
    make CFLAGS='-fPIC -O2'
    cp -f libzstd.a ${INSTALLDIR}/lib
    cp -f zstd.h common/zstd_errors.h deprecated/zbuff.h dictBuilder/zdict.h ${INSTALLDIR}/include
    popd
fi

if [ ! -f lib/liblz4.a ]; then
    rm -rf lz4-${LZ4_VER}
    test -e lz4-${LZ4_VER}.tar.gz || wget https://github.com/lz4/lz4/archive/v${LZ4_VER}.tar.gz -O lz4-${LZ4_VER}.tar.gz ${check}
    if [ $? -ne 0 ]; then
        exit 1
    fi
    tar -zxf lz4-${LZ4_VER}.tar.gz
    if [ $? -ne 0 ]; then
        exit 1
    fi
    pushd lz4-${LZ4_VER}/lib
    make CFLAGS='-fPIC -O2'
    cp -f liblz4.a ${INSTALLDIR}/lib
    cp -f lz4frame_static.h lz4.h lz4hc.h lz4frame.h ${INSTALLDIR}/include
    popd
fi

if [ ! -f lib/libsnappy.a ]; then
    rm -rf snappy-${SNAPPY_VER}
    test -e snappy-${SNAPPY_VER}.tar.gz || wget https://github.com/google/snappy/archive/${SNAPPY_VER}.tar.gz -O snappy-${SNAPPY_VER}.tar.gz ${check}
    if [ $? -ne 0 ]; then
        exit 1
    fi
    tar -zxf snappy-${SNAPPY_VER}.tar.gz
    if [ $? -ne 0 ]; then
        exit 1
    fi
    mkdir snappy-${SNAPPY_VER}/build
    pushd snappy-${SNAPPY_VER}/build
    cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON .. && make
    if [ $? -ne 0 ]; then
        exit 1
    fi
    cp -f libsnappy.a ${INSTALLDIR}/lib
    cp -f ../snappy-c.h ../snappy-sinksource.h ../snappy.h snappy-stubs-public.h ${INSTALLDIR}/include
    popd
fi

if [ ! -f lib/librocksdb.a ]; then
    rm -rf ${INSTALLDIR}/include/rocksdb rocksdb-${ROCKSDB_VER}/
    test -e rocksdb-${ROCKSDB_VER}.tar.gz || wget https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB_VER}.tar.gz -O rocksdb-${ROCKSDB_VER}.tar.gz ${check}
    if [ $? -ne 0 ]; then
        exit 1
    fi
    tar -zxf rocksdb-${ROCKSDB_VER}.tar.gz
    if [ $? -ne 0 ]; then
        exit 1
    fi
    pushd rocksdb-${ROCKSDB_VER}
    CCMAJOR=`gcc -dumpversion | awk -F. '{print $1}'`
    if [ ${CCMAJOR} -ge 9 ]; then
        FLAGS="-Wno-error=deprecated-copy -Wno-error=pessimizing-move"
    fi
    MAKECMDGOALS=static_lib make EXTRA_CXXFLAGS="-fPIC ${FLAGS} -I${INSTALLDIR}/include" static_lib
    if [ $? -ne 0 ]; then
        exit 1
    fi
    make install-static INSTALL_PATH=${INSTALLDIR}
    popd
    strip -S -x lib/librocksdb.a
fi
popd

make

