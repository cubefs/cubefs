#!/bin/bash




build_linux_x86_64() {
     make

}

# build arm64 with ubuntu amd64, apt-get install -y gcc-9-aarch64-linux-gnu gcc-aarch64-linux-gnu  g++-9-aarch64-linux-gnu g++-aarch64-linux-gnu 
#
build_linux_arm64() {
    echo "build linux arm64"
    export PORTABLE=1
    export ARCH=arm64
 #   export CC=aarch64-linux-gnu-gcc        
    export EXTRA_CFLAGS="-Wno-error=deprecated-copy -fno-strict-aliasing -Wclass-memaccess -Wno-error=class-memaccess -Wpessimizing-move -Wno-error=pessimizing-move"
    export EXTRA_CXXFLAGS=$EXTRA_CFLAGS

    CGO_ENABLED=1 GOOS=linux GOARCH=arm64 make
  make

}


CPUTYPE=${CPUTYPE} | tr 'A-Z' 'a-z'
echo ${CPUTYPE}
case ${CPUTYPE} in
    "arm64")
        build_linux_arm64
        ;;
    *)
        build_linux_x86_64
        ;;
esac
