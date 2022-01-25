# ChubaoFS Makefile
#

BIN_PATH := build/bin
BIN_SERVER := $(BIN_PATH)/cfs-server
BIN_CLIENT := $(BIN_PATH)/cfs-client
BIN_CLIENT2 := $(BIN_PATH)/cfs-client2
BIN_AUTHTOOL := $(BIN_PATH)/cfs-authtool
BIN_CLI := $(BIN_PATH)/cfs-cli
BIN_FSCK := $(BIN_PATH)/cfs-fsck
BIN_LIBSDK := $(BIN_PATH)/libsdk
BIN_FDSTORE := $(BIN_PATH)/fdstore

COMMON_SRC := build/build.sh Makefile
COMMON_SRC += $(wildcard storage/*.go util/*/*.go util/*.go repl/*.go raftstore/*.go proto/*.go)
SERVER_SRC := $(wildcard cmd/*.go authnode/*.go datanode/*.go master/*.go metanode/*.go objectnode/*.go)
CLIENT_SRC := $(wildcard client/*.go client/fs/*.go sdk/*.go)
CLIENT2_SRC := $(wildcard clientv2/*.go clientv2/fs/*.go sdk/*.go)
AUTHTOOL_SRC := $(wildcard authtool/*.go)
CLI_SRC := $(wildcard cli/*.go)
FSCK_SRC := $(wildcard fsck/*.go fsck/cmd/*.go)
LIBSDK_SRC := $(wildcard libsdk/*.go)
FDSTORE_SRC := $(wildcard fdstore/*.go)

RM := $(shell [ -x /bin/rm ] && echo "/bin/rm" || echo "/usr/bin/rm" )

default: all

phony := all
all: build

phony += build server authtool client client2 cli fsck fdstore
build: server authtool client cli libsdk fsck fdstore

server: $(BIN_SERVER)

client: $(BIN_CLIENT)

client2: $(BIN_CLIENT2)

authtool: $(BIN_AUTHTOOL)

cli: $(BIN_CLI)

fsck: $(BIN_FSCK)

libsdk: $(BIN_LIBSDK)

fdstore: $(BIN_FDSTORE)

$(BIN_SERVER): $(COMMON_SRC) $(SERVER_SRC)
	@build/build.sh server

$(BIN_CLIENT): $(COMMON_SRC) $(CLIENT_SRC)
	@build/build.sh client

$(BIN_CLIENT2): $(COMMON_SRC) $(CLIENT2_SRC)
	@build/build.sh client2

$(BIN_AUTHTOOL): $(COMMON_SRC) $(AUTHTOOL_SRC)
	@build/build.sh authtool

$(BIN_CLI): $(COMMON_SRC) $(CLI_SRC)
	@build/build.sh cli

$(BIN_FSCK): $(COMMON_SRC) $(FSCK_SRC)
	@build/build.sh fsck

$(BIN_LIBSDK): $(COMMON_SRC) $(LIBSDK_SRC)
	@build/build.sh libsdk

$(BIN_FDSTORE): $(FDSTORE_SRC)
	@build/build.sh fdstore

phony += clean
clean:
	@$(RM) -rf build/bin

phony += dist-clean
dist-clean:
	@$(RM) -rf build/bin
	@$(RM) -rf build/out
	@$(RM) -rf vendor/dep

phony += test
test:
	@build/build.sh test

.PHONY: $(phony)
