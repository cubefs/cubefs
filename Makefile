# ChubaoFS Makefile
#

BIN_PATH := build/bin
BIN_SERVER := $(BIN_PATH)/cfs-server
BIN_CLIENT := $(BIN_PATH)/cfs-client

default: all

phony := all
all: build

phony += build build_server build_client
build: build_server build_client

build_server: $(BIN_SERVER)

build_client: $(BIN_CLIENT)

$(BIN_SERVER):
	@build/build.sh server

$(BIN_CLIENT):
	@build/build.sh client

phony += clean
clean:
	@build/build.sh clean

phony += dist_clean
dist_clean:
	@build/build.sh dist_clean

phony += test
test:
	@build/build.sh test

.PHONY: $(phony)
