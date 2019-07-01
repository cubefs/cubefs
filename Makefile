
UNAME_S := $(shell uname -s)
ifneq ($(UNAME_S),Linux)
$(ChubaoFS only support Linux os)
endif

GOFLAGS      :=
TAGS         :=
INSTALL      := install
prefix       := /usr/local
SERVERTARGET  := ./cmd
CLIENTTARGET  := ./client
BranchName   :=$(shell git rev-parse --abbrev-ref HEAD)
CommitID     :=$(shell git rev-parse HEAD)
BuildTime    :=$(shell date +%Y-%m-%d\ %H:%M)

ifeq ($(NCPUS),)
  NCPUS := $(shell grep -c ^processor /proc/cpuinfo 2>/dev/null)
  ifeq ($(NCPUS),)
    NCPUS := 1
  endif
endif

MAKEFLAGS += -j$(NCPUS)
$(info Running make with -j$(NCPUS))


$(shell echo > bin/uptodate)

BUILDTYPE := development

CFLAGS += -g1
CXXFLAGS += -g1
LDFLAGS ?=

CGO_CFLAGS = -I$(ROCKSDB_SRC_DIR)/include
CGO_CXXFLAGS = $(CXXFLAGS)
CGO_LDFLAGS = $(addprefix -L,$(SNAPPY_DIR) $(ROCKSDB_DIR)) -lrocksdb -lsnappy

export CFLAGS CXXFLAGS LDFLAGS CGO_CFLAGS CGO_CXXFLAGS CGO_LDFLAGS

override LINKFLAGS = -X github.com/chubaofs/chubaofs/cmd/build.typ=$(BUILDTYPE) -X "main.CommitID=$(CommitID)" -X "main.BranchName=$(BranchName)" -X "main.BuildTime=$(BuildTime)" -extldflags "$(LDFLAGS)"


GO      ?= go
GOFLAGS ?=
TAR     ?= tar

GOPATH := $(shell $(GO) env GOPATH)

ifneq "$(or $(findstring :,$(GOPATH)),$(findstring ;,$(GOPATH)))" ""
$(error GOPATHs with multiple entries are not supported)
endif

GOPATH := $(realpath $(GOPATH))
ifeq "$(strip $(GOPATH))" ""
$(error GOPATH is not set and could not be automatically determined)
endif

ifeq "$(filter $(GOPATH)%,$(CURDIR))" ""
$(error Current directory "$(CURDIR)" is not within GOPATH "$(GOPATH)")
endif

ifeq "$(GOPATH)" "/"
$(error GOPATH=/ is not supported)
endif

$(info GOPATH set to $(GOPATH))


GO_INSTALL := GOBIN='$(abspath bin)' GOFLAGS= $(GO) install


export PATH := $(abspath bin):$(PATH)


export SHELL := env PWD=$(CURDIR) bash
ifeq ($(SHELL),)
$(error bash is required)
endif


override make-lazy = $(eval $1 = $$(eval $1 := $(value $1))$$($1))


TAR_XFORM_FLAG = $(shell $(TAR) --version | grep -q GNU && echo "--xform='flags=r;s'" || echo "-s")
$(call make-lazy,TAR_XFORM_FLAG)


SED_INPLACE = sed $(shell sed --version 2>&1 | grep -q GNU && echo -i || echo "-i ''")
$(call make-lazy,SED_INPLACE)


MAKE_TERMERR ?= $(shell [[ -t 2 ]] && echo true)

space := $(eval) $(eval)

yellow = $(shell { tput setaf 3 || tput AF 3; } 2>/dev/null)
cyan = $(shell { tput setaf 6 || tput AF 6; } 2>/dev/null)
term-reset = $(shell { tput sgr0 || tput me; } 2>/dev/null)
$(call make-lazy,yellow)
$(call make-lazy,cyan)
$(call make-lazy,term-reset)






TARGET_TRIPLE := $(HOST_TRIPLE)
XCMAKE_SYSTEM_NAME :=
XGOOS :=
XGOARCH :=
XCC := $(TARGET_TRIPLE)-cc
XCXX := $(TARGET_TRIPLE)-c++
EXTRA_XCMAKE_FLAGS :=
EXTRA_XCONFIGURE_FLAGS :=

ifneq ($(HOST_TRIPLE),$(TARGET_TRIPLE))
is-cross-compile := 1
endif


cmake-flags := -DCMAKE_TARGET_MESSAGES=OFF
configure-flags :=


xcmake-flags := $(cmake-flags) $(EXTRA_XCMAKE_FLAGS)
xconfigure-flags := $(configure-flags) $(EXTRA_XCONFIGURE_FLAGS)
override xgo := GOFLAGS= $(GO)

ifdef is-cross-compile
xconfigure-flags += --host=$(TARGET_TRIPLE) CC=$(XCC) CXX=$(XCXX)
xcmake-flags += -DCMAKE_SYSTEM_NAME=$(XCMAKE_SYSTEM_NAME) -DCMAKE_C_COMPILER=$(XCC) -DCMAKE_CXX_COMPILER=$(XCXX)
override xgo := GOFLAGS= GOOS=$(XGOOS) GOARCH=$(XGOARCH) CC=$(XCC) CXX=$(XCXX) $(xgo)
endif

C_DEPS_DIR := $(abspath c-deps)
ROCKSDB_SRC_DIR  := $(C_DEPS_DIR)/rocksdb
SNAPPY_SRC_DIR   := $(C_DEPS_DIR)/snappy

# Derived build variants.
use-stdmalloc          := $(findstring stdmalloc,$(TAGS))
use-msan               := $(findstring msan,$(GOFLAGS))

# User-requested build variants.
USE_ROCKSDB_ASSERTIONS :=

BUILD_DIR := $(GOPATH)/pkg/chubaofs$(TARGET_TRIPLE)



ROCKSDB_DIR  := $(BUILD_DIR)/rocksdb$(if $(use-msan),_msan)$(if $(use-stdmalloc),_stdmalloc)$(if $(USE_ROCKSDB_ASSERTIONS),_assert)
SNAPPY_DIR   := $(BUILD_DIR)/snappy$(if $(use-msan),_msan)

LIBROCKSDB  := $(ROCKSDB_DIR)/librocksdb.a
LIBSNAPPY   := $(SNAPPY_DIR)/libsnappy.a

C_LIBS_COMMON = $(LIBSNAPPY) $(LIBROCKSDB)

native-tag := $(subst -,_,$(TARGET_TRIPLE))$(if $(use-stdmalloc),_stdmalloc)$(if $(use-msan),_msan)


.ALWAYS_REBUILD:
.PHONY: .ALWAYS_REBUILD

$(ROCKSDB_DIR)/Makefile: sse := $(if $(findstring x86_64,$(TARGET_TRIPLE)),-msse3)
$(ROCKSDB_DIR)/Makefile: $(C_DEPS_DIR)/rocksdb-rebuild $(LIBSNAPPY)
	rm -rf $(ROCKSDB_DIR)
	mkdir -p $(ROCKSDB_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/rocksdb-rebuild. See above for rationale.
	cd $(ROCKSDB_DIR) && CFLAGS+=" $(sse)" && CXXFLAGS+=" $(sse)" && cmake $(xcmake-flags) $(ROCKSDB_SRC_DIR) \
	  $(if $(findstring release,$(BUILDTYPE)),-DPORTABLE=ON) -DWITH_GFLAGS=OFF \
	  -DCMAKE_BUILD_TYPE=$(if $(ENABLE_ROCKSDB_ASSERTIONS),Debug,Release) \
	  -DFAIL_ON_WARNINGS=$(if $(findstring windows,$(XGOOS)),0,1) \
	  -DUSE_RTTI=1

$(SNAPPY_DIR)/Makefile: $(C_DEPS_DIR)/snappy-rebuild
	rm -rf $(SNAPPY_DIR)
	mkdir -p $(SNAPPY_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/snappy-rebuild. See above for rationale.
	cd $(SNAPPY_DIR) && cmake $(xcmake-flags) $(SNAPPY_SRC_DIR) \
	  -DCMAKE_BUILD_TYPE=Release


$(LIBSNAPPY): $(SNAPPY_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(SNAPPY_SRC_DIR) || $(MAKE) --no-print-directory -C $(SNAPPY_DIR) snappy

$(LIBROCKSDB): $(ROCKSDB_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(ROCKSDB_SRC_DIR) || $(MAKE) --no-print-directory -C $(ROCKSDB_DIR) rocksdb

ChubaoFSServer      := bin/cfs-server(SUFFIX)
ChubaoFSClient      :=bin/cfs-client

go-targets := $(ChubaoFSClient) $(ChubaoFSServer)

build-mode = build -o $@

go-install: build-mode = install

$(ChubaoFSClient): .ALWAYS_REBUILD
	$(xgo) $(build-mode) -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(CLIENTTARGET)
$(ChubaoFSServer) : $(C_LIBS_COMMON) .ALWAYS_REBUILD
	$(xgo) $(build-mode) -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(SERVERTARGET)

.PHONY: default
default: $(ChubaoFSServer) $(ChubaoFSClient)

clean:
	rm -rf $(BUILD_DIR) bin/cfs-server bin/cfs-client
