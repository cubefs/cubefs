
GOFLAGS      :=
TAGS         :=
INSTALL      := install
prefix       := /usr/local
BUILDTARGET  := ./cmd
BranchName   := git rev-parse --abbrev-ref HEAD
CommitID     := git rev-parse HEAD
BuildTime    := date +%Y-%m-%d\ %H:%M

ifeq "$(findstring -j,$(shell ps -o args= $$PPID))" ""
ifdef NCPUS
MAKEFLAGS += -j$(NCPUS)
$(info Running make with -j$(NCPUS))
endif
endif

BUILDTYPE := development

# Build C/C++ with basic debugging information.
CFLAGS += -g1
CXXFLAGS += -g1
LDFLAGS ?=

# golang/go#16651, respectively, are fixed.
CGO_CFLAGS = -I$(ROCKSDB_SRC_DIR)/include
CGO_CXXFLAGS = $(CXXFLAGS)
CGO_LDFLAGS = $(addprefix -L,$(SNAPPY_DIR) $(ROCKSDB_DIR)) -lrocksdb -lsnappy

export CFLAGS CXXFLAGS LDFLAGS CGO_CFLAGS CGO_CXXFLAGS CGO_LDFLAGS

# We intentionally use LINKFLAGS instead of the more traditional LDFLAGS
# because LDFLAGS has built-in semantics that don't make sense with the Go
# toolchain.
override LINKFLAGS = -X github.com/chubaofs/chubaofs/cmd/build.typ=$(BUILDTYPE) -X "main.CommitID=$(CommitID)" -X "main.BranchName=$(BranchName)" -X "main.BuildTime=$(BuildTime)" -extldflags "$(LDFLAGS)"


GO      ?= go
GOFLAGS ?=
TAR     ?= tar

# Ensure we have an unambiguous GOPATH.
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

# We install our vendored tools to a directory within this repository to avoid
# overwriting any user-installed binaries of the same name in the default GOBIN.
GO_INSTALL := GOBIN='$(abspath bin)' GOFLAGS= $(GO) install

# Prefer tools we've installed with go install and Yarn to those elsewhere on
# the PATH.
export PATH := $(abspath bin):$(PATH)


export SHELL := env PWD=$(CURDIR) bash
ifeq ($(SHELL),)
$(error bash is required)
endif


# make-lazy converts a recursive variable, which is evaluated every time it's
# referenced, to a lazy variable, which is evaluated only the first time it's
# used. See: http://blog.jgc.org/2016/07/lazy-gnu-make-variables.html
override make-lazy = $(eval $1 = $$(eval $1 := $(value $1))$$($1))

# GNU tar and BSD tar both support transforming filenames according to a regular
# expression, but have different flags to do so.
TAR_XFORM_FLAG = $(shell $(TAR) --version | grep -q GNU && echo "--xform='flags=r;s'" || echo "-s")
$(call make-lazy,TAR_XFORM_FLAG)

# To edit in-place without creating a backup file, GNU sed requires a bare -i,
# while BSD sed requires an empty string as the following argument.
SED_INPLACE = sed $(shell sed --version 2>&1 | grep -q GNU && echo -i || echo "-i ''")
$(call make-lazy,SED_INPLACE)

# MAKE_TERMERR is set automatically in Make v4.1+, but macOS is still shipping
# v3.81.
MAKE_TERMERR ?= $(shell [[ -t 2 ]] && echo true)

# This is how you get a literal space into a Makefile.
space := $(eval) $(eval)

# Color support.
yellow = $(shell { tput setaf 3 || tput AF 3; } 2>/dev/null)
cyan = $(shell { tput setaf 6 || tput AF 6; } 2>/dev/null)
term-reset = $(shell { tput sgr0 || tput me; } 2>/dev/null)
$(call make-lazy,yellow)
$(call make-lazy,cyan)
$(call make-lazy,term-reset)


host-is-macos := $(findstring Darwin,$(UNAME))
host-is-mingw := $(findstring MINGW,$(UNAME))


ifdef host-is-macos
# On macOS 10.11, XCode SDK v8.1 (and possibly others) indicate the presence of
# symbols that don't exist until macOS 10.12. Setting MACOSX_DEPLOYMENT_TARGET
# to the host machine's actual macOS version works around this. See:
# https://github.com/jemalloc/jemalloc/issues/494.
export MACOSX_DEPLOYMENT_TARGET ?= $(macos-version)
endif

# Cross-compilation occurs when you set TARGET_TRIPLE to something other than
# HOST_TRIPLE. You'll need to ensure the cross-compiling toolchain is on your
# path and override the rest of the variables that immediately follow as
# necessary. For an example, see build/builder/cmd/mkrelease, which sets these
# variables appropriately for the toolchains baked into the builder image.
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

# CMAKE_TARGET_MESSAGES=OFF prevents CMake from printing progress messages
# whenever a target is fully built to prevent spammy output from make when
# c-deps are all already built. Progress messages are still printed when actual
# compilation is being performed.
cmake-flags := -DCMAKE_TARGET_MESSAGES=OFF $(if $(host-is-mingw),-G 'MSYS Makefiles')
configure-flags :=

# Use xcmake-flags when invoking CMake on libraries/binaries for the target
# platform (i.e., the cross-compiled platform, if specified); use plain
# cmake-flags when invoking CMake on libraries/binaries for the host platform.
# Similarly for xconfigure-flags and configure-flags, and xgo and GO.
xcmake-flags := $(cmake-flags) $(EXTRA_XCMAKE_FLAGS)
xconfigure-flags := $(configure-flags) $(EXTRA_XCONFIGURE_FLAGS)
override xgo := GOFLAGS= $(GO)

# If we're cross-compiling, inform Autotools and CMake.
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

BUILD_DIR := $(GOPATH)/cfs/$(TARGET_TRIPLE)


# In MinGW, cgo flags don't handle Unix-style paths, so convert our base path to
# a Windows-style path.
#
# TODO(benesch): Figure out why. MinGW transparently converts Unix-style paths
# everywhere else.
ifdef host-is-mingw
BUILD_DIR := $(shell cygpath -m $(BUILD_DIR))
endif

ROCKSDB_DIR  := $(BUILD_DIR)/rocksdb$(if $(use-msan),_msan)$(if $(use-stdmalloc),_stdmalloc)$(if $(USE_ROCKSDB_ASSERTIONS),_assert)
SNAPPY_DIR   := $(BUILD_DIR)/snappy$(if $(use-msan),_msan)

LIBROCKSDB  := $(ROCKSDB_DIR)/librocksdb.a
LIBSNAPPY   := $(SNAPPY_DIR)/libsnappy.a

C_LIBS_COMMON = $(LIBSNAPPY) $(LIBROCKSDB)

# Go does not permit dashes in build tags. This is undocumented.
native-tag := $(subst -,_,$(TARGET_TRIPLE))$(if $(use-stdmalloc),_stdmalloc)$(if $(use-msan),_msan)

# Targets that name a real file that must be rebuilt on every Make invocation
# should depend on .ALWAYS_REBUILD. (.PHONY should only be used on targets that
# don't name a real file because .DELETE_ON_ERROR does not apply to .PHONY
# targets.)
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

CFS      := ./cfs-server(SUFFIX)

go-targets := $(CFS)

build-mode = build -o $@

go-install: build-mode = install

$(CFS) : $(C_LIBS_COMMON)
	 $(xgo) $(build-mode) -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(BUILDTARGET)

.PHONY: build
build: ## Build the CFS binary.
build: $(CFS)