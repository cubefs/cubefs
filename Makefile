# Cubefs Makefile
#
threads?=0
RM := $(shell [ -x /bin/rm ] && echo "/bin/rm" || echo "/usr/bin/rm" )
GOMOD=on
default: all

phony := all
all: build

phony += build server authtool client cli libsdkpre libsdk fsck fdstore bcache blobstore deploy
build: server authtool client cli libsdk fsck fdstore bcache blobstore deploy cfs-sync


server:
	@build/build.sh server $(GOMOD) --threads=$(threads)

phony += server-notrdma
server-notrdma:
	@RDMA=0 build/build.sh server $(GOMOD) --threads=$(threads)


deploy:
	@build/build.sh deploy $(GOMOD) --threads=$(threads)


blobstore:
	@build/build.sh blobstore $(GOMOD) --threads=$(threads)

client:
	@build/build.sh client $(GOMOD) --threads=$(threads)

authtool:
	@build/build.sh authtool $(GOMOD) --threads=$(threads)

cli:
	@build/build.sh cli $(GOMOD) --threads=$(threads)

fsck:
	@build/build.sh fsck $(GOMOD) --threads=$(threads)

libsdkpre:
	@build/build.sh libsdkpre $(GOMOD) --threads=$(threads)

libsdk:
	@build/build.sh libsdk $(GOMOD) --threads=$(threads)

fdstore:
	@build/build.sh fdstore $(GOMOD) --threads=$(threads)

bcache:
	@build/build.sh bcache $(GOMOD) --threads=$(threads)

rctest:
	@build/build.sh rctest $(GOMOD) --threads=$(threads)

rcconfig:
	@build/build.sh rcconfig $(GOMOD) --threads=$(threads)


# ── cfs-sync cross-compilation ──────────────────────────────────────────────
# The CubeFS SDK only compiles for Linux; Darwin/Windows are not supported targets.
# Usage:
#   make cfs-sync                       # linux/amd64 (default)

#
# Output: build/bin/cfs-sync  (or build/bin/cfs-sync-linux-<GOARCH> for non-amd64)

GOOS   := linux
GOARCH ?= amd64
_CFS_SYNC_BIN := build/bin/cfs-sync
ifneq ($(GOARCH),amd64)
_CFS_SYNC_BIN := build/bin/cfs-sync-linux-$(GOARCH)
endif

phony += cfs-sync
cfs-sync:
	@mkdir -p build/bin
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
	  go build -trimpath \
	    -o $(_CFS_SYNC_BIN) \
	    github.com/cubefs/cubefs/tool/cfs-sync
	@echo "built $(_CFS_SYNC_BIN)"

phony += clean
clean:
	@$(RM) -rf build/bin

phony += dist-clean
dist-clean:
	@build/build.sh dist_clean --threads=$(threads)

phony += test
test:
	@build/build.sh test $(GOMOD) --threads=$(threads)

phony += testcover
testcover:
	@build/build.sh testcover $(GOMOD) --threads=$(threads)

phony += mock
mock:
	rm -rf metanode/mocktest
	mockgen -source=raftstore/partition.go -package=raftstoremock -destination=metanode/mocktest/raftstore/partition.go

phony += docker
docker:
	@docker/run_docker.sh --build
	@docker/run_docker.sh --clean

IMAGE_NAME?=hub.shiyak-office.com/storage/cubefs:v3.5.3.rc1

phony += image
image:
	docker build --platform linux/amd64 -t $(IMAGE_NAME) -f Dockerfile .
	@echo "Built linux/amd64 image: $(IMAGE_NAME)"

.PHONY: $(phony)
