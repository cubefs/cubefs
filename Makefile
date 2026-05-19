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

# ── 镜像构建 / 推送 ──────────────────────────────────────────────────
#
# 用法：
#   make image version=v3.5.3.rc90              # 构建 cubefs 主镜像
#   make image version=v3.5.3.rc90 push=1       # 构建 + 推送
#   make image-push version=v3.5.3.rc90         # 等价于 push=1
#   make pjd-image                              # 构建 pjd-fstest 镜像（tag=20090130）
#   make pjd-image push=1                       # 构建 + 推送 pjd
#   make pjd-image pjd_version=20090130-rc1     # 自定义 pjd tag
#
# 变量（命令行覆盖优先）：
#   version     主镜像 tag — 必填，无默认值
#   pjd_version pjd-fstest tag，默认 20090130
#   registry    仓库前缀，默认 hub.shiyak-office.com/storage
#   image       主镜像名，默认 cubefs
#   platform    docker --platform，默认 linux/amd64
#   push        =1 时构建后 docker push
#
# 兼容旧用法：仍可 IMAGE_NAME=完整路径:tag 显式覆盖整条镜像名。
#
# 注意：Dockerfile COPY build/bin/，docker build 前请先 make build
#       (或对应单组件如 make server)。

registry    ?= hub.shiyak-office.com/storage
image       ?= cubefs
platform    ?= linux/amd64
pjd_version ?= 20090130

# 主镜像名：显式 IMAGE_NAME 优先；否则 registry/image:version
ifeq ($(origin IMAGE_NAME), undefined)
IMAGE_FULL = $(registry)/$(image):$(version)
else
IMAGE_FULL = $(IMAGE_NAME)
endif

PJD_IMAGE_FULL = $(registry)/pjd-fstest:$(pjd_version)

phony += image
image:
ifeq ($(origin IMAGE_NAME), undefined)
ifeq ($(strip $(version)),)
	$(error version is required. Usage: make image version=vX.Y.Z.rcN [push=1])
endif
endif
	@echo "==> building $(IMAGE_FULL) (platform=$(platform))"
	docker build --platform $(platform) -t $(IMAGE_FULL) -f Dockerfile .
	@echo "==> built $(IMAGE_FULL)"
ifeq ($(push),1)
	@echo "==> pushing $(IMAGE_FULL)"
	docker push $(IMAGE_FULL)
	@echo "==> pushed $(IMAGE_FULL)"
endif

phony += image-push
image-push:
	@$(MAKE) image push=1

phony += pjd-image
pjd-image:
	@echo "==> building $(PJD_IMAGE_FULL) (platform=$(platform))"
	docker build --platform $(platform) -t $(PJD_IMAGE_FULL) -f docker/pjd-fstest/Dockerfile docker/pjd-fstest/
	@echo "==> built $(PJD_IMAGE_FULL)"
ifeq ($(push),1)
	@echo "==> pushing $(PJD_IMAGE_FULL)"
	docker push $(PJD_IMAGE_FULL)
	@echo "==> pushed $(PJD_IMAGE_FULL)"
endif

phony += pjd-image-push
pjd-image-push:
	@$(MAKE) pjd-image push=1

.PHONY: $(phony)
