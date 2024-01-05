# Cubefs Makefile
#
threads?=0
RM := $(shell [ -x /bin/rm ] && echo "/bin/rm" || echo "/usr/bin/rm" )
GOMOD=on
default: all

phony := all
all: build

phony += build server authtool client cli libsdkpre libsdk fsck fdstore preload bcache blobstore deploy
build: server authtool client cli libsdk fsck fdstore preload bcache blobstore deploy

server:
	@build/build.sh server $(GOMOD) --threads=$(threads)


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

preload:
	@build/build.sh preload $(GOMOD) --threads=$(threads)

bcache:
	@build/build.sh bcache $(GOMOD) --threads=$(threads)

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

.PHONY: $(phony)
