# Cubefs Makefile
#

RM := $(shell [ -x /bin/rm ] && echo "/bin/rm" || echo "/usr/bin/rm" )
GOMOD=on
default: all

phony := all
all: build

phony += build server authtool client cli libsdk fsck fdstore preload bcache blobstore
build: server authtool client cli libsdk fsck fdstore preload bcache blobstore

server: 
	@build/build.sh server $(GOMOD)

blobstore:
	@build/build.sh blobstore $(GOMOD)

client: 
	@build/build.sh client $(GOMOD)

authtool: 
	@build/build.sh authtool $(GOMOD)

cli: 
	@build/build.sh cli $(GOMOD)

fsck: 
	@build/build.sh fsck $(GOMOD)

libsdk: 
	@build/build.sh libsdk $(GOMOD)

fdstore: 
	@build/build.sh fdstore $(GOMOD)

preload: 
	@build/build.sh preload $(GOMOD)

bcache: 
	@build/build.sh bcache $(GOMOD)

phony += clean
clean:
	@$(RM) -rf build/bin

phony += dist-clean
dist-clean:
	@build/build.sh dist_clean

phony += test
test:
	@build/build.sh test $(GOMOD)

phony += testcover
testcover:
	@build/build.sh testcover $(GOMOD)

.PHONY: $(phony)
