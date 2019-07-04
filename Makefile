# ChubaoFS Makefile
#

#UNAME_S := $(shell uname -s)
#ifneq ($(UNAME_S),Linux)
#$(ChubaoFS only support Linux os)
#endif

#GOPATH := $(realpath $(GOPATH))
#ifeq "$(strip $(GOPATH))" ""
#$(error GOPATH is not set and could not be automatically determined)
#endif

#ifeq "$(filter $(GOPATH)%,$(CURDIR))" ""
#$(error Current directory "$(CURDIR)" is not within GOPATH "$(GOPATH)")
#endif

#ifeq "$(GOPATH)" "/"
#$(error GOPATH=/ is not supported)
#endif


default: build

.PHONY: build

build:
	@build/build.sh

clean:
	@rm -rf build/bin/*

ci-test:
	@{ \
		echo "ci test" \
		&& ( go test ./... ) \
	}

