# ChubaoFS Makefile
#

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

