
default: build

build: build_server build_client
	@echo "build done"

pre_build:
	@mkdir -p docker/bin

build_server: pre_build
	@{ \
		echo -n "build server " \
		&& (go build -o docker/bin/cfs-server cmd/*.go ) \
		&& (echo "success") \
	}

build_client: pre_build
	@{ \
		echo -n "build client " \
		&& (go build -o docker/bin/cfs-client client/*.go ) \
		&& (echo "success") \
	}

ci-test:
	@{ \
		echo "ci test" \
		&& ( go test ./... ) \
	}
