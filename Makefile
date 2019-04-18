
default: build

build: pre_build build_server build_client
	@echo "build done"

pre_build:
	@mkdir -p build

build_server:
	@{ \
		echo "build server" \
		&& (cd cmd && sh ./build.sh && mv cmd ../build/cfs-server) \
	}

build_client:
	@{ \
		echo "build client" \
		&& (cd client && sh ./build.sh && mv client ../build/cfs-client ) \
	}

ci-test:
	go test $(go list ./...)
