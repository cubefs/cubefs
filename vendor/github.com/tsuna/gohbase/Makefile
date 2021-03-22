# Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
# This file is part of GoHBase.
# Use of this source code is governed by the Apache License 2.0
# that can be found in the COPYING file.

GO := go
TEST_TIMEOUT := 30s
INTEGRATION_TIMEOUT := 120s
GOTEST_FLAGS := -v
GOTEST_ARGS := 


check: vet fmtcheck
jenkins: check test integration

COVER_PKGS := `go list ./... | grep -v test`
COVER_MODE := atomic
integration_cover:
	$(GO) test -v -covermode=$(COVER_MODE) -race -timeout=$(INTEGRATION_TIMEOUT) -tags=integration -coverprofile=coverage.out $(COVER_PKGS)

coverage: integration_cover
	$(GO) tool cover -html=coverage.out

fmtcheck:
	errors=`gofmt -l .`; if test -n "$$errors"; then echo Check these files for style errors:; echo "$$errors"; exit 1; fi
	find . -name '*.go' ! -path "./pb/*" ! -path "./test/mock/*" !  -path './gen.go' -exec ./check_line_len.awk {} +

vet:
	$(GO) vet ./...

test:
	$(GO) test $(GOTEST_FLAGS) -race -timeout=$(TEST_TIMEOUT) ./...

integration:
	$(GO) test $(GOTEST_FLAGS) -race -timeout=$(INTEGRATION_TIMEOUT) -tags=integration -args $(GOTEST_ARGS)

.PHONY: check coverage integration_cover fmtcheck integration jenkins test vet
