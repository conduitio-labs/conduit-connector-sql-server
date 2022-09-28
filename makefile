.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-sql-server.version=${VERSION}'" -o conduit-connector-sql-server cmd/sql-server/main.go

test:
	go test $(GOTEST_FLAGS) ./...

lint:
	golangci-lint run

mockgen:
	mockgen -package mock -source destination/interface.go -destination destination/mock/destination.go
