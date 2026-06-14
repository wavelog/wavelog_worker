BINARY  := wavelog_worker
DIST    := dist
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

.PHONY: build build-all clean test \
        build-linux-amd64 build-linux-arm64 build-linux-arm

build:
	mkdir -p $(DIST)
	go build $(LDFLAGS) -o $(DIST)/$(BINARY) .

test:
	go test -race ./...

build-all: build-linux-amd64 build-linux-arm64 build-linux-arm

build-linux-amd64:
	mkdir -p $(DIST)
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(DIST)/$(BINARY)-amd64 .

build-linux-arm64:
	mkdir -p $(DIST)
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(DIST)/$(BINARY)-arm64 .

build-linux-arm:
	mkdir -p $(DIST)
	GOOS=linux GOARCH=arm GOARM=7 go build $(LDFLAGS) -o $(DIST)/$(BINARY)-armv7 .

clean:
	rm -rf $(DIST)
