# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOCLEAN=$(GOCMD) clean
GOMOD=$(GOCMD) mod
BINARY_NAME=seg-layout

# Build flags
LDFLAGS=-ldflags "-w -s"
DEBUG_LDFLAGS=-ldflags "-w -s -X main.Debug=true"

# Build targets
.PHONY: all build clean test deps debug

all: deps build

build:
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME) ./cmd/main/main.go

debug:
	$(GOBUILD) $(DEBUG_LDFLAGS) -gcflags "all=-N -l" -o $(BINARY_NAME) ./cmd/main/main.go

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f *.out
	rm -f *.prof

test:
	$(GOTEST) -v ./...

test-coverage:
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

deps:
	$(GOMOD) tidy

# Development tools
.PHONY: fmt lint

fmt:
	$(GOCMD) fmt ./...

lint:
	$(GOCMD) vet ./...

# Run the program
.PHONY: run test-run debug-run profile-run

run: build
	./$(BINARY_NAME)

debug-run: debug
	./$(BINARY_NAME) --delete-ratio=0.3 --max-size=4194304 --min-size=512 --operations=1000

# Run performance test with default parameters
test-run: build
	./$(BINARY_NAME) --delete-ratio=0.3 --max-size=4194304 --min-size=512 --operations=1000

# Run with profiling
profile-run: debug
	./$(BINARY_NAME) --delete-ratio=0.3 --max-size=4194304 --min-size=512 --operations=1000 -cpuprofile=cpu.prof -memprofile=mem.prof

# Default target
.DEFAULT_GOAL := build 