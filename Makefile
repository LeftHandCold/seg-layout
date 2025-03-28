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
DEBUG_FLAGS=-gcflags "all=-N -l"

# Build targets
.PHONY: all build clean test deps debug

all: deps build

build:
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME) ./cmd/main/main.go

debug:
	$(GOBUILD) $(DEBUG_FLAGS) -o $(BINARY_NAME) ./cmd/main/main.go

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
.PHONY: run test-run debug-run profile-run endurance-test-10t endurance-test-100t

run: build
	./$(BINARY_NAME)

debug-run: debug
	./$(BINARY_NAME) --debug --delete-ratio=0.3 --max-size=4194304 --min-size=512 --operations=1000

# Run performance test with default parameters
test-run: build
	./$(BINARY_NAME) --delete-ratio=0.3 --max-size=4194304 --min-size=512 --operations=1000

# Run endurance tests
endurance-test-10t: debug
	./$(BINARY_NAME) --debug --mode=endurance --target-write=10995116277760 --max-size=4194304 --min-size=512 --cpuprofile=cpu_10t.prof --memprofile=mem_10t.prof

endurance-test-100t: debug
	./$(BINARY_NAME) --debug --mode=endurance --target-write=109951162777600 --max-size=4194304 --min-size=512 --cpuprofile=cpu_100t.prof --memprofile=mem_100t.prof

# Run with profiling
profile-run: debug
	./$(BINARY_NAME) --debug --delete-ratio=0.3 --max-size=4194304 --min-size=512 --operations=1000 -cpuprofile=cpu.prof -memprofile=mem.prof

# Default target
.DEFAULT_GOAL := build 