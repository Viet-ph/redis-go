BINARY_NAME := redis-go

# Detect OS
UNAME_S := $(shell uname -s)

# Build for Linux
build-linux:
	@echo "Building for Linux..."
	GOOS=linux GOARCH=amd64 go build -o ./bin/$(BINARY_NAME) ./cmd/redis-go 

# Build for macOS
build-darwin:
	@echo "Building for macOS..."
	GOOS=darwin GOARCH=amd64 go build -o ./bin/$(BINARY_NAME) ./cmd/redis-go 

# Automatically detect and build for the current OS
build:
	@if [ "$(UNAME_S)" = "Linux" ]; then \
		$(MAKE) build-linux; \
	elif [ "$(UNAME_S)" = "Darwin" ]; then \
		$(MAKE) build-darwin; \
	else \
		echo "Unsupported OS: $(UNAME_S)"; \
	fi


# Clean up binaries
clean:
	@echo "Cleaning up..."
	rm -f ./bin/$(BINARY_NAME)

# Run the binary
run: build
	./bin/$(BINARY_NAME)

# Clean and rebuild
rebuild: clean build

.PHONY: build build-linux build-darwin clean run rebuild
