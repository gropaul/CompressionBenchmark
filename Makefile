.PHONY: all build clean test release debug help

# Default build type
BUILD_DIR ?= build/release
BUILD_TYPE ?= Release

all: build

# Configure and build the project
build:
	@mkdir -p $(BUILD_DIR)
	@cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) ../..
	@cmake --build $(BUILD_DIR) -j

# Build release version
release:
	@$(MAKE) build BUILD_DIR=build/release BUILD_TYPE=Release

# Build debug version
debug:
	@$(MAKE) build BUILD_DIR=build/debug BUILD_TYPE=Debug

# Clean build artifacts
clean:
	@rm -rf build/

# Clean and rebuild
rebuild: clean build

# Show help
help:
	@echo "Available targets:"
	@echo "  all (default) - Build the project (release mode)"
	@echo "  build         - Build the project"
	@echo "  release       - Build release version"
	@echo "  debug         - Build debug version"
	@echo "  clean         - Remove build artifacts"
	@echo "  rebuild       - Clean and rebuild"
	@echo "  help          - Show this help message"