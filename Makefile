BUILD_TYPE ?= Debug

.PHONY: build clean help

build:
	@mkdir -p build
	@cd build && cmake .. -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) && make -j$(nproc)

clean:
	@rm -rf build

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build       Build the project"
	@echo "  clean       Clean the project"
	@echo "  help        Show this help message"
	@echo ""
	@echo "Variables:"
	@echo "  BUILD_TYPE  Build type (Debug, Release), default: $(BUILD_TYPE)"
	@echo ""
	@echo "Example:"
	@echo "  make build BUILD_TYPE=Release"

