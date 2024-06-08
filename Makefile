BUILD_TYPE ?= Debug

.PHONY: build clean help

build:
	@mkdir -p build
	@cd build && \
		cmake .. -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DCMAKE_EXPORT_COMPILE_COMMANDS=ON && \
		make -j$(nproc)
	@ln -sf build/compile_commands.json compile_commands.json

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

