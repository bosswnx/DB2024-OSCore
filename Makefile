BUILD_TYPE ?= Debug

.PHONY: build clean help format format-all

build: format
	@mkdir -p build
	@cd build && \
		cmake .. -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DCMAKE_EXPORT_COMPILE_COMMANDS=ON && \
		make -j$(nproc)
	@ln -sf build/compile_commands.json compile_commands.json

clean:
	@rm -rf build

format-all:		# 格式化所有文件，速度较慢
	git ls-files | grep -E '.*\.(cpp|h)' | xargs clang-format -i

# run clang-format on all lines that differ between the working directory and <commit>, which defaults to HEAD
# 只格式化修改过的行
# --force: 允许格式化unstaged的文件
# 返回1表示有文件格式化过，第二次格式化时没有文件需要格式化，返回码为0，makefile不会报错
format:
	@git clang-format --force --extensions cpp,h || git clang-format --force --extensions cpp,h

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

