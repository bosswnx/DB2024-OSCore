MAKEFLAGS += -s  # 去除 make 默认输出
BUILD_TYPE ?= Debug

.PHONY: build clean help run

build:
	@mkdir -p build
	@cd build && \
		cmake .. -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DCMAKE_EXPORT_COMPILE_COMMANDS=ON && \
		make -j$(nproc)
	@ln -sf build/compile_commands.json compile_commands.json

clean:
	@rm -rf build

debug: build
	@cd build && rm -rf debug_db && \
	tmux kill-session -t rmdb_session 2>/dev/null || true && \
	tmux new-session -d -s rmdb_session && \
	tmux split-window -h && \
	tmux send-keys -t rmdb_session:0.1 './bin/rmdb debug_db' C-m && \
	tmux send-keys -t rmdb_session:0.0 'sleep 1; ./bin/rmdb_client' C-m && \
	tmux select-pane -t rmdb_session:0.0 && \
	tmux attach-session -t rmdb_session

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

