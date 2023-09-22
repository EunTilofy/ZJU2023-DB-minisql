SHELL := /bin/bash

all:
	@if [[ ! -e build/Makefile ]]; then \
		mkdir -p build; \
		cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug; \
	fi;
	@make $(TEST) -j8 -C build
	@build/test/minisql_test

run:
	@build/test/minisql_test

clean:
	@rm -r build

.PHONY: all run clean