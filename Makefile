CXX ?= c++
CXXFLAGS ?= -O2 -std=c++17 -Wall -Wextra -Wno-unused-parameter
PYTHON ?= python3
DIR ?= bench/suites/dev
PER ?= 3

.PHONY: all tools sol check suite bench

all: tools sol

tools: tools/interactor tools/ref

sol: sol/main

tools/interactor: tools/interactor.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

tools/ref: tools/ref.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

sol/main: sol/main.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

check: tools/interactor sol/main
	./tools/interactor tests/example1.txt --quiet -- ./sol/main

suite: tools
	@test -n "$(SEED)" || { printf '%s\n' 'Usage: make suite SEED=<int> [DIR=...] [PER=3]'; exit 2; }
	@mkdir -p "$(DIR)"
	$(PYTHON) tools/gen.py --suite "$(DIR)" --seed "$(SEED)" --per-family "$(PER)"
	$(PYTHON) tools/calibrate.py "$(DIR)"/*.txt

bench: all
	$(PYTHON) bench/run.py --suite "$(DIR)"
