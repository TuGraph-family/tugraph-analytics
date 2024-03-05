# Geaflow CStore Makefile #

# Default command.
.DEFAULT_GOAL=help

# Get the version from cargo.toml.
VERSION := $(shell awk -F '["]' '/^version/ {print $$2}' Cargo.toml)

# Variable of cargo.
CARGO := cargo

# Variable of enviroment
RUST_BACKTRACE := RUST_BACKTRACE=full
RUST_FLAGS_LOCAL := RUSTFLAGS="-Dwarnings"
CARGO_NET_FLAGS := CARGO_NET_RETRY=5 HTTP_TIMEOUT=60

# Generate differentiated commands based on environment variable 'FEATURES'
ifeq ($(FEATURES),1)
    CARGO_DWF := $(RUST_BACKTRACE) $(CARGO_NET_FLAGS) $(CARGO)
    SUFFIX := --features hdfs
else
	CARGO_DWF := $(RUST_BACKTRACE) $(CARGO_NET_FLAGS) $(CARGO)
	SUFFIX := 
endif

# Variable of enable nightly.
NIGHTLY := +nightly-2023-08-15

# Variable of build command.
BUILD_RELEASE := $(CARGO_DWF) rustc --release --crate-type=cdylib $(SUFFIX)
BUILD_DEV := $(CARGO_DWF) build $(SUFFIX)

# Variable of fmt command.
FMT := $(CARGO_DWF) $(NIGHTLY) fmt --all

# Variable of static check command.
CLIPPY := $(CARGO_DWF) $(NIGHTLY) clippy $(SUFFIX)

# Variable of test command.
RUN_UNIT_TEST := $(CARGO_DWF) test --lib $(SUFFIX)
RUN_ALL_TEST := $(CARGO_DWF) test $(SUFFIX) && $(CARGO_DWF) test $(SUFFIX) -- --ignored
RUN_TEST_SPECIFY_TARGET_TEST_FUNCTION := $(CARGO_DWF) test
RUN_TEST_SPECIFY_TARGET_INTEGRATION_MOD := $(CARGO_DWF) test --test 

# Variable of benchmark command.
BUILD_BENCH := $(CARGO_DWF) bench --no-run $(SUFFIX)
RUN_BENCH := $(CARGO_DWF) bench $(SUFFIX)
RUN_SPE_BENCH := $(CARGO_DWF) bench --bench

# Variable of generate code from proto, 
# FLATC := flatc --rust -o src/common/gen/ proto/graph_data.fbs

# Variable of doc command.
GENERATE_DOC := $(CARGO) doc --open

# Variable of update
UPDATE := $(CARGO) upgrade && $(CARGO) update && rustup update && rustup update nightly

# Variable of clean.
CLEAN := $(CARGO_DWF) clean

# Optional parameters.
mod := 

## All optional user commands.
.PHONY: build-dev
build-dev: ## Build the geaflow-cstore with dev version. Both <make build> and <make build-dev> work.
		$(FMT)
		$(CLIPPY)
		$(BUILD_DEV)
		$(BUILD_BENCH)

.PHONY: build
build: build-dev

.PHONY: build-release
build-release: ## Build the geaflow-cstore with release version.
		$(FMT)
		$(CLIPPY)
		$(BUILD_BENCH)
		$(BUILD_RELEASE)

.PHONY: fmt
fmt: ## Format the code by rustfmt.toml.
		$(FMT)

.PHONY: clippy
clippy: ## Check statically code with clippy.
		$(CLIPPY)

.PHONY: test-all
test-all: ## Run all integration tests and unit tests, include ignored tests.
		$(RUN_ALL_TEST)

.PHONY: test
test: ## Execute all the unit tests.
		$(RUN_UNIT_TEST)

.PHONY: test-fn
test-fn: ## Use "make test-fn mod=<func_name>" to specify which function of test to run.
		$(RUN_TEST_SPECIFY_TARGET_TEST_FUNCTION) $(mod) $(SUFFIX)

.PHONY: test-in
test-in: ## Use "make test-in mod=<func_name>" to specify which target of integration tests to run.
		$(RUN_TEST_SPECIFY_TARGET_INTEGRATION_MOD) $(mod) $(SUFFIX)

.PHONY: bench-all
bench-all: ## Run all benches.
		$(RUN_BENCH)

.PHONY: bench
bench: ## Use "make bench mod=<mod_name>" to specify which bench of mod to run.
		$(RUN_SPE_BENCH) $(mod) $(SUFFIX)

.PHONY: doc
doc: ## Generate the document of geaflow-cstore and open it in html.
		$(GENERATE_DOC)

.PHONY: all
all: ## Execute code style and static checks, release version compilation and tests in sequence.
		$(FMT)
		$(CLIPPY)
		$(BUILD_RELEASE)
		$(BUILD_BENCH)
		$(RUN_UNIT_TEST)

.PHONY: update
update: ## Update all the dependences to the newest version, include rust analyzer.
		$(UPDATE)

.PHONY: gen
gen: ## Generate the code described by proto.
		$(FLATC)

.PHONY: features
features:  ## Use "make features mod=<id>" {0->[default] 1->[hdfs]} to set the env in ~/.cstore_buildrc.
		@if [ $(mod) -eq 1 ] || [ $(mod) -eq 0 ] || [ $(mod) -eq 2 ]; then \
			touch ~/.cstore_buildrc; \
			if [ $(shell grep "export FEATURES=" ~/.cstore_buildrc | wc -l) -eq 1 ] ; then \
				sed -i 's/FEATURES=.*/FEATURES=$(mod)/g' ~/.cstore_buildrc; \
				echo "change geaflow_cstore mod to $(mod)"; \
			else \
				echo "export FEATURES=$(mod)" >> ~/.cstore_buildrc; \
			fi \
		else \
			echo "Warning: Chose effective features" ;\
			echo "Sug: 0->[default] 1->[hdfs]"; \
		fi

.PHONY: clean
clean:  ## Clean up the cargo cache.
		$(CLEAN)

.PHONY: vesion
version: ## Show the version of geaflow-cstore.
		@echo "\033[34mgeaflow-cstore $(VERSION)\033[0m"

.PHONY: help
help: ## List optional commands.
		@echo "\033[34mgeaflow-cstore $(VERSION)\033[0m" 
		@echo "Usage: make <target>"
		@echo "Targets:"
		@awk -F ':|##' '/^[^\t].+?:.*?##/ {\
		printf "  \033[36m%-30s\033[0m %s\n", $$1, $$NF \
		}' $(MAKEFILE_LIST)




