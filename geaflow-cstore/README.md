# CStore
CStore is a open-source **Graph-Native** storage engine designed for graph based OLAP(Online Analytics Processing) scenario.

## Quick Start
### Build Prerequisites
```shell
# install rust.
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# install nightly toolchain.
rustup update && rustup toolchain install nightly && rustc --version
# install other dependencies. 
yum install make gcc gcc-c++ protobuf-devel protobuf clang
```

### Build from Source
The project support building out of box.
```shell
make build
```

## Develop
Please use "make help" to get more dev information.
```shell
geaflow-cstore 0.1.0
Usage: make <target>
Targets:
  build-dev                       Build the geaflow-cstore with dev version. Both <make build> and <make build-dev> work.
  build-release                   Build the geaflow-cstore with release version.
  fmt                             Format the code by rustfmt.toml.
  clippy                          Check statically code with clippy.
  test-all                        Run all integration tests and unit tests, include ignored tests.
  test                            Execute all the unit tests.
  test-fn                         Use "make test-fn mod=<func_name>" to specify which function of test to run.
  test-in                         Use "make test-in mod=<func_name>" to specify which target of integration tests to run.
  bench-all                       Run all benches.
  bench                           Use "make bench mod=<mod_name>" to specify which bench of mod to run.
  doc                             Generate the document of geaflow-cstore and open it in html.
  all                             Execute code style and static checks, release version compilation and tests in sequence.
  update                          Update all the dependences to the newest version, include rust analyzer.
  gen                             Generate the code described by proto.
  features                        Use "make features mod=<id>" {0->[default] 1->[hdfs]} to set the env in ~/.cstore_buildrc.
  clean                           Clean up the cargo cache.
  version                         Show the version of geaflow-cstore.
  help                            List optional commands.
```

