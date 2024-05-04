default:
    just --list

# run tests
test:
    cargo test --features default-engine

# lint codebase
lint:
    cargo clippy --tests --features default-engine

# fix all fixable linitin errors
fix:
    cargo clippy --fix --tests --features default-engine

# build and serve the documentation
docs:
    cargo docs --open

# build and test ffi
ffi:
    pushd ffi
    cargo b --features default-engine
    table=../kernel/tests/data/table-without-dv-small make run
    popd
