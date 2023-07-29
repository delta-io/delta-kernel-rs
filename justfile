default:
    just --list

# run tests
test:
    cargo test --features default-client

# lint codebase
lint:
    cargo clippy --tests --features default-client

# fix all fixable linitin errors
fix:
    cargo clippy --fix --tests --features default-client

# build and serve the documentation
docs:
    cardo docs --open
