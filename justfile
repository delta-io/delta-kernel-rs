dat_version := "0.0.2"
dat_out_folder := "tests/dat"
dat_done_file := 'tests/dat/.done'
dat_exists_file_check := path_exists('./tests/dat/.done')
tar_tmp := "dat.tar.gz"

default:
    just --list

# load test cases from delta acceptance tests repo
load-dat:
    mkdir -p {{ dat_out_folder }}
    curl -L "https://github.com/delta-incubator/dat/releases/download/v{{ dat_version }}/deltalake-dat-v{{ dat_version }}.tar.gz" > {{ tar_tmp }}
    tar -xvf {{ tar_tmp }} -C {{ dat_out_folder }}
    rm ./{{ tar_tmp }}
    touch {{ dat_done_file }}

# run tests
test:
    cargo test --features acceptance,default-client

# lint codebase
lint:
    cargo clippy --tests --features acceptance,default-client

# fix all fixable linitin errors
fix:
    cargo clippy --fix --tests --features acceptance,default-client

# build and serve the documentation
docs:
    cardo docs --open
