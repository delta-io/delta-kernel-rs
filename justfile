dat_version := "0.0.2"
dat_out_folder := "tests/dat"
dat_done_file := 'tests/dat/.done'
dat_exists_file_check := path_exists('./tests/dat/.done')
tar_tmp := "dat.tar.gz"

default:
  just --list

load-dat:
  mkdir -p {{ dat_out_folder }}
  curl -L "https://github.com/delta-incubator/dat/releases/download/v{{ dat_version }}/deltalake-dat-v{{ dat_version }}.tar.gz" > {{ tar_tmp }}
  tar -xvf ./{{ tar_tmp }} -C ./{{ dat_out_folder }}
  rm ./{{ tar_tmp }}
  touch {{ dat_done_file }}
