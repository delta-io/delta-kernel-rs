#!/usr/bin/env bash
# Record runner and compiler inputs that can explain differences between CI jobs.
# Callers treat probe failures as diagnostic so they cannot skip the tests.
set -euo pipefail

printf 'recorded_at=%s\n' "$(date -u +%FT%TZ)"
printf 'source_sha=%s\n' "${GITHUB_SHA:-local}"
printf 'runner_os=%s\n' "${RUNNER_OS:-$(uname -s)}"
printf 'runner_arch=%s\n' "${RUNNER_ARCH:-unknown}"
printf 'image_os=%s\n' "${ImageOS:-unknown}"
printf 'image_version=%s\n' "${ImageVersion:-unknown}"
printf 'CARGO_PROFILE_DEV_DEBUG=%s\n' "${CARGO_PROFILE_DEV_DEBUG:-unset}"
printf 'CARGO_PROFILE_TEST_DEBUG=%s\n' "${CARGO_PROFILE_TEST_DEBUG:-unset}"

case "${RUNNER_OS:-$(uname -s)}" in
  Linux)
    lscpu
    ;;
  macOS | Darwin)
    sysctl -n machdep.cpu.brand_string 2>/dev/null || sysctl -n hw.model
    sysctl -n hw.logicalcpu
    ;;
  Windows)
    pwsh -NoProfile -Command \
      'Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors'
    ;;
esac

rustup toolchain list
rustc -Vv
cargo nextest --version
