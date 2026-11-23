#!/usr/bin/env bash
# Build the Rangefind engine for wasm32-unknown-unknown and stage it next to
# the JavaScript glue (rust/rangefind/js/). No wasm-bindgen / wasm-pack
# needed: the module exports a plain C ABI (see src/ffi.rs).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
crate="$here/../../rangefind"
export PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH"
cd "$crate"
# --small: optimize for size (≈24% smaller gzip, ≈20% slower queries).
args=()
for arg in "$@"; do
  if [ "$arg" = "--small" ]; then export CARGO_PROFILE_WASM_RELEASE_OPT_LEVEL=s; else args+=("$arg"); fi
done
set -- "${args[@]+"${args[@]}"}"
cargo build --profile wasm-release --target wasm32-unknown-unknown --lib "$@" 2>&1 | grep -v '^\s*$' | tail -3
cp "$crate/../target/wasm32-unknown-unknown/wasm-release/rangefind.wasm" "$crate/js/rangefind_wasm.wasm"
ls -l "$crate/js/rangefind_wasm.wasm" | awk '{print "wasm:", $5, "bytes"}'
gzip -9 -c "$crate/js/rangefind_wasm.wasm" | wc -c | awk '{print "wasm.gz:", $1, "bytes"}'
# Stage the publishable pair next to the other browser bundles.
mkdir -p "$crate/../../dist"
cp "$crate/js/rangefind_wasm.mjs" "$crate/js/rangefind_wasm.wasm" "$crate/../../dist/"
