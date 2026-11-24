# Porting conventions (Rust query engine)

The crate at `rust/rangefind` is a faithful port of the JavaScript query
runtime in `src/`. The JS source is the specification: port behavior
exactly, including edge cases, tie-breaks and iteration order.

## Rules

- **Own only your files.** Other module groups are being ported in parallel
  in this same crate. Never edit `Cargo.toml`, `src/lib.rs`, `src/util/*`,
  `src/rt.rs`, `src/transport.rs`, `src/pointer.rs`, `src/error.rs`, or
  another group's files. If you need something from them, report it.
- **Build only your feature**, with your own target dir, so a half-written
  sibling module cannot break your build:
  `PATH=/opt/homebrew/opt/rustup/bin:$PATH CARGO_TARGET_DIR=target/<group> cargo test --no-default-features --features <group>`
  (run from `rust/rangefind`). Finish with zero warnings in your files.
- **No new dependencies.** Available: `serde_json` (preserve_order),
  `flate2`, `sha2`, `unicode-normalization`, `unicode-properties`,
  `unicode-script`, std.
- **JS string semantics matter.** Index files are written by Node: string
  lengths, slice offsets and `<`/`>` ordering are UTF-16 code units. Use
  `crate::util::jsstr` (`len16`, `cmp16`, `prefix16`, `shard_key`, ...)
  wherever the JS uses `.length`, `.slice`, comparisons or `charCodeAt` on
  strings that can be non-ASCII. `Array.prototype.sort` is stable — use
  stable sorts. JS `Map`/`Set` iterate in insertion order — preserve it
  where output order depends on it (use `Vec` or an ordered structure).
- **Numbers.** JS numbers are f64. Use integers where the format guarantees
  integers; keep f64 arithmetic in the same operation order where results
  are floats (scores, distances) so outputs are bit-identical.
- **Shared helpers:** `crate::util::bytes::{Bytes, Reader, read_fixed_int,
  assert_magic, hex}` (varint/utf8/f64 readers that mirror `readVarint` /
  `readUtf8`), `crate::util::fold::{fold, fold_multi}` (= `foldMulti`),
  `crate::util::json` (JS-style `Number()`, truthiness, `num()` to emit
  JSON numbers like `JSON.stringify`), `crate::pointer::{Pointer, Checksum}`
  for every `{pack, offset, length, logicalLength, checksum}` object
  pointer, `crate::error::{Error, Result}` (+ `bail!`). Error messages
  should match the JS text.
- **Decode side only.** Port parsers/decoders and query-time logic. Skip
  builder/encoder functions unless a test needs them.
- **Typed, public API.** Expose plain structs with `pub` fields mirroring
  the JS object shapes (snake_case), and free functions named after the JS
  ones. Add a short module doc naming the JS file it mirrors. The engine
  layer is written by someone else against your public API, so end your
  work with a precise summary of every public item (signatures).

## Tests (required)

Every group proves parity against the JS implementation:

1. Write `rust/tools/fixtures/<group>.mjs`: a Node script that imports the
   JS modules from `../../src/...`, runs them over a broad, deterministic
   set of inputs (edge cases included: empty, non-ASCII, astral, large
   values, every codec variant) and writes JSON (binary as base64) to
   `rust/target/fixtures/<group>/`. Use the JS *encoders/builders* to
   produce real binary inputs. Run it with `node rust/tools/fixtures/<group>.mjs`
   from the repo root.
2. Write Rust tests (unit tests in your modules or `tests/<group>_parity.rs`)
   that load those fixtures and assert exact equality with the JS outputs.
   If the fixture directory is missing, fail with a message telling the
   developer to run the script. A tiny base64 decoder for tests may live in
   your own test file.
3. Aim for hundreds of generated cases per function where cheap, not a
   handful.
