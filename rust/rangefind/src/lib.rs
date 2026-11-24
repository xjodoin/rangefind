//! Rangefind query engine.
//!
//! A Rust port of the JavaScript runtime (`src/runtime.js`) that reads the
//! same static, range-packed index files. The engine is transport-agnostic
//! and single-threaded by design, so the identical code runs natively
//! (blocking file/HTTP reads) and as WebAssembly (host-driven fetches).

#![allow(clippy::too_many_arguments, clippy::type_complexity)]

pub mod budget;
pub mod error;
pub mod pointer;
pub mod rt;
pub mod transport;
pub mod util;

#[cfg(feature = "analysis")]
pub mod analysis;
#[cfg(feature = "analysis")]
pub mod highlight;
#[cfg(feature = "analysis")]
pub mod terms;

#[cfg(feature = "codec")]
pub mod codec;

#[cfg(feature = "geo")]
pub mod geo;

#[cfg(feature = "authority")]
pub mod authority;

#[cfg(feature = "vector")]
pub mod typo;
#[cfg(feature = "vector")]
pub mod vector;

#[cfg(feature = "engine")]
pub mod engine;
#[cfg(feature = "engine")]
pub mod ffi;
#[cfg(feature = "engine")]
pub mod host;
#[cfg(feature = "osm")]
pub mod osm;

pub use error::{Error, Result};
