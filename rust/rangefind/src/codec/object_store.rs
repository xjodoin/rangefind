//! Checksummed object verification — `verifyBlockPointer` from
//! `src/object_store.js`.

use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::pointer::Pointer;
use crate::util::bytes::hex;

pub const OBJECT_STORE_FORMAT: &str = "rfobject-v1";
pub const OBJECT_POINTER_FORMAT: &str = "rfbp-v1";
pub const OBJECT_CHECKSUM_ALGORITHM: &str = "sha256";

/// Lower-case hex SHA-256 (`sha256Hex`).
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// `verifyBlockPointer(value, pointer, label)`: the bytes must hash to the
/// pointer's sha256 checksum (an empty algorithm means sha256).
pub fn verify_pointer(bytes: &[u8], pointer: &Pointer, label: &str) -> Result<()> {
    let checksum = match pointer.checksum.as_ref().filter(|checksum| !checksum.value.is_empty()) {
        Some(checksum) => checksum,
        None => return Err(Error::new(format!("Rangefind missing checksum for {label}."))),
    };
    let algorithm = if checksum.algorithm.is_empty() { OBJECT_CHECKSUM_ALGORITHM } else { &checksum.algorithm };
    if algorithm != OBJECT_CHECKSUM_ALGORITHM {
        return Err(Error::new(format!("Rangefind unsupported checksum {algorithm} for {label}.")));
    }
    if sha256_hex(bytes) != *checksum.value {
        return Err(Error::new(format!("Rangefind checksum mismatch for {label}.")));
    }
    Ok(())
}
