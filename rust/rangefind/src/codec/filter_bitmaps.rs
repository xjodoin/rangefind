//! Filter bitmaps (`rffilterbitmap-v1`) — `parseFilterBitmap` and
//! `filterBitmapHas` from `src/filter_bitmaps.js`.

use super::binary::FILTER_BITMAP_MAGIC;
use super::postings::data_view_error;
use crate::error::{Error, Result};
use crate::util::bytes::Bytes;

pub const FILTER_BITMAP_FORMAT: &str = "rffilterbitmap-v1";

#[derive(Clone, Debug)]
pub struct FilterBitmap {
    pub total: u32,
    pub bytes: Bytes,
}

impl FilterBitmap {
    pub fn format(&self) -> &'static str {
        FILTER_BITMAP_FORMAT
    }

    /// `filterBitmapHas(bitmap, doc)`.
    pub fn has(&self, doc: i64) -> bool {
        if doc < 0 || doc >= self.total as i64 {
            return false;
        }
        let byte = self.bytes.get((doc >> 3) as usize).copied().unwrap_or(0);
        byte & (1u8 << (doc & 7)) != 0
    }
}

/// `filterBitmapHas(bitmap, doc)` with an optional bitmap.
pub fn filter_bitmap_has(bitmap: Option<&FilterBitmap>, doc: i64) -> bool {
    bitmap.map(|bitmap| bitmap.has(doc)).unwrap_or(false)
}

fn u32_at(bytes: &[u8], pos: usize) -> Result<u32> {
    if pos + 4 > bytes.len() {
        return Err(data_view_error());
    }
    Ok(u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("4 bytes")))
}

/// `parseFilterBitmap(buffer)`; `bytes` stays a zero-copy view.
pub fn parse_filter_bitmap(bytes: Bytes) -> Result<FilterBitmap> {
    for (index, magic) in FILTER_BITMAP_MAGIC.iter().enumerate() {
        if bytes.get(index) != Some(magic) {
            return Err(Error::new("Unsupported Rangefind filter bitmap."));
        }
    }
    let version = u32_at(&bytes, 4)?;
    if version != 1 {
        return Err(Error::new(format!("Unsupported Rangefind filter bitmap version {version}.")));
    }
    let total = u32_at(&bytes, 8)?;
    let payload = bytes.slice(12, bytes.len());
    Ok(FilterBitmap { total, bytes: payload })
}
