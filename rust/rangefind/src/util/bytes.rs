//! Reference-counted byte buffers with cheap sub-slicing, plus the varint
//! and fixed-width readers every Rangefind binary format shares
//! (`src/binary.js`).

use std::ops::Deref;
use std::rc::Rc;

#[derive(Clone)]
pub struct Bytes {
    buf: Rc<Vec<u8>>,
    start: usize,
    end: usize,
}

impl Bytes {
    pub fn new(data: Vec<u8>) -> Self {
        let end = data.len();
        Bytes { buf: Rc::new(data), start: 0, end }
    }

    pub fn empty() -> Self {
        Bytes::new(Vec::new())
    }

    /// Sub-slice relative to this view, clamped like `ArrayBuffer.slice`.
    pub fn slice(&self, start: usize, end: usize) -> Bytes {
        let len = self.len();
        let s = start.min(len);
        let e = end.min(len).max(s);
        Bytes { buf: self.buf.clone(), start: self.start + s, end: self.start + e }
    }

    /// Size of the whole backing buffer this view keeps alive.
    pub fn backing_len(&self) -> usize {
        self.buf.capacity()
    }
}

impl Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }
}

impl std::fmt::Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bytes({})", self.len())
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(data: Vec<u8>) -> Self {
        Bytes::new(data)
    }
}

/// Cursor over a byte slice. Reads past the end yield zeros, matching the
/// JS readers (which see `undefined` → 0) so truncated inputs fail in the
/// same shape instead of panicking.
pub struct Reader<'a> {
    pub bytes: &'a [u8],
    pub pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Reader { bytes, pos: 0 }
    }

    pub fn at(bytes: &'a [u8], pos: usize) -> Self {
        Reader { bytes, pos }
    }

    pub fn eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    pub fn u8(&mut self) -> u8 {
        let value = self.bytes.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        value
    }

    /// LEB128-style varint as written by `pushVarint`. JS accumulates in a
    /// double, so values are exact up to 2^53; u64 covers that range.
    pub fn varint(&mut self) -> u64 {
        let mut value: u64 = 0;
        let mut shift = 0u32;
        while self.pos < self.bytes.len() {
            let byte = self.bytes[self.pos];
            self.pos += 1;
            if shift < 64 {
                value |= ((byte & 0x7f) as u64) << shift;
            }
            if byte & 0x80 == 0 {
                return value;
            }
            shift += 7;
        }
        value
    }

    pub fn varint_usize(&mut self) -> usize {
        self.varint() as usize
    }

    pub fn take(&mut self, len: usize) -> &'a [u8] {
        let start = self.pos.min(self.bytes.len());
        let end = self.pos.saturating_add(len).min(self.bytes.len());
        self.pos = self.pos.saturating_add(len);
        &self.bytes[start..end]
    }

    /// Length-prefixed UTF-8 (`readUtf8`). Invalid sequences decode to
    /// U+FFFD exactly like `TextDecoder`.
    pub fn utf8(&mut self) -> String {
        let len = self.varint_usize();
        decode_utf8(self.take(len))
    }

    pub fn f64_le(&mut self) -> f64 {
        let slice = self.take(8);
        let mut raw = [0u8; 8];
        raw[..slice.len()].copy_from_slice(slice);
        f64::from_le_bytes(raw)
    }

    pub fn f32_le(&mut self) -> f32 {
        let slice = self.take(4);
        let mut raw = [0u8; 4];
        raw[..slice.len()].copy_from_slice(slice);
        f32::from_le_bytes(raw)
    }

    pub fn u32_le(&mut self) -> u32 {
        let slice = self.take(4);
        let mut raw = [0u8; 4];
        raw[..slice.len()].copy_from_slice(slice);
        u32::from_le_bytes(raw)
    }

    pub fn i32_le(&mut self) -> i32 {
        self.u32_le() as i32
    }

    pub fn u16_le(&mut self) -> u16 {
        let slice = self.take(2);
        let mut raw = [0u8; 2];
        raw[..slice.len()].copy_from_slice(slice);
        u16::from_le_bytes(raw)
    }
}

/// `new TextDecoder().decode(bytes)`: lossy UTF-8 that also drops one
/// leading byte-order mark, as the WHATWG decoder does by default.
pub fn decode_utf8(bytes: &[u8]) -> String {
    let body = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes);
    String::from_utf8_lossy(body).into_owned()
}

/// Little-endian fixed-width unsigned integer (`readFixedInt`).
pub fn read_fixed_int(bytes: &[u8], offset: usize, width: usize) -> u64 {
    let mut value: u64 = 0;
    for i in 0..width.min(8) {
        value |= (bytes.get(offset + i).copied().unwrap_or(0) as u64) << (8 * i);
    }
    value
}

pub fn assert_magic(bytes: &[u8], magic: &[u8], message: &str) -> crate::error::Result<()> {
    if bytes.len() < magic.len() || &bytes[..magic.len()] != magic {
        return Err(crate::error::Error::new(message));
    }
    Ok(())
}

pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 15) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trip() {
        let mut reader = Reader::new(&[0x00, 0x7f, 0x80, 0x01, 0xff, 0xff, 0x03]);
        assert_eq!(reader.varint(), 0);
        assert_eq!(reader.varint(), 127);
        assert_eq!(reader.varint(), 128);
        assert_eq!(reader.varint(), 65535);
        assert!(reader.eof());
        assert_eq!(reader.varint(), 0);
    }

    #[test]
    fn slices_clamp() {
        let bytes = Bytes::new(vec![1, 2, 3, 4]);
        assert_eq!(&*bytes.slice(1, 3), &[2, 3]);
        assert_eq!(&*bytes.slice(3, 99), &[4]);
        assert_eq!(bytes.slice(9, 12).len(), 0);
        assert_eq!(&*bytes.slice(1, 4).slice(1, 2), &[3]);
    }

    #[test]
    fn fixed_ints() {
        assert_eq!(read_fixed_int(&[0x34, 0x12], 0, 2), 0x1234);
        assert_eq!(read_fixed_int(&[1, 0, 0, 0, 0, 1], 0, 6), 1 + (1u64 << 40));
    }
}
