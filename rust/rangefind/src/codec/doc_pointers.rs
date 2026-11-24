//! Fixed-width doc pointer tables (`rfdocptr-v1`, and the doc-page pointer
//! twin `rfdocpageptr-v1`) — the decode half of `src/doc_pointers.js`:
//! `parseDocPointerHeader`, `decodeDocPointerRecord`, `parseDocPointerPage`.

use std::rc::Rc;

use serde_json::Value;

use super::binary::{check_magic, DOC_PAGE_POINTER_MAGIC, DOC_POINTER_PAGE_MAGIC};
use super::{fixed_int, string_table};
use crate::error::{Error, Result};
use crate::pointer::{Checksum, Pointer};
use crate::util::bytes::{hex, Reader};
use crate::util::json;

pub const DOC_POINTER_FORMAT: &str = "rfdocptr-v1";
pub const DOC_PAGE_POINTER_FORMAT: &str = "rfdocpageptr-v1";
const DOC_POINTER_VERSION: u64 = 1;
const SHA256_BYTES: u64 = 32;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DocPointerWidths {
    pub pack: usize,
    pub offset: usize,
    pub length: usize,
    pub logical_length: usize,
}

/// Header of a pointer table (`{ format, version, count, checksum_bytes,
/// recordBytes, dataOffset, widths }`). The manifest stores the same object
/// (plus `file` and `pack_table`) under `docs.pointers` / `docs.pages.pointers`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DocPointerMeta {
    pub format: String,
    pub version: u64,
    pub count: usize,
    pub checksum_bytes: u64,
    pub record_bytes: usize,
    pub data_offset: usize,
    pub widths: DocPointerWidths,
    /// Manifest-only: the table file.
    pub file: Option<String>,
    /// Manifest-only: `pack_table`.
    pub pack_table: Vec<String>,
}

fn usize_of(value: &Value) -> usize {
    let n = json::to_number(value);
    if n.is_finite() && n > 0.0 {
        n as usize
    } else {
        0
    }
}

impl DocPointerMeta {
    /// From the manifest's pointer meta object.
    pub fn from_json(value: &Value) -> DocPointerMeta {
        let widths = json::path(value, &["widths"]);
        DocPointerMeta {
            format: value.get("format").and_then(Value::as_str).unwrap_or("").to_string(),
            version: usize_of(json::path(value, &["version"])) as u64,
            count: usize_of(json::path(value, &["count"])),
            checksum_bytes: usize_of(json::path(value, &["checksum_bytes"])) as u64,
            record_bytes: usize_of(json::path(value, &["recordBytes"])),
            data_offset: usize_of(json::path(value, &["dataOffset"])),
            widths: DocPointerWidths {
                pack: usize_of(json::path(widths, &["pack"])),
                offset: usize_of(json::path(widths, &["offset"])),
                length: usize_of(json::path(widths, &["length"])),
                logical_length: usize_of(json::path(widths, &["logicalLength"])),
            },
            file: value.get("file").and_then(Value::as_str).map(str::to_string),
            pack_table: string_table(json::path(value, &["pack_table"])),
        }
    }
}

/// Which pointer table flavor to parse (`options.magic/format/version`).
#[derive(Clone, Copy, Debug)]
pub struct DocPointerOptions {
    pub magic: &'static [u8; 4],
    pub format: &'static str,
    pub version: u64,
}

impl DocPointerOptions {
    pub const DOCS: DocPointerOptions = DocPointerOptions { magic: DOC_POINTER_PAGE_MAGIC, format: DOC_POINTER_FORMAT, version: DOC_POINTER_VERSION };
    pub const DOC_PAGES: DocPointerOptions =
        DocPointerOptions { magic: DOC_PAGE_POINTER_MAGIC, format: DOC_PAGE_POINTER_FORMAT, version: DOC_POINTER_VERSION };
}

/// `parseDocPointerHeader(buffer, options)`.
pub fn parse_doc_pointer_header_with(bytes: &[u8], options: DocPointerOptions) -> Result<DocPointerMeta> {
    check_magic(bytes, options.magic, "Unsupported Rangefind doc pointer table")?;
    let mut r = Reader::at(bytes, options.magic.len());
    let version = r.varint();
    if version != options.version {
        return Err(Error::new(format!("Unsupported Rangefind doc pointer table version {version}")));
    }
    let count = r.varint_usize();
    let widths = DocPointerWidths {
        pack: r.varint_usize(),
        offset: r.varint_usize(),
        length: r.varint_usize(),
        logical_length: r.varint_usize(),
    };
    let checksum_bytes = r.varint();
    let record_bytes = r.varint_usize();
    if checksum_bytes != SHA256_BYTES {
        return Err(Error::new(format!("Unsupported Rangefind doc pointer checksum width {checksum_bytes}")));
    }
    Ok(DocPointerMeta {
        format: options.format.to_string(),
        version,
        count,
        checksum_bytes,
        record_bytes,
        data_offset: r.pos,
        widths,
        file: None,
        pack_table: Vec::new(),
    })
}

pub fn parse_doc_pointer_header(bytes: &[u8]) -> Result<DocPointerMeta> {
    parse_doc_pointer_header_with(bytes, DocPointerOptions::DOCS)
}

/// `decodeDocPointerRecord(buffer, offset, meta, packTable)`.
pub fn decode_doc_pointer_record(bytes: &[u8], offset: usize, meta: &DocPointerMeta, pack_table: &[String]) -> Result<Pointer> {
    let w = meta.widths;
    let mut pos = offset;
    let pack_index = fixed_int(bytes, pos, w.pack);
    pos += w.pack;
    let pointer_offset = fixed_int(bytes, pos, w.offset);
    pos += w.offset;
    let length = fixed_int(bytes, pos, w.length);
    pos += w.length;
    let logical_length = fixed_int(bytes, pos, w.logical_length);
    pos += w.logical_length;
    if pos.saturating_add(SHA256_BYTES as usize) > bytes.len() {
        return Err(Error::new("Cannot read properties of undefined (reading 'toString')"));
    }
    let value = hex(&bytes[pos..pos + SHA256_BYTES as usize]);
    let pack = match pack_index.and_then(|index| pack_table.get(index as usize)) {
        Some(file) if !file.is_empty() => file.clone(),
        _ => {
            let shown = pack_index.map(|index| index.to_string()).unwrap_or_else(|| "NaN".to_string());
            return Err(Error::new(format!("Rangefind doc pointer references missing pack table index {shown}.")));
        }
    };
    let mut pointer = Pointer::new(pack, pointer_offset.unwrap_or(0), length.unwrap_or(0));
    pointer.logical_length = logical_length.filter(|n| *n != 0);
    pointer.checksum = Some(Checksum { algorithm: Rc::from("sha256"), value: Rc::from(value.as_str()) });
    Ok(pointer)
}

/// `decodeDocPagePointerRecord` — identical record layout.
pub fn decode_doc_page_pointer_record(bytes: &[u8], offset: usize, meta: &DocPointerMeta, pack_table: &[String]) -> Result<Pointer> {
    decode_doc_pointer_record(bytes, offset, meta, pack_table)
}

/// `parseDocPointerPage(buffer, options)`: `{ ...meta, start: 0, entries }`.
#[derive(Clone, Debug, PartialEq)]
pub struct DocPointerPage {
    pub meta: DocPointerMeta,
    pub start: usize,
    pub entries: Vec<Pointer>,
}

pub fn parse_doc_pointer_page_with(bytes: &[u8], options: DocPointerOptions, pack_table: &[String]) -> Result<DocPointerPage> {
    let meta = parse_doc_pointer_header_with(bytes, options)?;
    let mut entries = Vec::with_capacity(meta.count.min(1 << 24));
    for i in 0..meta.count {
        entries.push(decode_doc_pointer_record(bytes, meta.data_offset + i * meta.record_bytes, &meta, pack_table)?);
    }
    Ok(DocPointerPage { meta, start: 0, entries })
}

pub fn parse_doc_pointer_page(bytes: &[u8], pack_table: &[String]) -> Result<DocPointerPage> {
    parse_doc_pointer_page_with(bytes, DocPointerOptions::DOCS, pack_table)
}

/// `parseDocPagePointerHeader(buffer)`.
pub fn parse_doc_page_pointer_header(bytes: &[u8]) -> Result<DocPointerMeta> {
    parse_doc_pointer_header_with(bytes, DocPointerOptions::DOC_PAGES)
}

/// `parseDocPagePointerPage(buffer, { packTable })`.
pub fn parse_doc_page_pointer_page(bytes: &[u8], pack_table: &[String]) -> Result<DocPointerPage> {
    parse_doc_pointer_page_with(bytes, DocPointerOptions::DOC_PAGES, pack_table)
}
