//! Doc-values manifest — `parseDocValueManifest` (`rfdvm-v1`, from
//! `src/doc_value_manifest.js`) and the equivalent JSON manifest
//! (`doc-values/manifest.json.gz`, written by `writeDocValuePacks` in
//! `src/builder.js`), both decoded into one [`DocValuesMeta`].
//!
//! The two sources carry the same information except for checksums: the
//! JSON chunks hold `logicalLength` and an inline `checksum`, while binary
//! chunks hold a `checksumRow` into the uncompressed sidecar named by
//! `checksum_rows` (and no logical length).

use std::collections::HashMap;
use std::rc::Rc;

use serde_json::Value;

use super::binary::{check_magic, DOC_VALUE_MANIFEST_MAGIC};
use super::postings::data_view_error;
use super::read_utf8;
use crate::error::{Error, Result};
use crate::pointer::{Checksum, Pointer};
use crate::util::bytes::Reader;
use crate::util::json;

pub const DOC_VALUE_MANIFEST_FORMAT: &str = "rfdvm-v1";
const FORMAT_VERSION: u64 = 1;
pub const CHECKSUM_ROW_BYTES: usize = 32;

#[derive(Clone, Debug, PartialEq)]
pub struct DocValueChunkMeta {
    pub start: usize,
    pub count: usize,
    /// `pack`, `offset`, `length` (= `physicalLength`), and either
    /// `logicalLength` + `checksum` (JSON) or `checksum_row` (binary).
    pub pointer: Pointer,
    pub pack_index: Option<usize>,
    pub width: u64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub words: Option<Vec<u64>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DocValueFieldMeta {
    pub name: String,
    pub kind: String,
    pub type_: String,
    pub words: u64,
    pub chunks: Vec<Option<DocValueChunkMeta>>,
    /// Empty when the field has no lookup chunks.
    pub lookup_chunks: Vec<Option<DocValueChunkMeta>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChecksumRowsMeta {
    pub file: String,
    pub algorithm: String,
    pub row_bytes: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DocValuesMeta {
    pub storage: String,
    pub compression: String,
    pub format: String,
    /// `chunk_size` (0 when absent).
    pub chunk_size: usize,
    pub lookup_chunk_size: Option<usize>,
    /// Binary manifests record the corpus total; JSON ones do not.
    pub total: Option<u64>,
    /// Fields in manifest order.
    pub fields: Vec<DocValueFieldMeta>,
    /// Pack file names (`packs[i].file`).
    pub packs: Vec<String>,
    pub checksum_rows: Option<ChecksumRowsMeta>,
    index: HashMap<String, usize>,
}

impl DocValuesMeta {
    /// `docValues.fields[field]`
    pub fn field(&self, name: &str) -> Option<&DocValueFieldMeta> {
        self.index.get(name).map(|&i| &self.fields[i])
    }

    fn build_index(&mut self) {
        self.index = self.fields.iter().enumerate().map(|(i, field)| (field.name.clone(), i)).collect();
    }

    /// Insert-or-replace keeping the first position (object assignment).
    fn set_field(&mut self, field: DocValueFieldMeta) {
        match self.fields.iter_mut().find(|item| item.name == field.name) {
            Some(slot) => *slot = field,
            None => self.fields.push(field),
        }
    }
}

fn read_zigzag(r: &mut Reader) -> f64 {
    let encoded = r.varint() as f64;
    if encoded % 2.0 == 1.0 {
        -(encoded + 1.0) / 2.0
    } else {
        encoded / 2.0
    }
}

fn read_bitmap(r: &mut Reader, count: usize) -> Vec<bool> {
    let bits = (0..count).map(|i| (r.bytes.get(r.pos + (i >> 3)).copied().unwrap_or(0) >> (i & 7)) & 1 == 1).collect();
    r.pos += count.div_ceil(8);
    bits
}

fn view_f64(r: &Reader, pos: usize) -> Result<f64> {
    if pos.saturating_add(8) > r.bytes.len() {
        return Err(data_view_error());
    }
    Ok(f64::from_le_bytes(r.bytes[pos..pos + 8].try_into().expect("8 bytes")))
}

fn decode_chunk_section(r: &mut Reader, chunk_size: u64, total: u64, packs: &[String], next_row: &mut Option<u64>) -> Result<Vec<Option<DocValueChunkMeta>>> {
    let count = r.varint_usize();
    let mut chunks = Vec::with_capacity(count.min(1 << 20));
    let mut pack_index = 0f64;
    let mut offset = 0f64;
    for i in 0..count {
        pack_index += read_zigzag(r);
        offset += read_zigzag(r);
        let length = r.varint();
        let width = r.bytes.get(r.pos).copied().unwrap_or(0) as u64;
        r.pos += 1;
        let start = i as u64 * chunk_size;
        let index = if pack_index >= 0.0 { Some(pack_index as usize) } else { None };
        let pack = index.and_then(|i| packs.get(i)).cloned().unwrap_or_default();
        let mut pointer = Pointer::new(pack, offset.max(0.0) as u64, length);
        if let Some(row) = next_row.as_mut() {
            pointer.checksum_row = Some(*row);
            *row += 1;
        }
        chunks.push(Some(DocValueChunkMeta {
            start: start as usize,
            count: chunk_size.min(total.saturating_sub(start)) as usize,
            pointer,
            pack_index: index,
            width,
            min: None,
            max: None,
            words: None,
        }));
    }
    let flags = r.bytes.get(r.pos).copied().unwrap_or(0);
    r.pos += 1;
    if flags & 1 != 0 {
        let present = read_bitmap(r, count);
        for (i, present) in present.into_iter().enumerate() {
            if !present {
                continue;
            }
            let min = view_f64(r, r.pos)?;
            let max = view_f64(r, r.pos + 8)?;
            r.pos += 16;
            if let Some(chunk) = chunks[i].as_mut() {
                chunk.min = Some(min);
                chunk.max = Some(max);
            }
        }
    }
    if flags & 2 != 0 {
        let words_len = r.varint_usize();
        let present = read_bitmap(r, count);
        for (i, present) in present.into_iter().enumerate() {
            if !present {
                continue;
            }
            let mut words = Vec::with_capacity(words_len.min(1 << 16));
            for _ in 0..words_len {
                if r.pos + 4 > r.bytes.len() {
                    return Err(data_view_error());
                }
                words.push(r.u32_le() as u64);
            }
            if let Some(chunk) = chunks[i].as_mut() {
                chunk.words = Some(words);
            }
        }
    }
    Ok(chunks)
}

/// `parseDocValueManifest(buffer)` (binary `rfdvm-v1`).
pub fn parse_doc_value_manifest(bytes: &[u8]) -> Result<DocValuesMeta> {
    check_magic(bytes, DOC_VALUE_MANIFEST_MAGIC, "Unsupported Rangefind doc-value manifest")?;
    let mut r = Reader::at(bytes, DOC_VALUE_MANIFEST_MAGIC.len());
    let version = r.varint();
    if version != FORMAT_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind doc-value manifest version {version}")));
    }
    let chunk_size = r.varint();
    let lookup_chunk_size = r.varint();
    let total = r.varint();
    let storage = read_utf8(&mut r);
    let compression = read_utf8(&mut r);
    let format = read_utf8(&mut r);
    let checksums_file = read_utf8(&mut r);
    let checksum_algorithm = read_utf8(&mut r);
    let pack_count = r.varint_usize();
    let mut packs = Vec::with_capacity(pack_count.min(1 << 20));
    for _ in 0..pack_count {
        packs.push(read_utf8(&mut r));
    }
    let mut next_row = if checksums_file.is_empty() { None } else { Some(0u64) };
    let field_count = r.varint_usize();
    let mut meta = DocValuesMeta {
        storage,
        compression,
        format,
        chunk_size: chunk_size as usize,
        lookup_chunk_size: Some(lookup_chunk_size as usize),
        total: Some(total),
        fields: Vec::new(),
        packs: Vec::new(),
        checksum_rows: None,
        index: HashMap::new(),
    };
    for _ in 0..field_count {
        let name = read_utf8(&mut r);
        let kind = read_utf8(&mut r);
        let type_ = read_utf8(&mut r);
        let words = r.varint();
        let has_lookup = r.bytes.get(r.pos).copied() == Some(1);
        r.pos += 1;
        let chunks = decode_chunk_section(&mut r, chunk_size, total, &packs, &mut next_row)?;
        let lookup_chunks = if has_lookup { decode_chunk_section(&mut r, lookup_chunk_size, total, &packs, &mut next_row)? } else { Vec::new() };
        meta.set_field(DocValueFieldMeta { name, kind, type_, words, chunks, lookup_chunks });
    }
    if r.pos != bytes.len() {
        return Err(Error::new("Rangefind doc-value manifest has trailing bytes."));
    }
    meta.packs = packs;
    if !checksums_file.is_empty() {
        meta.checksum_rows = Some(ChecksumRowsMeta { file: checksums_file, algorithm: checksum_algorithm, row_bytes: CHECKSUM_ROW_BYTES });
    }
    meta.build_index();
    Ok(meta)
}

fn opt_num(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn uint(value: &Value) -> u64 {
    let n = json::to_number(value);
    if n.is_finite() && n > 0.0 {
        n as u64
    } else {
        0
    }
}

fn str_or_empty(value: &Value) -> String {
    value.as_str().unwrap_or("").to_string()
}

fn chunk_from_json(value: &Value) -> Option<DocValueChunkMeta> {
    let object = value.as_object()?;
    let get = |key: &str| object.get(key).unwrap_or(&json::NULL);
    let mut pointer = Pointer::new(str_or_empty(get("pack")), uint(get("offset")), uint(get("length")));
    if json::truthy(get("logicalLength")) {
        pointer.logical_length = Some(uint(get("logicalLength")));
    }
    let checksum = get("checksum");
    if let Some(value) = checksum.get("value").and_then(Value::as_str).filter(|value| !value.is_empty()) {
        let algorithm = checksum.get("algorithm").and_then(Value::as_str).filter(|a| !a.is_empty()).unwrap_or("sha256");
        pointer.checksum = Some(Checksum { algorithm: Rc::from(algorithm), value: Rc::from(value) });
    }
    if let Some(row) = get("checksumRow").as_u64() {
        pointer.checksum_row = Some(row);
    }
    Some(DocValueChunkMeta {
        start: uint(get("start")) as usize,
        count: uint(get("count")) as usize,
        pointer,
        pack_index: get("packIndex").as_u64().map(|i| i as usize),
        width: uint(get("width")),
        min: opt_num(get("min")),
        max: opt_num(get("max")),
        words: get("words").as_array().map(|words| words.iter().map(uint).collect()),
    })
}

fn chunks_from_json(value: &Value) -> Vec<Option<DocValueChunkMeta>> {
    value.as_array().map(|items| items.iter().map(chunk_from_json).collect()).unwrap_or_default()
}

/// Convert the JSON doc-values manifest (`doc-values/manifest.json.gz`, or
/// `manifest.doc_values`) into the same [`DocValuesMeta`] the binary parser
/// produces. `packs` may be pack objects, file names, or a count.
pub fn doc_values_meta_from_json(value: &Value) -> DocValuesMeta {
    let mut meta = DocValuesMeta {
        storage: str_or_empty(json::path(value, &["storage"])),
        compression: str_or_empty(json::path(value, &["compression"])),
        format: str_or_empty(json::path(value, &["format"])),
        chunk_size: uint(json::path(value, &["chunk_size"])) as usize,
        lookup_chunk_size: opt_num(json::path(value, &["lookup_chunk_size"])).map(|n| n.max(0.0) as usize),
        total: json::path(value, &["total"]).as_u64(),
        fields: Vec::new(),
        packs: json::path(value, &["packs"])
            .as_array()
            .map(|packs| {
                packs
                    .iter()
                    .map(|pack| match pack {
                        Value::String(file) => file.clone(),
                        other => str_or_empty(json::path(other, &["file"])),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        checksum_rows: None,
        index: HashMap::new(),
    };
    let rows = json::path(value, &["checksum_rows"]);
    if rows.get("file").and_then(Value::as_str).is_some_and(|file| !file.is_empty()) {
        meta.checksum_rows = Some(ChecksumRowsMeta {
            file: str_or_empty(json::path(rows, &["file"])),
            algorithm: str_or_empty(json::path(rows, &["algorithm"])),
            row_bytes: match uint(json::path(rows, &["rowBytes"])) {
                0 => CHECKSUM_ROW_BYTES,
                n => n as usize,
            },
        });
    }
    if let Some(fields) = json::path(value, &["fields"]).as_object() {
        for (key, field) in fields {
            meta.set_field(DocValueFieldMeta {
                // Runtime lookups go through the object key.
                name: key.clone(),
                kind: str_or_empty(json::path(field, &["kind"])),
                type_: str_or_empty(json::path(field, &["type"])),
                words: uint(json::path(field, &["words"])),
                chunks: chunks_from_json(json::path(field, &["chunks"])),
                lookup_chunks: chunks_from_json(json::path(field, &["lookup_chunks"])),
            });
        }
    }
    meta.build_index();
    meta
}
