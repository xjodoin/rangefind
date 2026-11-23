//! Query side of the IVF int8 vector index (`src/vector_index.js`):
//! root and cluster-page decoders plus the numeric kernels the runtime's
//! `vectorTopK` uses (`vectorFromValue`, `normalizeVector`,
//! `applyPermutation`, `dotInt8`) and its `normalizedQueryVector` guard.
//!
//! Numeric fidelity: JS `Float32Array` arithmetic loads f32 elements as
//! doubles, computes in f64 and rounds back to f32 only on store. The
//! kernels here follow that exactly, so scores are bit-identical.

use serde_json::Value;

use crate::error::{Error, Result};
use crate::pointer::{Checksum, Pointer};
use crate::typo::js_number;
use crate::util::bytes::{assert_magic, Reader};

pub const VECTOR_ROOT_FORMAT: &str = "rfvecroot-v1";
pub const VECTOR_CLUSTER_PAGE_FORMAT: &str = "rfveccluster-v1";
/// `RFVR`
pub const VECTOR_ROOT_MAGIC: [u8; 4] = [0x52, 0x46, 0x56, 0x52];
/// `RFVC`
pub const VECTOR_CLUSTER_PAGE_MAGIC: [u8; 4] = [0x52, 0x46, 0x56, 0x43];

const FORMAT_VERSION: u64 = 1;
/// Corrupt inputs can claim absurd element counts that JS would allocate
/// lazily; past this many elements the decoders fail instead.
const MAX_DECODED_ELEMENTS: u64 = 1 << 26;

/// One cluster page reference in the root (`clusters[c]` in JS:
/// `{ index, count, pack, offset, length, physicalLength, logicalLength,
/// checksum }`).
#[derive(Clone, Debug, PartialEq)]
pub struct VectorCluster {
    pub index: usize,
    pub count: u64,
    /// `pack`, `offset`, `length`, `logicalLength` (0 → `None`) and
    /// `checksum` (`None` when the stored value is empty; an empty stored
    /// algorithm becomes `"sha256"`). A pack index outside the pack table
    /// (JS `undefined`) yields an empty pack name.
    pub pointer: Pointer,
    pub physical_length: u64,
}

/// Fixed-width refine store layout: row `ordinal` lives in
/// `packs[ordinal / rows_per_pack]` at `(ordinal % rows_per_pack) * row_bytes`.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorRefine {
    pub row_bytes: u64,
    pub rows_per_pack: u64,
    pub packs: Vec<String>,
}

/// Decoded `rfvecroot-v1` (`parseVectorRoot`).
#[derive(Clone, Debug, PartialEq)]
pub struct VectorRoot {
    pub format: &'static str,
    pub page_format: &'static str,
    pub field: String,
    pub metric: String,
    pub dims: usize,
    pub coarse_dims: usize,
    pub total: u64,
    pub permutation: Vec<u16>,
    pub pack_table: Vec<String>,
    pub cluster_count: usize,
    pub centroid_scales: Vec<f32>,
    /// `cluster_count * dims` int8 codes, row-major by cluster.
    pub centroid_codes: Vec<i8>,
    pub clusters: Vec<VectorCluster>,
    pub refine: VectorRefine,
}

/// Decoded `rfveccluster-v1` page (`decodeVectorClusterPage`).
#[derive(Clone, Debug, PartialEq)]
pub struct VectorClusterPage {
    pub field: String,
    pub cluster_index: u64,
    pub coarse_dims: usize,
    pub count: usize,
    pub docs: Vec<u32>,
    pub ordinals: Vec<u32>,
    pub scales: Vec<f32>,
    /// `count * coarse_dims` int8 codes, row-major by candidate.
    pub codes: Vec<i8>,
}

fn dataview_oob() -> Error {
    Error::new("Offset is outside the bounds of the DataView")
}

fn check_count(count: u64) -> Result<()> {
    if count > MAX_DECODED_ELEMENTS {
        return Err(Error::new(format!("Invalid typed array length: {count}")));
    }
    Ok(())
}

/// `new Int8Array(buffer, pos, len)`: throws when the view would overrun.
fn int8_view(bytes: &[u8], pos: usize, len: u64) -> Result<Vec<i8>> {
    let end = (pos as u128) + (len as u128);
    if end > bytes.len() as u128 {
        return Err(Error::new(format!("Invalid typed array length: {len}")));
    }
    Ok(bytes[pos..end as usize].iter().map(|&b| b as i8).collect())
}

/// Reads `count` little-endian 4-byte values at `pos` through a DataView
/// spanning the whole buffer.
fn read_u32s(bytes: &[u8], pos: &mut usize, count: usize) -> Result<Vec<u32>> {
    let end = (*pos as u128) + (count as u128) * 4;
    if count > 0 && end > bytes.len() as u128 {
        return Err(dataview_oob());
    }
    let out = (0..count)
        .map(|i| {
            let at = *pos + i * 4;
            u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
        })
        .collect();
    *pos += count * 4;
    Ok(out)
}

fn read_f32s(bytes: &[u8], pos: &mut usize, count: usize) -> Result<Vec<f32>> {
    Ok(read_u32s(bytes, pos, count)?.into_iter().map(f32::from_bits).collect())
}

fn read_object_pointer(reader: &mut Reader, pack_table: &[String]) -> (Pointer, u64) {
    let pack_index = reader.varint();
    let pack = usize::try_from(pack_index)
        .ok()
        .and_then(|index| pack_table.get(index))
        .map(String::as_str)
        .unwrap_or("");
    let offset = reader.varint();
    let length = reader.varint();
    let physical_length = reader.varint();
    let logical_length = reader.varint();
    let algorithm = reader.utf8();
    let value = reader.utf8();
    let mut pointer = Pointer::new(pack, offset, length);
    pointer.logical_length = if logical_length == 0 { None } else { Some(logical_length) };
    pointer.checksum = if value.is_empty() {
        None
    } else {
        Some(Checksum {
            algorithm: if algorithm.is_empty() { "sha256".into() } else { algorithm.as_str().into() },
            value: value.as_str().into(),
        })
    };
    (pointer, physical_length)
}

/// `parseVectorRoot(buffer)`.
pub fn parse_vector_root(bytes: &[u8]) -> Result<VectorRoot> {
    assert_magic(bytes, &VECTOR_ROOT_MAGIC, "Unsupported Rangefind vector root")?;
    let mut reader = Reader::at(bytes, VECTOR_ROOT_MAGIC.len());
    let version = reader.varint();
    if version != FORMAT_VERSION {
        crate::bail!("Unsupported Rangefind vector root version {version}");
    }
    let field = reader.utf8();
    let metric = reader.utf8();
    let dims = reader.varint();
    let coarse_dims = reader.varint();
    let total = reader.varint();
    check_count(dims)?;
    let dims = dims as usize;
    let permutation = (0..dims).map(|_| reader.varint() as u16).collect();
    let pack_count = reader.varint();
    check_count(pack_count)?;
    let pack_table: Vec<String> = (0..pack_count).map(|_| reader.utf8()).collect();
    let cluster_count = reader.varint();
    check_count(cluster_count)?;
    let cluster_count = cluster_count as usize;
    let mut pos = reader.pos;
    let centroid_scales = read_f32s(bytes, &mut pos, cluster_count)?;
    let code_len = (cluster_count as u64) * (dims as u64);
    let centroid_codes = int8_view(bytes, pos, code_len)?;
    pos += code_len as usize;
    reader.pos = pos;
    let mut clusters = Vec::with_capacity(cluster_count);
    for index in 0..cluster_count {
        let count = reader.varint();
        let (pointer, physical_length) = read_object_pointer(&mut reader, &pack_table);
        clusters.push(VectorCluster { index, count, pointer, physical_length });
    }
    let row_bytes = reader.varint();
    let rows_per_pack = reader.varint();
    let refine_pack_count = reader.varint();
    check_count(refine_pack_count)?;
    let packs = (0..refine_pack_count).map(|_| reader.utf8()).collect();
    if reader.pos != bytes.len() {
        crate::bail!("Rangefind vector root has trailing bytes.");
    }
    Ok(VectorRoot {
        format: VECTOR_ROOT_FORMAT,
        page_format: VECTOR_CLUSTER_PAGE_FORMAT,
        field,
        metric,
        dims,
        coarse_dims: coarse_dims as usize,
        total,
        permutation,
        pack_table,
        cluster_count,
        centroid_scales,
        centroid_codes,
        clusters,
        refine: VectorRefine { row_bytes, rows_per_pack, packs },
    })
}

/// `decodeVectorClusterPage(buffer, { name })`. An empty or absent
/// `expected_name` skips the field check, like the JS truthiness test.
pub fn decode_vector_cluster_page(bytes: &[u8], expected_name: Option<&str>) -> Result<VectorClusterPage> {
    assert_magic(bytes, &VECTOR_CLUSTER_PAGE_MAGIC, "Unsupported Rangefind vector cluster page")?;
    let mut reader = Reader::at(bytes, VECTOR_CLUSTER_PAGE_MAGIC.len());
    let version = reader.varint();
    if version != FORMAT_VERSION {
        crate::bail!("Unsupported Rangefind vector cluster page version {version}");
    }
    let field = reader.utf8();
    if let Some(name) = expected_name {
        if !name.is_empty() && name != field {
            crate::bail!("Rangefind vector cluster page field mismatch: {field}");
        }
    }
    let cluster_index = reader.varint();
    let coarse_dims = reader.varint();
    let count = reader.varint();
    check_count(count)?;
    let count = count as usize;
    let mut pos = reader.pos;
    let docs = read_u32s(bytes, &mut pos, count)?;
    let ordinals = read_u32s(bytes, &mut pos, count)?;
    let scales = read_f32s(bytes, &mut pos, count)?;
    let code_len = (count as u128) * (coarse_dims as u128);
    if code_len > u64::MAX as u128 {
        return Err(Error::new(format!("Invalid typed array length: {code_len}")));
    }
    let codes = int8_view(bytes, pos, code_len as u64)?;
    pos += code_len as usize;
    if pos != bytes.len() {
        crate::bail!("Rangefind vector cluster page has trailing bytes.");
    }
    Ok(VectorClusterPage { field, cluster_index, coarse_dims: coarse_dims as usize, count, docs, ordinals, scales, codes })
}

/// `vectorFromValue(value, dims)`: a JSON array of `dims` numbers (each
/// through JS `Number()`), or a base64 little-endian float32 buffer decoded
/// with Node's lenient `Buffer.from(value, "base64")`. Any non-finite
/// component rejects the vector.
pub fn vector_from_value(value: &Value, dims: usize) -> Option<Vec<f32>> {
    let out: Vec<f32> = match value {
        Value::Null => return None,
        Value::Array(items) => {
            if items.len() != dims {
                return None;
            }
            items.iter().map(|item| js_number(item) as f32).collect()
        }
        Value::String(text) => {
            if text.is_empty() {
                return None;
            }
            let bytes = node_base64_decode(text);
            if bytes.len() != dims * 4 {
                return None;
            }
            bytes.chunks_exact(4).map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]])).collect()
        }
        _ => return None,
    };
    if out.iter().any(|v| !v.is_finite()) {
        return None;
    }
    Some(out)
}

/// `Buffer.from(text, "base64")` on Node: each UTF-16 unit is narrowed to
/// its low byte, characters outside the base64/base64url alphabets are
/// skipped, the first `=` ends the input, and a trailing group of 2 or 3
/// symbols yields 1 or 2 bytes.
pub fn node_base64_decode(text: &str) -> Vec<u8> {
    let mut symbols = Vec::with_capacity(text.len());
    for unit in text.encode_utf16() {
        let byte = (unit & 0xff) as u8;
        let symbol = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            b'=' => break,
            _ => continue,
        };
        symbols.push(symbol as u32);
    }
    let mut out = Vec::with_capacity(symbols.len() / 4 * 3 + 2);
    let mut groups = symbols.chunks_exact(4);
    for group in &mut groups {
        let n = (group[0] << 18) | (group[1] << 12) | (group[2] << 6) | group[3];
        out.extend_from_slice(&[(n >> 16) as u8, (n >> 8) as u8, n as u8]);
    }
    let rest = groups.remainder();
    if rest.len() >= 2 {
        let n = (rest[0] << 18) | (rest[1] << 12) | (rest.get(2).copied().unwrap_or(0) << 6);
        out.push((n >> 16) as u8);
        if rest.len() == 3 {
            out.push((n >> 8) as u8);
        }
    }
    out
}

/// `normalizeVector(vector)`: L2-normalizes in place (f64 accumulation,
/// f32 stores). Returns `None` — leaving the input untouched — when the
/// squared norm is not positive.
pub fn normalize_vector(mut vector: Vec<f32>) -> Option<Vec<f32>> {
    let mut norm = 0f64;
    for &v in &vector {
        let v = v as f64;
        norm += v * v;
    }
    if !(norm > 0.0) {
        return None;
    }
    let inverse = 1.0 / norm.sqrt();
    for v in vector.iter_mut() {
        *v = ((*v as f64) * inverse) as f32;
    }
    Some(vector)
}

/// `applyPermutation(vector, offset, permutation, target)`:
/// `target[d] = vector[offset + permutation[d]]`. Reads past `vector` store
/// NaN and writes past `target` are dropped, as with typed arrays.
pub fn apply_permutation(vector: &[f32], offset: usize, permutation: &[u16], target: &mut [f32]) {
    for (d, &p) in permutation.iter().enumerate() {
        if let Some(slot) = target.get_mut(d) {
            *slot = vector.get(offset + p as usize).copied().unwrap_or(f32::NAN);
        }
    }
}

/// `dotInt8(query, codes, offset, dims, scale)`: f64 sum of
/// `query[i] * codes[offset + i]`, times `scale`. Out-of-range reads make
/// the result NaN, like `undefined` in JS arithmetic.
pub fn dot_int8(query: &[f32], codes: &[i8], offset: usize, dims: usize, scale: f64) -> f64 {
    let mut sum = 0f64;
    for i in 0..dims {
        match (query.get(i), codes.get(offset + i)) {
            (Some(&q), Some(&c)) => sum += (q as f64) * (c as f64),
            _ => sum = f64::NAN,
        }
    }
    sum * scale
}

/// `dotInt8` over raw bytes viewed as `Int8Array` (refine-store rows).
pub fn dot_int8_bytes(query: &[f32], bytes: &[u8], offset: usize, dims: usize, scale: f64) -> f64 {
    let mut sum = 0f64;
    for i in 0..dims {
        match (query.get(i), bytes.get(offset + i)) {
            (Some(&q), Some(&c)) => sum += (q as f64) * ((c as i8) as f64),
            _ => sum = f64::NAN,
        }
    }
    sum * scale
}

/// `normalizedQueryVector(value, dims)` from `runtime.js` (JSON input form).
pub fn normalized_query_vector(value: &Value, dims: usize) -> Result<Vec<f32>> {
    vector_from_value(value, dims)
        .and_then(normalize_vector)
        .ok_or_else(|| Error::new(format!("Rangefind: vector queries need {dims} finite dimensions.")))
}

/// `normalizedQueryVector` for a `Float32Array` query: copied when the
/// length matches, then normalized.
pub fn normalized_query_vector_f32(values: &[f32], dims: usize) -> Result<Vec<f32>> {
    (if values.len() == dims { Some(values.to_vec()) } else { None })
        .and_then(normalize_vector)
        .ok_or_else(|| Error::new(format!("Rangefind: vector queries need {dims} finite dimensions.")))
}
