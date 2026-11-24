//! Codec parity: every binary format decoded by the Rust `codec` module must
//! produce exactly what the JavaScript decoders produce for the same bytes.
//!
//! Fixtures come from `node rust/tools/fixtures/codec.mjs` (run from the
//! repo root); they are written to `rust/target/fixtures/codec/`.

#![cfg(feature = "codec")]

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;

use rangefind::codec::directory::{compare_directory_keys, directory_might_contain, find_directory_page_index, parse_directory_page, parse_directory_root};
use rangefind::codec::doc_pages::decode_doc_page_columns;
use rangefind::codec::doc_pointers::{decode_doc_pointer_record, parse_doc_page_pointer_page, parse_doc_pointer_page, DocPointerMeta, DocPointerPage};
use rangefind::codec::doc_value_manifest::{doc_values_meta_from_json, parse_doc_value_manifest, DocValueChunkMeta, DocValuesMeta};
use rangefind::codec::doc_value_tree::{decode_doc_value_sort_page, parse_doc_value_sort_directory};
use rangefind::codec::doc_values::{parse_codes, parse_doc_value_chunk, parse_facet_dictionary, DocValue};
use rangefind::codec::filter_bitmaps::parse_filter_bitmap;
use rangefind::codec::http_ranges::{parse_multipart_byte_ranges, select_multipart_byte_ranges};
use rangefind::codec::postings::{
    decode_posting_block, decode_posting_bytes, decode_postings, lookup_decoded_posting_rows, lookup_posting_block, lookup_posting_bytes, parse_posting_segment,
    BlockCodec, BlockFilters, FilterSummary, PostingManifest, TermEntry,
};
use rangefind::codec::query_bundle::{parse_query_bundle, BundleFilterValue};
use rangefind::codec::segment::{decode_segment_rows, parse_segment_terms};
use rangefind::codec::text_routing::{floor_directory_page_index, parse_text_routing_segment};
use rangefind::codec::verify_pointer;
use rangefind::pointer::{Checksum, Pointer};
use rangefind::util::bytes::Bytes;
use rangefind::util::json::num;
use serde_json::{json, Map, Value};

// ---------------------------------------------------------------------------
// fixture plumbing
// ---------------------------------------------------------------------------

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/fixtures/codec").join(format!("{name}.json"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!("missing codec fixture {}: run `node rust/tools/fixtures/codec.mjs` from the repo root first", path.display())
    });
    serde_json::from_str(&text).expect("fixture JSON")
}

fn cases(name: &str) -> Vec<Value> {
    match fixture(name) {
        Value::Array(items) => items,
        _ => panic!("fixture {name} is not an array"),
    }
}

fn b64(text: &Value) -> Vec<u8> {
    let text = text.as_str().expect("base64 string");
    let mut table = [255u8; 256];
    for (i, c) in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".iter().enumerate() {
        table[*c as usize] = i as u8;
    }
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let mut buffer = 0u32;
    let mut bits = 0;
    for c in text.bytes() {
        if c == b'=' {
            break;
        }
        let value = table[c as usize];
        assert!(value != 255, "bad base64");
        buffer = (buffer << 6) | value as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    out
}

fn b64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = (chunk[0] as u32) << 16 | (*chunk.get(1).unwrap_or(&0) as u32) << 8 | *chunk.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(TABLE[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// JS numbers are doubles: re-emit every number the way the Rust side does.
fn norm(value: &Value) -> Value {
    match value {
        Value::Number(number) => num(number.as_f64().unwrap()),
        Value::Array(items) => Value::Array(items.iter().map(norm).collect()),
        Value::Object(map) => Value::Object(map.iter().map(|(k, v)| (k.clone(), norm(v))).collect()),
        other => other.clone(),
    }
}

#[track_caller]
fn assert_json(actual: &Value, expected: &Value, context: &str) {
    let actual = norm(actual);
    let expected = norm(expected);
    if actual != expected {
        let a = serde_json::to_string(&actual).unwrap();
        let e = serde_json::to_string(&expected).unwrap();
        let at = a.bytes().zip(e.bytes()).position(|(x, y)| x != y).unwrap_or(a.len().min(e.len()));
        let from = at.saturating_sub(200);
        panic!(
            "{context}: mismatch at byte {at}\n  rust: …{}\n  js:   …{}",
            &a[from.min(a.len())..(at + 300).min(a.len())],
            &e[from.min(e.len())..(at + 300).min(e.len())]
        );
    }
}

fn str_of(value: &Value) -> &str {
    value.as_str().expect("string")
}

fn usize_of(value: &Value) -> usize {
    value.as_f64().expect("number") as usize
}

fn units_of(value: &Value) -> Vec<u16> {
    value.as_array().expect("units").iter().map(|unit| unit.as_u64().unwrap() as u16).collect()
}

fn strings(value: &Value) -> Vec<String> {
    value.as_array().map(|items| items.iter().map(|item| item.as_str().unwrap_or("").to_string()).collect()).unwrap_or_default()
}

fn int_set(value: &Value) -> HashSet<i32> {
    value.as_array().unwrap().iter().map(|doc| doc.as_i64().unwrap() as i32).collect()
}

fn rows_json(rows: &[i32]) -> Value {
    Value::Array(rows.iter().map(|v| json!(*v)).collect())
}

fn opt_f64(value: Option<f64>) -> Value {
    value.map(num).unwrap_or(Value::Null)
}

fn pointer_json(pointer: &Pointer) -> Value {
    json!({
        "pack": &*pointer.pack,
        "offset": pointer.offset,
        "length": pointer.length,
        "physicalLength": pointer.length,
        "logicalLength": pointer.logical_length,
        "checksum": pointer.checksum.as_ref().map(|c| json!({ "algorithm": &*c.algorithm, "value": &*c.value })),
    })
}

fn filters_json(filters: &BlockFilters) -> Value {
    let mut map = Map::new();
    for (name, summary) in filters.entries() {
        let value = match summary {
            FilterSummary::Facet { words } => json!({ "words": words }),
            FilterSummary::Number { min, max } => json!({ "min": opt_f64(*min), "max": opt_f64(*max) }),
            FilterSummary::Boolean { min, max } => json!({ "min": min, "max": max }),
        };
        map.insert(name.to_string(), value);
    }
    Value::Object(map)
}

// ---------------------------------------------------------------------------
// postings
// ---------------------------------------------------------------------------

fn entry_json(entry: &TermEntry) -> Value {
    let bound = |b: &rangefind::codec::postings::DocRangeBound| json!({ "index": b.index, "maxImpact": b.max_impact, "quantizedMaxImpact": b.quantized_max_impact });
    json!({
        "format": entry.format(),
        "df": entry.df,
        "count": entry.count,
        "offset": entry.offset,
        "byteLength": entry.byte_length,
        "blockSize": entry.block_size,
        "external": entry.external,
        "blocks": entry.blocks.iter().map(|block| json!({
            "offset": block.offset,
            "rowCount": block.row_count,
            "maxImpact": block.max_impact,
            "maxImpactDoc": block.max_impact_doc,
            "docMin": block.doc_min,
            "docMax": block.doc_max,
            "codec": block.codec.name(),
            "filters": filters_json(&block.filters),
            "range": block.range.as_ref().map(pointer_json),
            "docRanges": block.doc_ranges.as_ref().map(|ranges| ranges.iter().map(bound).collect::<Vec<_>>()),
        })).collect::<Vec<_>>(),
        "superblocks": entry.superblocks.iter().map(|s| json!({
            "firstBlock": s.first_block,
            "blockCount": s.block_count,
            "rowCount": s.row_count,
            "maxImpact": s.max_impact,
            "maxImpactDoc": s.max_impact_doc,
            "docMin": s.doc_min,
            "docMax": s.doc_max,
            "filters": filters_json(&s.filters),
        })).collect::<Vec<_>>(),
        "docRanges": entry.doc_ranges.as_ref().map(|d| json!({
            "rangeSize": d.range_size,
            "scale": d.scale,
            "rangeCount": d.range_count,
            "quantizationBits": d.quantization_bits,
            "ranges": d.ranges.iter().map(bound).collect::<Vec<_>>(),
        })),
        "impactTiers": entry.impact_tiers.as_ref().map(|t| json!({
            "blocks": t.blocks,
            "tiers": t.tiers.iter().map(|tier| json!({ "maxImpact": tier.max_impact, "first": tier.first, "count": tier.count })).collect::<Vec<_>>(),
        })),
    })
}

#[test]
fn posting_segments_match_js() {
    let mut terms_checked = 0;
    let mut lookups = 0;
    let mut external_blocks = 0;
    let mut codecs = HashSet::new();
    for case in cases("postings") {
        let name = str_of(&case["name"]);
        let manifest = PostingManifest::from_manifest(&case["manifest"]);
        let segment = parse_posting_segment(Bytes::new(b64(&case["segment"])), &manifest).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(segment.data_start, usize_of(&case["dataStart"]), "{name} dataStart");
        let expected_terms = case["terms"].as_array().unwrap();
        assert_eq!(segment.terms.len(), expected_terms.len(), "{name} term count");
        for (index, item) in expected_terms.iter().enumerate() {
            let term = str_of(&item["term"]);
            let context = format!("{name} term {term}");
            assert_eq!(segment.terms[index].0, term, "{context} order");
            let entry = segment.term(term).expect("term lookup");
            assert_json(&entry_json(&entry), &item["entry"], &context);
            for block in &entry.blocks {
                codecs.insert(block.codec);
            }
            if !entry.external {
                // Blocks first (fills the per-block cache), then the whole list.
                for (block_index, rows) in item["blocks"].as_array().unwrap().iter().enumerate() {
                    let decoded = decode_posting_block(&segment, &entry, block_index).unwrap();
                    assert_json(&rows_json(&decoded), rows, &format!("{context} block {block_index}"));
                    let again = decode_posting_block(&segment, &entry, block_index).unwrap();
                    assert!(Rc::ptr_eq(&decoded, &again), "{context} block cache");
                }
                let postings = decode_postings(&segment, &entry).unwrap();
                assert_json(&rows_json(&postings), &item["postings"], &format!("{context} postings"));
                assert!(Rc::ptr_eq(&postings, &decode_postings(&segment, &entry).unwrap()));
                for lookup in item["lookups"].as_array().unwrap() {
                    let block = usize_of(&lookup["block"]);
                    let (rows, scanned) = lookup_posting_block(&segment, &entry, block, &int_set(&lookup["targets"])).unwrap();
                    assert_json(&rows_json(&rows), &lookup["rows"], &format!("{context} lookup block {block}"));
                    assert_eq!(scanned, usize_of(&lookup["scanned"]), "{context} lookup scanned");
                    lookups += 1;
                }
                let out = &item["outOfRange"];
                assert_json(&rows_json(&decode_posting_block(&segment, &entry, entry.blocks.len()).unwrap()), &out["decoded"], &context);
                let probe: HashSet<i32> = [1].into_iter().collect();
                assert_json(&rows_json(&lookup_posting_block(&segment, &entry, entry.blocks.len() + 3, &probe).unwrap().0), &out["lookup"], &context);
            } else {
                let error = decode_postings(&segment, &entry).unwrap_err();
                assert_eq!(error.message(), str_of(&item["decodeError"]), "{context}");
                for external in item["external"].as_array().unwrap() {
                    let block = usize_of(&external["block"]);
                    let codec = BlockCodec::from_name(str_of(&external["codec"]));
                    assert_eq!(codec, entry.blocks[block].codec);
                    let bytes = b64(&external["bytes"]);
                    let decoded = decode_posting_bytes(&bytes, codec);
                    assert_json(&rows_json(&decoded), &external["decoded"], &format!("{context} external {block}"));
                    for lookup in external["lookups"].as_array().unwrap() {
                        let targets = int_set(&lookup["targets"]);
                        let (rows, scanned) = lookup_posting_bytes(&bytes, &targets, codec);
                        assert_json(&rows_json(&rows), &lookup["rows"], &format!("{context} external lookup {block}"));
                        assert_eq!(scanned, usize_of(&lookup["scanned"]), "{context} external scanned");
                        let (rows, scanned) = lookup_decoded_posting_rows(&decoded, &targets);
                        assert_json(&rows_json(&rows), &lookup["decodedRows"], &format!("{context} decoded lookup {block}"));
                        assert_eq!(scanned, usize_of(&lookup["decodedScanned"]));
                        lookups += 2;
                    }
                    external_blocks += 1;
                }
            }
            terms_checked += 1;
        }
    }
    assert_eq!(codecs.len(), 4, "every block codec exercised");
    assert!(external_blocks > 50 && lookups > 1000, "coverage: {external_blocks} external blocks, {lookups} lookups");
    eprintln!("postings: {terms_checked} terms, {lookups} lookups, {external_blocks} external blocks");
}

// ---------------------------------------------------------------------------
// directories / text routing
// ---------------------------------------------------------------------------

#[test]
fn directories_match_js() {
    let fixture = fixture("directories");
    let mut probes = 0;
    for case in fixture["cases"].as_array().unwrap() {
        let name = str_of(&case["name"]);
        let pack_table = strings(&case["packTable"]);
        let root = parse_directory_root(&b64(&case["root"])).unwrap();
        let expected_root = &case["expectedRoot"];
        let root_json = json!({
            "version": root.version,
            "entries": root.entries,
            "pages": root.pages.iter().map(|p| json!({ "first": p.first, "last": p.last, "file": p.file, "count": p.count, "rawBytes": p.raw_bytes })).collect::<Vec<_>>(),
            "bloom": { "bits": root.bloom.bits, "hashes": root.bloom.hashes, "bytes": b64_encode(&root.bloom.bytes) },
        });
        assert_json(&root_json, expected_root, name);
        let mut pages = Vec::new();
        for (page_index, page) in case["pages"].as_array().unwrap().iter().enumerate() {
            let parsed = parse_directory_page(&b64(&page["bytes"]), &pack_table).unwrap();
            let expected = page["entries"].as_array().unwrap();
            assert_eq!(parsed.entries.len(), expected.len(), "{name} page {page_index}");
            for (i, item) in expected.iter().enumerate() {
                let units = units_of(&item[0]);
                assert_eq!(parsed.keys[i].units16(), units, "{name} key {i}");
                assert_eq!(parsed.entries[i].0, String::from_utf16_lossy(&units));
                assert_json(&pointer_json(&parsed.entries[i].1), &item[1], &format!("{name} entry {i}"));
                match String::from_utf16(&units) {
                    Ok(key) => assert_eq!(parsed.get(&key), Some(&parsed.entries[i].1), "{name} get {key}"),
                    Err(_) => assert!(parsed.get(&parsed.entries[i].0).is_none() || parsed.keys.iter().any(|k| k.well_formed() && k.text == parsed.entries[i].0)),
                }
            }
            pages.push(parsed);
        }
        for probe in case["probes"].as_array().unwrap() {
            let key = str_of(&probe["key"]);
            assert_eq!(directory_might_contain(&root, key), probe["mightContain"].as_bool().unwrap(), "{name} bloom {key:?}");
            assert_eq!(find_directory_page_index(&root, key), probe["page"].as_u64().map(|p| p as usize), "{name} find {key:?}");
            assert_eq!(floor_directory_page_index(&root, key), probe["floorPage"].as_i64().unwrap(), "{name} floor page {key:?}");
            for (page, expected) in pages.iter().zip(probe["floorKeys"].as_array().unwrap()) {
                assert_eq!(page.floor_key_index(key), expected.as_i64().unwrap(), "{name} floor key {key:?}");
            }
            probes += 1;
        }
    }
    for pair in fixture["pairs"].as_array().unwrap() {
        assert_eq!(compare_directory_keys(str_of(&pair[0]), str_of(&pair[1])), pair[2].as_i64().unwrap() as i32);
    }
    assert!(probes > 500);
    eprintln!("directories: {probes} probes");
}

#[test]
fn text_routing_matches_js() {
    let mut probes = 0;
    for case in cases("text_routing") {
        let name = str_of(&case["name"]);
        let segment = parse_text_routing_segment(&b64(&case["bytes"])).unwrap();
        let expected = case["terms"].as_array().unwrap();
        assert_eq!(segment.terms.len(), expected.len(), "{name}");
        for ((key, shards), item) in segment.terms.iter().zip(expected) {
            assert_eq!(key.units16(), units_of(&item[0]), "{name}");
            assert_json(&json!(shards), &item[1], name);
        }
        for probe in case["probes"].as_array().unwrap() {
            let key = str_of(&probe["key"]);
            assert_json(&segment.get(key).map(|s| json!(s)).unwrap_or(Value::Null), &probe["shards"], &format!("{name} get {key:?}"));
            let keys: Vec<_> = segment.terms.iter().map(|(k, _)| k.clone()).collect();
            assert_eq!(rangefind::codec::text_routing::floor_sorted_key_index(&keys, key), probe["floor"].as_i64().unwrap(), "{name} floor {key:?}");
            probes += 1;
        }
    }
    assert!(probes > 300);
}

// ---------------------------------------------------------------------------
// doc pointers / doc pages
// ---------------------------------------------------------------------------

fn pointer_page_json(page: &DocPointerPage) -> Value {
    let meta = &page.meta;
    json!({
        "format": meta.format,
        "version": meta.version,
        "count": meta.count,
        "checksum_bytes": meta.checksum_bytes,
        "recordBytes": meta.record_bytes,
        "dataOffset": meta.data_offset,
        "widths": { "pack": meta.widths.pack, "offset": meta.widths.offset, "length": meta.widths.length, "logicalLength": meta.widths.logical_length },
        "start": page.start,
        "entries": page.entries.iter().map(pointer_json).collect::<Vec<_>>(),
    })
}

#[test]
fn doc_pointers_match_js() {
    let mut records = 0;
    for case in cases("doc_pointers") {
        let name = str_of(&case["name"]);
        let bytes = b64(&case["bytes"]);
        let packs = strings(&case["packTable"]);
        let pages = str_of(&case["flavor"]) == "pages";
        let parse = |table: &[String]| if pages { parse_doc_page_pointer_page(&bytes, table) } else { parse_doc_pointer_page(&bytes, table) };
        let page = parse(&packs).unwrap();
        assert_json(&pointer_page_json(&page), &case["expected"], name);
        let meta = DocPointerMeta::from_json(&case["meta"]);
        for (i, expected) in case["records"].as_array().unwrap().iter().enumerate() {
            let pointer = decode_doc_pointer_record(&bytes, meta.data_offset + i * meta.record_bytes, &meta, &packs).unwrap();
            assert_json(&pointer_json(&pointer), expected, &format!("{name} record {i}"));
            records += 1;
        }
        if let Some(error) = case["shortPackTableError"].as_str() {
            assert_eq!(parse(&packs[..1]).unwrap_err().message(), error, "{name}");
        }
    }
    assert!(records > 100);
}

#[test]
fn doc_pages_match_js() {
    let mut docs = 0;
    for case in cases("doc_pages") {
        let name = str_of(&case["name"]);
        let fields = case["fields"].as_array().unwrap();
        let decoded = decode_doc_page_columns(&b64(&case["bytes"]), fields, usize_of(&case["start"])).unwrap_or_else(|e| panic!("{name}: {e}"));
        let expected: Value = serde_json::from_str(str_of(&case["expected"])).unwrap();
        // Key order matters: compare serialized forms.
        let actual = serde_json::to_string(&norm(&Value::Array(decoded.clone()))).unwrap();
        let wanted = serde_json::to_string(&norm(&expected)).unwrap();
        assert_eq!(actual, wanted, "{name}");
        docs += decoded.len();
    }
    assert!(docs > 200);
}

// ---------------------------------------------------------------------------
// doc values, codes, facet dictionaries, sort trees
// ---------------------------------------------------------------------------

fn doc_value_json(value: &DocValue) -> Value {
    value.to_json()
}

#[test]
fn doc_value_chunks_match_js() {
    let mut values = 0;
    for case in cases("doc_values") {
        let name = str_of(&case["name"]);
        let chunk = parse_doc_value_chunk(&b64(&case["bytes"])).unwrap_or_else(|e| panic!("{name}: {e}"));
        let actual = json!({
            "name": chunk.name,
            "kind": chunk.kind,
            "type": chunk.type_,
            "width": chunk.width,
            "words": chunk.words,
            "encoding": chunk.encoding,
            "start": chunk.start,
            "count": chunk.count,
            "min": num(chunk.min),
            "values": chunk.values.iter().map(doc_value_json).collect::<Vec<_>>(),
        });
        assert_json(&actual, &case["expected"], name);
        values += chunk.values.len();
    }
    assert!(values > 5000);
}

#[test]
fn facet_matching_follows_runtime() {
    use rangefind::codec::doc_values::FacetValue;
    let selected: HashSet<u32> = [3, 33, 63].into_iter().collect();
    assert!(DocValue::Facet(FacetValue::Codes(vec![1, 33])).facet_matches(&selected));
    assert!(!DocValue::Facet(FacetValue::Codes(vec![1, 2])).facet_matches(&selected));
    assert!(DocValue::Facet(FacetValue::Words(vec![0, 1 << 31])).facet_matches(&selected));
    assert!(!DocValue::Facet(FacetValue::Words(vec![1 << 2])).facet_matches(&selected));
    assert!(DocValue::Number(3.0).facet_matches(&selected));
    assert!(!DocValue::Missing.facet_matches(&selected));
    assert!(DocValue::Missing.facet_matches(&HashSet::new()));
}

#[test]
fn codes_match_js() {
    for case in cases("codes") {
        let name = str_of(&case["name"]);
        let codes = parse_codes(&b64(&case["bytes"])).unwrap();
        let actual = json!({
            "columns": codes.columns.iter().map(|(name, values)| json!([name, values.iter().map(doc_value_json).collect::<Vec<_>>()])).collect::<Vec<_>>(),
            "meta": codes.meta.as_ref().map(|meta| json!({
                "version": meta.version,
                "total": meta.total,
                "fields": meta.fields.iter().map(|f| json!({ "name": f.name, "kind": f.kind, "type": f.type_, "width": f.width, "words": f.words, "min": num(f.min) })).collect::<Vec<_>>(),
            })),
        });
        assert_json(&actual, &case["expected"], name);
        if let Some((field, values)) = codes.columns.first() {
            if !values.is_empty() {
                assert_eq!(codes.value(field, 0), values[0]);
            }
            assert_eq!(codes.value(field, values.len() + 5), DocValue::Missing);
        }
    }
}

#[test]
fn facet_dictionaries_match_js() {
    for case in cases("facet_dictionaries") {
        let parsed = parse_facet_dictionary(&b64(&case["bytes"])).unwrap();
        assert_json(&Value::Array(parsed), &case["expected"], str_of(&case["name"]));
    }
}

#[test]
fn sort_trees_match_js() {
    let mut rows = 0;
    for case in cases("sort_pages") {
        let name = str_of(&case["name"]);
        let page = decode_doc_value_sort_page(&b64(&case["bytes"]), case["expectedName"].as_str()).unwrap_or_else(|e| panic!("{name}: {e}"));
        let actual = json!({
            "name": page.field.name,
            "kind": page.field.kind,
            "type": page.field.type_,
            "rankStart": page.rank_start,
            "count": page.count,
            "rows": page.rows.iter().map(|row| json!({ "doc": row.doc, "value": doc_value_json(&row.value), "sortValue": num(row.sort_value), "rank": row.rank })).collect::<Vec<_>>(),
        });
        assert_json(&actual, &case["expected"], name);
        rows += page.rows.len();
    }
    for case in cases("sort_directories") {
        let name = str_of(&case["name"]);
        let directory = parse_doc_value_sort_directory(&b64(&case["bytes"])).unwrap();
        let field = |f: &rangefind::codec::doc_value_tree::SortField| json!({ "name": f.name, "kind": f.kind, "type": f.type_ });
        let actual = json!({
            "format": directory.format(),
            "pageFormat": directory.page_format(),
            "field": field(&directory.field),
            "pageSize": directory.page_size,
            "total": directory.total,
            "packTable": directory.pack_table,
            "summaryFields": directory.summary_fields.iter().map(field).collect::<Vec<_>>(),
            "pages": directory.pages.iter().map(|page| json!({
                "index": page.index,
                "rankStart": page.rank_start,
                "count": page.count,
                "min": num(page.min),
                "max": num(page.max),
                "pack": &*page.pointer.pack,
                "offset": page.pointer.offset,
                "length": page.pointer.length,
                "physicalLength": page.physical_length,
                "logicalLength": page.pointer.logical_length,
                "checksum": page.pointer.checksum.as_ref().map(|c| json!({ "algorithm": &*c.algorithm, "value": &*c.value })),
                "summaries": Value::Object(page.summaries.iter().map(|(n, s)| (n.clone(), json!({ "min": opt_f64(s.min), "max": opt_f64(s.max) }))).collect()),
            })).collect::<Vec<_>>(),
        });
        assert_json(&actual, &case["expected"], name);
    }
    assert!(rows > 1000);
}

// ---------------------------------------------------------------------------
// doc-values manifest
// ---------------------------------------------------------------------------

fn dv_chunk_json(chunk: &Option<DocValueChunkMeta>) -> Value {
    let Some(chunk) = chunk else { return Value::Null };
    let mut value = json!({
        "start": chunk.start,
        "count": chunk.count,
        "pack": &*chunk.pointer.pack,
        "packIndex": chunk.pack_index,
        "offset": chunk.pointer.offset,
        "length": chunk.pointer.length,
        "width": chunk.width,
        "min": opt_f64(chunk.min),
        "max": opt_f64(chunk.max),
        "words": chunk.words,
    });
    if let Some(row) = chunk.pointer.checksum_row {
        value["checksumRow"] = json!(row);
    }
    value
}

fn dv_meta_json(meta: &DocValuesMeta) -> Value {
    let mut fields = Map::new();
    for field in &meta.fields {
        let mut value = json!({
            "name": field.name,
            "kind": field.kind,
            "type": field.type_,
            "words": field.words,
            "chunks": field.chunks.iter().map(dv_chunk_json).collect::<Vec<_>>(),
        });
        if !field.lookup_chunks.is_empty() {
            value["lookup_chunks"] = Value::Array(field.lookup_chunks.iter().map(dv_chunk_json).collect());
        }
        fields.insert(field.name.clone(), value);
    }
    let mut value = json!({
        "storage": meta.storage,
        "compression": meta.compression,
        "format": meta.format,
        "chunk_size": meta.chunk_size,
        "lookup_chunk_size": meta.lookup_chunk_size,
        "total": meta.total,
        "fields": fields,
        "packs": meta.packs.iter().map(|file| json!({ "file": file })).collect::<Vec<_>>(),
    });
    if let Some(rows) = &meta.checksum_rows {
        value["checksum_rows"] = json!({ "file": rows.file, "algorithm": rows.algorithm, "rowBytes": rows.row_bytes });
    }
    value
}

fn strip_checksums(meta: &mut DocValuesMeta) {
    meta.total = None;
    meta.checksum_rows = None;
    for field in meta.fields.iter_mut() {
        for chunk in field.chunks.iter_mut().chain(field.lookup_chunks.iter_mut()).flatten() {
            chunk.pointer.logical_length = None;
            chunk.pointer.checksum = None;
            chunk.pointer.checksum_row = None;
            chunk.pack_index = None;
        }
    }
}

#[test]
fn doc_value_manifests_match_js() {
    let mut chunks = 0;
    for case in cases("dv_manifests") {
        let name = str_of(&case["name"]);
        let binary = parse_doc_value_manifest(&b64(&case["bytes"])).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_json(&dv_meta_json(&binary), &case["expected"], name);
        for field in &binary.fields {
            assert!(std::ptr::eq(binary.field(&field.name).unwrap(), field));
        }
        // The JSON manifest decodes to the same struct (modulo where the
        // checksums live), and its inline checksums equal the sidecar rows.
        let from_json = doc_values_meta_from_json(&case["json"]);
        let sidecar = case["checksums"].as_str().map(|_| b64(&case["checksums"]));
        for (json_field, bin_field) in from_json.fields.iter().zip(&binary.fields) {
            for (a, b) in json_field.chunks.iter().chain(&json_field.lookup_chunks).zip(bin_field.chunks.iter().chain(&bin_field.lookup_chunks)) {
                let (a, b) = (a.as_ref().unwrap(), b.as_ref().unwrap());
                if let Some(sidecar) = &sidecar {
                    let row = b.pointer.checksum_row.expect("checksum row") as usize;
                    let value = rangefind::util::bytes::hex(&sidecar[row * 32..row * 32 + 32]);
                    assert_eq!(&*a.pointer.checksum.as_ref().unwrap().value, value, "{name} checksum row {row}");
                } else {
                    assert!(b.pointer.checksum_row.is_none());
                }
                chunks += 1;
            }
        }
        let (mut a, mut b) = (from_json, binary);
        strip_checksums(&mut a);
        strip_checksums(&mut b);
        assert_eq!(a, b, "{name}: JSON and binary manifests differ");
    }
    assert!(chunks > 100);
}

// ---------------------------------------------------------------------------
// filter bitmaps, query bundles, segments
// ---------------------------------------------------------------------------

#[test]
fn filter_bitmaps_match_js() {
    let mut probes = 0;
    for case in cases("filter_bitmaps") {
        let name = str_of(&case["name"]);
        let bitmap = parse_filter_bitmap(Bytes::new(b64(&case["bytes"]))).unwrap();
        assert_eq!(bitmap.total as u64, case["total"].as_u64().unwrap(), "{name}");
        assert_eq!(&*bitmap.bytes, &b64(&case["payload"])[..], "{name}");
        for probe in case["probes"].as_array().unwrap() {
            assert_eq!(bitmap.has(probe[0].as_f64().unwrap() as i64), probe[1].as_bool().unwrap(), "{name} {}", probe[0]);
            probes += 1;
        }
    }
    assert!(probes > 1000);
}

fn bundle_value_json(value: &BundleFilterValue) -> Value {
    match value {
        BundleFilterValue::Facet(codes) => json!({ "codes": codes }),
        BundleFilterValue::Boolean(flag) => json!(flag),
        BundleFilterValue::Number(n) => opt_f64(*n),
    }
}

#[test]
fn query_bundles_match_js() {
    for case in cases("query_bundles") {
        let name = str_of(&case["name"]);
        let manifest = PostingManifest::from_manifest(&case["manifest"]);
        let bundle = parse_query_bundle(&b64(&case["bytes"]), &manifest).unwrap_or_else(|e| panic!("{name}: {e}"));
        let values: Map<String, Value> = bundle
            .filter_values
            .iter()
            .map(|(field, docs)| {
                let docs: BTreeMap<u64, &BundleFilterValue> = docs.iter().map(|(d, v)| (*d, v)).collect();
                (field.clone(), Value::Object(docs.into_iter().map(|(d, v)| (d.to_string(), bundle_value_json(v))).collect()))
            })
            .collect();
        let actual = json!({
            "key": bundle.key,
            "baseTerms": bundle.base_terms,
            "expandedTerms": bundle.expanded_terms,
            "total": bundle.total,
            "complete": bundle.complete,
            "nextScoreBound": bundle.next_score_bound,
            "nextTieDoc": bundle.next_tie_doc,
            "rows": bundle.rows.iter().map(|(d, s)| json!([d, s])).collect::<Vec<_>>(),
            "rowGroups": bundle.row_groups.iter().map(|g| json!({
                "rowStart": g.row_start, "rowCount": g.row_count, "scoreMax": g.score_max, "scoreMin": g.score_min,
                "docMin": g.doc_min, "docMax": g.doc_max, "filters": filters_json(&g.filters),
            })).collect::<Vec<_>>(),
            "filterValues": values,
        });
        assert_json(&actual, &case["expected"], name);
    }
}

#[test]
fn segments_match_js() {
    let mut rows = 0;
    for case in cases("segments") {
        let name = str_of(&case["name"]);
        let terms = parse_segment_terms(&b64(&case["terms"])).unwrap();
        let postings = b64(&case["postings"]);
        let actual: Vec<Value> = terms.terms.iter().map(|t| json!({ "term": t.term, "offset": t.offset, "bytes": t.bytes, "df": t.df, "count": t.count })).collect();
        assert_json(&Value::Array(actual), &case["expected"], name);
        for item in case["decoded"].as_array().unwrap() {
            let entry = terms.get(str_of(&item["term"])).unwrap();
            let slice = &postings[entry.offset as usize..(entry.offset + entry.bytes) as usize];
            let options = &item["options"];
            let decoded = decode_segment_rows(slice, entry, options["df"].as_f64(), options["total"].as_f64()).unwrap();
            assert_json(&rows_json(&decoded), &item["rows"], &format!("{name} {}", entry.term));
            rows += decoded.len() / 2;
        }
    }
    assert!(rows > 1000);
}

// ---------------------------------------------------------------------------
// http ranges, checksums, error messages
// ---------------------------------------------------------------------------

#[test]
fn multipart_ranges_match_js() {
    for case in cases("http_ranges") {
        let name = str_of(&case["name"]);
        let body = Bytes::new(b64(&case["body"]));
        match parse_multipart_byte_ranges(&body, str_of(&case["contentType"])) {
            Ok(parts) => {
                let expected = case["parts"].as_array().unwrap_or_else(|| panic!("{name}: JS failed with {}", case["error"]));
                let actual: Vec<Value> = parts.iter().map(|p| json!({ "start": p.start, "end": p.end, "bytes": b64_encode(&p.bytes) })).collect();
                assert_json(&Value::Array(actual), &Value::Array(expected.clone()), name);
                for selection in case["selections"].as_array().unwrap() {
                    let ranges: Vec<(u64, u64)> = selection["ranges"].as_array().unwrap().iter().map(|r| (r[0].as_u64().unwrap(), r[1].as_u64().unwrap())).collect();
                    match select_multipart_byte_ranges(&parts, &ranges) {
                        Ok(buffers) => assert_json(&json!(buffers.iter().map(|b| b64_encode(b)).collect::<Vec<_>>()), &selection["result"], name),
                        Err(error) => assert_eq!(error.message(), str_of(&selection["error"]), "{name}"),
                    }
                }
            }
            Err(error) => assert_eq!(Some(error.message()), case["error"].as_str(), "{name}"),
        }
    }
}

#[test]
fn checksum_verification_matches_js() {
    for case in cases("verify") {
        let mut pointer = Pointer::new("x", 0, 0);
        if let Some(checksum) = case["checksum"].as_object() {
            pointer.checksum = Some(Checksum { algorithm: Rc::from(checksum["algorithm"].as_str().unwrap()), value: Rc::from(checksum["value"].as_str().unwrap()) });
        }
        let result = verify_pointer(&b64(&case["bytes"]), &pointer, str_of(&case["label"]));
        assert_eq!(result.err().map(|e| e.message().to_string()), case["error"].as_str().map(str::to_string), "{}", case["name"]);
    }
}

#[test]
fn error_messages_match_js() {
    let mut checked = 0;
    for case in cases("errors") {
        let function = str_of(&case["fn"]);
        let bytes = b64(&case["bytes"]);
        let args = &case["args"];
        let error = match function {
            "decodeDocPageColumns" => decode_doc_page_columns(&bytes, args["fields"].as_array().unwrap(), 0).err(),
            "parseDocValueChunk" => parse_doc_value_chunk(&bytes).err(),
            "parseCodes" => parse_codes(&bytes).err(),
            "parseFacetDictionary" => parse_facet_dictionary(&bytes).err(),
            "decodeDocValueSortPage" => decode_doc_value_sort_page(&bytes, args["name"].as_str()).err(),
            "parseDocValueManifest" => parse_doc_value_manifest(&bytes).err(),
            "parseFilterBitmap" => parse_filter_bitmap(Bytes::new(bytes.clone())).err(),
            "parseQueryBundle" => parse_query_bundle(&bytes, &PostingManifest::default()).err(),
            "parseSegmentTerms" => parse_segment_terms(&bytes).err(),
            "decodeSegmentRows" => {
                let terms = parse_segment_terms(b"RFSGTER1\x00").unwrap();
                assert!(terms.terms.is_empty());
                let entry = rangefind::codec::segment::SegmentTermEntry { term: str_of(&args["term"]).to_string(), offset: 0, bytes: 0, df: 1, count: args["count"].as_u64().unwrap() };
                decode_segment_rows(&bytes, &entry, None, None).err()
            }
            other => panic!("unknown error case {other}"),
        };
        let error = error.unwrap_or_else(|| panic!("{function} should fail like JS: {}", case["error"]));
        assert_eq!(error.message(), str_of(&case["error"]), "{function}");
        checked += 1;
    }
    assert!(checked >= 20);
}
