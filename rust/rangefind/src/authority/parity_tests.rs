//! Parity tests against the JS implementation. Fixtures come from
//! `node rust/tools/fixtures/authority.mjs` (run from the repo root).

use std::collections::HashMap;
use std::path::PathBuf;

use serde_json::{json, Map, Value};
use unicode_normalization::UnicodeNormalization;

use super::abbreviations::*;
use super::address::*;
use super::codec::*;
use super::lexicon::*;
use super::suggest_routing::*;
use super::text;
use crate::util::fold::fold_multi;
use crate::util::json::{num, to_number, unum};

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/fixtures/authority").join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "missing parity fixture {} — run `node rust/tools/fixtures/authority.mjs` from the repo root",
            path.display()
        )
    });
    serde_json::from_str(&text).expect("fixture JSON")
}

fn base64(input: &str) -> Vec<u8> {
    let mut table = [255u8; 256];
    for (index, c) in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".iter().enumerate() {
        table[*c as usize] = index as u8;
    }
    let mut out = Vec::new();
    let mut buffer = 0u32;
    let mut bits = 0;
    for c in input.bytes() {
        if c == b'=' {
            break;
        }
        buffer = (buffer << 6) | table[c as usize] as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    out
}

fn from_bits(value: &Value) -> f64 {
    f64::from_bits(u64::from_str_radix(value.as_str().expect("bits"), 16).expect("hex bits"))
}

fn bits(value: f64) -> Value {
    Value::String(format!("{:016x}", value.to_bits()))
}

fn s(value: &Value) -> &str {
    value.as_str().expect("string")
}

fn strings(value: &Value) -> Vec<String> {
    value.as_array().expect("array").iter().map(|item| s(item).to_string()).collect()
}

/// Collects mismatches so one run reports many, compared as serialized
/// JSON (key order and number representation included).
#[derive(Default)]
struct Checker {
    checked: usize,
    failures: Vec<String>,
}

impl Checker {
    fn eq(&mut self, label: &str, input: &dyn std::fmt::Debug, expected: &Value, actual: &Value) {
        self.checked += 1;
        let expected = serde_json::to_string(expected).unwrap();
        let actual = serde_json::to_string(actual).unwrap();
        if expected != actual {
            self.failures.push(format!("{label} for {input:?}\n  expected {expected}\n  actual   {actual}"));
        }
    }

    fn finish(self, name: &str) {
        if !self.failures.is_empty() {
            let shown: Vec<&String> = self.failures.iter().take(25).collect();
            panic!(
                "{name}: {} of {} checks failed:\n{}",
                self.failures.len(),
                self.checked,
                shown.iter().map(|item| item.as_str()).collect::<Vec<_>>().join("\n")
            );
        }
        eprintln!("{name}: {} checks passed", self.checked);
    }
}

fn str_vec(values: &[String]) -> Value {
    Value::Array(values.iter().cloned().map(Value::String).collect())
}

fn keys_json(keys: &[AuthorityKey]) -> Value {
    Value::Array(keys.iter().map(|item| json!({ "key": item.key, "kind": item.kind.as_str() })).collect())
}

fn candidates_json(candidates: &[AddressRangeCandidate]) -> Value {
    Value::Array(
        candidates
            .iter()
            .map(|item| json!({ "houseNumber": item.house_number, "lookupValue": item.lookup_value }))
            .collect(),
    )
}

#[test]
fn abbreviation_tables_match_js() {
    let tables = fixture("tables.json");
    let mut checker = Checker::default();
    let mut expected_readings = tables["readings"].as_array().unwrap().clone();
    expected_readings.sort_by(|left, right| s(&left[0]).as_bytes().cmp(s(&right[0]).as_bytes()));
    let actual_readings: Vec<Value> = ADDRESS_TOKEN_READINGS.iter().map(|(key, readings)| json!([key, readings])).collect();
    checker.eq("ADDRESS_TOKEN_READINGS", &"table", &Value::Array(expected_readings), &Value::Array(actual_readings));
    let sorted = |value: &Value| {
        let mut list = strings(value);
        list.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        json!(list)
    };
    checker.eq("CONCATENATED_STREET_SUFFIXES", &"table", &sorted(&tables["concatenatedStreetSuffixes"]), &json!(CONCATENATED_STREET_SUFFIXES));
    checker.eq("STREET_TYPE_TOKENS", &"table", &sorted(&tables["streetTypeTokens"]), &json!(STREET_TYPE_TOKENS));
    checker.eq("CONCATENATED_SUFFIX_READINGS", &"table", &tables["concatenatedSuffixReadings"], &json!(CONCATENATED_SUFFIX_READINGS));
    checker.eq("TOKEN_ALIASES", &"table", &tables["tokenAliases"], &json!(TOKEN_ALIASES));
    checker.finish("abbreviation tables");
}

struct Ranges(Vec<(u32, u32)>);

impl Ranges {
    fn from(value: &Value) -> Ranges {
        Ranges(
            value
                .as_array()
                .unwrap()
                .iter()
                .map(|pair| (pair[0].as_u64().unwrap() as u32, pair[1].as_u64().unwrap() as u32))
                .collect(),
        )
    }

    fn contains(&self, cp: u32) -> bool {
        let index = self.0.partition_point(|(_, end)| *end < cp);
        index < self.0.len() && self.0[index].0 <= cp
    }
}

#[test]
fn unicode_predicates_match_v8() {
    let unicode = fixture("unicode.json");
    let unassigned = Ranges::from(&unicode["unassigned"]);
    let mut checker = Checker::default();
    let predicates: [(&str, fn(char) -> bool); 8] = [
        ("letterNumber", text::is_letter_or_number),
        ("space", text::is_js_space),
        ("trimmed", text::is_js_space),
        ("wordIu", text::is_word_iu),
        ("word", text::is_ascii_word),
        ("fsa", text::is_fsa_iu),
        ("ldu", text::is_ldu_iu),
        ("digit", text::is_digit),
    ];
    let mut newer_letters = 0;
    for (name, predicate) in predicates {
        let expected = Ranges::from(&unicode[name]);
        for cp in 0..0x110000u32 {
            let Some(c) = char::from_u32(cp) else { continue };
            let want = expected.contains(cp);
            let got = predicate(c);
            if want != got && name == "letterNumber" && unassigned.contains(cp) {
                // Assigned after V8's Unicode version (see report): the Rust
                // tables are newer, so the property can only differ here.
                newer_letters += 1;
                continue;
            }
            checker.eq(name, &format!("U+{cp:04X}"), &json!(want), &json!(got));
        }
    }
    let mut newer_mappings = 0;
    for (name, transform) in [
        ("nfkcLower", (|c: &str| c.nfkc().collect::<String>().to_lowercase()) as fn(&str) -> String),
        ("fold", |c: &str| fold_multi(c, true)),
    ] {
        let expected: HashMap<u32, String> = unicode[name]
            .as_array()
            .unwrap()
            .iter()
            .map(|pair| (pair[0].as_u64().unwrap() as u32, s(&pair[1]).to_string()))
            .collect();
        for cp in 0..0x110000u32 {
            let Some(c) = char::from_u32(cp) else { continue };
            let input = c.to_string();
            let want = expected.get(&cp).cloned().unwrap_or_else(|| input.clone());
            let got = transform(&input);
            if want != got && unassigned.contains(cp) {
                newer_mappings += 1;
                continue;
            }
            checker.eq(name, &format!("U+{cp:04X}"), &json!(want), &json!(got));
        }
    }
    eprintln!(
        "unicode {}: {newer_letters} letter/number and {newer_mappings} mapping differences, all on code points V8 treats as unassigned",
        s(&unicode["unicode"])
    );
    checker.finish("unicode predicates");
}

#[test]
fn address_functions_match_js() {
    let cases = fixture("address.json");
    let cases = cases.as_array().unwrap();
    let tokens: HashMap<String, Vec<String>> =
        cases.iter().map(|case| (s(&case["input"]).to_string(), strings(&case["tokens"]))).collect();
    let tokenize = |value: &str| -> Vec<String> {
        tokens.get(value).cloned().unwrap_or_else(|| panic!("no recorded analyzer tokens for {value:?}"))
    };
    let mut checker = Checker::default();
    for case in cases {
        let input = s(&case["input"]);
        let base_terms = strings(&case["baseTerms"]);
        let variants = |max| Value::Array(address_query_token_variants(input, max).iter().map(|tokens| str_vec(tokens)).collect());
        checker.eq("normalizeAddressKey", &input, &case["key"], &json!(normalize_address_key(input)));
        checker.eq("normalizeAddressAuthorityKey", &input, &case["authorityKey"], &json!(normalize_address_authority_key(input)));
        checker.eq("addressQueryTokenVariants", &input, &case["variants"], &variants(40));
        checker.eq("addressQueryTokenVariants(24)", &input, &case["variants24"], &variants(24));
        checker.eq("addressQueryTokenVariants(5)", &input, &case["variants5"], &variants(5));
        checker.eq("addressAuthorityQueryKeys", &input, &case["queryKeys"], &str_vec(&address_authority_query_keys(input)));
        checker.eq("looksLikeAddressQuery", &input, &case["looksLike"], &json!(looks_like_address_query(input)));
        checker.eq("normalizePostalCodeSpacing", &input, &case["postal"], &json!(normalize_postal_code_spacing(input)));
        checker.eq("normalizePostalCodePrefixSpacing", &input, &case["postalPrefix"], &json!(normalize_postal_code_prefix_spacing(input)));
        checker.eq(
            "addressRangeQueryCandidates",
            &input,
            &case["rangeCandidates"],
            &candidates_json(&address_range_query_candidates(input, ADDRESS_RANGE_BUCKET_SIZE)),
        );
        checker.eq(
            "addressRangeQueryCandidates(7)",
            &input,
            &case["rangeCandidates7"],
            &candidates_json(&address_range_query_candidates(input, 7.0)),
        );
        checker.eq("authorityNormalizeSurface", &input, &case["surface"], &json!(authority_normalize_surface(input)));
        checker.eq("authorityNormalizeRawSurface", &input, &case["rawSurface"], &json!(authority_normalize_raw_surface(input)));
        checker.eq("authorityAddressRangeKey", &input, &case["rangeKey"], &json!(authority_address_range_key(input)));
        checker.eq("suggestKey", &input, &case["suggestKey"], &json!(suggest_key(input)));
        checker.eq(
            "authorityKeysForQuery",
            &input,
            &case["queryKeysPlain"],
            &keys_json(&authority_keys_for_query::<String>(input, &[], &tokenize, false)),
        );
        checker.eq(
            "authorityKeysForQuery(address)",
            &input,
            &case["queryKeysAddress"],
            &keys_json(&authority_keys_for_query(input, &base_terms, &tokenize, true)),
        );
        let address_options = AuthorityKeyOptions { normalizer: Some(AuthorityNormalizer::Address), ..Default::default() };
        checker.eq(
            "authorityKeysForValue(address)",
            &input,
            &case["valueKeysAddress"],
            &keys_json(&authority_keys_for_value(input, &address_options, &tokenize)),
        );
        let range_options = AuthorityKeyOptions { normalizer: Some(AuthorityNormalizer::AddressRange), ..Default::default() };
        checker.eq(
            "authorityKeysForValue(address-range)",
            &input,
            &case["valueKeysRange"],
            &keys_json(&authority_keys_for_value(input, &range_options, &tokenize)),
        );
        let bare = AuthorityKeyOptions { surface: false, tokens: false, ..address_options };
        checker.eq(
            "authorityKeysForValue(no surface/tokens)",
            &input,
            &case["valueKeysNoSurface"],
            &keys_json(&authority_keys_for_value(input, &bare, &|_| panic!("tokenize must not run"))),
        );
    }
    checker.finish("address functions");
}

#[test]
fn regex_ports_match_js_on_fuzzed_input() {
    let cases = fixture("regex.json");
    let mut checker = Checker::default();
    for case in cases.as_array().unwrap() {
        let input = s(&case["input"]);
        checker.eq("normalizeAddressKey", &input, &case["key"], &json!(normalize_address_key(input)));
        checker.eq("looksLikeAddressQuery", &input, &case["looksLike"], &json!(looks_like_address_query(input)));
        checker.eq("normalizePostalCodeSpacing", &input, &case["postal"], &json!(normalize_postal_code_spacing(input)));
        checker.eq("normalizePostalCodePrefixSpacing", &input, &case["postalPrefix"], &json!(normalize_postal_code_prefix_spacing(input)));
        checker.eq("authorityNormalizeSurface", &input, &case["surface"], &json!(authority_normalize_surface(input)));
        checker.eq("authorityNormalizeRawSurface", &input, &case["rawSurface"], &json!(authority_normalize_raw_surface(input)));
        checker.eq("suggestKey", &input, &case["suggestKey"], &json!(suggest_key(input)));
    }
    checker.finish("regex ports");
}

#[test]
fn address_range_math_matches_js_bit_for_bit() {
    let mut checker = Checker::default();
    for case in fixture("range_contains.json").as_array().unwrap() {
        let args = [&case["start"], &case["end"], &case["step"], &case["house"]].map(from_bits);
        checker.eq(
            "addressRangeContains",
            &args,
            &case["result"],
            &json!(address_range_contains(args[0], args[1], args[2], args[3])),
        );
    }
    let point_json = |point: &AddressPoint| json!([bits(point.lat), bits(point.lon)]);
    for case in fixture("geometry.json").as_array().unwrap() {
        let encoded = s(&case["encoded"]);
        let decoded = match decode_address_range_geometry(encoded) {
            Ok(points) => json!({ "ok": points.iter().map(point_json).collect::<Vec<_>>() }),
            Err(error) => json!({ "error": error.message() }),
        };
        checker.eq("decodeAddressRangeGeometry", &encoded, &case["decoded"], &decoded);
        for probe in case["interpolations"].as_array().unwrap() {
            let args = [&probe["start"], &probe["end"], &probe["house"]].map(from_bits);
            let result = match interpolate_address_range_point(encoded, args[0], args[1], args[2]) {
                Ok(point) => json!({ "ok": point.as_ref().map(point_json) }),
                Err(error) => json!({ "error": error.message() }),
            };
            checker.eq("interpolateAddressRangePoint", &(encoded, args), &probe["result"], &result);
        }
    }
    checker.finish("address range math");
}

fn rows_json(rows: &[[u64; 2]]) -> Value {
    json!(rows)
}

fn shard_json(shard: &AuthorityShard) -> Value {
    let entries: Vec<Value> = shard
        .entries
        .iter()
        .map(|entry| {
            let mut object = Map::new();
            object.insert("total".into(), json!(entry.total));
            object.insert("complete".into(), json!(entry.complete));
            object.insert("rows".into(), rows_json(&entry.rows));
            if let Some(weight) = entry.autocomplete_weight {
                object.insert("autocompleteWeight".into(), json!(weight));
            }
            if entry.ordinal_rows {
                object.insert("ordinalRows".into(), json!(true));
            }
            if entry.doc_rows {
                object.insert("docRows".into(), json!(true));
            }
            if let (Some(ordinal), Some(doc)) = (entry.doc_ordinal, entry.doc) {
                object.insert("docOrdinal".into(), json!(ordinal));
                object.insert("doc".into(), json!(doc));
            }
            json!([entry.key, object])
        })
        .collect();
    json!({ "format": shard.format, "entries": entries })
}

fn check_shard_case(checker: &mut Checker, case: &Value) {
    let bytes = base64(s(&case["bytes"]));
    let label = case["name"].as_str().unwrap_or("shard");
    match parse_authority_shard(&bytes) {
        Ok(shard) => {
            checker.eq("parseAuthorityShard", &label, &case["result"], &shard_json(&shard));
            for entry in &shard.entries {
                checker.eq("AuthorityShard::get", &(label, &entry.key), &json!(true), &json!(shard.get(&entry.key) == Some(entry)));
            }
        }
        Err(error) => checker.eq("parseAuthorityShard error", &label, &case["error"], &json!(error.message())),
    }
}

#[test]
fn authority_shards_decode_like_js() {
    let mut checker = Checker::default();
    for case in fixture("authority_shards.json").as_array().unwrap() {
        check_shard_case(&mut checker, case);
    }
    checker.finish("authority shards");
}

fn lexicon_root_json(root: &AuthorityLexiconRoot) -> Value {
    json!({
        "format": root.format,
        "weighted": root.weighted,
        "shards": root.shards.iter().map(|shard| json!({ "shard": shard.shard, "maxRank": shard.max_rank, "count": shard.count })).collect::<Vec<_>>(),
        "hot": root.hot.iter().map(|(prefix, entry)| json!([prefix, { "file": entry.file, "bytes": entry.bytes, "count": entry.count }])).collect::<Vec<_>>(),
    })
}

fn segment_json(segment: &AuthorityLexiconSegment) -> Value {
    let entries: Vec<Value> = segment
        .entries
        .iter()
        .map(|entry| {
            let checksum = entry.entry.checksum.as_ref().expect("checksum");
            json!({
                "shard": entry.shard,
                "maxRank": entry.max_rank,
                "count": entry.count,
                "entry": {
                    "pack": &*entry.entry.pack,
                    "offset": entry.entry.offset,
                    "length": entry.entry.length,
                    "physicalLength": entry.entry.length,
                    "logicalLength": entry.entry.logical_length,
                    "checksum": { "algorithm": &*checksum.algorithm, "value": &*checksum.value }
                }
            })
        })
        .collect();
    json!({ "format": segment.format, "entries": entries, "first": segment.first, "last": segment.last })
}

fn hot_json(entries: &[HotListEntry]) -> Value {
    Value::Array(
        entries
            .iter()
            .map(|entry| {
                let mut object = Map::new();
                object.insert("normalized".into(), json!(entry.normalized));
                object.insert("display".into(), json!(entry.display));
                object.insert("weight".into(), json!(entry.weight));
                object.insert("count".into(), json!(entry.count));
                if let Some(doc) = entry.doc {
                    object.insert("doc".into(), json!(doc));
                }
                if let Some(docs) = &entry.docs {
                    let rows: Vec<Value> = docs
                        .iter()
                        .map(|row| {
                            let mut item = Map::new();
                            item.insert("doc".into(), json!(row.doc));
                            item.insert("score".into(), json!(row.score));
                            if let Some(ordinal) = row.ordinal {
                                item.insert("ordinal".into(), json!(ordinal));
                            }
                            Value::Object(item)
                        })
                        .collect();
                    object.insert("docs".into(), Value::Array(rows));
                }
                Value::Object(object)
            })
            .collect(),
    )
}

fn check_root_case(checker: &mut Checker, case: &Value) {
    let bytes = base64(s(&case["bytes"]));
    match parse_authority_lexicon_root(&bytes) {
        Ok(root) => {
            checker.eq("parseAuthorityLexiconRoot", &case["bytes"], &case["result"], &lexicon_root_json(&root));
            for (prefix, entry) in &root.hot {
                checker.eq("hot_get", &prefix, &json!(true), &json!(root.hot_get(prefix) == Some(entry)));
            }
        }
        Err(error) => checker.eq("parseAuthorityLexiconRoot error", &case["bytes"], &case["error"], &json!(error.message())),
    }
}

fn check_segment_case(checker: &mut Checker, case: &Value) {
    let bytes = base64(s(&case["bytes"]));
    let pack_table = strings(&case["packTable"]);
    match parse_authority_lexicon_segment(&bytes, &pack_table) {
        Ok(segment) => checker.eq("parseAuthorityLexiconSegment", &case["bytes"], &case["result"], &segment_json(&segment)),
        Err(error) => checker.eq("parseAuthorityLexiconSegment error", &case["bytes"], &case["error"], &json!(error.message())),
    }
}

fn check_hot_case(checker: &mut Checker, case: &Value) {
    let bytes = base64(s(&case["bytes"]));
    match parse_authority_hot_list(&bytes) {
        Ok(entries) => checker.eq("parseAuthorityHotList", &case["bytes"], &case["result"], &hot_json(&entries)),
        Err(error) => checker.eq("parseAuthorityHotList error", &case["bytes"], &case["error"], &json!(error.message())),
    }
}

#[test]
fn lexicon_codecs_decode_like_js() {
    let mut checker = Checker::default();
    for case in fixture("lexicon_roots.json").as_array().unwrap() {
        check_root_case(&mut checker, case);
    }
    for case in fixture("lexicon_segments.json").as_array().unwrap() {
        check_segment_case(&mut checker, case);
    }
    for case in fixture("hot_lists.json").as_array().unwrap() {
        check_hot_case(&mut checker, case);
    }
    checker.finish("lexicon codecs");
}

struct Item {
    display: String,
    normalized: String,
    weight: f64,
    rank: Option<f64>,
}

impl AutocompleteOrder for Item {
    fn rank_or_weight(&self) -> f64 {
        self.rank.unwrap_or(self.weight)
    }
    fn display(&self) -> &str {
        &self.display
    }
    fn normalized(&self) -> &str {
        &self.normalized
    }
}

fn summary_json(summary: &AutocompleteSummary) -> Value {
    let mut object = Map::new();
    object.insert("key".into(), json!(summary.key));
    object.insert("normalized".into(), json!(summary.normalized));
    object.insert("display".into(), json!(summary.display));
    object.insert("weight".into(), num(summary.weight));
    object.insert("count".into(), unum(summary.count));
    object.insert("full".into(), json!(summary.full));
    if let Some(docs) = &summary.docs {
        object.insert("doc".into(), summary.doc.map(num).unwrap_or(Value::Null));
        object.insert(
            "docs".into(),
            Value::Array(docs.iter().map(|row| json!({ "doc": num(row.doc), "score": num(row.score) })).collect()),
        );
    }
    object.insert("rank".into(), num(summary.rank));
    Value::Object(object)
}

#[test]
fn autocomplete_ranking_matches_js() {
    let fixture = fixture("autocomplete.json");
    let mut checker = Checker::default();
    for case in fixture["ranks"].as_array().unwrap() {
        let weight = if s(&case["weightKind"]) == "number" { from_bits(&case["weight"]) } else { to_number(&case["weight"]) };
        let full = case["full"].as_bool().unwrap_or_else(|| autocomplete_full(s(&case["display"]), s(&case["normalized"])));
        let weighted = case["weighted"].as_bool().unwrap();
        checker.eq("autocompleteRank", case, &case["rank"], &bits(autocomplete_rank(weight, full, weighted)));
    }
    let items: Vec<Item> = fixture["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| Item {
            display: s(&item["display"]).to_string(),
            normalized: s(&item["normalized"]).to_string(),
            weight: from_bits(&item["weight"]),
            rank: if item["rank"].is_null() { None } else { Some(from_bits(&item["rank"])) },
        })
        .collect();
    for pair in fixture["pairs"].as_array().unwrap() {
        let left = &items[pair["left"].as_u64().unwrap() as usize];
        let right = &items[pair["right"].as_u64().unwrap() as usize];
        let sign = compare_autocomplete(left, right) as i32;
        checker.eq("compareAutocomplete", pair, &pair["sign"], &json!(sign));
    }
    for case in fixture["sorts"].as_array().unwrap() {
        let mut positions: Vec<usize> = case["input"].as_array().unwrap().iter().map(|v| v.as_u64().unwrap() as usize).collect();
        positions.sort_by(|left, right| compare_autocomplete(&items[*left], &items[*right]));
        checker.eq("sort(compareAutocomplete)", &case["input"], &case["output"], &json!(positions));
    }
    for case in fixture["summaries"].as_array().unwrap() {
        let rows: Vec<[f64; 2]> = case["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| [row[0].as_f64().unwrap(), row[1].as_f64().unwrap()])
            .collect();
        let options = AutocompleteSummaryOptions {
            doc_rows: case["options"]["docRows"].as_bool().unwrap_or(false),
            max_docs: case["options"]["maxDocs"].as_f64(),
            weighted: case["options"]["weighted"].as_bool().unwrap_or(false),
        };
        let actual = autocomplete_entry_summary(s(&case["key"]), &rows, &options)
            .map(|summary| summary_json(&summary))
            .unwrap_or(Value::Null);
        checker.eq("autocompleteEntrySummary", case, &case["result"], &actual);
    }
    for case in fixture["parseKeys"].as_array().unwrap() {
        let key = s(&case["key"]);
        checker.eq("isAutocompleteKey", &key, &case["isKey"], &json!(is_autocomplete_key(key)));
        let parsed = parse_autocomplete_key(key)
            .map(|parsed| json!({ "normalized": parsed.normalized, "display": parsed.display }))
            .unwrap_or(Value::Null);
        checker.eq("parseAutocompleteKey", &key, &case["parsed"], &parsed);
    }
    checker.finish("autocomplete ranking");
}

#[test]
fn suggest_routing_matches_js() {
    let routing = fixture("routing.json");
    let mut checker = Checker::default();
    checker.eq("SUGGEST_ROUTING_COMPAT_FORMATS", &"const", &routing["compatFormats"], &json!(SUGGEST_ROUTING_COMPAT_FORMATS));
    checker.eq("SUGGEST_ROUTING_FORMAT", &"const", &routing["manifest"]["format"], &json!(SUGGEST_ROUTING_FORMAT));
    check_root_case(&mut checker, &routing["lexiconRoot"]);
    for case in routing["segments"].as_array().unwrap() {
        check_segment_case(&mut checker, case);
    }
    for case in routing["shards"].as_array().unwrap() {
        check_shard_case(&mut checker, case);
    }
    for case in routing["hot"].as_array().unwrap() {
        check_hot_case(&mut checker, case);
    }
    for case in routing["selection"].as_array().unwrap() {
        let root = &case["root"];
        let directory = suggest_routing_directory(crate::util::json::path(root, &["suggest_routing"]));
        checker.eq("suggestRoutingDirectory", root, &case["directory"], directory);
        checker.eq("suggestRouting active", root, &case["active"], &json!(active_suggest_routing(root).is_some()));
    }
    for case in routing["remap"].as_array().unwrap() {
        let block = if case["shardIds"].is_null() { None } else { Some(json!({ "shard_ids": case["shardIds"] })) };
        let shard_ids = suggest_routing_shard_ids(block.as_ref());
        let known = strings(&case["known"]);
        let is_known = |id: &str| known.iter().any(|item| item == id);
        let item = case["item"].as_object().unwrap();
        let shards = suggest_routing_shards(&shard_ids, &is_known, item.get("ordinals").unwrap_or(&Value::Null));
        checker.eq("suggestRoutingShards", case, &case["shards"], &json!(shards));
        let remapped = remap_suggest_routing_item(item, &shard_ids, &is_known);
        checker.eq("remapSuggestRoutingItem", case, &case["remapped"], &Value::Object(remapped));
    }
    checker.finish("suggest routing");
}

#[test]
fn cos_matches_v8_bit_for_bit() {
    let mut checker = Checker::default();
    for pair in fixture("cos.json").as_array().unwrap() {
        let x = from_bits(&pair[0]);
        checker.eq("Math.cos", &x, &pair[1], &bits(super::fdlibm::cos(x)));
    }
    checker.finish("Math.cos");
}
