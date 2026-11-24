//! Parity with the JS implementation (`src/vector_index.js`,
//! `src/typo_main_index.js`) over fixtures from
//! `node rust/tools/fixtures/vector.mjs`.
#![cfg(feature = "vector")]

use std::path::PathBuf;

use rangefind::typo::*;
use rangefind::vector::*;
use serde_json::Value;

fn load(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/fixtures/vector").join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!("missing fixture {}: run `node rust/tools/fixtures/vector.mjs` from the repo root", path.display())
    });
    serde_json::from_str(&text).expect("fixture json")
}

fn b64(text: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut acc = 0u32;
    let mut bits = 0;
    for byte in text.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => continue,
        } as u32;
        acc = (acc << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    out
}

/// Decodes the fixture's exact number encoding.
fn num(value: &Value) -> f64 {
    match value {
        Value::Number(n) => n.as_f64().unwrap(),
        Value::String(s) => match s.as_str() {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            "-0" => -0.0,
            other => panic!("bad number tag {other}"),
        },
        other => panic!("not a number: {other}"),
    }
}

fn same(actual: f64, expected: f64) -> bool {
    (actual.is_nan() && expected.is_nan()) || actual.to_bits() == expected.to_bits()
}

fn assert_f64(actual: f64, expected: &Value, context: &str) {
    let expected = num(expected);
    assert!(same(actual, expected), "{context}: got {actual:e} ({:#x}), want {expected:e} ({:#x})", actual.to_bits(), expected.to_bits());
}

fn assert_f32s(actual: &[f32], expected: &Value, context: &str) {
    let expected = expected.as_array().unwrap();
    assert_eq!(actual.len(), expected.len(), "{context}: length");
    for (i, (a, e)) in actual.iter().zip(expected).enumerate() {
        assert_f64(*a as f64, e, &format!("{context}[{i}]"));
    }
}

fn f32s(value: &Value) -> Vec<f32> {
    value.as_array().unwrap().iter().map(|v| num(v) as f32).collect()
}

fn ints<T: TryFrom<i64>>(value: &Value) -> Vec<T>
where
    T::Error: std::fmt::Debug,
{
    value.as_array().unwrap().iter().map(|v| T::try_from(v.as_i64().unwrap()).unwrap()).collect()
}

fn s(value: &Value) -> &str {
    value.as_str().unwrap()
}

fn u(value: &Value) -> u64 {
    value.as_u64().unwrap()
}

fn assert_root(root: &VectorRoot, expected: &Value, context: &str) {
    assert_eq!(root.format, s(&expected["format"]), "{context}");
    assert_eq!(root.page_format, s(&expected["pageFormat"]), "{context}");
    assert_eq!(root.field, s(&expected["field"]), "{context}");
    assert_eq!(root.metric, s(&expected["metric"]), "{context}");
    assert_eq!(root.dims as u64, u(&expected["dims"]), "{context}");
    assert_eq!(root.coarse_dims as u64, u(&expected["coarseDims"]), "{context}");
    assert_eq!(root.total, u(&expected["total"]), "{context}");
    assert_eq!(root.permutation, ints::<u16>(&expected["permutation"]), "{context}");
    let packs: Vec<String> = expected["packTable"].as_array().unwrap().iter().map(|p| p.as_str().unwrap_or("").to_string()).collect();
    assert_eq!(root.pack_table, packs, "{context}");
    assert_eq!(root.cluster_count as u64, u(&expected["clusterCount"]), "{context}");
    assert_f32s(&root.centroid_scales, &expected["centroidScales"], context);
    assert_eq!(root.centroid_codes, ints::<i8>(&expected["centroidCodes"]), "{context}");
    let clusters = expected["clusters"].as_array().unwrap();
    assert_eq!(root.clusters.len(), clusters.len(), "{context}");
    for (cluster, want) in root.clusters.iter().zip(clusters) {
        assert_eq!(cluster.index as u64, u(&want["index"]), "{context}");
        assert_eq!(cluster.count, u(&want["count"]), "{context}");
        assert_eq!(&*cluster.pointer.pack, want["pack"].as_str().unwrap_or(""), "{context}");
        assert_eq!(cluster.pointer.offset, u(&want["offset"]), "{context}");
        assert_eq!(cluster.pointer.length, u(&want["length"]), "{context}");
        assert_eq!(cluster.physical_length, u(&want["physicalLength"]), "{context}");
        assert_eq!(cluster.pointer.logical_length, want["logicalLength"].as_u64(), "{context}");
        match (&cluster.pointer.checksum, &want["checksum"]) {
            (None, Value::Null) => {}
            (Some(checksum), want) => {
                assert_eq!(&*checksum.algorithm, s(&want["algorithm"]), "{context}");
                assert_eq!(&*checksum.value, s(&want["value"]), "{context}");
            }
            (None, other) => panic!("{context}: missing checksum {other}"),
        }
        assert_eq!(cluster.pointer.checksum_row, None);
    }
    assert_eq!(root.refine.row_bytes, u(&expected["refine"]["rowBytes"]), "{context}");
    assert_eq!(root.refine.rows_per_pack, u(&expected["refine"]["rowsPerPack"]), "{context}");
    let refine_packs: Vec<String> = expected["refine"]["packs"].as_array().unwrap().iter().map(|p| s(p).to_string()).collect();
    assert_eq!(root.refine.packs, refine_packs, "{context}");
}

fn assert_page(page: &VectorClusterPage, expected: &Value, context: &str) {
    assert_eq!(page.field, s(&expected["field"]), "{context}");
    assert_eq!(page.cluster_index, u(&expected["clusterIndex"]), "{context}");
    assert_eq!(page.coarse_dims as u64, u(&expected["coarseDims"]), "{context}");
    assert_eq!(page.count as u64, u(&expected["count"]), "{context}");
    assert_eq!(page.docs, ints::<u32>(&expected["docs"]), "{context}");
    assert_eq!(page.ordinals, ints::<u32>(&expected["ordinals"]), "{context}");
    assert_f32s(&page.scales, &expected["scales"], context);
    assert_eq!(page.codes, ints::<i8>(&expected["codes"]), "{context}");
}

fn assert_result<T>(actual: rangefind::Result<T>, expected: &Value, context: &str, check: impl Fn(&T, &Value)) {
    match (actual, expected.get("error")) {
        (Ok(value), None) => check(&value, &expected["ok"]),
        (Err(error), Some(message)) => assert_eq!(error.message(), s(message), "{context}"),
        (Ok(_), Some(message)) => panic!("{context}: expected error {message}"),
        (Err(error), None) => panic!("{context}: unexpected error {}", error.message()),
    }
}

#[test]
fn vector_indexes_and_runtime_scoring() {
    let fixture = load("vector.json");
    let indexes = fixture["indexes"].as_array().unwrap();
    let mut scores = 0;
    for (n, index) in indexes.iter().enumerate() {
        let context = format!("index {n}");
        let root = parse_vector_root(&b64(s(&index["root"]))).expect("root parses");
        assert_root(&root, &index["rootParsed"], &context);
        let mut pages = Vec::new();
        for (p, page) in index["pages"].as_array().unwrap().iter().enumerate() {
            let decoded = decode_vector_cluster_page(&b64(s(&page["bytes"])), Some(&root.field)).expect("page decodes");
            assert_page(&decoded, &page["parsed"], &format!("{context} page {p}"));
            pages.push(decoded);
        }
        let refine = b64(s(&index["refine"]));
        let row_bytes = root.refine.row_bytes as usize;
        for (q, query) in index["queries"].as_array().unwrap().iter().enumerate() {
            let context = format!("{context} query {q}");
            let normalized = normalized_query_vector(&query["value"], root.dims).expect("normalizes");
            assert_f32s(&normalized, &query["normalized"], &context);
            let mut permuted = vec![0f32; root.dims];
            apply_permutation(&normalized, 0, &root.permutation, &mut permuted);
            assert_f32s(&permuted, &query["query"], &context);
            for (c, want) in query["centroidScores"].as_array().unwrap().iter().enumerate() {
                let score = dot_int8(&permuted, &root.centroid_codes, c * root.dims, root.dims, root.centroid_scales[c] as f64);
                assert_f64(score, want, &format!("{context} centroid {c}"));
                scores += 1;
            }
            for (p, want) in query["coarseScores"].as_array().unwrap().iter().enumerate() {
                let page = &pages[p];
                for (i, want) in want.as_array().unwrap().iter().enumerate() {
                    let score = dot_int8(&permuted, &page.codes, i * page.coarse_dims, page.coarse_dims, page.scales[i] as f64);
                    assert_f64(score, want, &format!("{context} page {p} row {i}"));
                    scores += 1;
                }
            }
            for (i, want) in query["refineScores"].as_array().unwrap().iter().enumerate() {
                let at = i * row_bytes;
                let scale = f32::from_le_bytes([refine[at], refine[at + 1], refine[at + 2], refine[at + 3]]);
                let score = dot_int8_bytes(&permuted, &refine, at + 4, root.dims, scale as f64);
                assert_f64(score, want, &format!("{context} refine {i}"));
                scores += 1;
            }
        }
    }
    assert!(indexes.len() >= 40 && scores > 5000, "{} indexes, {scores} scores", indexes.len());
}

#[test]
fn vector_corrupt_roots_and_pages() {
    let fixture = load("vector.json");
    let roots = fixture["corruptRoots"].as_array().unwrap();
    let mut errors = 0;
    for (n, case) in roots.iter().enumerate() {
        let context = format!("corrupt root {n}");
        errors += usize::from(case["result"].get("error").is_some());
        assert_result(parse_vector_root(&b64(s(&case["bytes"]))), &case["result"], &context, |root, want| {
            assert_root(root, want, &context)
        });
    }
    let pages = fixture["corruptPages"].as_array().unwrap();
    for (n, case) in pages.iter().enumerate() {
        let context = format!("corrupt page {n}");
        errors += usize::from(case["result"].get("error").is_some());
        let name = case["name"].as_str();
        assert_result(decode_vector_cluster_page(&b64(s(&case["bytes"])), name), &case["result"], &context, |page, want| {
            assert_page(page, want, &context)
        });
    }
    assert!(roots.len() > 1000 && pages.len() > 1000 && errors > 1000);
}

#[test]
fn vector_from_value_normalize_permute_dot() {
    let fixture = load("vector.json");
    let cases = fixture["fromValueCases"].as_array().unwrap();
    for (n, case) in cases.iter().enumerate() {
        let dims = u(&case["dims"]) as usize;
        let actual = vector_from_value(&case["value"], dims);
        match (&actual, &case["result"]) {
            (None, Value::Null) => {}
            (Some(vector), want @ Value::Array(_)) => assert_f32s(vector, want, &format!("fromValue {n} {}", case["value"])),
            (actual, want) => panic!("fromValue {n} {}: got {actual:?}, want {want}", case["value"]),
        }
    }
    let cases = fixture["normalizeCases"].as_array().unwrap();
    for (n, case) in cases.iter().enumerate() {
        match (normalize_vector(f32s(&case["input"])), &case["result"]) {
            (None, Value::Null) => {}
            (Some(vector), want @ Value::Array(_)) => assert_f32s(&vector, want, &format!("normalize {n}")),
            (actual, want) => panic!("normalize {n}: got {actual:?}, want {want}"),
        }
    }
    let cases = fixture["permutationCases"].as_array().unwrap();
    for (n, case) in cases.iter().enumerate() {
        let mut target = vec![0f32; u(&case["targetLen"]) as usize];
        apply_permutation(&f32s(&case["vector"]), u(&case["offset"]) as usize, &ints::<u16>(&case["permutation"]), &mut target);
        assert_f32s(&target, &case["result"], &format!("permutation {n}"));
    }
    let cases = fixture["dotCases"].as_array().unwrap();
    for (n, case) in cases.iter().enumerate() {
        let query = f32s(&case["query"]);
        let codes = ints::<i8>(&case["codes"]);
        let offset = u(&case["offset"]) as usize;
        let dims = u(&case["dims"]) as usize;
        let scale = num(&case["scale"]);
        assert_f64(dot_int8(&query, &codes, offset, dims, scale), &case["result"], &format!("dot {n}"));
        let raw: Vec<u8> = codes.iter().map(|&c| c as u8).collect();
        assert_f64(dot_int8_bytes(&query, &raw, offset, dims, scale), &case["result"], &format!("dot bytes {n}"));
    }
    let cases = fixture["queryVectorCases"].as_array().unwrap();
    for (n, case) in cases.iter().enumerate() {
        let context = format!("query vector {n}");
        let dims = u(&case["dims"]) as usize;
        assert_result(normalized_query_vector(&case["value"], dims), &case["result"], &context, |vector, want| {
            assert_f32s(vector, want, &context)
        });
    }
    let f32_input = [3.0f32, 4.0];
    assert_eq!(normalized_query_vector_f32(&f32_input, 2).unwrap(), normalize_vector(f32_input.to_vec()).unwrap());
    assert_eq!(normalized_query_vector_f32(&f32_input, 3).unwrap_err().message(), "Rangefind: vector queries need 3 finite dimensions.");
}

#[test]
fn typo_pairs_distances_overlap_scores() {
    let fixture = load("typo.json");
    let cases = fixture["pairCases"].as_array().unwrap();
    for (n, case) in cases.iter().enumerate() {
        let (a, b) = (s(&case["a"]), s(&case["b"]));
        let context = format!("pair {n} {a:?} {b:?}");
        let max_distance = u(&case["maxDistance"]) as u32;
        assert_eq!(bounded_damerau_levenshtein(a, b, max_distance) as u64, u(&case["dl"]), "{context} dl");
        let best = best_main_index_typo_distance(a, b, u(&case["maxEdits"]) as u32);
        assert_eq!(best.surface, s(&case["best"]["surface"]), "{context} best surface");
        assert_eq!(best.distance as u64, u(&case["best"]["distance"]), "{context} best distance");
        assert_f64(ngram_overlap(a, b), &case["overlap"], &format!("{context} overlap"));
        let score = main_index_typo_candidate_score(a, b, num(&case["df"]), num(&case["distance"]));
        assert_f64(score, &case["score"], &format!("{context} score"));
    }
    assert!(cases.len() >= 4000);
}

#[test]
fn typo_log1p_matches_v8() {
    let fixture = load("typo.json");
    let cases = fixture["log1pCases"].as_array().unwrap();
    for case in cases {
        let x = num(&case[0]);
        assert_f64(js_log1p(x), &case[1], &format!("log1p({x:e})"));
    }
    assert!(cases.len() > 7000);
}

#[test]
fn typo_tokens_and_unicode_tables() {
    let fixture = load("typo.json");
    for case in fixture["tokenCases"].as_array().unwrap() {
        let token = s(&case["token"]);
        assert_eq!(is_typo_correction_token(token), case["ok"].as_bool().unwrap(), "token {token:?}");
    }
    for (key, predicate) in [("letterRanges", is_letter as fn(char) -> bool), ("numberRanges", is_number)] {
        let mut expected = vec![false; 0x110000];
        for range in fixture[key].as_array().unwrap() {
            for cp in u(&range[0])..=u(&range[1]) {
                expected[cp as usize] = true;
            }
        }
        let mismatches: Vec<String> = (0..0x110000u32)
            .filter_map(|cp| char::from_u32(cp).map(|c| (cp, c)))
            .filter(|&(cp, c)| predicate(c) != expected[cp as usize])
            .map(|(cp, _)| format!("{cp:#x}"))
            .collect();
        assert!(mismatches.is_empty(), "{key}: {} mismatches: {:?}", mismatches.len(), &mismatches[..mismatches.len().min(400)]);
    }
}

#[test]
fn typo_max_edits_and_probe_values() {
    let fixture = load("typo.json");
    for case in fixture["editCases"].as_array().unwrap() {
        let options = MainIndexTypoOptions { max_edits: u(&case["maxEdits"]) as u32, ..Default::default() };
        let token = s(&case["token"]);
        assert_eq!(typo_max_edits_for(token, &options) as u64, u(&case["result"]), "maxEdits {token:?}");
    }
    let cases = fixture["probeCases"].as_array().unwrap();
    for case in cases {
        let options = MainIndexTypoOptions { max_shard_lookups: u(&case["maxShardLookups"]) as u32, ..Default::default() };
        let (raw, term) = (s(&case["raw"]), s(&case["term"]));
        let expected: Vec<String> = case["result"].as_array().unwrap().iter().map(|v| s(v).to_string()).collect();
        assert_eq!(main_index_typo_probe_values(raw, term, &options), expected, "probes {raw:?} {term:?} {}", case["maxShardLookups"]);
    }
    assert!(cases.len() > 3000);
}

#[test]
fn typo_option_normalization() {
    let fixture = load("typo.json");
    let cases = fixture["optionCases"].as_array().unwrap();
    for case in cases {
        let actual = normalize_main_index_typo_options(&case["options"], &case["manifest"]);
        let want = &case["result"];
        let expected = MainIndexTypoOptions {
            mode: s(&want["mode"]).to_string(),
            trigger: s(&want["trigger"]).to_string(),
            max_edits: u(&want["maxEdits"]) as u32,
            max_token_candidates: u(&want["maxTokenCandidates"]) as u32,
            max_query_plans: u(&want["maxQueryPlans"]) as u32,
            max_corrected_searches: u(&want["maxCorrectedSearches"]) as u32,
            max_shard_lookups: u(&want["maxShardLookups"]) as u32,
            weak_result_total: u(&want["weakResultTotal"]) as u32,
        };
        assert_eq!(actual, expected, "options {} manifest {}", case["options"], case["manifest"]);
    }
    assert_eq!(normalize_main_index_typo_options(&Value::Null, &Value::Null), MainIndexTypoOptions::default());
    assert!(cases.len() > 2000);
}
