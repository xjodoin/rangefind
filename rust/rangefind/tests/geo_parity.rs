//! Parity with the JS implementation (`src/geo_tree.js`, `src/geo_cells.js`,
//! `src/geo_route.js`, `src/geo_shard_routing.js`) over fixtures from
//! `node rust/tools/fixtures/geo.mjs`.
#![cfg(feature = "geo")]

use std::path::PathBuf;
use std::rc::Rc;

use rangefind::geo::*;
use rangefind::pointer::Pointer;
use rangefind::util::json::{num, to_number};
use serde_json::{json, Map, Value};

fn load(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/fixtures/geo").join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!("missing fixture {}: run `node rust/tools/fixtures/geo.mjs` from the repo root", path.display())
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

/// Fixture float: numbers, or "NaN" / "Infinity" / "-Infinity" / "-0".
fn fnum(value: &Value) -> f64 {
    match value {
        Value::Number(number) => number.as_f64().unwrap(),
        Value::String(text) => match text.as_str() {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            "-0" => -0.0,
            other => panic!("unexpected float string {other}"),
        },
        other => panic!("unexpected float value {other}"),
    }
}

/// The fixture encoding of a float.
fn enc(value: f64) -> Value {
    if value.is_nan() {
        json!("NaN")
    } else if value == f64::INFINITY {
        json!("Infinity")
    } else if value == f64::NEG_INFINITY {
        json!("-Infinity")
    } else if value == 0.0 && value.is_sign_negative() {
        json!("-0")
    } else {
        num(value)
    }
}

fn int(value: &Value) -> i64 {
    value.as_f64().unwrap() as i64
}

fn same_bits(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits()
}

/// Tolerances for values downstream of `sin`/`cos`/`tan`/`atan`/`atan2`/
/// `asin`/`asinh`, whose platform results differ from V8's by 1-2 ulp on
/// some machines (see `primitives_match_v8`). Everything else is compared
/// bit for bit.
///
/// - `TRIG_RELATIVE`: the normal case.
/// - `TRIG_ABSOLUTE`: values produced by cancellation (a projection ratio
///   clamped near 0 gives `1.6e-9` vs `0`), in meters / degrees / ratios.
/// - `ANTIPODAL_METERS`: `asin(sqrt(a))` with `a` within ulps of 1 turns a
///   1-ulp input difference into ~2e-8 relative (~0.2 m) for near-antipodal
///   distances (> 19,000 km).
const TRIG_RELATIVE: f64 = 1e-9;
const TRIG_ABSOLUTE: f64 = 1e-6;
const ANTIPODAL_METERS: f64 = 1.0;
const ANTIPODAL_THRESHOLD: f64 = 1.9e7;

/// Comparison context: exact contexts demand bit-identical numbers; trig
/// contexts allow the tolerances above. Counts how many numbers were
/// compared and how many were not bit-identical.
#[derive(Default)]
struct Cmp {
    trig: bool,
    compared: usize,
    inexact: usize,
    antipodal: usize,
}

impl Cmp {
    fn exact() -> Self {
        Cmp::default()
    }

    fn trig() -> Self {
        Cmp { trig: true, ..Cmp::default() }
    }

    /// Structural JSON comparison; object keys must match in order.
    fn json(&mut self, path: &str, expected: &Value, actual: &Value) {
        let special = |value: &Value| matches!(value.as_str(), Some("NaN" | "Infinity" | "-Infinity" | "-0"));
        let is_num = |value: &Value| value.is_number() || special(value);
        if is_num(expected) && is_num(actual) {
            let (e, a) = (fnum(expected), fnum(actual));
            self.compared += 1;
            if same_bits(e, a) {
                return;
            }
            self.inexact += 1;
            let scale = e.abs().max(a.abs());
            let diff = (e - a).abs();
            let within = diff <= TRIG_RELATIVE * scale + TRIG_ABSOLUTE;
            let antipodal = !within && scale > ANTIPODAL_THRESHOLD && diff <= ANTIPODAL_METERS;
            self.antipodal += antipodal as usize;
            assert!(
                self.trig && (within || antipodal),
                "{path}: expected {e:?} ({:#x}), got {a:?} ({:#x})",
                e.to_bits(),
                a.to_bits()
            );
            return;
        }
        match (expected, actual) {
            (Value::Array(e), Value::Array(a)) => {
                assert_eq!(e.len(), a.len(), "{path}: array length");
                for (index, (x, y)) in e.iter().zip(a).enumerate() {
                    self.json(&format!("{path}[{index}]"), x, y);
                }
            }
            (Value::Object(e), Value::Object(a)) => {
                let ek: Vec<_> = e.keys().collect();
                let ak: Vec<_> = a.keys().collect();
                assert_eq!(ek, ak, "{path}: object keys");
                for (key, x) in e {
                    self.json(&format!("{path}.{key}"), x, &a[key]);
                }
            }
            _ => assert_eq!(expected, actual, "{path}"),
        }
    }

    fn summary(&self) -> String {
        format!(
            "{} numbers, {} not bit-identical ({} beyond 1e-9 relative, near-antipodal)",
            self.compared, self.inexact, self.antipodal
        )
    }
}

fn assert_json(path: &str, expected: &Value, actual: &Value) {
    Cmp::exact().json(path, expected, actual);
}

// ------------------------------------------------------------ primitives

#[test]
fn primitives_match_v8() {
    let cases = load("primitives.json");
    let cases = cases.as_array().unwrap();
    let names = ["sin", "cos", "tan", "atan", "atan2", "asin", "asinh", "sqrt", "pow2"];
    let mut mismatches = [0usize; 9];
    let mut worst = [0f64; 9];
    for case in cases {
        let x = fnum(&case["x"]);
        let y = fnum(&case["y"]);
        let half = fnum(&case["half"]);
        let unit = fnum(&case["unit"]);
        let got = [
            x.sin(),
            x.cos(),
            half.tan(),
            x.atan(),
            y.atan2(x),
            unit.asin(),
            x.asinh(),
            x.abs().sqrt(),
            x * x,
        ];
        for (index, name) in names.iter().enumerate() {
            let want = fnum(&case[*name]);
            if !same_bits(want, got[index]) {
                mismatches[index] += 1;
                let rel = ((want - got[index]) / want).abs();
                worst[index] = worst[index].max(rel);
            }
        }
        assert!(same_bits(fnum(&case["pow2"]), fnum(&case["mul2"])), "V8 x ** 2 != x * x for {x}");
    }
    let report: Vec<String> = names
        .iter()
        .enumerate()
        .map(|(index, name)| format!("{name}: {} / {} differ (max rel {:e})", mismatches[index], cases.len(), worst[index]))
        .collect();
    eprintln!("libm vs V8 on this machine:\n  {}", report.join("\n  "));
    // IEEE-exact operations must agree everywhere.
    assert_eq!(mismatches[7], 0, "{}", report[7]);
    assert_eq!(mismatches[8], 0, "{}", report[8]);
    // Transcendentals may differ in the last ulp or two (platform libm vs
    // V8's fdlibm port), never by more.
    for (index, name) in names.iter().enumerate().take(7) {
        assert!(worst[index] <= 5e-16, "{name}: {}", report[index]);
    }
}

// ------------------------------------------------------------------ math

fn box_json(b: &GeoBox) -> Value {
    json!({ "minLatE7": b.min_lat_e7, "maxLatE7": b.max_lat_e7, "minLonE7": b.min_lon_e7, "maxLonE7": b.max_lon_e7 })
}

fn box_of(value: &Value) -> GeoBox {
    GeoBox::new(int(&value["minLatE7"]), int(&value["maxLatE7"]), int(&value["minLonE7"]), int(&value["maxLonE7"]))
}

#[test]
fn e7_conversion_matches_js() {
    let math = load("math.json");
    let cases = math["toE7"].as_array().unwrap();
    for case in cases {
        let input = &case["input"];
        let lat = case["lat"].as_i64();
        let lon = case["lon"].as_i64();
        assert_eq!(lat_to_e7(input), lat, "latToE7({input})");
        assert_eq!(lon_to_e7(input), lon, "lonToE7({input})");
        if let Some(n) = input.as_f64() {
            assert_eq!(lat_to_e7_f64(n), lat, "latToE7({n})");
            assert_eq!(lon_to_e7_f64(n), lon, "lonToE7({n})");
        }
    }
    eprintln!("toE7 cases: {}", cases.len());
}

#[test]
fn haversine_matches_js() {
    let math = load("math.json");
    let cases = math["haversine"].as_array().unwrap();
    let mut cmp = Cmp::trig();
    for (index, case) in cases.iter().enumerate() {
        let d: Vec<f64> = case["deg"].as_array().unwrap().iter().map(fnum).collect();
        let e: Vec<f64> = case["e7"].as_array().unwrap().iter().map(fnum).collect();
        cmp.json(&format!("haversine[{index}].deg"), &case["degOut"], &enc(haversine_meters(d[0], d[1], d[2], d[3])));
        cmp.json(&format!("haversine[{index}].e7"), &case["e7Out"], &enc(haversine_meters_e7(e[0], e[1], e[2], e[3])));
    }
    eprintln!("haversine cases: {} ({})", cases.len(), cmp.summary());
}

#[test]
fn radius_boxes_match_js() {
    let math = load("math.json");
    let cases = math["radius"].as_array().unwrap();
    for (index, case) in cases.iter().enumerate() {
        let radius = if case["raw"] == json!(true) { to_number(&case["radius"]) } else { fnum(&case["radius"]) };
        let boxes = boxes_for_radius_e7(int(&case["latE7"]), int(&case["lonE7"]), radius);
        let actual = Value::Array(boxes.iter().map(box_json).collect());
        assert_json(&format!("radius[{index}]"), &case["out"], &actual);
    }
    eprintln!("radius cases: {}", cases.len());
}

#[test]
fn box_predicates_and_distances_match_js() {
    let math = load("math.json");
    let cases = math["boxes"].as_array().unwrap();
    let mut cmp = Cmp::trig();
    for (index, case) in cases.iter().enumerate() {
        let a = box_of(&case["a"]);
        let b = box_of(&case["b"]);
        let lat = int(&case["latE7"]);
        let lon = int(&case["lonE7"]);
        assert_eq!(box_intersects_e7(&a, &b), case["intersects"].as_bool().unwrap(), "boxes[{index}] intersects");
        assert_eq!(box_contains_box_e7(&a, &b), case["contains"].as_bool().unwrap(), "boxes[{index}] contains");
        assert_eq!(box_contains_point_e7(&a, lat, lon), case["containsPoint"].as_bool().unwrap(), "boxes[{index}] point");
        cmp.json(&format!("boxes[{index}].minDist"), &case["minDist"], &enc(point_to_box_distance_meters_e7(lat, lon, &a)));
        cmp.json(&format!("boxes[{index}].maxDist"), &case["maxDist"], &enc(point_to_box_max_distance_meters_e7(lat, lon, &a)));
    }
    eprintln!("box cases: {} ({})", cases.len(), cmp.summary());
}

// ------------------------------------------------------------------ tree

fn summaries_json(filters: &Option<FilterSummaries>) -> Value {
    let Some(filters) = filters else { return Value::Null };
    let mut out = Map::new();
    for (name, summary) in filters {
        let value = match summary {
            BlockFilterSummary::Facet { words } => json!({ "words": words }),
            BlockFilterSummary::Number { min, max } => {
                json!({ "min": min.map(num).unwrap_or(Value::Null), "max": max.map(num).unwrap_or(Value::Null) })
            }
            BlockFilterSummary::Range { min, max } => json!({ "min": min, "max": max }),
        };
        out.insert(name.clone(), value);
    }
    Value::Object(out)
}

fn pointer_fields(out: &mut Map<String, Value>, pointer: &Pointer, physical_length: u64) {
    out.insert("pack".into(), json!(&*pointer.pack));
    out.insert("offset".into(), json!(pointer.offset));
    out.insert("length".into(), json!(pointer.length));
    out.insert("physicalLength".into(), json!(physical_length));
    out.insert("logicalLength".into(), pointer.logical_length.map(|n| json!(n)).unwrap_or(Value::Null));
    out.insert(
        "checksum".into(),
        pointer
            .checksum
            .as_ref()
            .map(|c| json!({ "algorithm": &*c.algorithm, "value": &*c.value }))
            .unwrap_or(Value::Null),
    );
}

fn bbox_fields(out: &mut Map<String, Value>, b: &GeoBox) {
    out.insert("minLatE7".into(), json!(b.min_lat_e7));
    out.insert("maxLatE7".into(), json!(b.max_lat_e7));
    out.insert("minLonE7".into(), json!(b.min_lon_e7));
    out.insert("maxLonE7".into(), json!(b.max_lon_e7));
}

fn leaf_entry_json(leaf: &GeoLeafEntry) -> Value {
    let mut out = Map::new();
    out.insert("index".into(), json!(leaf.index));
    bbox_fields(&mut out, &leaf.bbox);
    out.insert("count".into(), json!(leaf.count));
    pointer_fields(&mut out, &leaf.pointer, leaf.physical_length);
    out.insert("filters".into(), summaries_json(&leaf.filters));
    Value::Object(out)
}

fn branch_entry_json(branch: &GeoBranchEntry) -> Value {
    let mut out = Map::new();
    out.insert("index".into(), json!(branch.index));
    bbox_fields(&mut out, &branch.bbox);
    out.insert("count".into(), json!(branch.count));
    out.insert("firstLeafIndex".into(), json!(branch.first_leaf_index));
    out.insert("leafCount".into(), json!(branch.leaf_count));
    pointer_fields(&mut out, &branch.pointer, branch.physical_length);
    out.insert("filters".into(), summaries_json(&branch.filters));
    Value::Object(out)
}

fn root_json(root: &GeoTreeRoot) -> Value {
    json!({
        "format": root.format,
        "pageFormat": root.page_format,
        "field": root.field,
        "total": root.total,
        "leafSize": root.leaf_size,
        "leafCount": root.leaf_count,
        "levels": root.levels,
        "hasSummaries": root.has_summaries,
        "bbox": box_json(&root.bbox),
        "packTable": root.pack_table.iter().map(|pack| &**pack).collect::<Vec<_>>(),
        "leaves": root.leaves.as_ref().map(|leaves| leaves.iter().map(leaf_entry_json).collect::<Vec<_>>()),
        "branches": root.branches.as_ref().map(|items| items.iter().map(branch_entry_json).collect::<Vec<_>>()),
    })
}

fn branch_page_json(page: &GeoBranchPage) -> Value {
    json!({
        "field": page.field,
        "branchIndex": page.branch_index,
        "firstLeafIndex": page.first_leaf_index,
        "bbox": box_json(&page.bbox),
        "leaves": page.leaves.iter().map(leaf_entry_json).collect::<Vec<_>>(),
    })
}

fn leaf_page_json(page: &GeoLeafPage) -> Value {
    let mut out = Map::new();
    out.insert("field".into(), json!(page.field));
    out.insert("count".into(), json!(page.count));
    bbox_fields(&mut out, &page.bbox);
    out.insert("latsE7".into(), json!(page.lats_e7));
    out.insert("lonsE7".into(), json!(page.lons_e7));
    out.insert("docs".into(), json!(page.docs));
    out.insert("capsuleFields".into(), json!(page.capsule_fields));
    out.insert(
        "capsules".into(),
        page.capsules
            .as_ref()
            .map(|rows| Value::Array(rows.iter().cloned().map(Value::Object).collect()))
            .unwrap_or(Value::Null),
    );
    Value::Object(out)
}

fn pack_table_of(value: &Value) -> Vec<Rc<str>> {
    value.as_array().unwrap().iter().map(|pack| Rc::from(pack.as_str().unwrap())).collect()
}

fn expected_name(value: &Value) -> Option<&str> {
    value.as_str()
}

#[test]
fn tree_pages_match_js() {
    let tree = load("tree.json");
    let roots = tree["roots"].as_array().unwrap();
    for (index, case) in roots.iter().enumerate() {
        let filters = BlockFilter::list_from_manifest(&case["filters"]);
        let root = parse_geo_tree_root(&b64(case["bytes"].as_str().unwrap()), &filters).unwrap();
        assert_json(&format!("roots[{index}]"), &case["out"], &root_json(&root));
    }
    let branches = tree["branchPages"].as_array().unwrap();
    for (index, case) in branches.iter().enumerate() {
        let filters = BlockFilter::list_from_manifest(&case["filters"]);
        let page = decode_geo_branch_page(
            &b64(case["bytes"].as_str().unwrap()),
            &pack_table_of(&case["packTable"]),
            &filters,
            expected_name(&case["expected"]),
        )
        .unwrap();
        assert_json(&format!("branchPages[{index}]"), &case["out"], &branch_page_json(&page));
    }
    let leaves = tree["leafPages"].as_array().unwrap();
    let mut capsule_pages = 0;
    for (index, case) in leaves.iter().enumerate() {
        let page = decode_geo_leaf_page(&b64(case["bytes"].as_str().unwrap()), expected_name(&case["expected"])).unwrap();
        capsule_pages += page.capsules.is_some() as usize;
        let actual = leaf_page_json(&page);
        assert_json(&format!("leafPages[{index}]"), &case["out"], &actual);
        // Key order inside capsules, serialized.
        assert_eq!(
            serde_json::to_string(&case["out"]["capsules"]).unwrap(),
            serde_json::to_string(&actual["capsules"]).unwrap(),
            "leafPages[{index}] capsules"
        );
    }
    assert!(capsule_pages > 5, "fixture should exercise capsule pages");
    let errors = tree["errors"].as_array().unwrap();
    for (index, case) in errors.iter().enumerate() {
        let bytes = b64(case["bytes"].as_str().unwrap());
        let filters = BlockFilter::list_from_manifest(&case["filters"]);
        let result = match case["kind"].as_str().unwrap() {
            "root" => parse_geo_tree_root(&bytes, &filters).map(|_| ()),
            "branch" => {
                decode_geo_branch_page(&bytes, &pack_table_of(&case["packTable"]), &filters, expected_name(&case["expected"]))
                    .map(|_| ())
            }
            _ => decode_geo_leaf_page(&bytes, expected_name(&case["expected"])).map(|_| ()),
        };
        match case["out"]["error"].as_str() {
            Some(message) => assert_eq!(result.err().map(|e| e.message().to_string()).as_deref(), Some(message), "errors[{index}]"),
            None => assert!(result.is_ok(), "errors[{index}]: expected success, got {result:?}"),
        }
    }
    eprintln!(
        "tree: {} roots, {} branch pages, {} leaf pages ({capsule_pages} with capsules), {} error cases",
        roots.len(),
        branches.len(),
        leaves.len(),
        errors.len()
    );
}

// ----------------------------------------------------------------- cells

fn cell_json(cell: &GeoCell) -> Value {
    json!({ "zoom": cell.zoom, "x": cell.x, "y": cell.y })
}

fn cell_of(value: &Value) -> GeoCell {
    GeoCell { zoom: int(&value["zoom"]) as u32, x: int(&value["x"]) as u32, y: int(&value["y"]) as u32 }
}

fn cells_json(cells: &Option<Vec<GeoCell>>) -> Value {
    cells.as_ref().map(|cells| Value::Array(cells.iter().map(cell_json).collect())).unwrap_or(Value::Null)
}

#[test]
fn cells_match_js() {
    let cells = load("cells.json");
    for (index, case) in cells["cellForE7"].as_array().unwrap().iter().enumerate() {
        let cell = geo_cell_for_e7(fnum(&case["latE7"]), fnum(&case["lonE7"]), fnum(&case["zoom"]));
        assert_json(&format!("cellForE7[{index}]"), &case["out"], &cell_json(&cell));
    }
    for (index, case) in cells["cellBlock"].as_array().unwrap().iter().enumerate() {
        let block = geo_cell_block(&cell_of(&case["cell"]), fnum(&case["blockZoom"]));
        assert_json(&format!("cellBlock[{index}]"), &case["out"], &cell_json(&block));
    }
    for (index, case) in cells["blockKeys"].as_array().unwrap().iter().enumerate() {
        let key = geo_cell_block_key(
            case["field"].as_str().unwrap(),
            case["facet"].as_str().unwrap(),
            int(&case["group"]),
            &cell_of(&case["block"]),
        );
        assert_eq!(key, case["out"].as_str().unwrap(), "blockKeys[{index}]");
    }
    for (index, case) in cells["routeKeys"].as_array().unwrap().iter().enumerate() {
        let args: Vec<u64> = case["args"].as_array().unwrap().iter().map(|v| v.as_u64().unwrap()).collect();
        assert_eq!(geo_cell_route_key(args[0], args[1], args[2], args[3]), case["out"].as_str().unwrap(), "routeKeys[{index}]");
    }
    let for_boxes = cells["forBoxes"].as_array().unwrap();
    for (index, case) in for_boxes.iter().enumerate() {
        let boxes: Vec<GeoBox> = case["boxes"].as_array().unwrap().iter().map(box_of).collect();
        let out = geo_cells_for_boxes(&boxes, int(&case["zoom"]) as u32, fnum(&case["limit"]));
        assert_json(&format!("forBoxes[{index}]"), &case["out"], &cells_json(&out));
    }
    let blocks = cells["blocks"].as_array().unwrap();
    for (index, case) in blocks.iter().enumerate() {
        let block = decode_geo_cell_block(&b64(case["bytes"].as_str().unwrap())).unwrap();
        let routes: Vec<Value> = block
            .routes
            .iter()
            .map(|route| {
                let members: Vec<Value> = route.members.iter().map(|m| json!([m.leaf, m.ordinals])).collect();
                json!([route.key, members])
            })
            .collect();
        let actual = json!({
            "format": block.format,
            "blockZoom": block.block_zoom,
            "blockX": block.block_x,
            "blockY": block.block_y,
            "routes": routes,
        });
        assert_json(&format!("blocks[{index}]"), &case["out"], &actual);
        for route in &block.routes {
            assert_eq!(block.route(&route.key), Some(route));
        }
        assert_eq!(block.route("nope"), None);
    }
    let errors = cells["blockErrors"].as_array().unwrap();
    for (index, case) in errors.iter().enumerate() {
        let result = decode_geo_cell_block(&b64(case["bytes"].as_str().unwrap()));
        match case["out"]["error"].as_str() {
            Some(message) => assert_eq!(result.err().map(|e| e.message().to_string()).as_deref(), Some(message), "blockErrors[{index}]"),
            None => assert!(result.is_ok(), "blockErrors[{index}]"),
        }
    }
    eprintln!(
        "cells: {} cellForE7, {} cellBlock, {} blockKeys, {} forBoxes, {} blocks, {} block errors",
        cells["cellForE7"].as_array().unwrap().len(),
        cells["cellBlock"].as_array().unwrap().len(),
        cells["blockKeys"].as_array().unwrap().len(),
        for_boxes.len(),
        blocks.len(),
        errors.len()
    );
}

// ----------------------------------------------------------------- route

fn point_json(point: &RoutePoint) -> Value {
    json!({ "lat": enc(point.lat), "lon": enc(point.lon) })
}

fn prepared_json(route: &PreparedRoute) -> Value {
    json!({
        "points": route.points.iter().map(point_json).collect::<Vec<_>>(),
        "segments": route.segments.iter().map(|s| json!({
            "index": s.index,
            "start": point_json(&s.start),
            "end": point_json(&s.end),
            "startMeters": enc(s.start_meters),
            "lengthMeters": enc(s.length_meters),
            "bearingDegrees": enc(s.bearing_degrees),
        })).collect::<Vec<_>>(),
        "totalMeters": enc(route.total_meters),
        "corridorMeters": enc(route.corridor_meters),
        "boxes": route.boxes.iter().map(box_json).collect::<Vec<_>>(),
    })
}

fn match_json(found: &Option<RouteMatch>) -> Value {
    let Some(m) = found else { return Value::Null };
    json!({
        "distanceMeters": enc(m.distance_meters),
        "progressMeters": enc(m.progress_meters),
        "progressRatio": enc(m.progress_ratio),
        "segmentIndex": m.segment_index,
        "segmentProgress": enc(m.segment_progress),
        "bearingDegrees": enc(m.bearing_degrees),
        "rejoinPoint": point_json(&m.rejoin_point),
    })
}

#[test]
fn routes_match_js() {
    let routes = load("route.json");
    let routes = routes.as_array().unwrap();
    let (mut prepared_count, mut match_count, mut cell_count) = (0, 0, 0);
    let (mut exact, mut trig) = (Cmp::exact(), Cmp::trig());
    let mut vertex_ties = 0;
    for (index, case) in routes.iter().enumerate() {
        let result = prepare_route(&case["input"]["route"], &case["input"]["options"]);
        let prepared = match case["error"].as_str() {
            Some(message) => {
                assert_eq!(result.err().map(|e| e.message().to_string()).as_deref(), Some(message), "routes[{index}]");
                continue;
            }
            None => result.unwrap_or_else(|e| panic!("routes[{index}]: {e}")),
        };
        prepared_count += 1;
        let actual = prepared_json(&prepared);
        let expected = &case["prepared"];
        for key in ["points", "boxes", "corridorMeters"] {
            exact.json(&format!("routes[{index}].prepared.{key}"), &expected[key], &actual[key]);
        }
        for key in ["segments", "totalMeters"] {
            trig.json(&format!("routes[{index}].prepared.{key}"), &expected[key], &actual[key]);
        }
        for (q, m) in case["matches"].as_array().unwrap().iter().enumerate() {
            match_count += 1;
            let result = match_point_to_route(&prepared, fnum(&m["lat"]), fnum(&m["lon"]));
            match m["error"].as_str() {
                Some(message) => {
                    assert_eq!(result.err().map(|e| e.message().to_string()).as_deref(), Some(message), "routes[{index}].matches[{q}]");
                }
                None => {
                    let found = result.unwrap();
                    let public = route_match_public(found.as_ref());
                    let (expected, actual) = (&m["match"], match_json(&found));
                    if expected["segmentIndex"] != actual["segmentIndex"] {
                        // Nearest position is a shared vertex (segment i at
                        // its end, segment i+1 at its start, or a degenerate
                        // segment): candidates tie within an ulp and the
                        // winner flips. Same place, same distance and progress.
                        vertex_ties += 1;
                        let (ei, ai) = (int(&expected["segmentIndex"]), int(&actual["segmentIndex"]));
                        assert_eq!((ei - ai).abs(), 1, "routes[{index}].matches[{q}]: non-adjacent segment flip");
                        let path = format!("routes[{index}].matches[{q}].tie");
                        for key in ["distanceMeters", "progressMeters", "progressRatio"] {
                            trig.json(&format!("{path}.{key}"), &expected[key], &actual[key]);
                        }
                        trig.json(&format!("{path}.lat"), &expected["rejoinPoint"]["lat"], &actual["rejoinPoint"]["lat"]);
                        // The shared vertex may sit on the antimeridian (+180 vs -180).
                        let (el, mut al) = (fnum(&expected["rejoinPoint"]["lon"]), fnum(&actual["rejoinPoint"]["lon"]));
                        if (el - al).abs() > 180.0 {
                            al += if el > al { 360.0 } else { -360.0 };
                        }
                        trig.json(&format!("{path}.lon"), &enc(el), &enc(al));
                        continue;
                    }
                    trig.json(&format!("routes[{index}].matches[{q}].match"), expected, &actual);
                    trig.json(&format!("routes[{index}].matches[{q}].public"), &m["public"], &match_json(&public));
                }
            }
        }
        for (c, cells) in case["cells"].as_array().unwrap().iter().enumerate() {
            cell_count += 1;
            let out = geo_cells_for_route(&prepared, int(&cells["zoom"]) as u32, fnum(&cells["limit"]));
            exact.json(&format!("routes[{index}].cells[{c}]"), &cells["out"], &cells_json(&out));
        }
    }
    eprintln!(
        "routes: {} inputs ({prepared_count} prepared), {match_count} matches ({vertex_ties} vertex ties), {cell_count} route cell sets; exact: {}; trig: {}",
        routes.len(),
        exact.summary(),
        trig.summary()
    );
}

#[test]
fn polyline_and_geometry_helpers() {
    let points = decode_polyline("_p~iF~ps|U_ulLnnqC_mqNvxq`@", 5.0).unwrap();
    assert_eq!(points.len(), 3);
    assert_eq!(points[0], RoutePoint { lat: 38.5, lon: -120.2 });
    assert_eq!(route_match_public(None), None);
    let normalized = normalize_route_geometry(&json!([[1, 2], [1, 2], [3, 4]]), &Value::Null).unwrap();
    assert_eq!(normalized, vec![RoutePoint { lat: 2.0, lon: 1.0 }, RoutePoint { lat: 4.0, lon: 3.0 }]);
    let error = normalize_route_geometry(&json!([null, [3, 4]]), &Value::Null).unwrap_err();
    assert_eq!(error.message(), "Cannot read properties of null (reading 'lat')");
}

// ----------------------------------------------------------------- shard

fn bbox4(value: &Value) -> [f64; 4] {
    let items = value.as_array().unwrap();
    [fnum(&items[0]), fnum(&items[1]), fnum(&items[2]), fnum(&items[3])]
}

#[test]
fn shard_routing_matches_js() {
    let shard = load("shard.json");
    let distance = shard["distance"].as_array().unwrap();
    let mut cmp = Cmp::trig();
    for (index, case) in distance.iter().enumerate() {
        let out = shard_bbox_distance_meters(&bbox4(&case["bbox"]), fnum(&case["lat"]), fnum(&case["lon"]));
        cmp.json(&format!("distance[{index}]"), &case["out"], &enc(out));
    }
    let intersects = shard["intersects"].as_array().unwrap();
    for (index, case) in intersects.iter().enumerate() {
        let b = &case["box"];
        let query = LatLonBox {
            min_lat: fnum(&b["minLat"]),
            max_lat: fnum(&b["maxLat"]),
            min_lon: fnum(&b["minLon"]),
            max_lon: fnum(&b["maxLon"]),
        };
        assert_eq!(shard_box_intersects(&bbox4(&case["bbox"]), &query), case["out"].as_bool().unwrap(), "intersects[{index}]");
    }
    eprintln!("shard: {} distances ({}), {} intersections", distance.len(), cmp.summary(), intersects.len());
}
