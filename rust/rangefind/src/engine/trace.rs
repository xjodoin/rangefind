//! Request accounting. The JS runtime's `trace` option reports per-bucket
//! request counts and bytes; the Rust engine keeps the same numbers per
//! engine so hosts and benchmarks can compare transfer cost across ports.

use std::cell::RefCell;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BucketStats {
    pub requests: u64,
    pub ranges: u64,
    pub bytes: u64,
}

#[derive(Default)]
pub struct Trace {
    buckets: RefCell<BTreeMap<String, BucketStats>>,
}

/// `traceBucketFromPath`: the first meaningful path segment(s) of an index
/// object, e.g. `terms.packs`, `docs.pages`, `manifest`.
pub fn bucket_for(url: &str) -> String {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    let anchors = [
        "terms", "docs", "doc-values", "facets", "authority", "geo", "vectors", "bundles", "filter-bitmaps", "segments", "sort-replicas", "suggest",
    ];
    for (index, part) in parts.iter().enumerate() {
        if anchors.contains(part) {
            let next = parts.get(index + 1).copied().unwrap_or("");
            let is_file = next.contains('.') || next.is_empty();
            return if is_file { (*part).to_string() } else { format!("{part}.{next}") };
        }
    }
    match parts.last() {
        Some(last) if last.starts_with("manifest") => "manifest".to_string(),
        Some(last) if last.starts_with("codes") => "codes".to_string(),
        _ => "other".to_string(),
    }
}

impl Trace {
    pub fn request(&self, url: &str, ranges: usize) {
        let mut buckets = self.buckets.borrow_mut();
        let entry = buckets.entry(bucket_for(url)).or_default();
        entry.requests += 1;
        entry.ranges += ranges as u64;
    }

    pub fn bytes(&self, url: &str, bytes: usize) {
        self.buckets.borrow_mut().entry(bucket_for(url)).or_default().bytes += bytes as u64;
    }

    pub fn snapshot(&self) -> BTreeMap<String, BucketStats> {
        self.buckets.borrow().clone()
    }

    pub fn totals(&self) -> BucketStats {
        let mut total = BucketStats::default();
        for stats in self.buckets.borrow().values() {
            total.requests += stats.requests;
            total.ranges += stats.ranges;
            total.bytes += stats.bytes;
        }
        total
    }

    pub fn reset(&self) {
        self.buckets.borrow_mut().clear();
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut out = serde_json::Map::new();
        for (bucket, stats) in self.buckets.borrow().iter() {
            out.insert(
                bucket.clone(),
                serde_json::json!({ "requests": stats.requests, "ranges": stats.ranges, "bytes": stats.bytes }),
            );
        }
        serde_json::Value::Object(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets() {
        assert_eq!(bucket_for("https://x/rangefind/terms/packs/0000.ab.bin.gz"), "terms.packs");
        assert_eq!(bucket_for("/idx/terms/directory-root.ab.bin.gz"), "terms");
        assert_eq!(bucket_for("/idx/manifest.min.json"), "manifest");
        assert_eq!(bucket_for("/idx/doc-values/packs/1.bin"), "doc-values.packs");
    }
}
