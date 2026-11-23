//! Object pointers (`rfbp-v1`) and the range-merge planner
//! (`groupRanges` in `src/shards.js`).

use std::rc::Rc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checksum {
    pub algorithm: Rc<str>,
    pub value: Rc<str>,
}

impl Checksum {
    pub fn sha256(value: impl AsRef<str>) -> Self {
        Checksum { algorithm: Rc::from("sha256"), value: Rc::from(value.as_ref()) }
    }
}

/// A byte span inside a pack file, optionally checksummed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pointer {
    pub pack: Rc<str>,
    pub offset: u64,
    pub length: u64,
    pub logical_length: Option<u64>,
    pub checksum: Option<Checksum>,
    /// Row in the doc-values checksum sidecar (`rfdvm-v1`), when the
    /// checksum is stored out of line.
    pub checksum_row: Option<u64>,
}

impl Pointer {
    pub fn new(pack: impl AsRef<str>, offset: u64, length: u64) -> Self {
        Pointer { pack: Rc::from(pack.as_ref()), offset, length, ..Default::default() }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RangePlan {
    pub merge_gap_bytes: f64,
    pub max_merged_bytes: f64,
    pub max_overfetch_bytes: f64,
    pub max_overfetch_ratio: f64,
}

impl RangePlan {
    pub const fn new(merge_gap_bytes: f64, max_overfetch_bytes: f64, max_overfetch_ratio: f64) -> Self {
        RangePlan { merge_gap_bytes, max_merged_bytes: f64::INFINITY, max_overfetch_bytes, max_overfetch_ratio }
    }

    pub const fn with_max_merged(mut self, max_merged_bytes: f64) -> Self {
        self.max_merged_bytes = max_merged_bytes;
        self
    }
}

pub struct RangeGroup<T> {
    pub pack: Rc<str>,
    pub start: u64,
    pub end: u64,
    pub items: Vec<(Pointer, T)>,
}

/// Merge nearby byte ranges of the same pack into single requests when the
/// extra transfer stays inside the plan's overfetch bounds. Packs keep
/// first-seen order and items sort by offset, exactly like the JS planner.
pub fn group_ranges<T>(items: Vec<(Pointer, T)>, plan: &RangePlan) -> Vec<RangeGroup<T>> {
    let mut packs: Vec<(Rc<str>, Vec<(Pointer, T)>)> = Vec::new();
    for item in items {
        match packs.iter_mut().find(|(pack, _)| **pack == *item.0.pack) {
            Some((_, list)) => list.push(item),
            None => packs.push((item.0.pack.clone(), vec![item])),
        }
    }
    let mut groups: Vec<RangeGroup<T>> = Vec::new();
    for (pack, mut list) in packs {
        list.sort_by_key(|(pointer, _)| pointer.offset);
        let mut exact_bytes = 0f64;
        let mut open = false;
        for item in list {
            let start = item.0.offset;
            let end = item.0.offset + item.0.length;
            let item_bytes = (end - start) as f64;
            if open {
                let current = groups.last_mut().expect("open group");
                let next_end = current.end.max(end);
                let merged = (next_end - current.start) as f64;
                let exact = exact_bytes + item_bytes;
                let overfetch = merged - exact;
                let should_merge = (start as f64) <= current.end as f64 + plan.merge_gap_bytes
                    && merged <= plan.max_merged_bytes
                    && overfetch <= plan.max_overfetch_bytes
                    && merged <= exact * plan.max_overfetch_ratio;
                if should_merge {
                    current.items.push(item);
                    current.end = next_end;
                    exact_bytes = exact;
                    continue;
                }
            }
            groups.push(RangeGroup { pack: pack.clone(), start, end, items: vec![item] });
            exact_bytes = item_bytes;
            open = true;
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_within_bounds() {
        let plan = RangePlan::new(8.0, 64.0, 4.0);
        let items = vec![
            (Pointer::new("b", 0, 10), 0),
            (Pointer::new("a", 100, 10), 1),
            (Pointer::new("a", 0, 10), 2),
            (Pointer::new("a", 14, 10), 3),
        ];
        let groups = group_ranges(items, &plan);
        assert_eq!(groups.len(), 3);
        assert_eq!(&*groups[0].pack, "b");
        assert_eq!((groups[1].start, groups[1].end), (0, 24));
        assert_eq!(groups[1].items.iter().map(|item| item.1).collect::<Vec<_>>(), vec![2, 3]);
        assert_eq!((groups[2].start, groups[2].end), (100, 110));
    }
}
