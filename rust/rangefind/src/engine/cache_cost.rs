//! [`CacheCost`] estimates for the decoded objects the engine caches.
//!
//! Each estimate is computed once, when the entry settles, from the lengths
//! of the vectors and buffers the object owns (`size_of` of the element
//! times the capacity, plus string lengths and a flat allowance for hash
//! indexes). They are deliberately rough and on the high side: the budget
//! needs relative sizes and a sane total, not allocator-exact numbers.

use std::mem::size_of;

use crate::authority::codec::{AuthorityEntry, AuthorityShard};
use crate::authority::lexicon::{AuthorityLexiconSegment, AuthorityLexiconSegmentEntry, HotListDoc, HotListEntry};
use crate::budget::{json_cost, CacheCost};
use crate::codec::directory::DirectoryPage;
use crate::codec::doc_value_tree::{DocValueSortDirectory, SortPage, SortPageMeta, SortRow};
use crate::codec::doc_values::{DocValue, DocValueChunk, FacetValue};
use crate::codec::filter_bitmaps::FilterBitmap;
use crate::codec::postings::{DocRangeBound, PostingSegment, TermEntry};
use crate::codec::query_bundle::QueryBundle;
use crate::codec::segment::{SegmentTermEntry, SegmentTerms};
use crate::codec::text_routing::TextRoutingSegment;
use crate::geo::cells::{GeoCellBlock, GeoCellMember, GeoCellRoute};
use crate::geo::tree::{GeoBranchEntry, GeoBranchPage, GeoLeafEntry, GeoLeafPage, GeoTreeRoot};
use crate::pointer::Pointer;
use crate::vector::{VectorCluster, VectorClusterPage, VectorRoot};

/// Hash-index slot allowance per key (bucket, hash, `usize` value).
const INDEX_SLOT: usize = 40;
/// `Rc` header (strong + weak counts).
const RC_HEADER: usize = 16;

fn vec_bytes<T>(items: &Vec<T>) -> usize {
    items.capacity() * size_of::<T>()
}

fn string_bytes(text: &str) -> usize {
    text.len()
}

fn pointer_heap(pointer: &Pointer) -> usize {
    // Pack names are shared `Rc<str>`s; checksums are short strings.
    pointer.checksum.as_ref().map(|checksum| checksum.value.len() + 16).unwrap_or(0)
}

impl CacheCost for TermEntry {
    fn cache_cost(&self) -> usize {
        let blocks: usize = vec_bytes(&self.blocks)
            + self.blocks.iter().map(|block| block.doc_ranges.as_ref().map(vec_bytes::<DocRangeBound>).unwrap_or(0)).sum::<usize>();
        size_of::<TermEntry>()
            + RC_HEADER
            + blocks
            + vec_bytes(&self.superblocks)
            + self.doc_ranges.as_ref().map(|ranges| vec_bytes(&ranges.ranges)).unwrap_or(0)
            + self.impact_tiers.as_ref().map(|tiers| vec_bytes(&tiers.blocks) + vec_bytes(&tiers.tiers)).unwrap_or(0)
    }
}

impl CacheCost for PostingSegment {
    fn cache_cost(&self) -> usize {
        let terms: usize = self
            .terms
            .iter()
            // The term string is stored twice (term list and lookup index).
            .map(|(term, entry)| 2 * string_bytes(term) + INDEX_SLOT + entry.cache_cost())
            .sum();
        size_of::<PostingSegment>() + self.bytes.backing_len() + vec_bytes(&self.terms) + terms
    }

    const DYNAMIC: bool = true;

    /// Rows decoded into the term entries' interior caches since parsing.
    fn dynamic_cost(&self) -> usize {
        self.decoded_bytes.get()
    }
}

impl CacheCost for DirectoryPage {
    fn cache_cost(&self) -> usize {
        let keys: usize = self
            .entries
            .iter()
            .map(|(key, pointer)| 2 * string_bytes(key) + INDEX_SLOT + pointer_heap(pointer))
            .sum::<usize>()
            + self.keys.iter().map(|key| key.text.len() + key.units.as_ref().map(|units| units.len() * 2 + RC_HEADER).unwrap_or(0)).sum::<usize>();
        size_of::<DirectoryPage>() + vec_bytes(&self.entries) + vec_bytes(&self.keys) + keys
    }
}

impl CacheCost for DocValueChunk {
    fn cache_cost(&self) -> usize {
        let facets: usize = self
            .values
            .iter()
            .map(|value| match value {
                DocValue::Facet(FacetValue::Codes(codes)) | DocValue::Facet(FacetValue::Words(codes)) => vec_bytes(codes),
                _ => 0,
            })
            .sum();
        size_of::<DocValueChunk>() + self.name.len() + vec_bytes(&self.values) + facets
    }
}

impl CacheCost for DocValueSortDirectory {
    fn cache_cost(&self) -> usize {
        let pages: usize = self
            .pages
            .iter()
            .map(|page: &SortPageMeta| pointer_heap(&page.pointer) + vec_bytes(&page.summaries) + page.summaries.iter().map(|(name, _)| name.len()).sum::<usize>())
            .sum();
        size_of::<DocValueSortDirectory>()
            + vec_bytes(&self.pack_table)
            + self.pack_table.iter().map(|pack| pack.len()).sum::<usize>()
            + vec_bytes(&self.summary_fields)
            + vec_bytes(&self.pages)
            + pages
    }
}

impl CacheCost for SortPage {
    fn cache_cost(&self) -> usize {
        let facets: usize = self
            .rows
            .iter()
            .map(|row: &SortRow| match &row.value {
                DocValue::Facet(FacetValue::Codes(codes)) | DocValue::Facet(FacetValue::Words(codes)) => vec_bytes(codes),
                _ => 0,
            })
            .sum();
        size_of::<SortPage>() + vec_bytes(&self.rows) + facets
    }
}

impl CacheCost for FilterBitmap {
    fn cache_cost(&self) -> usize {
        size_of::<FilterBitmap>() + self.bytes.cache_cost()
    }
}

impl CacheCost for QueryBundle {
    fn cache_cost(&self) -> usize {
        let terms: usize = self.base_terms.iter().chain(&self.expanded_terms).map(|term| size_of::<String>() + term.len()).sum();
        let filters: usize = self
            .filter_values
            .iter()
            // BTreeMap node allowance per value.
            .map(|(field, values)| field.len() + values.len() * 64)
            .sum();
        size_of::<QueryBundle>() + self.key.len() + terms + vec_bytes(&self.rows) + vec_bytes(&self.row_groups) + filters
    }
}

impl CacheCost for SegmentTerms {
    fn cache_cost(&self) -> usize {
        let terms: usize = self.terms.iter().map(|entry: &SegmentTermEntry| 2 * entry.term.len() + INDEX_SLOT).sum();
        size_of::<SegmentTerms>() + vec_bytes(&self.terms) + terms
    }
}

impl CacheCost for TextRoutingSegment {
    fn cache_cost(&self) -> usize {
        let terms: usize = self
            .terms
            .iter()
            .map(|(key, shards)| {
                2 * key.text.len() + INDEX_SLOT + key.units.as_ref().map(|units| units.len() * 2 + RC_HEADER).unwrap_or(0) + vec_bytes(shards)
            })
            .sum();
        size_of::<TextRoutingSegment>() + vec_bytes(&self.terms) + terms
    }
}

impl CacheCost for VectorRoot {
    fn cache_cost(&self) -> usize {
        let clusters: usize = self.clusters.iter().map(|cluster: &VectorCluster| pointer_heap(&cluster.pointer)).sum();
        size_of::<VectorRoot>()
            + vec_bytes(&self.permutation)
            + vec_bytes(&self.pack_table)
            + vec_bytes(&self.centroid_scales)
            + vec_bytes(&self.centroid_codes)
            + vec_bytes(&self.clusters)
            + clusters
    }
}

impl CacheCost for VectorClusterPage {
    fn cache_cost(&self) -> usize {
        size_of::<VectorClusterPage>() + vec_bytes(&self.docs) + vec_bytes(&self.ordinals) + vec_bytes(&self.scales) + vec_bytes(&self.codes)
    }
}

impl CacheCost for GeoTreeRoot {
    fn cache_cost(&self) -> usize {
        let leaves = self
            .leaves
            .as_ref()
            .map(|leaves| vec_bytes(leaves) + leaves.iter().map(|leaf: &GeoLeafEntry| pointer_heap(&leaf.pointer)).sum::<usize>())
            .unwrap_or(0);
        let branches = self
            .branches
            .as_ref()
            // Filter summaries per branch: flat allowance.
            .map(|branches| vec_bytes(branches) + branches.iter().map(|branch: &GeoBranchEntry| pointer_heap(&branch.pointer) + if branch.filters.is_some() { 256 } else { 0 }).sum::<usize>())
            .unwrap_or(0);
        size_of::<GeoTreeRoot>() + vec_bytes(&self.pack_table) + leaves + branches
    }
}

impl CacheCost for GeoBranchPage {
    fn cache_cost(&self) -> usize {
        size_of::<GeoBranchPage>() + vec_bytes(&self.leaves) + self.leaves.iter().map(|leaf| pointer_heap(&leaf.pointer)).sum::<usize>()
    }
}

impl CacheCost for GeoLeafPage {
    fn cache_cost(&self) -> usize {
        let capsules = self
            .capsules
            .as_ref()
            .map(|rows| {
                vec_bytes(rows)
                    + rows
                        .iter()
                        .map(|row| row.iter().map(|(key, value)| key.len() + 48 + json_cost(value)).sum::<usize>())
                        .sum::<usize>()
            })
            .unwrap_or(0);
        size_of::<GeoLeafPage>()
            + vec_bytes(&self.lats_e7)
            + vec_bytes(&self.lons_e7)
            + vec_bytes(&self.docs)
            + vec_bytes(&self.capsule_fields)
            + capsules
    }
}

impl CacheCost for GeoCellBlock {
    fn cache_cost(&self) -> usize {
        let routes: usize = self
            .routes
            .iter()
            .map(|route: &GeoCellRoute| {
                2 * route.key.len()
                    + INDEX_SLOT
                    + vec_bytes(&route.members)
                    + route.members.iter().map(|member: &GeoCellMember| vec_bytes(&member.ordinals)).sum::<usize>()
            })
            .sum();
        size_of::<GeoCellBlock>() + vec_bytes(&self.routes) + routes
    }
}

impl CacheCost for AuthorityShard {
    fn cache_cost(&self) -> usize {
        let entries: usize = self.entries.iter().map(|entry: &AuthorityEntry| 2 * entry.key.len() + INDEX_SLOT + vec_bytes(&entry.rows)).sum();
        size_of::<AuthorityShard>() + vec_bytes(&self.entries) + entries
    }
}

impl CacheCost for AuthorityLexiconSegment {
    fn cache_cost(&self) -> usize {
        let entries: usize =
            self.entries.iter().map(|entry: &AuthorityLexiconSegmentEntry| entry.shard.len() + pointer_heap(&entry.entry)).sum();
        size_of::<AuthorityLexiconSegment>() + vec_bytes(&self.entries) + entries + self.first.len() + self.last.len()
    }
}

impl CacheCost for Vec<HotListEntry> {
    fn cache_cost(&self) -> usize {
        let entries: usize = self
            .iter()
            .map(|entry| entry.normalized.len() + entry.display.len() + entry.docs.as_ref().map(vec_bytes::<HotListDoc>).unwrap_or(0))
            .sum();
        size_of::<Vec<HotListEntry>>() + vec_bytes(self) + entries
    }
}
