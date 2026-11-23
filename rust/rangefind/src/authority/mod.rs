//! Authority lookups: exact/address keys, the authority shard codec, the
//! autocomplete lexicon codecs and the root suggest-routing helpers.
//!
//! Mirrors, query side only:
//! - `src/address.js` (+ the generated `src/address_abbreviations.js` table) → [`address`], [`abbreviations`]
//! - `src/authority_codec.js` → [`codec`]
//! - `src/authority_lexicon.js` → [`lexicon`]
//! - the suggest-routing pieces `src/runtime.js` consumes from
//!   `src/suggest_routing.js` / `createShardedSearch` → [`suggest_routing`]
//!
//! The JS regular expressions these modules use are re-implemented by hand
//! in [`text`]; every one is proven against the JS engine by the parity
//! fixtures (`node rust/tools/fixtures/authority.mjs`).

pub mod abbreviations;
pub mod address;
pub mod codec;
mod fdlibm;
pub mod lexicon;
pub mod suggest_routing;
mod text;

#[cfg(test)]
mod parity_tests;

pub use address::{
    address_authority_query_keys, address_query_token_variants, address_range_contains,
    address_range_query_candidates, decode_address_range_geometry, interpolate_address_range_point,
    looks_like_address_query, normalize_address_authority_key, normalize_address_key,
    normalize_postal_code_prefix_spacing, normalize_postal_code_spacing, AddressPoint, AddressRangeCandidate,
    ADDRESS_RANGE_BUCKET_SIZE,
};
pub use codec::{
    authority_address_range_key, authority_keys_for_query, authority_keys_for_value,
    authority_normalize_raw_surface, authority_normalize_surface, authority_token_key_from_terms,
    parse_authority_shard, AuthorityEntry, AuthorityKey, AuthorityKeyKind, AuthorityKeyOptions,
    AuthorityNormalizer, AuthorityShard, AUTHORITY_FORMAT, SUGGEST_ROUTING_COMPAT_FORMATS, SUGGEST_ROUTING_FORMAT,
};
pub use lexicon::{
    autocomplete_entry_summary, autocomplete_full, autocomplete_rank, compare_autocomplete, is_autocomplete_key,
    parse_authority_hot_list, parse_authority_lexicon_root, parse_authority_lexicon_segment,
    parse_autocomplete_key, suggest_key, AuthorityHotRef, AuthorityLexiconRoot, AuthorityLexiconSegment,
    AuthorityLexiconSegmentEntry, AuthorityLexiconShard, AutocompleteDoc, AutocompleteOrder, AutocompleteSummary,
    AutocompleteSummaryOptions, HotListDoc, HotListEntry, ParsedAutocompleteKey, AUTHORITY_LEXICON_FORMAT,
    AUTHORITY_LEXICON_PAGED_FORMAT, AUTHORITY_LEXICON_SEGMENT_FORMAT, AUTOCOMPLETE_PREFIX, AUTOCOMPLETE_SEPARATOR,
    DEFAULT_AUTOCOMPLETE_DOC_ROWS,
};
pub use suggest_routing::{
    active_suggest_routing, is_suggest_routing_format, remap_suggest_routing_item, suggest_routing_directory,
    suggest_routing_shard_ids, suggest_routing_shards,
};
