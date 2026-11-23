//! Root suggest-routing helpers (`src/suggest_routing.js` format constants
//! plus the query-side logic `createShardedSearch` in `src/runtime.js`
//! applies to a root's `suggest_routing` block).
//!
//! The routing artifact itself is a standard authority sidecar: its paged
//! lexicon segments, v5 authority shards and hot lists decode with
//! [`super::lexicon`] and [`super::codec`]. This module selects the block,
//! resolves its directory and maps shard ordinals back to federation shard
//! ids. The builder (`writeSuggestRoutingIndex` and friends) is not ported.

use serde_json::{Map, Value};

pub use super::codec::{SUGGEST_ROUTING_COMPAT_FORMATS, SUGGEST_ROUTING_FORMAT};
use super::lexicon::AUTHORITY_LEXICON_PAGED_FORMAT;
use crate::util::json::{js_number_to_string, path, truthy, NULL};
use crate::util::jsstr::cmp16;

/// `SUGGEST_ROUTING_COMPAT_FORMATS.includes(format)` (strict equality, so
/// only a matching string qualifies).
pub fn is_suggest_routing_format(format: &Value) -> bool {
    format.as_str().is_some_and(|format| SUGGEST_ROUTING_COMPAT_FORMATS.contains(&format))
}

/// `suggestRoutingDirectory`: the paged lexicon's segment directory when
/// `authority.autocomplete.format` is `rflexicon-v2`, otherwise
/// `authority.directory`. `Value::Null` when absent.
pub fn suggest_routing_directory(suggest_routing: &Value) -> &Value {
    let authority = path(suggest_routing, &["authority"]);
    if path(authority, &["autocomplete", "format"]).as_str() == Some(AUTHORITY_LEXICON_PAGED_FORMAT) {
        path(authority, &["autocomplete", "segments", "directory"])
    } else {
        path(authority, &["directory"])
    }
}

/// `suggestRouting`: the root's `suggest_routing` block when its format is
/// supported and its directory has a (truthy) root.
pub fn active_suggest_routing(root: &Value) -> Option<&Value> {
    let block = path(root, &["suggest_routing"]);
    let directory_root = path(suggest_routing_directory(block), &["root"]);
    if is_suggest_routing_format(path(block, &["format"])) && truthy(directory_root) {
        Some(block)
    } else {
        None
    }
}

/// JS `String(value)` for JSON values.
fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => js_number_to_string(number.as_f64().unwrap_or(f64::NAN)),
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .map(|item| if item.is_null() { String::new() } else { js_string(item) })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}

/// `(suggestRouting?.shard_ids || []).map(String)`.
pub fn suggest_routing_shard_ids(suggest_routing: Option<&Value>) -> Vec<String> {
    suggest_routing
        .and_then(|block| block.get("shard_ids"))
        .and_then(Value::as_array)
        .map(|ids| ids.iter().map(js_string).collect())
        .unwrap_or_default()
}

/// `shardIds[ordinal]`: JS property lookup, so only a canonical array-index
/// key (after `String(ordinal)`) resolves.
fn shard_id_at<'a>(shard_ids: &'a [String], ordinal: &Value) -> Option<&'a str> {
    let key = js_string(ordinal);
    let index: u32 = key.parse().ok()?;
    if index.to_string() != key || index == u32::MAX {
        return None;
    }
    shard_ids.get(index as usize).map(String::as_str)
}

/// `suggestRoutingShards(ordinals)`: the distinct known federation shard
/// ids behind a suggestion's ordinals, sorted; `None` when there are none.
/// `is_known(id)` is `shardIdIndex.has(id)`.
pub fn suggest_routing_shards(shard_ids: &[String], is_known: &dyn Fn(&str) -> bool, ordinals: &Value) -> Option<Vec<String>> {
    let ordinals = ordinals.as_array()?;
    if ordinals.is_empty() {
        return None;
    }
    let mut ids: Vec<String> = Vec::new();
    for ordinal in ordinals {
        if let Some(id) = shard_id_at(shard_ids, ordinal) {
            if is_known(id) && !ids.iter().any(|existing| existing == id) {
                ids.push(id.to_string());
            }
        }
    }
    if ids.is_empty() {
        return None;
    }
    ids.sort_by(|left, right| cmp16(left, right));
    Some(ids)
}

/// `remapSuggestRoutingItem(item)`: drops `ordinals`/`docOrdinal`, resolves
/// the doc owner to `docShard` (or drops `doc` when the owner is unknown),
/// and adds `shards` when any ordinal maps to a known shard. Key order
/// follows the JS object spread.
pub fn remap_suggest_routing_item(
    item: &Map<String, Value>,
    shard_ids: &[String],
    is_known: &dyn Fn(&str) -> bool,
) -> Map<String, Value> {
    let mut rest = item.clone();
    let ordinals = rest.shift_remove("ordinals").unwrap_or(Value::Null);
    let doc_ordinal = rest.shift_remove("docOrdinal").unwrap_or(Value::Null);
    let ids = suggest_routing_shards(shard_ids, is_known, &ordinals);
    if !rest.get("doc").unwrap_or(&NULL).is_null() {
        let doc_shard = if doc_ordinal.is_null() { None } else { shard_id_at(shard_ids, &doc_ordinal) };
        match doc_shard {
            Some(shard) if is_known(shard) => {
                rest.insert("docShard".into(), Value::String(shard.to_string()));
            }
            _ => {
                rest.shift_remove("doc");
            }
        }
    }
    if let Some(ids) = ids {
        rest.insert("shards".into(), Value::Array(ids.into_iter().map(Value::String).collect()));
    }
    rest
}
