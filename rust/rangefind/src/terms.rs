//! Language-agnostic term combinatorics (`src/terms.js`): phrase n-gram
//! expansion, proximity pair names and query-bundle keys over
//! already-analyzed base terms.

use std::cmp::Ordering;
use std::collections::HashSet;

use serde_json::{json, Value};

use crate::util::jsstr::cmp16;

/// `[...new Set(items)]`.
fn dedupe(items: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();
    items.into_iter().filter(|item| seen.insert(item.clone())).collect()
}

/// `expandedTermsFromBaseTerms(terms)`: base terms plus their contiguous 2-
/// and 3-grams joined with "_", deduplicated in first-seen order.
pub fn expanded_terms_from_base_terms<S: AsRef<str>>(terms: &[S]) -> Vec<String> {
    let mut expanded: Vec<String> = terms.iter().map(|term| term.as_ref().to_string()).collect();
    for n in [2usize, 3] {
        if terms.len() < n {
            continue;
        }
        for window in terms.windows(n) {
            expanded.push(window.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join("_"));
        }
    }
    dedupe(expanded)
}

/// `proximityTerm(left, right)`: order-independent pair name (UTF-16 order),
/// "" when either side is empty or both are equal.
pub fn proximity_term(left: &str, right: &str) -> String {
    if left.is_empty() || right.is_empty() || left == right {
        return String::new();
    }
    let (a, b) = if cmp16(left, right) == Ordering::Less { (left, right) } else { (right, left) };
    format!("n_{a}_{b}")
}

fn distinct_non_empty<S: AsRef<str>>(base_terms: &[S]) -> Vec<String> {
    dedupe(base_terms.iter().map(|term| term.as_ref().to_string()).filter(|term| !term.is_empty()))
}

/// `queryBundleKeyFromBaseTerms(baseTerms)`: "" unless the distinct
/// non-empty base set has 2 or 3 terms.
pub fn query_bundle_key_from_base_terms<S: AsRef<str>>(base_terms: &[S]) -> String {
    let terms = distinct_non_empty(base_terms);
    if terms.len() < 2 || terms.len() > 3 {
        return String::new();
    }
    format!("exact-expanded-v1|{}", terms.join(" "))
}

/// One `{ key, baseTerms, expandedTerms }` bundle lookup plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryBundleKey {
    pub key: String,
    pub base_terms: Vec<String>,
    pub expanded_terms: Vec<String>,
}

impl QueryBundleKey {
    pub fn to_json(&self) -> Value {
        json!({ "key": self.key, "baseTerms": self.base_terms, "expandedTerms": self.expanded_terms })
    }
}

/// `queryBundleKeysFromBaseTerms(baseTerms)`: every 2- and 3-term
/// contiguous window of the distinct base terms, longest first.
pub fn query_bundle_keys_from_base_terms<S: AsRef<str>>(base_terms: &[S]) -> Vec<QueryBundleKey> {
    let terms = distinct_non_empty(base_terms);
    let mut out = Vec::new();
    let mut n = terms.len().min(3);
    while n >= 2 {
        for window in terms.windows(n) {
            let key = query_bundle_key_from_base_terms(window);
            if !key.is_empty() {
                out.push(QueryBundleKey {
                    key,
                    base_terms: window.to_vec(),
                    expanded_terms: expanded_terms_from_base_terms(window),
                });
            }
        }
        n -= 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expansion_and_pairs() {
        assert_eq!(expanded_terms_from_base_terms(&["a", "b", "c"]), vec!["a", "b", "c", "a_b", "b_c", "a_b_c"]);
        assert_eq!(proximity_term("\u{ff5e}", "😀"), "n_😀_\u{ff5e}");
        assert_eq!(proximity_term("a", "a"), "");
        assert_eq!(query_bundle_key_from_base_terms(&["x", "y", "x"]), "exact-expanded-v1|x y");
    }
}
