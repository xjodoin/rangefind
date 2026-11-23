//! The seam between this port and its sibling modules
//! (`category_lexicon.js` → [`crate::osm::category_lexicon`],
//! `constraints.js` → [`crate::osm::constraints`]). Everything `query.js`
//! imports from them goes through here.

use std::collections::HashSet;
use std::rc::Rc;

use serde_json::Value;

use crate::error::Result;
use crate::osm::category_lexicon as lexicon;
use crate::osm::constraints::{self, ConstraintOptions, OsmConstraints};

pub use lexicon::{lookup_category, lookup_category_fuzzy, within_one_edit, CategoryLexicon, CategoryMatch as CategoryIntent};

/// `fold(value)` from `category_lexicon.js`.
pub fn fold(value: &str) -> String {
    lexicon::fold(value)
}

/// `fold(value)` on an arbitrary JS value (`String(value || "")`).
pub fn fold_value(value: Option<&Value>) -> String {
    fold(&super::js::js_string_or_empty(value))
}

/// `buildCategoryLexicon(typeValues)`; `None` builds the bundled vocabulary.
pub fn build_category_lexicon(type_values: Option<&Value>) -> CategoryLexicon {
    lexicon::build_category_lexicon(type_values.unwrap_or(&Value::Null))
}

thread_local! {
    static DEFAULT_LEXICON: Rc<CategoryLexicon> = Rc::new(lexicon::default_category_lexicon().clone());
}

/// `defaultCategoryLexicon()`.
pub fn default_category_lexicon() -> Rc<CategoryLexicon> {
    DEFAULT_LEXICON.with(Rc::clone)
}

/// `parseOsmConstraints(query, explicit)`.
pub struct ParsedConstraints {
    pub query: String,
    pub constraints: OsmConstraints,
}

pub fn parse_osm_constraints(query: &str, explicit: Option<&Value>) -> ParsedConstraints {
    let parsed = constraints::parse_osm_constraints(query, explicit.unwrap_or(&Value::Null));
    ParsedConstraints { query: parsed.query, constraints: parsed.constraints }
}

/// `compileOsmConstraintFilters(constraints, availableFacetNames(manifest))`.
pub fn compile_osm_constraint_filters(constraints: &OsmConstraints, available: &[String]) -> serde_json::Map<String, Value> {
    let available: HashSet<String> = available.iter().cloned().collect();
    constraints::compile_osm_constraint_filters(constraints, &available)
}

/// `evaluateOsmConstraints(result, constraints, constraintOptions(raw))`
/// then `annotateConstraintResult(result, evaluation)`: `(matches, annotated)`.
pub fn evaluate_and_annotate(result: &Value, constraints: &OsmConstraints, options: &ConstraintOptions) -> Result<(bool, Value)> {
    let evaluation = constraints::evaluate_osm_constraints(result, constraints, options)?;
    let annotated = constraints::annotate_constraint_result(result, Some(&evaluation));
    Ok((evaluation.matches, annotated))
}
