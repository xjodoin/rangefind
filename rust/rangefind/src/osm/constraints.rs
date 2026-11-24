//! Port of `src/integrations/osm/constraints.js`: natural-language
//! modifiers ("wheelchair accessible", "open now", "avec terrasse") parsed
//! out of a query, compiled to facet filters, and verified per result.
//!
//! The JS phrase regexes (`/\b...\b/giu`) are literal word sequences with
//! optional parts; each is expanded here into its alternatives in the
//! order the backtracking matcher tries them, and matched with the same
//! `iu` word-boundary and case-folding rules.

pub(crate) mod js;

use std::collections::HashSet;

use serde_json::{Map, Value};

use crate::error::Result;
use crate::osm::opening_hours::{evaluate_opening_hours, OpeningHoursEvaluation, OpeningHoursOptions, ZoneResolver};
use js::{is_js_space, is_word_iu, js_string_or_empty, js_trim, match_iu, prop};

/// One entry of `OSM_CONSTRAINT_DEFINITIONS`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConstraintDefinition {
    /// The constraint key (`wheelchair`, `toiletsWheelchair`, …).
    pub key: &'static str,
    /// The facet the constraint compiles to.
    pub facet: &'static str,
    /// The `details` field verified per result (`definition.detail`).
    pub detail: Option<&'static str>,
    /// Accepted (lowercase) detail values.
    pub values: &'static [&'static str],
    /// The phrase regexes, each expanded to its literal alternatives in
    /// backtracking order (lowercase; `\b` on both ends).
    pub phrases: &'static [&'static [&'static str]],
}

impl ConstraintDefinition {
    /// `definition.detail || definition.facet`.
    pub fn detail_field(&self) -> &'static str {
        self.detail.unwrap_or(self.facet)
    }
}

/// `OSM_CONSTRAINT_DEFINITIONS` in object order.
pub const OSM_CONSTRAINT_DEFINITIONS: &[ConstraintDefinition] = &[
    ConstraintDefinition {
        key: "toiletsWheelchair",
        facet: "toilets_wheelchair",
        detail: Some("toilets_wheelchair"),
        values: &["yes", "designated"],
        phrases: &[
            &[
                "wheelchair-accessible toilets",
                "wheelchair-accessible toilet",
                "wheelchair accessible toilets",
                "wheelchair accessible toilet",
            ],
            &[
                "toilettes accessibles en fauteuil roulant",
                "toilettes accessible en fauteuil roulant",
                "toilette accessibles en fauteuil roulant",
                "toilette accessible en fauteuil roulant",
            ],
        ],
    },
    ConstraintDefinition {
        key: "wheelchair",
        facet: "wheelchair",
        detail: None,
        values: &["yes", "designated"],
        phrases: &[
            &["wheelchair-accessible", "wheelchair accessible"],
            &["accessible to wheelchairs"],
            &["accessible en fauteuil roulant"],
        ],
    },
    ConstraintDefinition {
        key: "contactless",
        facet: "payment_contactless",
        detail: Some("payment_contactless"),
        values: &["yes"],
        phrases: &[
            &["with contactless payment", "with contactless", "contactless payment", "contactless"],
            &["tap payment", "tap"],
            &["paiement sans contact"],
        ],
    },
    ConstraintDefinition {
        key: "delivery",
        facet: "delivery",
        detail: Some("delivery"),
        values: &["yes", "only"],
        phrases: &[&["with delivery"], &["delivery available"], &["avec livraison"]],
    },
    ConstraintDefinition {
        key: "takeaway",
        facet: "takeaway",
        detail: Some("takeaway"),
        values: &["yes", "only"],
        phrases: &[&["with takeaway", "takeaway"], &["takeout"], &["pour emporter"]],
    },
    ConstraintDefinition {
        key: "driveThrough",
        facet: "drive_through",
        detail: Some("drive_through"),
        values: &["yes"],
        phrases: &[&["drive-through", "drive through"], &["service au volant"]],
    },
    ConstraintDefinition {
        key: "outdoorSeating",
        facet: "outdoor_seating",
        detail: Some("outdoor_seating"),
        values: &["yes"],
        phrases: &[&["outdoor seating"], &["patio"], &["avec terrasse"]],
    },
    ConstraintDefinition {
        key: "internet",
        facet: "internet_access",
        detail: Some("internet_access"),
        values: &["yes", "wlan", "terminal", "wired"],
        phrases: &[&["with wi-fi", "with wifi", "with internet", "wi-fi", "wifi", "internet"]],
    },
    ConstraintDefinition {
        key: "reservation",
        facet: "reservation",
        detail: Some("reservation"),
        values: &["yes", "required", "recommended"],
        phrases: &[&[
            "takes reservations",
            "takes reservation",
            "take reservations",
            "take reservation",
            "accepts reservations",
            "accepts reservation",
            "accept reservations",
            "accept reservation",
        ]],
    },
    ConstraintDefinition {
        key: "free",
        facet: "fee",
        detail: Some("fee"),
        values: &["no"],
        phrases: &[&["free admission", "free entry", "free"], &["no fee"]],
    },
];

const OPEN_PHRASES: &[&[&str]] = &[
    &["open now"],
    &["currently open"],
    &["ouverte maintenant", "ouverts maintenant", "ouvertes maintenant", "ouvert maintenant"],
];

/// `OSM_CONSTRAINT_DEFINITIONS[key]`.
pub fn constraint_definition(key: &str) -> Option<&'static ConstraintDefinition> {
    OSM_CONSTRAINT_DEFINITIONS.iter().find(|definition| definition.key == key)
}

/// The key of the open-now constraint (no definition; verified against
/// `opening_hours`).
pub const OPEN_NOW: &str = "openNow";

fn static_key(key: &str) -> Option<&'static str> {
    if key == OPEN_NOW {
        return Some(OPEN_NOW);
    }
    constraint_definition(key).map(|definition| definition.key)
}

/// A constraints object (`{ wheelchair: true, openNow: true, … }`) with JS
/// object key order: explicit keys first, then phrases in definition order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OsmConstraints {
    entries: Vec<(&'static str, bool)>,
}

impl OsmConstraints {
    pub fn new() -> Self {
        Self::default()
    }

    /// `constraints[key]` (`None` when absent).
    pub fn get(&self, key: &str) -> Option<bool> {
        self.entries.iter().find(|(name, _)| *name == key).map(|(_, value)| *value)
    }

    /// `constraints[key] = value`; unknown keys are ignored (the JS never
    /// creates them).
    pub fn set(&mut self, key: &str, value: bool) {
        let Some(key) = static_key(key) else { return };
        match self.entries.iter_mut().find(|(name, _)| *name == key) {
            Some(slot) => slot.1 = value,
            None => self.entries.push((key, value)),
        }
    }

    /// `Object.keys(constraints).length`.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// `Boolean(constraints.openNow)`.
    pub fn open_now(&self) -> bool {
        self.get(OPEN_NOW) == Some(true)
    }

    /// `Object.entries(constraints)`.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, bool)> + '_ {
        self.entries.iter().copied()
    }

    pub fn to_json(&self) -> Value {
        Value::Object(self.entries.iter().map(|(key, value)| (key.to_string(), Value::Bool(*value))).collect())
    }

    /// Known keys of a JSON constraints object, `Boolean(value)` each, in
    /// object order (unknown keys are skipped by every consumer anyway).
    pub fn from_json(value: &Value) -> Self {
        let mut out = Self::new();
        if let Some(map) = value.as_object() {
            for (key, item) in map {
                out.set(key, crate::util::json::truthy(item));
            }
        }
        out
    }
}

/// `parseOsmConstraints` result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedConstraints {
    pub query: String,
    pub constraints: OsmConstraints,
}

impl ParsedConstraints {
    pub fn to_json(&self) -> Value {
        let mut out = Map::new();
        out.insert("query".into(), Value::String(self.query.clone()));
        out.insert("constraints".into(), self.constraints.to_json());
        Value::Object(out)
    }
}

fn normalize_explicit(value: &Value) -> OsmConstraints {
    let mut out = OsmConstraints::new();
    let Some(map) = value.as_object() else { return out };
    for definition in OSM_CONSTRAINT_DEFINITIONS {
        if let Some(item) = map.get(definition.key).filter(|item| !item.is_null()) {
            out.set(definition.key, crate::util::json::truthy(item));
        }
    }
    if let Some(item) = map.get(OPEN_NOW).filter(|item| !item.is_null()) {
        out.set(OPEN_NOW, crate::util::json::truthy(item));
    }
    out
}

/// All non-overlapping matches of one phrase regex, left to right.
fn phrase_matches(text: &str, alternatives: &[&str]) -> Vec<(usize, usize)> {
    let mut found = Vec::new();
    let mut at = 0;
    while at < text.len() {
        let boundary = !text[..at].chars().next_back().is_some_and(is_word_iu);
        let hit = boundary
            .then(|| {
                alternatives.iter().find_map(|alternative| {
                    let end = match_iu(text, at, alternative)?;
                    (!text[end..].chars().next().is_some_and(is_word_iu)).then_some(end)
                })
            })
            .flatten();
        match hit {
            Some(end) => {
                found.push((at, end));
                at = end;
            }
            None => at += text[at..].chars().next().map(char::len_utf8).unwrap_or(1),
        }
    }
    found
}

/// Apply one phrase: `phrase.test(text)` and `text.replace(phrase, " ")`.
fn strip_phrase(text: &mut String, alternatives: &[&str]) -> bool {
    let found = phrase_matches(text, alternatives);
    if found.is_empty() {
        return false;
    }
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    for (start, end) in found {
        out.push_str(&text[last..start]);
        out.push(' ');
        last = end;
    }
    out.push_str(&text[last..]);
    *text = out;
    true
}

/// `text.replace(/\s+/gu, " ")`.
fn collapse_spaces(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_space = false;
    for c in text.chars() {
        if is_js_space(c) {
            if !in_space {
                out.push(' ');
            }
            in_space = true;
        } else {
            out.push(c);
            in_space = false;
        }
    }
    out
}

/// `with|and` at byte `at` (case-insensitive), returning the end.
fn connector_at(text: &str, at: usize) -> Option<usize> {
    match_iu(text, at, "with").or_else(|| match_iu(text, at, "and"))
}

fn space_run(text: &str, at: usize) -> usize {
    text[at..].chars().take_while(|c| is_js_space(*c)).map(char::len_utf8).sum()
}

/// `text.replace(/^\s*(?:with|and)\s+|\s+(?:with|and)\s*$/giu, "")`.
fn strip_edge_connectors(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    let mut at = 0;
    while at <= text.len() {
        let mut hit = None;
        if at == 0 {
            let lead = space_run(text, 0);
            if let Some(end) = connector_at(text, lead) {
                let trail = space_run(text, end);
                if trail > 0 {
                    hit = Some(end + trail);
                }
            }
        }
        if hit.is_none() {
            let lead = space_run(text, at);
            if lead > 0 {
                if let Some(end) = connector_at(text, at + lead) {
                    let trail = space_run(text, end);
                    if end + trail == text.len() {
                        hit = Some(text.len());
                    }
                }
            }
        }
        match hit {
            Some(end) => {
                out.push_str(&text[last..at]);
                last = end;
                if end == at {
                    // Empty matches are impossible here, but never loop.
                    at += 1;
                } else {
                    at = end;
                }
            }
            None => {
                if at == text.len() {
                    break;
                }
                at += text[at..].chars().next().map(char::len_utf8).unwrap_or(1);
            }
        }
    }
    out.push_str(&text[last.min(text.len())..]);
    out
}

/// `parseOsmConstraints(query, explicit)`: strip constraint phrases from the
/// query text and merge them over the explicit `{key: boolean}` object.
pub fn parse_osm_constraints(query: &str, explicit: &Value) -> ParsedConstraints {
    let mut text = query.to_string();
    let mut constraints = normalize_explicit(explicit);
    for definition in OSM_CONSTRAINT_DEFINITIONS {
        for phrase in definition.phrases {
            if strip_phrase(&mut text, phrase) {
                constraints.set(definition.key, true);
            }
        }
    }
    for phrase in OPEN_PHRASES {
        if strip_phrase(&mut text, phrase) {
            constraints.set(OPEN_NOW, true);
        }
    }
    let collapsed = collapse_spaces(&text);
    let stripped = strip_edge_connectors(&collapsed);
    ParsedConstraints { query: js_trim(&stripped).to_string(), constraints }
}

/// `compileOsmConstraintFilters(constraints, availableFacets)`: facet name →
/// accepted values, for enabled constraints whose facet the index has.
pub fn compile_osm_constraint_filters(constraints: &OsmConstraints, available_facets: &HashSet<String>) -> Map<String, Value> {
    let mut facets = Map::new();
    for (key, enabled) in constraints.iter() {
        let Some(definition) = constraint_definition(key) else { continue };
        if !enabled || !available_facets.contains(definition.facet) {
            continue;
        }
        facets.insert(
            definition.facet.to_string(),
            Value::Array(definition.values.iter().map(|value| Value::String(value.to_string())).collect()),
        );
    }
    facets
}

/// `evaluateOsmConstraints` options.
#[derive(Clone, Default)]
pub struct ConstraintOptions {
    pub at: Value,
    pub time_zone: Value,
    /// Only `true` counts (`=== true`).
    pub include_unknown_open_now: Value,
    /// Passed as `requireComplete` (only `false` disables it).
    pub require_complete_opening_hours: Value,
    /// Pins `Date.now()` for `at: null`.
    pub now_ms: Option<f64>,
    /// Overrides the host default time zone.
    pub default_time_zone: Option<String>,
    pub zone_resolver: Option<ZoneResolver>,
}

impl ConstraintOptions {
    /// `constraintOptions(rawParams)` from `query.js`: `at ?? now`,
    /// `timeZone`, `includeUnknownOpenNow`, `requireCompleteOpeningHours`.
    pub fn from_params(raw: &Value) -> Self {
        let field = |key: &str| raw.get(key).cloned().unwrap_or(Value::Null);
        let at = match raw.get("at") {
            Some(value) if !value.is_null() => value.clone(),
            _ => field("now"),
        };
        ConstraintOptions {
            at,
            time_zone: field("timeZone"),
            include_unknown_open_now: field("includeUnknownOpenNow"),
            require_complete_opening_hours: field("requireCompleteOpeningHours"),
            ..Default::default()
        }
    }
}

/// `evaluateOsmConstraints` result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConstraintEvaluation {
    pub matches: bool,
    /// Per-constraint verdicts in constraint order (`openNow` last).
    pub checks: Vec<(&'static str, bool)>,
    pub opening_hours: Option<OpeningHoursEvaluation>,
}

impl ConstraintEvaluation {
    pub fn checks_json(&self) -> Value {
        Value::Object(self.checks.iter().map(|(key, value)| (key.to_string(), Value::Bool(*value))).collect())
    }

    pub fn to_json(&self) -> Value {
        let mut out = Map::new();
        out.insert("matches".into(), Value::Bool(self.matches));
        out.insert("checks".into(), self.checks_json());
        if let Some(hours) = &self.opening_hours {
            out.insert("openingHours".into(), hours.to_json());
        }
        Value::Object(out)
    }
}

/// `normalizedDetailValues(value)`.
fn detail_values(value: Option<&Value>) -> HashSet<String> {
    js_string_or_empty(value)
        .to_lowercase()
        .split(';')
        .map(|item| js_trim(item).to_string())
        .filter(|item| !item.is_empty())
        .collect()
}

/// `evaluateOsmConstraints(result, constraints, options)`. Errs only when
/// open-now must be checked and `options.at` is not a valid date.
pub fn evaluate_osm_constraints(
    result: &Value,
    constraints: &OsmConstraints,
    options: &ConstraintOptions,
) -> Result<ConstraintEvaluation> {
    let details = prop(Some(result), "details").filter(|value| crate::util::json::truthy(value));
    let mut checks = Vec::new();
    for (key, enabled) in constraints.iter() {
        if !enabled || key == OPEN_NOW {
            continue;
        }
        let Some(definition) = constraint_definition(key) else { continue };
        let values = detail_values(prop(details, definition.detail_field()));
        let ok = definition.values.iter().any(|value| values.contains(*value));
        match checks.iter_mut().find(|(name, _)| *name == key) {
            Some(slot) => *slot = (key, ok),
            None => checks.push((key, ok)),
        }
    }
    let mut opening_hours = None;
    if constraints.open_now() {
        let hours = evaluate_opening_hours(
            prop(details, "opening_hours").unwrap_or(&Value::Null),
            &OpeningHoursOptions {
                at: options.at.clone(),
                time_zone: options.time_zone.clone(),
                require_complete: options.require_complete_opening_hours.clone(),
                now_ms: options.now_ms,
                default_time_zone: options.default_time_zone.clone(),
                zone_resolver: options.zone_resolver.clone(),
            },
        )?;
        let open = hours.is_open == Some(true)
            || (hours.is_open.is_none() && options.include_unknown_open_now == Value::Bool(true));
        checks.push((OPEN_NOW, open));
        opening_hours = Some(hours);
    }
    Ok(ConstraintEvaluation { matches: checks.iter().all(|(_, ok)| *ok), checks, opening_hours })
}

/// `annotateConstraintResult(result, evaluation)`: `{...result,
/// constraintMatches, [openNow, openingHoursState, openingHoursEvaluation]}`.
pub fn annotate_constraint_result(result: &Value, evaluation: Option<&ConstraintEvaluation>) -> Value {
    let Some(evaluation) = evaluation else { return result.clone() };
    let mut out = Map::new();
    js::spread_into(&mut out, Some(result));
    out.insert("constraintMatches".into(), evaluation.checks_json());
    if let Some(hours) = &evaluation.opening_hours {
        out.insert("openNow".into(), hours.is_open.map(Value::Bool).unwrap_or(Value::Null));
        out.insert("openingHoursState".into(), Value::String(hours.state.into()));
        out.insert("openingHoursEvaluation".into(), hours.to_json());
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_like_the_js_test() {
        let parsed = parse_osm_constraints("wheelchair-accessible pharmacy open now with contactless", &Value::Null);
        assert_eq!(parsed.query, "pharmacy");
        assert_eq!(parsed.constraints.to_json(), json!({ "wheelchair": true, "contactless": true, "openNow": true }));
        let available: HashSet<String> = ["wheelchair", "payment_contactless"].iter().map(|s| s.to_string()).collect();
        assert_eq!(
            Value::Object(compile_osm_constraint_filters(&parsed.constraints, &available)),
            json!({ "wheelchair": ["yes", "designated"], "payment_contactless": ["yes"] })
        );
        assert_eq!(strip_edge_connectors("with and"), "and");
        assert_eq!(strip_edge_connectors("coffee with"), "coffee");
    }
}
