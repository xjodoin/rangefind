//! Port of `src/integrations/osm/query.js`: the OSM query cascade on top of
//! a Rangefind engine (coordinates and reverse geocoding, category +
//! locality intents, addresses and intersections, near-anchored text,
//! suggestions, route corridors, constraint post-filtering).
//!
//! Every function exists twice: on [`Engine`] (what hosts use) and on any
//! [`OsmEngine`] (`*_with`), the Rust shape of the JS functions accepting
//! any engine-like object. [`dispatch`] exposes the same functions to the
//! host protocol as `osm.*` methods.

mod core;
mod deps;
mod host;
mod js;
mod locality;
mod search;

use std::rc::Rc;

use serde_json::Value;

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::osm::google_maps_adapter::{create_rangefind_maps_adapter, MapsAdapterOptions, OsmQueryBackend, RangefindMapsAdapter};

pub use self::core::{collapse_street_suggestions, parse_coordinate_intent, LatLon, QueryIntent};
pub use self::deps::{CategoryIntent, CategoryLexicon};
pub use self::host::{engine_caches, EngineHost, ExactAuthority, LocalFuture, Locality, OsmCaches, OsmEngine};

/// `parseOsmQueryIntent(value, lexicon = defaultCategoryLexicon())`.
pub fn parse_osm_query_intent(value: &str, lexicon: Option<&CategoryLexicon>) -> Option<QueryIntent> {
    match lexicon {
        Some(lexicon) => self::core::parse_osm_query_intent(value, lexicon),
        None => self::core::parse_osm_query_intent(value, &deps::default_category_lexicon()),
    }
}

/// `parseNearbyCategoryIntent(value, lexicon = defaultCategoryLexicon())`.
pub fn parse_nearby_category_intent(value: &str, lexicon: Option<&CategoryLexicon>) -> Option<CategoryIntent> {
    match lexicon {
        Some(lexicon) => self::core::parse_nearby_category_intent(value, lexicon),
        None => self::core::parse_nearby_category_intent(value, &deps::default_category_lexicon()),
    }
}

/// `buildCategoryLexicon(typeValues)` (re-exported for callers that pass a
/// custom lexicon to the parsers).
pub fn build_category_lexicon(type_values: Option<&Value>) -> CategoryLexicon {
    deps::build_category_lexicon(type_values)
}

pub async fn search_osm_query_with(engine: &dyn OsmEngine, params: &Value) -> Result<Value> {
    search::search_osm_query(&host::Ctx::new(engine), params).await
}

pub async fn search_along_route_osm_with(engine: &dyn OsmEngine, params: &Value) -> Result<Value> {
    search::search_along_route(&host::Ctx::new(engine), params).await
}

pub async fn reverse_geocode_osm_with(engine: &dyn OsmEngine, params: &Value) -> Result<Value> {
    search::reverse_geocode(&host::Ctx::new(engine), params).await
}

pub async fn suggest_osm_query_with(engine: &dyn OsmEngine, params: &Value) -> Result<Value> {
    search::suggest_osm_query(&host::Ctx::new(engine), params).await
}

pub async fn resolve_osm_suggestion_with(engine: &dyn OsmEngine, suggestion: &Value) -> Result<Value> {
    search::resolve_osm_suggestion(&host::Ctx::new(engine), suggestion).await
}

pub async fn hydrate_osm_suggestions_with(engine: &dyn OsmEngine, suggestions: &mut [Value]) {
    search::hydrate_osm_suggestions(&host::Ctx::new(engine), suggestions).await
}

/// `searchOsmQuery(engine, params)`.
pub async fn search_osm_query(engine: &Engine, params: &Value) -> Result<Value> {
    search_osm_query_with(&EngineHost::new(engine), params).await
}

/// `searchAlongRouteOsm(engine, params)`.
pub async fn search_along_route_osm(engine: &Engine, params: &Value) -> Result<Value> {
    search_along_route_osm_with(&EngineHost::new(engine), params).await
}

/// `reverseGeocodeOsm(engine, params)`.
pub async fn reverse_geocode_osm(engine: &Engine, params: &Value) -> Result<Value> {
    reverse_geocode_osm_with(&EngineHost::new(engine), params).await
}

/// `suggestOsmQuery(engine, params)`.
pub async fn suggest_osm_query(engine: &Engine, params: &Value) -> Result<Value> {
    suggest_osm_query_with(&EngineHost::new(engine), params).await
}

/// `resolveOsmSuggestion(engine, suggestion)`.
pub async fn resolve_osm_suggestion(engine: &Engine, suggestion: &Value) -> Result<Value> {
    resolve_osm_suggestion_with(&EngineHost::new(engine), suggestion).await
}

/// `hydrateOsmSuggestions(engine, suggestions)`.
pub async fn hydrate_osm_suggestions(engine: &Engine, suggestions: &mut [Value]) {
    hydrate_osm_suggestions_with(&EngineHost::new(engine), suggestions).await
}

/// [`OsmQueryBackend`] over the Rust engine: the Google Maps migration
/// adapter (`createRangefindMapsAdapter(engine)`) runs on these ports.
#[derive(Clone)]
pub struct EngineBackend {
    pub engine: Engine,
}

impl OsmQueryBackend for EngineBackend {
    fn search_osm_query(&self, params: Value) -> impl std::future::Future<Output = Result<Value>> {
        async move { search_osm_query(&self.engine, &params).await }
    }

    fn suggest_osm_query(&self, params: Value) -> impl std::future::Future<Output = Result<Value>> {
        async move { suggest_osm_query(&self.engine, &params).await }
    }

    fn reverse_geocode_osm(&self, params: Value) -> impl std::future::Future<Output = Result<Value>> {
        async move { reverse_geocode_osm(&self.engine, &params).await }
    }

    fn search_along_route_osm(&self, params: Value) -> impl std::future::Future<Output = Result<Value>> {
        async move { search_along_route_osm(&self.engine, &params).await }
    }
}

/// `createRangefindMapsAdapter(engine, options)` on the Rust engine.
pub fn create_maps_adapter(engine: &Engine, options: MapsAdapterOptions) -> RangefindMapsAdapter<EngineBackend> {
    create_rangefind_maps_adapter(EngineBackend { engine: engine.clone() }, options)
}

/// The engine's shared Maps adapter (one place cache per engine, like a
/// JS app holding one `createRangefindMapsAdapter(engine)`).
fn engine_maps_adapter(engine: &Engine) -> Rc<RangefindMapsAdapter<EngineBackend>> {
    let caches = engine_caches(engine);
    let existing = caches.maps_adapter.borrow().clone();
    if let Some(adapter) = existing.and_then(|adapter| adapter.downcast::<RangefindMapsAdapter<EngineBackend>>().ok()) {
        return adapter;
    }
    let adapter = Rc::new(create_maps_adapter(engine, MapsAdapterOptions::default()));
    *caches.maps_adapter.borrow_mut() = Some(adapter.clone());
    adapter
}

/// `osm.maps.<method>`: the Google Maps migration adapter. `configure`
/// takes `{ defaults }` and resets the place cache.
async fn dispatch_maps(engine: &Engine, method: &str, params: Value) -> Result<Value> {
    if method == "configure" {
        let defaults = params.get("defaults").cloned().unwrap_or(Value::Null);
        let adapter = Rc::new(create_maps_adapter(engine, MapsAdapterOptions { defaults }));
        *engine_caches(engine).maps_adapter.borrow_mut() = Some(adapter);
        return Ok(Value::Null);
    }
    let adapter = engine_maps_adapter(engine);
    match method {
        "autocomplete" => adapter.autocomplete(&params).await,
        "textSearch" => adapter.text_search(&params).await,
        "nearbySearch" => adapter.nearby_search(&params).await,
        "geocode" => adapter.geocode(&params).await,
        "reverseGeocode" => adapter.reverse_geocode(&params).await,
        "searchAlongRoute" => adapter.search_along_route(&params).await,
        "placeDetails" => adapter.place_details(&params).await,
        "clearPlaceCache" => {
            adapter.clear_place_cache();
            Ok(Value::Null)
        }
        other => Err(Error::new(format!("Rangefind: unknown method \"osm.maps.{other}\"."))),
    }
}

/// Host-protocol entry point for `osm.*` methods (the part after `osm.`).
/// `params` is the JS argument object; `resolveSuggestion` takes the
/// suggestion, `hydrateSuggestions` takes `{ suggestions }` and returns the
/// hydrated array, `collapseStreetSuggestions` takes `{ response, params }`.
pub async fn dispatch(engine: &Engine, method: &str, params: Value) -> Result<Value> {
    if let Some(maps) = method.strip_prefix("maps.") {
        return dispatch_maps(engine, maps, params).await;
    }
    dispatch_with(&EngineHost::new(engine), method, params).await
}

/// Lexicon for the parse methods: the index's own category lexicon (what
/// search uses) unless `lexicon: "default"` asks for the built-in one, which
/// is the JS functions' default argument.
async fn dispatch_lexicon(engine: &dyn OsmEngine, params: &Value) -> crate::osm::category_lexicon::CategoryLexicon {
    if params.get("lexicon").and_then(Value::as_str) == Some("default") {
        return crate::osm::category_lexicon::default_category_lexicon().clone();
    }
    let ctx = host::Ctx::new(engine);
    (*locality::engine_category_lexicon(&ctx).await).clone()
}

pub async fn dispatch_with(engine: &dyn OsmEngine, method: &str, params: Value) -> Result<Value> {
    match method {
        "search" => search_osm_query_with(engine, &params).await,
        "suggest" => suggest_osm_query_with(engine, &params).await,
        "reverseGeocode" => reverse_geocode_osm_with(engine, &params).await,
        "searchAlongRoute" => search_along_route_osm_with(engine, &params).await,
        "resolveSuggestion" => resolve_osm_suggestion_with(engine, &params).await,
        "hydrateSuggestions" => {
            let mut suggestions = match params.get("suggestions") {
                Some(Value::Array(items)) => items.clone(),
                _ => Vec::new(),
            };
            hydrate_osm_suggestions_with(engine, &mut suggestions).await;
            Ok(Value::Array(suggestions))
        }
        "collapseStreetSuggestions" => {
            let response = params.get("response").cloned().unwrap_or(Value::Null);
            let options = params.get("params").cloned().unwrap_or(serde_json::json!({}));
            Ok(collapse_street_suggestions(&response, &options))
        }
        "parseCoordinateIntent" => {
            let q = js::js_string_or_empty(params.get("q"));
            Ok(parse_coordinate_intent(&q).map(LatLon::to_json).unwrap_or(Value::Null))
        }
        "parseQueryIntent" => {
            let q = js::js_string_or_empty(params.get("q"));
            let lexicon = dispatch_lexicon(engine, &params).await;
            Ok(self::core::parse_osm_query_intent(&q, &lexicon).map(|intent| intent.to_json()).unwrap_or(Value::Null))
        }
        "parseNearbyCategoryIntent" => {
            let q = js::js_string_or_empty(params.get("q"));
            let lexicon = dispatch_lexicon(engine, &params).await;
            Ok(self::core::parse_nearby_category_intent(&q, &lexicon).map(|intent| intent.to_json()).unwrap_or(Value::Null))
        }
        other => Err(Error::new(format!("Rangefind: unknown method \"osm.{other}\"."))),
    }
}
