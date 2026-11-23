//! OSM query layer: a port of the query-time half of
//! `src/integrations/osm/` (query.js and the modules it uses). The build-side
//! extraction (`documents.js`, `locality_index.js`, `node/*`) stays in JS.
//!
//! Functions take an [`crate::engine::Engine`] and call its public methods,
//! exactly like the JS functions take an engine object.

pub mod category_lexicon;
pub mod constraints;
pub mod google_maps_adapter;
pub mod opening_hours;
pub mod query;
pub mod schema;
