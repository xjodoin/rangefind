// Published entry `rangefind/osm/extract`: programmatic OSM PBF → places
// JSONL extraction. The implementation lives with the fixture script (which
// ships in the package); this module gives it a stable import path so
// consumers never reach into `scripts/` by file path.
export { extractOsmPlaces } from "../../../../scripts/osm_fixture.mjs";
