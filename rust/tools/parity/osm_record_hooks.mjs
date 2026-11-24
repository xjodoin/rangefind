// `node --import ./osm_record_hooks.mjs test/osm_query.test.js`: redirects
// the test file's import of src/integrations/osm/query.js to a recording
// wrapper and writes every top-level call to $OSM_RECORD_OUT on exit.
import { register } from "node:module";

register(new URL("./osm_record_loader.mjs", import.meta.url));
