// Recording stand-in for src/integrations/osm/query.js (see
// osm_record_hooks.mjs). Re-exports every export, wrapping the functions
// the Rust port implements.
import { writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import * as real from "../../../src/integrations/osm/query.js";
import { createRecorder, ENGINE_FUNCTIONS, PURE_FUNCTIONS } from "./osm_record.mjs";

const recorder = createRecorder();
const wrapped = {};
for (const name of ENGINE_FUNCTIONS) wrapped[name] = recorder.wrapEngineFunction(name, real[name]);
for (const name of PURE_FUNCTIONS) wrapped[name] = recorder.wrapPureFunction(name, real[name]);

export const reverseGeocodeOsm = wrapped.reverseGeocodeOsm;
export const resolveOsmSuggestion = wrapped.resolveOsmSuggestion;
export const hydrateOsmSuggestions = wrapped.hydrateOsmSuggestions;
export const suggestOsmQuery = wrapped.suggestOsmQuery;
export const searchAlongRouteOsm = wrapped.searchAlongRouteOsm;
export const searchOsmQuery = wrapped.searchOsmQuery;
export const parseCoordinateIntent = wrapped.parseCoordinateIntent;
export const parseOsmQueryIntent = wrapped.parseOsmQueryIntent;
export const parseNearbyCategoryIntent = wrapped.parseNearbyCategoryIntent;
export const collapseStreetSuggestions = wrapped.collapseStreetSuggestions;
export const decodePolyline = real.decodePolyline;

process.on("exit", () => {
  const out = process.env.OSM_RECORD_OUT;
  if (!out) return;
  writeFileSync(out, gzipSync(JSON.stringify({ source: process.argv[1], records: recorder.records })));
});
