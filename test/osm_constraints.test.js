import assert from "node:assert/strict";
import test from "node:test";
import {
  compileOsmConstraintFilters,
  evaluateOsmConstraints,
  parseOsmConstraints
} from "../src/integrations/osm/constraints.js";
import { evaluateOpeningHours } from "../src/integrations/osm/opening_hours.js";
import { searchOsmQuery } from "../src/integrations/osm/query.js";

test("constraint parser removes natural-language modifiers and compiles indexed predicates", () => {
  const parsed = parseOsmConstraints("wheelchair-accessible pharmacy open now with contactless");
  assert.equal(parsed.query, "pharmacy");
  assert.deepEqual(parsed.constraints, { wheelchair: true, contactless: true, openNow: true });
  assert.deepEqual(compileOsmConstraintFilters(parsed.constraints, new Set(["wheelchair", "payment_contactless"])), {
    wheelchair: ["yes", "designated"],
    payment_contactless: ["yes"]
  });
});

test("opening-hours evaluator handles weekly, overnight, closed, 24/7, and unknown holiday rules", () => {
  const monday = new Date("2026-08-03T15:00:00Z");
  assert.equal(evaluateOpeningHours("Mo-Fr 09:00-17:00", { at: monday, timeZone: "UTC" }).state, "open");
  assert.equal(evaluateOpeningHours("Mo-Fr 09:00-14:00", { at: monday, timeZone: "UTC" }).state, "closed");
  assert.equal(evaluateOpeningHours("Mo-Fr 22:00-02:00", { at: new Date("2026-08-04T01:00:00Z"), timeZone: "UTC" }).state, "open");
  assert.equal(evaluateOpeningHours("24/7", { at: monday, timeZone: "UTC" }).state, "open");
  assert.equal(evaluateOpeningHours("Mo-Fr 09:00-17:00; PH off", { at: monday, timeZone: "UTC" }).state, "unknown");
});

test("constraint verification is conservative for open-now and OSM detail values", () => {
  const evaluation = evaluateOsmConstraints({
    details: { opening_hours: "Mo-Su 00:00-24:00", wheelchair: "yes", payment_contactless: "yes" }
  }, { openNow: true, wheelchair: true, contactless: true }, {
    at: new Date("2026-08-03T15:00:00Z"), timeZone: "UTC"
  });
  assert.equal(evaluation.matches, true);
  assert.equal(evaluation.openingHours.isOpen, true);
  assert.equal(evaluateOsmConstraints({ details: {} }, { openNow: true }).matches, false);
});

test("ordinary OSM search applies the same constraint engine and annotates open state", async () => {
  let received;
  const engine = {
    manifest: { facets: {} },
    loadFacetValues: async () => [],
    search: async params => {
      received = params;
      return {
        total: 2,
        page: 1,
        size: params.size,
        results: [
          { id: "open", details: { opening_hours: "24/7" } },
          { id: "closed", details: { opening_hours: "Mo-Su off" } }
        ],
        stats: {}
      };
    }
  };
  const response = await searchOsmQuery(engine, {
    q: "open now",
    size: 5,
    at: "2026-08-03T15:00:00Z",
    timeZone: "UTC"
  });
  assert.equal(received.q, "");
  assert.deepEqual(response.results.map(result => result.id), ["open"]);
  assert.equal(response.results[0].openNow, true);
  assert.equal(response.stats.osmConstraintCandidates, 2);
});
