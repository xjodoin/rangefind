import assert from "node:assert/strict";
import test from "node:test";
import { shardBboxDistanceMeters, shardBoxIntersects } from "../src/geo_shard_routing.js";

const farEasternRussia = [37.3, 127, 82.1, -169];

test("wrapped shard bboxes contain points across the antimeridian", () => {
  assert.equal(shardBboxDistanceMeters(farEasternRussia, 43.1, 131.9), 0);
  assert.equal(shardBboxDistanceMeters(farEasternRussia, 65, 179), 0);
  assert.equal(shardBboxDistanceMeters(farEasternRussia, 65, -175), 0);
  assert.ok(shardBboxDistanceMeters(farEasternRussia, 45.5, -73.6) > 5_000_000);
});

test("wrapped shard bboxes intersect only matching map viewports", () => {
  assert.equal(shardBoxIntersects(farEasternRussia, {
    minLat: 42,
    maxLat: 44,
    minLon: 130,
    maxLon: 134
  }), true);
  assert.equal(shardBoxIntersects(farEasternRussia, {
    minLat: 60,
    maxLat: 70,
    minLon: 170,
    maxLon: -175
  }), true);
  assert.equal(shardBoxIntersects(farEasternRussia, {
    minLat: 44,
    maxLat: 47,
    minLon: -75,
    maxLon: -72
  }), false);
});
