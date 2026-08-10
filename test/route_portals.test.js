import assert from "node:assert/strict";
import test from "node:test";
import {
  decodeRoutePortalIds,
  decodeRoutePortalRecords,
  encodeRoutePortalIds,
  encodeRoutePortalRecords
} from "../src/route_portals.js";

test("route portal blocks delta-code exact 53-bit OSM ids and signed E7 coordinates", () => {
  const values = [
    123, 455000000, -736000000,
    9_876_543_210, -123456789, 1799999999,
    9_876_543_999, -123456700, -1799999999
  ];
  const ids = decodeRoutePortalIds(encodeRoutePortalIds(values));
  const records = decodeRoutePortalRecords(encodeRoutePortalRecords(values));
  assert.deepEqual([...ids], values.filter((_, index) => index % 3 === 0));
  assert.deepEqual([...records.ids], [...ids]);
  assert.deepEqual([...records.latE7], values.filter((_, index) => index % 3 === 1));
  assert.deepEqual([...records.lonE7], values.filter((_, index) => index % 3 === 2));
});

test("route portal encoding rejects unsorted and duplicate OSM ids", () => {
  assert.throws(() => encodeRoutePortalIds([2, 0, 0, 1, 0, 0]), /ascending order/);
  assert.throws(() => encodeRoutePortalRecords([2, 0, 0, 2, 0, 0]), /ascending order/);
});
