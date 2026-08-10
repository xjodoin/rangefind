import assert from "node:assert/strict";
import test from "node:test";
import {
  decodeRoutePortalIds,
  decodeRoutePortalRecords,
  encodeRoutePortalIds,
  encodeRoutePortalRecords,
  routePortalCount
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

test("route portal codecs consume disk-backed columnar candidates", () => {
  const values = {
    ids: Float64Array.from([10, 25, 90]),
    latE7: Int32Array.from([455000000, 455000010, 455000020]),
    lonE7: Int32Array.from([-736000000, -735999990, -735999980])
  };
  assert.equal(routePortalCount(values), 3);
  assert.deepEqual([...decodeRoutePortalIds(encodeRoutePortalIds(values))], [...values.ids]);
  const records = decodeRoutePortalRecords(encodeRoutePortalRecords(values));
  assert.deepEqual([...records.ids], [...values.ids]);
  assert.deepEqual([...records.latE7], [...values.latE7]);
  assert.deepEqual([...records.lonE7], [...values.lonE7]);
});
