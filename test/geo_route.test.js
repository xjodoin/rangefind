import assert from "node:assert/strict";
import test from "node:test";
import { geoCellsForRoute } from "../src/geo_cells.js";
import {
  decodePolyline,
  encodePolyline,
  matchPointToRoute,
  normalizeRouteGeometry,
  prepareRoute,
  routeMatchPublic
} from "../src/geo_route.js";

const ROUTE = [
  { lat: 45.5, lon: -73.65 },
  { lat: 45.52, lon: -73.60 },
  { lat: 45.55, lon: -73.55 }
];

test("encoded polylines and GeoJSON normalize to the same route", () => {
  const encoded = encodePolyline(ROUTE, 5);
  const decoded = decodePolyline(encoded, 5);
  assert.deepEqual(decoded, ROUTE);
  assert.deepEqual(normalizeRouteGeometry({
    type: "Feature",
    geometry: { type: "LineString", coordinates: ROUTE.map(point => [point.lon, point.lat]) },
    properties: {}
  }), ROUTE);
});

test("route matching returns cross-track distance, travel progress, bearing, and rejoin point", () => {
  const prepared = prepareRoute(ROUTE, { corridorMeters: 1500 });
  const first = matchPointToRoute(prepared, 45.505, -73.625);
  const later = matchPointToRoute(prepared, 45.54, -73.566);
  assert.ok(first.distanceMeters < 500);
  assert.ok(later.progressMeters > first.progressMeters);
  assert.ok(first.bearingDegrees > 0 && first.bearingDegrees < 180);
  const publicMatch = routeMatchPublic(first);
  assert.ok(Number.isFinite(publicMatch.rejoinPoint.lat));
  assert.ok(publicMatch.progressRatio > 0 && publicMatch.progressRatio < 1);
});

test("route category-cell raster stays thin and respects its hard cell budget", () => {
  const prepared = prepareRoute(ROUTE, { corridorMeters: 800 });
  const cells = geoCellsForRoute(prepared, 12, 48);
  assert.ok(cells.length > 1 && cells.length <= 48);
  assert.equal(geoCellsForRoute(prepared, 18, 2), null);
});
