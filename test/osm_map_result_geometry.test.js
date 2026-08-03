import assert from "node:assert/strict";
import test from "node:test";
import {
  geometryFeatureBounds,
  parseResultBbox,
  postalCoverageBounds,
  resultCoverageBounds,
  resultGeometryFeature
} from "../examples/osm-geo/public/result_geometry.js";

test("postal coverage accepts array and hydrated text bboxes", () => {
  assert.deepEqual(parseResultBbox([45.63, -73.82, 45.65, -73.78]), {
    south: 45.63,
    west: -73.82,
    north: 45.65,
    east: -73.78
  });
  assert.deepEqual(parseResultBbox("45.63 -73.82 45.65 -73.78"), {
    south: 45.63,
    west: -73.82,
    north: 45.65,
    east: -73.78
  });
});

test("single-sample postal results receive a visible approximate box", () => {
  const item = {
    id: "postal/CA/J7A1V6",
    type: "postal_code",
    postcode: "J7A 1V6",
    bbox: "45.64 -73.7971 45.64 -73.7971",
    lat: 45.64,
    lon: -73.7971
  };
  const bounds = postalCoverageBounds(item);
  assert.ok(bounds[0][0] < item.lon && bounds[1][0] > item.lon);
  assert.ok(bounds[0][1] < item.lat && bounds[1][1] > item.lat);

  const feature = resultGeometryFeature(item);
  assert.equal(feature.properties.kind, "postal-area");
  assert.equal(feature.properties.postcode, "J7A 1V6");
  assert.equal(feature.geometry.type, "Polygon");
  assert.deepEqual(feature.geometry.coordinates[0][0], feature.geometry.coordinates[0].at(-1));
});

test("bbox rendering is generic for cities and other bounded results", () => {
  const item = {
    id: "relation/1",
    type: "city",
    category: "boundary",
    bbox: [45, -74, 46, -73],
    lat: 45.5,
    lon: -73.5
  };
  assert.deepEqual(resultCoverageBounds(item), [[-74, 45], [-73, 46]]);
  const feature = resultGeometryFeature(item);
  assert.equal(feature.properties.kind, "result-area");
  assert.equal(feature.geometry.type, "Polygon");
  assert.deepEqual(geometryFeatureBounds(feature), [[-74, 45], [-73, 46]]);
});

test("ordinary points without geometry or bbox remain markers only", () => {
  assert.equal(resultGeometryFeature({ id: "node/1", type: "cafe", lat: 45.5, lon: -73.5 }), null);
});

test("real encoded geometry takes precedence over a postal fallback", () => {
  const feature = resultGeometryFeature({
    id: "postal/CA/J7A1V6",
    type: "postal_code",
    bbox: [45, -74, 46, -73],
    geometry: { encoding: "polyline", encoded: "value", type: "Polygon", precision: 5 }
  }, () => [{ lat: 45, lon: -74 }, { lat: 45, lon: -73 }, { lat: 46, lon: -74 }]);
  assert.equal(feature.properties.kind, "postal-area");
  assert.equal(feature.properties.approximate, false);
  assert.equal(feature.geometry.type, "Polygon");
  assert.deepEqual(geometryFeatureBounds(feature), [[-74, 45], [-73, 46]]);
});
