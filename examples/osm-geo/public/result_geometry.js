const METERS_PER_LATITUDE_DEGREE = 111_320;
const MIN_POSTAL_HALF_SIZE_METERS = 250;

function finiteNumbers(value) {
  if (Array.isArray(value)) return value.map(Number);
  if (typeof value === "string") {
    return (value.match(/[+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?/giu) || []).map(Number);
  }
  return [];
}

/** Parse Rangefind's canonical [minLat, minLon, maxLat, maxLon] extent. */
export function parseResultBbox(value) {
  const numbers = finiteNumbers(value);
  if (numbers.length !== 4 || numbers.some(number => !Number.isFinite(number))) return null;
  const [firstLat, firstLon, secondLat, secondLon] = numbers;
  const south = Math.min(firstLat, secondLat);
  const north = Math.max(firstLat, secondLat);
  const west = Math.min(firstLon, secondLon);
  const east = Math.max(firstLon, secondLon);
  if (south < -90 || north > 90 || west < -180 || east > 180) return null;
  return { south, west, north, east };
}

function parseGeometryBbox(value) {
  const numbers = finiteNumbers(value);
  if (numbers.length !== 4 || numbers.some(number => !Number.isFinite(number))) return null;
  const [firstLon, firstLat, secondLon, secondLat] = numbers;
  return parseResultBbox([firstLat, firstLon, secondLat, secondLon]);
}

/**
 * Return a visible postal coverage box as MapLibre [[west,south],[east,north]].
 * A single source point is padded to a small 500 m display box; the UI labels
 * this as approximate coverage rather than claiming a legal postal boundary.
 */
export function resultCoverageBounds(item, options = {}) {
  const postal = String(item?.type || "").toLowerCase() === "postal_code";
  const pointLat = Number(item?.lat);
  const pointLon = Number(item?.lon);
  const parsed = parseResultBbox(item?.bbox)
    || parseGeometryBbox(item?.geometry?.bbox)
    || (postal && Number.isFinite(pointLat) && Number.isFinite(pointLon)
      ? { south: pointLat, west: pointLon, north: pointLat, east: pointLon }
      : null);
  if (!parsed) return null;

  const minimumHalfSizeMeters = Math.max(0, Number(
    options.minimumHalfSizeMeters ?? (postal ? MIN_POSTAL_HALF_SIZE_METERS : 0)
  ));
  const centerLat = (parsed.south + parsed.north) / 2;
  const centerLon = (parsed.west + parsed.east) / 2;
  const latitudeHalfSpan = Math.max(
    (parsed.north - parsed.south) / 2,
    minimumHalfSizeMeters / METERS_PER_LATITUDE_DEGREE
  );
  const longitudeScale = Math.max(0.2, Math.cos(centerLat * Math.PI / 180));
  const longitudeHalfSpan = Math.max(
    (parsed.east - parsed.west) / 2,
    minimumHalfSizeMeters / (METERS_PER_LATITUDE_DEGREE * longitudeScale)
  );
  return [
    [Math.max(-180, centerLon - longitudeHalfSpan), Math.max(-90, centerLat - latitudeHalfSpan)],
    [Math.min(180, centerLon + longitudeHalfSpan), Math.min(90, centerLat + latitudeHalfSpan)]
  ];
}

// Backwards-friendly semantic helper for callers that specifically need a
// postal extent. General map rendering should use resultCoverageBounds().
export function postalCoverageBounds(item, options = {}) {
  return String(item?.type || "").toLowerCase() === "postal_code"
    ? resultCoverageBounds(item, options)
    : null;
}

function bboxAreaFeature(item) {
  const bounds = resultCoverageBounds(item);
  if (!bounds) return null;
  const [[west, south], [east, north]] = bounds;
  const postal = String(item?.type || "").toLowerCase() === "postal_code";
  return {
    type: "Feature",
    id: item.id,
    properties: {
      id: item.id,
      name: item.name || item.title || item.id,
      ...(postal ? { postcode: item.postcode || item.name || "Postal area" } : {}),
      kind: postal ? "postal-area" : "result-area",
      approximate: true
    },
    geometry: {
      type: "Polygon",
      coordinates: [[
        [west, south],
        [east, south],
        [east, north],
        [west, north],
        [west, south]
      ]]
    }
  };
}

/** Build the map overlay for a real encoded geometry or postal bbox fallback. */
export function resultGeometryFeature(item, decodePolyline) {
  const geometry = item?.geometry;
  const postal = String(item?.type || "").toLowerCase() === "postal_code";
  if (geometry?.encoding === "polyline" && geometry.encoded && typeof decodePolyline === "function") {
    try {
      const coordinates = decodePolyline(geometry.encoded, geometry.precision || 5)
        .map(point => [point.lon, point.lat]);
      if (coordinates.length >= 2) {
        return {
          type: "Feature",
          id: item.id,
          properties: {
            id: item.id,
            name: item.name || item.title || item.id,
            ...(postal ? { postcode: item.postcode || item.name || "Postal area" } : {}),
            kind: postal ? "postal-area" : "result-geometry",
            approximate: false
          },
          geometry: geometry.type === "Polygon"
            ? { type: "Polygon", coordinates: [coordinates] }
            : { type: "LineString", coordinates }
        };
      }
    } catch {
      // A bbox remains a useful fallback when optional geometry is bad.
    }
  }
  return bboxAreaFeature(item);
}

/** Compute a MapLibre extent for any GeoJSON result overlay. */
export function geometryFeatureBounds(feature) {
  const positions = [];
  const collect = value => {
    if (!Array.isArray(value)) return;
    if (value.length >= 2 && Number.isFinite(Number(value[0])) && Number.isFinite(Number(value[1]))) {
      positions.push([Number(value[0]), Number(value[1])]);
      return;
    }
    for (const child of value) collect(child);
  };
  collect(feature?.geometry?.coordinates);
  if (!positions.length) return null;
  const lons = positions.map(position => position[0]);
  const lats = positions.map(position => position[1]);
  const west = Math.min(...lons);
  const east = Math.max(...lons);
  const south = Math.min(...lats);
  const north = Math.max(...lats);
  if (south < -90 || north > 90 || west < -180 || east > 180) return null;
  return [[west, south], [east, north]];
}
