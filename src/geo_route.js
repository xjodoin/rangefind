import { boxesForRadiusE7, haversineMeters, latToE7, lonToE7 } from "./geo_tree.js";

const EARTH_RADIUS_METERS = 6371008.7714;
const DEG_TO_RAD = Math.PI / 180;

function finiteCoordinate(lat, lon) {
  const latitude = Number(lat);
  const longitude = Number(lon);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)
      || latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
    throw new RangeError("Rangefind: route coordinates require latitude in [-90, 90] and longitude in [-180, 180].");
  }
  return { lat: latitude, lon: longitude };
}

export function decodePolyline(value, precision = 5) {
  const encoded = String(value || "");
  const factor = 10 ** Math.max(0, Math.min(7, Math.floor(Number(precision) || 5)));
  const points = [];
  let index = 0;
  let lat = 0;
  let lon = 0;
  const component = () => {
    let result = 0;
    let shift = 0;
    while (index < encoded.length) {
      const byte = encoded.charCodeAt(index++) - 63;
      if (byte < 0 || byte > 63) throw new Error("Rangefind: route is not a valid encoded polyline.");
      result |= (byte & 0x1f) << shift;
      if (byte < 0x20) return (result & 1) ? ~(result >> 1) : result >> 1;
      shift += 5;
      if (shift > 30) throw new Error("Rangefind: encoded polyline component is too large.");
    }
    throw new Error("Rangefind: encoded polyline ends inside a coordinate.");
  };
  while (index < encoded.length) {
    lat += component();
    lon += component();
    points.push(finiteCoordinate(lat / factor, lon / factor));
  }
  return points;
}

export function encodePolyline(points, precision = 5) {
  const factor = 10 ** Math.max(0, Math.min(7, Math.floor(Number(precision) || 5)));
  let previousLat = 0;
  let previousLon = 0;
  let encoded = "";
  const component = delta => {
    let value = delta < 0 ? ~(delta << 1) : delta << 1;
    let out = "";
    while (value >= 0x20) {
      out += String.fromCharCode((0x20 | (value & 0x1f)) + 63);
      value >>= 5;
    }
    return out + String.fromCharCode(value + 63);
  };
  for (const raw of points || []) {
    const point = Array.isArray(raw) ? finiteCoordinate(raw[1], raw[0]) : finiteCoordinate(raw.lat, raw.lon ?? raw.lng);
    const lat = Math.round(point.lat * factor);
    const lon = Math.round(point.lon * factor);
    encoded += component(lat - previousLat) + component(lon - previousLon);
    previousLat = lat;
    previousLon = lon;
  }
  return encoded;
}

function geoJsonCoordinateLines(value) {
  const geometry = value?.type === "Feature" ? value.geometry : value;
  if (value?.type === "FeatureCollection") {
    return value.features.flatMap(feature => geoJsonCoordinateLines(feature));
  }
  if (geometry?.type === "LineString") return [geometry.coordinates];
  if (geometry?.type === "MultiLineString") return geometry.coordinates;
  if (geometry?.type === "GeometryCollection") return geometry.geometries.flatMap(item => geoJsonCoordinateLines(item));
  throw new TypeError("Rangefind: route GeoJSON must contain a LineString or MultiLineString.");
}

export function normalizeRouteGeometry(route, options = {}) {
  let lines;
  if (typeof route === "string") {
    lines = [decodePolyline(route, options.precision ?? options.polylinePrecision ?? 5)];
  } else if (Array.isArray(route)) {
    lines = [route];
  } else {
    lines = geoJsonCoordinateLines(route).map(line => line.map(coordinate => finiteCoordinate(coordinate[1], coordinate[0])));
  }
  const points = [];
  for (const line of lines) {
    for (const raw of line) {
      const point = Array.isArray(raw) ? finiteCoordinate(raw[1], raw[0]) : finiteCoordinate(raw.lat, raw.lon ?? raw.lng);
      const previous = points.at(-1);
      if (!previous || previous.lat !== point.lat || previous.lon !== point.lon) points.push(point);
    }
  }
  if (points.length < 2) throw new RangeError("Rangefind: a route requires at least two distinct coordinates.");
  return points;
}

function initialBearing(start, end) {
  const phi1 = start.lat * DEG_TO_RAD;
  const phi2 = end.lat * DEG_TO_RAD;
  const lambda = (end.lon - start.lon) * DEG_TO_RAD;
  const y = Math.sin(lambda) * Math.cos(phi2);
  const x = Math.cos(phi1) * Math.sin(phi2) - Math.sin(phi1) * Math.cos(phi2) * Math.cos(lambda);
  return (Math.atan2(y, x) / DEG_TO_RAD + 360) % 360;
}

function uniqueBoxes(boxes) {
  const seen = new Set();
  return boxes.filter(box => {
    const key = `${box.minLatE7}:${box.maxLatE7}:${box.minLonE7}:${box.maxLonE7}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function prepareRoute(route, options = {}) {
  const points = normalizeRouteGeometry(route, options);
  const corridorMeters = Number(options.corridorMeters ?? 1000);
  if (!Number.isFinite(corridorMeters) || corridorMeters <= 0 || corridorMeters > 100000) {
    throw new RangeError("Rangefind: corridorMeters must be a positive number no greater than 100000.");
  }
  const segments = [];
  let totalMeters = 0;
  const boxes = [];
  for (let index = 1; index < points.length; index++) {
    const start = points[index - 1];
    const end = points[index];
    const lengthMeters = haversineMeters(start.lat, start.lon, end.lat, end.lon);
    if (lengthMeters <= 0) continue;
    segments.push({
      index: segments.length,
      start,
      end,
      startMeters: totalMeters,
      lengthMeters,
      bearingDegrees: initialBearing(start, end)
    });
    // A chain of radius boxes is a conservative, antimeridian-safe prune for
    // the tree and shard routers. Category-cell routing uses the original
    // segments directly, so diagonal routes do not explode into huge boxes.
    const steps = Math.max(1, Math.ceil(lengthMeters / Math.max(1000, corridorMeters * 1.25)));
    for (let step = 0; step <= steps; step++) {
      const ratio = step / steps;
      const lat = start.lat + (end.lat - start.lat) * ratio;
      let lonDelta = end.lon - start.lon;
      if (lonDelta > 180) lonDelta -= 360;
      if (lonDelta < -180) lonDelta += 360;
      let lon = start.lon + lonDelta * ratio;
      if (lon > 180) lon -= 360;
      if (lon < -180) lon += 360;
      boxes.push(...boxesForRadiusE7(latToE7(lat), lonToE7(lon), corridorMeters));
    }
    totalMeters += lengthMeters;
  }
  if (!segments.length) throw new RangeError("Rangefind: a route requires at least one non-zero segment.");
  // Do not union this chain into one diagonal bounding rectangle: keeping
  // the small overlapping boxes separate is what lets the ordinary geo tree
  // stay corridor-thin when no category-cell route is available (brand/name
  // searches such as "Tim Hortons").
  return { points, segments, totalMeters, corridorMeters, boxes: uniqueBoxes(boxes) };
}

// Closest point on a route using an azimuthal-equidistant approximation per
// segment. Route polyline legs are short in real routing output; the final
// distance uses haversine and remains stable across latitude and datelines.
export function matchPointToRoute(prepared, lat, lon) {
  const point = finiteCoordinate(lat, lon);
  let best = null;
  for (const segment of prepared.segments) {
    const referenceLat = (segment.start.lat + segment.end.lat + point.lat) / 3 * DEG_TO_RAD;
    let endDeltaLon = segment.end.lon - segment.start.lon;
    if (endDeltaLon > 180) endDeltaLon -= 360;
    if (endDeltaLon < -180) endDeltaLon += 360;
    let pointDeltaLon = point.lon - segment.start.lon;
    if (pointDeltaLon > 180) pointDeltaLon -= 360;
    if (pointDeltaLon < -180) pointDeltaLon += 360;
    const ax = 0;
    const ay = 0;
    const bx = endDeltaLon * DEG_TO_RAD * Math.cos(referenceLat) * EARTH_RADIUS_METERS;
    const by = (segment.end.lat - segment.start.lat) * DEG_TO_RAD * EARTH_RADIUS_METERS;
    const px = pointDeltaLon * DEG_TO_RAD * Math.cos(referenceLat) * EARTH_RADIUS_METERS;
    const py = (point.lat - segment.start.lat) * DEG_TO_RAD * EARTH_RADIUS_METERS;
    const denominator = (bx - ax) ** 2 + (by - ay) ** 2;
    const ratio = denominator ? Math.max(0, Math.min(1, ((px - ax) * (bx - ax) + (py - ay) * (by - ay)) / denominator)) : 0;
    let routeLon = segment.start.lon + endDeltaLon * ratio;
    if (routeLon > 180) routeLon -= 360;
    if (routeLon < -180) routeLon += 360;
    const routeLat = segment.start.lat + (segment.end.lat - segment.start.lat) * ratio;
    const distanceMeters = haversineMeters(point.lat, point.lon, routeLat, routeLon);
    const progressMeters = segment.startMeters + segment.lengthMeters * ratio;
    if (!best || distanceMeters < best.distanceMeters
        || (distanceMeters === best.distanceMeters && progressMeters < best.progressMeters)) {
      best = {
        distanceMeters,
        progressMeters,
        progressRatio: prepared.totalMeters ? progressMeters / prepared.totalMeters : 0,
        segmentIndex: segment.index,
        segmentProgress: ratio,
        bearingDegrees: segment.bearingDegrees,
        rejoinPoint: { lat: routeLat, lon: routeLon }
      };
    }
  }
  return best;
}

export function routeMatchPublic(match) {
  if (!match) return null;
  return {
    distanceMeters: Math.round(match.distanceMeters * 10) / 10,
    progressMeters: Math.round(match.progressMeters * 10) / 10,
    progressRatio: Math.round(match.progressRatio * 1e6) / 1e6,
    segmentIndex: match.segmentIndex,
    segmentProgress: Math.round(match.segmentProgress * 1e6) / 1e6,
    bearingDegrees: Math.round(match.bearingDegrees * 10) / 10,
    rejoinPoint: {
      lat: Math.round(match.rejoinPoint.lat * 1e7) / 1e7,
      lon: Math.round(match.rejoinPoint.lon * 1e7) / 1e7
    }
  };
}
