import { haversineMetersE7, latToE7, lonToE7 } from "./geo_tree.js";

function wrapLonDelta(a, b) {
  return ((a - b + 540) % 360) - 180;
}

function longitudeIntervals(minLon, maxLon) {
  return minLon <= maxLon
    ? [[minLon, maxLon]]
    : [[minLon, 180], [-180, maxLon]];
}

function longitudeContains(minLon, maxLon, lon) {
  return minLon <= maxLon
    ? lon >= minLon && lon <= maxLon
    : lon >= minLon || lon <= maxLon;
}

export function shardBboxDistanceMeters(bbox, lat, lon) {
  const clampedLat = Math.min(Math.max(lat, bbox[0]), bbox[2]);
  let clampedLon = lon;
  if (!longitudeContains(bbox[1], bbox[3], lon)) {
    clampedLon = Math.abs(wrapLonDelta(lon, bbox[1])) <= Math.abs(wrapLonDelta(lon, bbox[3]))
      ? bbox[1]
      : bbox[3];
  }
  return haversineMetersE7(
    latToE7(lat),
    lonToE7(lon),
    latToE7(clampedLat),
    lonToE7(clampedLon)
  );
}

export function shardBoxIntersects(bbox, box) {
  const minLat = Math.min(Number(box.minLat), Number(box.maxLat));
  const maxLat = Math.max(Number(box.minLat), Number(box.maxLat));
  if (bbox[2] < minLat || bbox[0] > maxLat) return false;
  const shardIntervals = longitudeIntervals(bbox[1], bbox[3]);
  const queryIntervals = longitudeIntervals(Number(box.minLon), Number(box.maxLon));
  return shardIntervals.some(shard => queryIntervals.some(query => (
    shard[1] >= query[0] && shard[0] <= query[1]
  )));
}
