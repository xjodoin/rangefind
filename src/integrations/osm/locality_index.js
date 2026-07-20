// Locality index for OSM extraction: answers "which city is this point in?"
// for documents that carry no addr:city tag (most POIs — a pharmacy node is
// typically just name + amenity). Sources, in precedence order:
//
// 1. Administrative boundary containment — boundary=administrative relations
//    at admin_level 7/8 (municipalities in nearly every country), assembled
//    from their member ways into closed rings and queried point-in-polygon.
//    Deeper (higher-numbered) levels win when nested.
// 2. Nearest place node fallback — place=city/town/… nodes with a
//    type-scaled search radius, for regions with missing or clipped
//    boundaries (Geofabrik extracts truncate boundaries at region edges;
//    unclosed rings drop the boundary rather than guess).
//
// Mapper-provided addr:city always wins over both — the extractor only
// consults this index when a document has no city of its own.

// Municipality-ish admin levels, preferred deepest-first. Level 8 is the
// municipality nearly everywhere; 7 covers countries like Andorra
// (parròquies) and municipality gaps where only the district is mapped.
export const LOCALITY_ADMIN_LEVELS = [8, 7];

// Fallback search radii by place type: a POI within this distance of the
// place node may claim it (scored by distance/radius, so a close village
// beats a distant city). Sizes follow Nominatim's place-rank intuition.
export const PLACE_RADII_METERS = Object.freeze({
  city: 15000,
  town: 8000,
  municipality: 8000,
  village: 3500,
  hamlet: 1800
});

const GRID_DEGREES = 0.1;
const EARTH_RADIUS_METERS = 6371000;
const DEGREE_METERS = (Math.PI / 180) * EARTH_RADIUS_METERS;

// Stitches member ways (sequences of node ids) into closed rings. Ways join
// end-to-end in either direction; a chain that cannot close is dropped and
// reported, because a clipped boundary must fall back to place nodes instead
// of producing a broken polygon.
export function assembleRings(ways) {
  const pending = ways
    .map(way => way.filter(ref => ref != null))
    .filter(way => way.length >= 2);
  const rings = [];
  let dropped = 0;
  while (pending.length) {
    const ring = pending.pop().slice();
    let extended = true;
    while (ring[0] !== ring[ring.length - 1] && extended) {
      extended = false;
      const tail = ring[ring.length - 1];
      for (let i = 0; i < pending.length; i++) {
        const candidate = pending[i];
        if (candidate[0] === tail) {
          for (let j = 1; j < candidate.length; j++) ring.push(candidate[j]);
        } else if (candidate[candidate.length - 1] === tail) {
          for (let j = candidate.length - 2; j >= 0; j--) ring.push(candidate[j]);
        } else {
          continue;
        }
        pending.splice(i, 1);
        extended = true;
        break;
      }
    }
    if (ring.length >= 4 && ring[0] === ring[ring.length - 1]) rings.push(ring);
    else dropped++;
  }
  return { rings, dropped };
}

function ringBbox(ring) {
  let minLat = Infinity;
  let minLon = Infinity;
  let maxLat = -Infinity;
  let maxLon = -Infinity;
  for (let i = 0; i < ring.length; i += 2) {
    const lat = ring[i];
    const lon = ring[i + 1];
    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
    if (lon < minLon) minLon = lon;
    if (lon > maxLon) maxLon = lon;
  }
  return [minLat, minLon, maxLat, maxLon];
}

// Even-odd ray cast over every ring of a boundary at once: a point inside an
// outer ring and inside an inner (hole) ring crosses an even number of edges
// and correctly tests outside.
function pointInRings(rings, lat, lon) {
  let inside = false;
  for (const ring of rings) {
    for (let i = 0, j = ring.length - 2; i < ring.length; j = i, i += 2) {
      const aLat = ring[i];
      const aLon = ring[i + 1];
      const bLat = ring[j];
      const bLon = ring[j + 1];
      if ((aLat > lat) !== (bLat > lat)
        && lon < ((bLon - aLon) * (lat - aLat)) / (bLat - aLat) + aLon) {
        inside = !inside;
      }
    }
  }
  return inside;
}

function gridKey(latCell, lonCell) {
  return latCell * 4096 + lonCell;
}

function cellOf(value) {
  return Math.floor((value + 360) / GRID_DEGREES);
}

function addToGrid(grid, bbox, value) {
  for (let latCell = cellOf(bbox[0]); latCell <= cellOf(bbox[2]); latCell++) {
    for (let lonCell = cellOf(bbox[1]); lonCell <= cellOf(bbox[3]); lonCell++) {
      const key = gridKey(latCell, lonCell);
      let bucket = grid.get(key);
      if (!bucket) {
        bucket = [];
        grid.set(key, bucket);
      }
      bucket.push(value);
    }
  }
}

function metersBetween(aLat, aLon, bLat, bLon) {
  const latMeters = (aLat - bLat) * DEGREE_METERS;
  const lonMeters = (aLon - bLon) * DEGREE_METERS * Math.cos(((aLat + bLat) / 2) * (Math.PI / 180));
  return Math.sqrt(latMeters * latMeters + lonMeters * lonMeters);
}

// rings arrive as arrays of [lat, lon] pairs; store them flat for cache-tight
// point-in-polygon scans over hundreds of thousands of vertices.
function flattenRing(ring) {
  const flat = new Float64Array(ring.length * 2);
  for (let i = 0; i < ring.length; i++) {
    flat[i * 2] = ring[i][0];
    flat[i * 2 + 1] = ring[i][1];
  }
  return flat;
}

export function createLocalityIndex() {
  const boundaries = [];
  const places = [];
  const boundaryGrid = new Map();
  const placeGrid = new Map();
  let finalized = false;

  return {
    // rings: array of closed [ [lat, lon], ... ] rings (outer and inner
    // together — even-odd containment handles holes without role tracking).
    addBoundary({ id, name, adminLevel, rings }) {
      if (finalized) throw new Error("Locality index is finalized.");
      if (!name || !rings?.length) return false;
      const flat = rings.map(flattenRing);
      let bbox = null;
      for (const ring of flat) {
        const box = ringBbox(ring);
        bbox = bbox
          ? [Math.min(bbox[0], box[0]), Math.min(bbox[1], box[1]), Math.max(bbox[2], box[2]), Math.max(bbox[3], box[3])]
          : box;
      }
      // Boundaries spanning more than half the globe are either broken
      // geometry or antimeridian crossers this planar test cannot handle.
      if (!bbox || bbox[3] - bbox[1] > 180) return false;
      boundaries.push({ id, name: String(name), adminLevel: Number(adminLevel) || 0, rings: flat, bbox });
      return true;
    },

    addPlace({ name, type, lat, lon }) {
      if (finalized) throw new Error("Locality index is finalized.");
      const radius = PLACE_RADII_METERS[String(type || "")];
      if (!name || !radius || !Number.isFinite(lat) || !Number.isFinite(lon)) return false;
      places.push({ name: String(name), radius, lat, lon });
      return true;
    },

    finalize() {
      if (finalized) return;
      finalized = true;
      // Deeper admin levels first so the first containment hit is the most
      // specific municipality.
      boundaries.sort((a, b) => b.adminLevel - a.adminLevel || a.id - b.id);
      boundaries.forEach((boundary, index) => addToGrid(boundaryGrid, boundary.bbox, index));
      for (let index = 0; index < places.length; index++) {
        const place = places[index];
        const latDelta = place.radius / DEGREE_METERS;
        const lonDelta = latDelta / Math.max(0.2, Math.cos(place.lat * (Math.PI / 180)));
        addToGrid(placeGrid, [place.lat - latDelta, place.lon - lonDelta, place.lat + latDelta, place.lon + lonDelta], index);
      }
    },

    stats() {
      return { boundaries: boundaries.length, places: places.length };
    },

    resolve(lat, lon) {
      if (!finalized) throw new Error("Locality index is not finalized.");
      const bucket = boundaryGrid.get(gridKey(cellOf(lat), cellOf(lon)));
      if (bucket) {
        for (const index of bucket) {
          const boundary = boundaries[index];
          const box = boundary.bbox;
          if (lat < box[0] || lat > box[2] || lon < box[1] || lon > box[3]) continue;
          if (pointInRings(boundary.rings, lat, lon)) {
            return { city: boundary.name, source: "boundary" };
          }
        }
      }
      const placeBucket = placeGrid.get(gridKey(cellOf(lat), cellOf(lon)));
      if (placeBucket) {
        let best = null;
        let bestScore = 1;
        for (const index of placeBucket) {
          const place = places[index];
          const score = metersBetween(lat, lon, place.lat, place.lon) / place.radius;
          if (score <= bestScore) {
            bestScore = score;
            best = place;
          }
        }
        if (best) return { city: best.name, source: "place" };
      }
      return null;
    }
  };
}
