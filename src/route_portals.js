// Compact independently-compressed blocks used by rfrouteportals-v2.
//
// A regional portal pack stores two blocks per candidate neighbor:
// - ids: sorted delta-coded OSM node ids, enough to prove membership;
// - records: the same ids plus delta-coded E7 coordinates.
//
// The runtime reads the smaller side's records and the larger side's ids,
// intersects them in linear time, and never downloads unrelated neighbors.

export const ROUTE_PORTAL_FORMAT = "rfrouteportals-v2";

const IDS_MAGIC = Uint8Array.from([0x52, 0x46, 0x50, 0x49, 0x32]); // RFPI2
const RECORDS_MAGIC = Uint8Array.from([0x52, 0x46, 0x50, 0x52, 0x32]); // RFPR2

function varintLength(value) {
  let number = Math.max(0, Math.floor(Number(value) || 0));
  let length = 1;
  while (number >= 0x80) {
    length++;
    number = Math.floor(number / 0x80);
  }
  return length;
}

function writeVarint(bytes, offset, value) {
  let number = Math.max(0, Math.floor(Number(value) || 0));
  while (number >= 0x80) {
    bytes[offset++] = (number % 0x80) | 0x80;
    number = Math.floor(number / 0x80);
  }
  bytes[offset++] = number;
  return offset;
}

function readVarint(bytes, state) {
  let value = 0;
  let multiplier = 1;
  while (state.offset < bytes.length) {
    const byte = bytes[state.offset++];
    value += (byte & 0x7f) * multiplier;
    if (byte < 0x80) {
      if (!Number.isSafeInteger(value)) throw new Error("Route portal varint exceeds the safe integer range.");
      return value;
    }
    multiplier *= 0x80;
  }
  throw new Error("Truncated route portal varint.");
}

function zigZag(value) {
  return value >= 0 ? value * 2 : -value * 2 - 1;
}

function unZigZag(value) {
  return value % 2 === 0 ? value / 2 : -(value + 1) / 2;
}

function assertMagic(bytes, magic) {
  if (bytes.length < magic.length) throw new Error("Truncated route portal block.");
  for (let index = 0; index < magic.length; index++) {
    if (bytes[index] !== magic[index]) throw new Error("Unsupported route portal block format.");
  }
}

/** Normalize both extractor-time flat triples and disk-backed columnar rows. */
export function routePortalColumns(values) {
  if (Array.isArray(values) || ArrayBuffer.isView(values)) {
    if (values.length % 3 !== 0) throw new Error("Route portals must be flat id/latE7/lonE7 triples.");
    return {
      count: values.length / 3,
      ids: values,
      latE7: values,
      lonE7: values,
      stride: 3,
      latOffset: 1,
      lonOffset: 2
    };
  }
  if (values?.ids && values?.latE7 && values?.lonE7
      && ArrayBuffer.isView(values.ids)
      && ArrayBuffer.isView(values.latE7)
      && ArrayBuffer.isView(values.lonE7)
      && values.ids.length === values.latE7.length
      && values.ids.length === values.lonE7.length) {
    return {
      count: values.ids.length,
      ids: values.ids,
      latE7: values.latE7,
      lonE7: values.lonE7,
      stride: 1,
      latOffset: 0,
      lonOffset: 0
    };
  }
  throw new Error("Route portals must be flat triples or equal-length id/latE7/lonE7 columns.");
}

export function routePortalCount(values) {
  return routePortalColumns(values).count;
}

function portalRows(values) {
  const columns = routePortalColumns(values);
  let previousId = -1;
  for (let row = 0; row < columns.count; row++) {
    const offset = row * columns.stride;
    const id = Number(columns.ids[offset]);
    const latE7 = Number(columns.latE7[offset + columns.latOffset]);
    const lonE7 = Number(columns.lonE7[offset + columns.lonOffset]);
    if (!Number.isSafeInteger(id) || id < 0 || id <= previousId) {
      throw new Error("Route portal OSM ids must be unique safe integers in ascending order.");
    }
    if (!Number.isInteger(latE7) || !Number.isInteger(lonE7)) {
      throw new Error("Route portal coordinates must be integer E7 values.");
    }
    previousId = id;
  }
  return columns;
}

export function encodeRoutePortalIds(values) {
  const columns = portalRows(values);
  const { count } = columns;
  let length = IDS_MAGIC.length + varintLength(count);
  let previousId = 0;
  for (let row = 0; row < count; row++) {
    const id = columns.ids[row * columns.stride];
    length += varintLength(id - previousId);
    previousId = id;
  }
  const bytes = new Uint8Array(length);
  bytes.set(IDS_MAGIC);
  let cursor = writeVarint(bytes, IDS_MAGIC.length, count);
  previousId = 0;
  for (let row = 0; row < count; row++) {
    const id = columns.ids[row * columns.stride];
    cursor = writeVarint(bytes, cursor, id - previousId);
    previousId = id;
  }
  return bytes;
}

export function encodeRoutePortalRecords(values) {
  const columns = portalRows(values);
  const { count } = columns;
  let length = RECORDS_MAGIC.length + varintLength(count);
  let previousId = 0;
  let previousLat = 0;
  let previousLon = 0;
  for (let row = 0; row < count; row++) {
    const offset = row * columns.stride;
    const id = columns.ids[offset];
    const lat = columns.latE7[offset + columns.latOffset];
    const lon = columns.lonE7[offset + columns.lonOffset];
    length += varintLength(id - previousId)
      + varintLength(zigZag(lat - previousLat))
      + varintLength(zigZag(lon - previousLon));
    previousId = id;
    previousLat = lat;
    previousLon = lon;
  }
  const bytes = new Uint8Array(length);
  bytes.set(RECORDS_MAGIC);
  let cursor = writeVarint(bytes, RECORDS_MAGIC.length, count);
  previousId = 0;
  previousLat = 0;
  previousLon = 0;
  for (let row = 0; row < count; row++) {
    const offset = row * columns.stride;
    const id = columns.ids[offset];
    const lat = columns.latE7[offset + columns.latOffset];
    const lon = columns.lonE7[offset + columns.lonOffset];
    cursor = writeVarint(bytes, cursor, id - previousId);
    cursor = writeVarint(bytes, cursor, zigZag(lat - previousLat));
    cursor = writeVarint(bytes, cursor, zigZag(lon - previousLon));
    previousId = id;
    previousLat = lat;
    previousLon = lon;
  }
  return bytes;
}

export function decodeRoutePortalIds(input) {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  assertMagic(bytes, IDS_MAGIC);
  const state = { offset: IDS_MAGIC.length };
  const count = readVarint(bytes, state);
  const ids = new Float64Array(count);
  let previousId = 0;
  for (let index = 0; index < count; index++) {
    const id = previousId + readVarint(bytes, state);
    if (!Number.isSafeInteger(id) || id <= previousId) throw new Error("Invalid route portal id ordering.");
    ids[index] = id;
    previousId = id;
  }
  if (state.offset !== bytes.length) throw new Error("Route portal id block has trailing bytes.");
  return ids;
}

export function decodeRoutePortalRecords(input) {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  assertMagic(bytes, RECORDS_MAGIC);
  const state = { offset: RECORDS_MAGIC.length };
  const count = readVarint(bytes, state);
  const ids = new Float64Array(count);
  const latE7 = new Int32Array(count);
  const lonE7 = new Int32Array(count);
  let previousId = 0;
  let previousLat = 0;
  let previousLon = 0;
  for (let index = 0; index < count; index++) {
    const id = previousId + readVarint(bytes, state);
    const lat = previousLat + unZigZag(readVarint(bytes, state));
    const lon = previousLon + unZigZag(readVarint(bytes, state));
    if (!Number.isSafeInteger(id) || id <= previousId || !Number.isInteger(lat) || !Number.isInteger(lon)) {
      throw new Error("Invalid route portal record ordering.");
    }
    ids[index] = id;
    latE7[index] = lat;
    lonE7[index] = lon;
    previousId = id;
    previousLat = lat;
    previousLon = lon;
  }
  if (state.offset !== bytes.length) throw new Error("Route portal record block has trailing bytes.");
  return { ids, latE7, lonE7 };
}
