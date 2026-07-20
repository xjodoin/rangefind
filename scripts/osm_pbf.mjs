// Minimal pure-Node OpenStreetMap PBF reader.
//
// Supports the subset of the OSM PBF format needed to extract tagged nodes,
// tagged ways, and relations from Geofabrik-style extracts: file blobs, zlib
// blob compression, dense nodes, plain nodes, ways (tags + node refs), and
// relations (tags + typed members with roles).
// See https://wiki.openstreetmap.org/wiki/PBF_Format for the schema.

import { openSync, readSync, closeSync } from "node:fs";
import { inflateSync } from "node:zlib";

const WIRE_VARINT = 0;
const WIRE_FIXED64 = 1;
const WIRE_BYTES = 2;
const WIRE_FIXED32 = 5;

function readVarint(bytes, state) {
  let result = 0;
  let shift = 0;
  for (;;) {
    const byte = bytes[state.pos++];
    if (byte === undefined) throw new Error("OSM PBF varint ran past end of buffer.");
    if (shift < 28) {
      result |= (byte & 0x7f) << shift;
    } else {
      result += (byte & 0x7f) * 2 ** shift;
    }
    if ((byte & 0x80) === 0) break;
    shift += 7;
  }
  return result >>> 0 === result ? result >>> 0 : result;
}

// Varints above 2^31 need float math for the high bits; OSM ids fit in 2^53.
function readVarint53(bytes, state) {
  let low = 0;
  let high = 0;
  let shift = 0;
  for (;;) {
    const byte = bytes[state.pos++];
    if (byte === undefined) throw new Error("OSM PBF varint ran past end of buffer.");
    if (shift < 28) low |= (byte & 0x7f) << shift;
    else high += (byte & 0x7f) * 2 ** (shift - 28);
    if ((byte & 0x80) === 0) break;
    shift += 7;
  }
  return (low >>> 0) + high * 2 ** 28;
}

function zigzag(value) {
  // Works for values encoded from |v| < 2^52.
  return value % 2 === 0 ? value / 2 : -(value + 1) / 2;
}

function skipField(bytes, state, wireType) {
  if (wireType === WIRE_VARINT) readVarint53(bytes, state);
  else if (wireType === WIRE_FIXED64) state.pos += 8;
  else if (wireType === WIRE_BYTES) {
    const length = readVarint(bytes, state);
    state.pos += length;
  } else if (wireType === WIRE_FIXED32) state.pos += 4;
  else throw new Error(`Unsupported OSM PBF wire type ${wireType}.`);
}

function readBytes(bytes, state) {
  const length = readVarint(bytes, state);
  const start = state.pos;
  state.pos += length;
  return bytes.subarray(start, state.pos);
}

function* fields(bytes) {
  const state = { pos: 0 };
  const end = bytes.length;
  while (state.pos < end) {
    const key = readVarint(bytes, state);
    yield { field: key >>> 3, wireType: key & 7, state };
  }
}

function parseBlobHeader(bytes) {
  let type = "";
  let datasize = 0;
  for (const { field, wireType, state } of fields(bytes)) {
    if (field === 1 && wireType === WIRE_BYTES) type = Buffer.from(readBytes(bytes, state)).toString("utf8");
    else if (field === 3 && wireType === WIRE_VARINT) datasize = readVarint(bytes, state);
    else skipField(bytes, state, wireType);
  }
  return { type, datasize };
}

function parseBlob(bytes) {
  let raw = null;
  let zlibData = null;
  for (const { field, wireType, state } of fields(bytes)) {
    if (field === 1 && wireType === WIRE_BYTES) raw = readBytes(bytes, state);
    else if (field === 3 && wireType === WIRE_BYTES) zlibData = readBytes(bytes, state);
    else skipField(bytes, state, wireType);
  }
  if (raw) return Buffer.from(raw);
  if (zlibData) return inflateSync(zlibData);
  throw new Error("OSM PBF blob uses an unsupported compression codec (only raw and zlib are supported).");
}

function parseStringTable(bytes) {
  const table = [];
  for (const { field, wireType, state } of fields(bytes)) {
    if (field === 1 && wireType === WIRE_BYTES) table.push(Buffer.from(readBytes(bytes, state)).toString("utf8"));
    else skipField(bytes, state, wireType);
  }
  return table;
}

function readPackedSint(bytes, state, out) {
  const packed = readBytes(bytes, state);
  const packedState = { pos: 0 };
  while (packedState.pos < packed.length) out.push(zigzag(readVarint53(packed, packedState)));
}

function readPackedUint(bytes, state, out) {
  const packed = readBytes(bytes, state);
  const packedState = { pos: 0 };
  while (packedState.pos < packed.length) out.push(readVarint53(packed, packedState));
}

function parseDenseNodes(bytes) {
  const ids = [];
  const lats = [];
  const lons = [];
  const keysVals = [];
  for (const { field, wireType, state } of fields(bytes)) {
    if (field === 1 && wireType === WIRE_BYTES) readPackedSint(bytes, state, ids);
    else if (field === 8 && wireType === WIRE_BYTES) readPackedSint(bytes, state, lats);
    else if (field === 9 && wireType === WIRE_BYTES) readPackedSint(bytes, state, lons);
    else if (field === 10 && wireType === WIRE_BYTES) readPackedUint(bytes, state, keysVals);
    else skipField(bytes, state, wireType);
  }
  return { ids, lats, lons, keysVals };
}

function parseNode(bytes) {
  const node = { id: 0, lat: 0, lon: 0, keys: [], vals: [] };
  for (const { field, wireType, state } of fields(bytes)) {
    if (field === 1) node.id = zigzag(readVarint53(bytes, state));
    else if (field === 2 && wireType === WIRE_BYTES) readPackedUint(bytes, state, node.keys);
    else if (field === 3 && wireType === WIRE_BYTES) readPackedUint(bytes, state, node.vals);
    else if (field === 8) node.lat = zigzag(readVarint53(bytes, state));
    else if (field === 9) node.lon = zigzag(readVarint53(bytes, state));
    else skipField(bytes, state, wireType);
  }
  return node;
}

function parseWay(bytes) {
  const way = { id: 0, keys: [], vals: [], refs: [] };
  for (const { field, wireType, state } of fields(bytes)) {
    if (field === 1) way.id = readVarint53(bytes, state);
    else if (field === 2 && wireType === WIRE_BYTES) readPackedUint(bytes, state, way.keys);
    else if (field === 3 && wireType === WIRE_BYTES) readPackedUint(bytes, state, way.vals);
    else if (field === 8 && wireType === WIRE_BYTES) readPackedSint(bytes, state, way.refs);
    else skipField(bytes, state, wireType);
  }
  let acc = 0;
  for (let i = 0; i < way.refs.length; i++) {
    acc += way.refs[i];
    way.refs[i] = acc;
  }
  return way;
}

// Relation member types per the OSM PBF enum.
const MEMBER_TYPES = ["node", "way", "relation"];

function parseRelation(bytes) {
  const relation = { id: 0, keys: [], vals: [], rolesSid: [], memids: [], types: [] };
  for (const { field, wireType, state } of fields(bytes)) {
    if (field === 1) relation.id = readVarint53(bytes, state);
    else if (field === 2 && wireType === WIRE_BYTES) readPackedUint(bytes, state, relation.keys);
    else if (field === 3 && wireType === WIRE_BYTES) readPackedUint(bytes, state, relation.vals);
    else if (field === 8 && wireType === WIRE_BYTES) readPackedUint(bytes, state, relation.rolesSid);
    else if (field === 9 && wireType === WIRE_BYTES) readPackedSint(bytes, state, relation.memids);
    else if (field === 10 && wireType === WIRE_BYTES) readPackedUint(bytes, state, relation.types);
    else skipField(bytes, state, wireType);
  }
  let acc = 0;
  for (let i = 0; i < relation.memids.length; i++) {
    acc += relation.memids[i];
    relation.memids[i] = acc;
  }
  return relation;
}

function relationMembers(relation, strings) {
  const members = new Array(relation.memids.length);
  for (let i = 0; i < relation.memids.length; i++) {
    members[i] = {
      type: MEMBER_TYPES[relation.types[i]] || "node",
      ref: relation.memids[i],
      role: strings[relation.rolesSid[i]] || ""
    };
  }
  return members;
}

function parsePrimitiveGroup(bytes, selected) {
  const group = { dense: null, nodes: [], ways: [], relations: [] };
  for (const { field, wireType, state } of fields(bytes)) {
    if (field === 1 && wireType === WIRE_BYTES && selected.nodes) group.nodes.push(parseNode(readBytes(bytes, state)));
    else if (field === 2 && wireType === WIRE_BYTES && selected.nodes) group.dense = parseDenseNodes(readBytes(bytes, state));
    else if (field === 3 && wireType === WIRE_BYTES && selected.ways) group.ways.push(parseWay(readBytes(bytes, state)));
    else if (field === 4 && wireType === WIRE_BYTES && selected.relations) group.relations.push(parseRelation(readBytes(bytes, state)));
    else skipField(bytes, state, wireType);
  }
  return group;
}

function parsePrimitiveBlock(bytes, selected) {
  const block = {
    strings: [],
    groups: [],
    granularity: 100,
    latOffset: 0,
    lonOffset: 0
  };
  for (const { field, wireType, state } of fields(bytes)) {
    if (field === 1 && wireType === WIRE_BYTES) block.strings = parseStringTable(readBytes(bytes, state));
    else if (field === 2 && wireType === WIRE_BYTES) block.groups.push(parsePrimitiveGroup(readBytes(bytes, state), selected));
    else if (field === 17) block.granularity = readVarint53(bytes, state);
    else if (field === 19) block.latOffset = readVarint53(bytes, state);
    else if (field === 20) block.lonOffset = readVarint53(bytes, state);
    else skipField(bytes, state, wireType);
  }
  return block;
}

function* rawBlobs(path) {
  const fd = openSync(path, "r");
  const lengthBytes = Buffer.alloc(4);
  let offset = 0;
  try {
    for (;;) {
      const read = readSync(fd, lengthBytes, 0, 4, offset);
      if (read === 0) return;
      if (read !== 4) throw new Error("OSM PBF file ended inside a blob header length.");
      offset += 4;
      const headerLength = lengthBytes.readUInt32BE(0);
      const headerBytes = Buffer.alloc(headerLength);
      if (readSync(fd, headerBytes, 0, headerLength, offset) !== headerLength) {
        throw new Error("OSM PBF file ended inside a blob header.");
      }
      offset += headerLength;
      const header = parseBlobHeader(headerBytes);
      const blobBytes = Buffer.alloc(header.datasize);
      if (readSync(fd, blobBytes, 0, header.datasize, offset) !== header.datasize) {
        throw new Error("OSM PBF file ended inside a blob body.");
      }
      offset += header.datasize;
      yield { type: header.type, bytes: blobBytes };
    }
  } finally {
    closeSync(fd);
  }
}

function denseTags(keysVals, strings, cursor) {
  const tags = new Map();
  let pos = cursor.pos;
  while (pos < keysVals.length && keysVals[pos] !== 0) {
    tags.set(strings[keysVals[pos]], strings[keysVals[pos + 1]]);
    pos += 2;
  }
  cursor.pos = pos + 1;
  return tags;
}

function elementTags(keys, vals, strings) {
  const tags = new Map();
  for (let i = 0; i < keys.length; i++) tags.set(strings[keys[i]], strings[vals[i]]);
  return tags;
}

// Streams { id, lat, lon, tags } for every node, { id, refs, tags } for
// every way, and { id, members, tags } for every relation when handlers ask
// for them. `onNode(id, lat, lon, tags)` receives tags as null when a node
// has none, so dense untagged nodes stay cheap. Members are
// { type: "node"|"way"|"relation", ref, role }.
export function scanPbf(path, { onNode = null, onWay = null, onRelation = null } = {}) {
  const counts = { nodes: 0, ways: 0, relations: 0, blocks: 0 };
  const selected = { nodes: Boolean(onNode), ways: Boolean(onWay), relations: Boolean(onRelation) };
  for (const blob of rawBlobs(path)) {
    if (blob.type !== "OSMData") continue;
    const block = parsePrimitiveBlock(parseBlob(blob.bytes), selected);
    counts.blocks += 1;
    const { strings, granularity, latOffset, lonOffset } = block;
    const latScale = granularity * 1e-9;
    const lonScale = granularity * 1e-9;
    const latBase = latOffset * 1e-9;
    const lonBase = lonOffset * 1e-9;
    for (const group of block.groups) {
      if (group.dense && onNode) {
        const { ids, lats, lons, keysVals } = group.dense;
        const cursor = { pos: 0 };
        let id = 0;
        let lat = 0;
        let lon = 0;
        const hasTags = keysVals.length > 0;
        for (let i = 0; i < ids.length; i++) {
          id += ids[i];
          lat += lats[i];
          lon += lons[i];
          const tags = hasTags ? denseTags(keysVals, strings, cursor) : null;
          onNode(id, latBase + lat * latScale, lonBase + lon * lonScale, tags && tags.size ? tags : null);
        }
        counts.nodes += ids.length;
      }
      if (group.nodes.length && onNode) {
        for (const node of group.nodes) {
          const tags = elementTags(node.keys, node.vals, strings);
          onNode(
            node.id,
            latBase + node.lat * latScale,
            lonBase + node.lon * lonScale,
            tags.size ? tags : null
          );
        }
        counts.nodes += group.nodes.length;
      }
      if (group.ways.length && onWay) {
        for (const way of group.ways) {
          onWay(way.id, way.refs, elementTags(way.keys, way.vals, strings));
        }
        counts.ways += group.ways.length;
      }
      if (group.relations.length && onRelation) {
        for (const relation of group.relations) {
          onRelation(relation.id, relationMembers(relation, strings), elementTags(relation.keys, relation.vals, strings));
        }
        counts.relations += group.relations.length;
      }
    }
  }
  return counts;
}
