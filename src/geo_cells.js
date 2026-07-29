import { pushVarint, readVarint } from "./binary.js";
import { assertMagic } from "./codec.js";

const GEO_CELL_BLOCK_MAGIC = [0x52, 0x46, 0x47, 0x43]; // RFGC
const GEO_CELL_BLOCK_VERSION = 1;
const WEB_MERCATOR_MAX_LAT = 85.05112878;

export const GEO_CATEGORY_CELL_FORMAT = "rfgeocategorycell-v1";

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

export function geoCellForE7(latE7, lonE7, zoom) {
  const z = clamp(Math.floor(Number(zoom) || 0), 0, 22);
  const scale = 2 ** z;
  const lon = clamp(Number(lonE7) / 1e7, -180, 180);
  const lat = clamp(Number(latE7) / 1e7, -WEB_MERCATOR_MAX_LAT, WEB_MERCATOR_MAX_LAT);
  const radians = lat * Math.PI / 180;
  const x = clamp(Math.floor(((lon + 180) / 360) * scale), 0, scale - 1);
  const y = clamp(
    Math.floor((1 - Math.asinh(Math.tan(radians)) / Math.PI) / 2 * scale),
    0,
    scale - 1
  );
  return { zoom: z, x, y };
}

export function geoCellBlock(cell, blockZoom) {
  const zoom = Math.min(cell.zoom, Math.max(0, Math.floor(Number(blockZoom) || 0)));
  const shift = cell.zoom - zoom;
  const divisor = 2 ** shift;
  return {
    zoom,
    x: Math.floor(cell.x / divisor),
    y: Math.floor(cell.y / divisor)
  };
}

export function geoCellBlockKey(field, facet, group, block) {
  return [
    encodeURIComponent(String(field)),
    encodeURIComponent(String(facet)),
    String(Math.max(0, Math.floor(group))).padStart(6, "0"),
    String(block.zoom).padStart(2, "0"),
    String(block.y).padStart(7, "0"),
    String(block.x).padStart(7, "0")
  ].join("|");
}

export function geoCellRouteKey(zoom, x, y, code) {
  return `${zoom}:${y}:${x}:${code}`;
}

export function geoCellsForBoxes(boxes, zoom, limit = Infinity) {
  const max = Math.max(1, Math.floor(Number(limit) || 1));
  const cells = new Map();
  for (const box of boxes || []) {
    const northWest = geoCellForE7(box.maxLatE7, box.minLonE7, zoom);
    const southEast = geoCellForE7(box.minLatE7, box.maxLonE7, zoom);
    for (let y = northWest.y; y <= southEast.y; y++) {
      for (let x = northWest.x; x <= southEast.x; x++) {
        const key = `${y}:${x}`;
        if (!cells.has(key)) {
          cells.set(key, { zoom: northWest.zoom, x, y });
          if (cells.size > max) return null;
        }
      }
    }
  }
  return [...cells.values()];
}

export function encodeGeoCellBlock({ blockZoom, blockX, blockY, routes }) {
  const out = [...GEO_CELL_BLOCK_MAGIC];
  pushVarint(out, GEO_CELL_BLOCK_VERSION);
  pushVarint(out, blockZoom);
  pushVarint(out, blockX);
  pushVarint(out, blockY);
  pushVarint(out, routes.length);
  for (const route of routes) {
    pushVarint(out, route.zoom);
    pushVarint(out, route.x);
    pushVarint(out, route.y);
    pushVarint(out, route.code);
    const members = (route.members || []).slice().sort((a, b) => a.leaf - b.leaf);
    pushVarint(out, members.length);
    let previousLeaf = 0;
    for (const member of members) {
      pushVarint(out, member.leaf - previousLeaf);
      previousLeaf = member.leaf;
      const ordinals = [...new Set(member.ordinals || [])].sort((a, b) => a - b);
      pushVarint(out, ordinals.length);
      let previousOrdinal = 0;
      for (const ordinal of ordinals) {
        pushVarint(out, ordinal - previousOrdinal);
        previousOrdinal = ordinal;
      }
    }
  }
  return Buffer.from(Uint8Array.from(out));
}

export function decodeGeoCellBlock(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, GEO_CELL_BLOCK_MAGIC, "Unsupported Rangefind geo category-cell block");
  const state = { pos: GEO_CELL_BLOCK_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== GEO_CELL_BLOCK_VERSION) {
    throw new Error(`Unsupported Rangefind geo category-cell block version ${version}`);
  }
  const blockZoom = readVarint(bytes, state);
  const blockX = readVarint(bytes, state);
  const blockY = readVarint(bytes, state);
  const count = readVarint(bytes, state);
  const routes = new Map();
  for (let index = 0; index < count; index++) {
    const zoom = readVarint(bytes, state);
    const x = readVarint(bytes, state);
    const y = readVarint(bytes, state);
    const code = readVarint(bytes, state);
    const memberCount = readVarint(bytes, state);
    const members = new Map();
    let previousLeaf = 0;
    for (let member = 0; member < memberCount; member++) {
      previousLeaf += readVarint(bytes, state);
      const ordinalCount = readVarint(bytes, state);
      const ordinals = new Array(ordinalCount);
      let previousOrdinal = 0;
      for (let ordinal = 0; ordinal < ordinalCount; ordinal++) {
        previousOrdinal += readVarint(bytes, state);
        ordinals[ordinal] = previousOrdinal;
      }
      members.set(previousLeaf, ordinals);
    }
    routes.set(geoCellRouteKey(zoom, x, y, code), members);
  }
  if (state.pos !== bytes.length) {
    throw new Error("Rangefind geo category-cell block has trailing bytes.");
  }
  return {
    format: GEO_CATEGORY_CELL_FORMAT,
    blockZoom,
    blockX,
    blockY,
    routes
  };
}
