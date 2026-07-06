import {
  VECTOR_CLUSTER_PAGE_MAGIC,
  VECTOR_ROOT_MAGIC,
  pushVarint,
  readVarint
} from "./binary.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";

export const VECTOR_ROOT_FORMAT = "rfvecroot-v1";
export const VECTOR_CLUSTER_PAGE_FORMAT = "rfveccluster-v1";

const FORMAT_VERSION = 1;

// Accepts a float array or a base64-encoded little-endian Float32 buffer
// (compact JSONL form for large corpora).
export function vectorFromValue(value, dims) {
  if (value == null || value === "") return null;
  let out;
  if (Array.isArray(value)) {
    if (value.length !== dims) return null;
    out = Float32Array.from(value, Number);
  } else if (typeof value === "string") {
    const bytes = Buffer.from(value, "base64");
    if (bytes.length !== dims * 4) return null;
    out = new Float32Array(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length));
  } else {
    return null;
  }
  for (let i = 0; i < out.length; i++) {
    if (!Number.isFinite(out[i])) return null;
  }
  return out;
}

// All vectors are stored L2-normalized so dot product equals cosine
// similarity; a zero vector cannot be normalized and is treated as missing.
export function normalizeVector(vector) {
  let norm = 0;
  for (let i = 0; i < vector.length; i++) norm += vector[i] * vector[i];
  if (!(norm > 0)) return null;
  const inverse = 1 / Math.sqrt(norm);
  for (let i = 0; i < vector.length; i++) vector[i] *= inverse;
  return vector;
}

// Symmetric int8 quantization with one scale per vector: values map to
// [-127, 127] by the vector's max magnitude. Dequantized dot products keep
// ranking fidelity while cutting transfer 4x versus float32.
export function quantizeVector(vector, target, offset = 0) {
  let maxMagnitude = 0;
  for (let i = 0; i < vector.length; i++) {
    const magnitude = Math.abs(vector[i]);
    if (magnitude > maxMagnitude) maxMagnitude = magnitude;
  }
  const scale = maxMagnitude > 0 ? maxMagnitude / 127 : 1;
  const inverse = maxMagnitude > 0 ? 127 / maxMagnitude : 0;
  for (let i = 0; i < vector.length; i++) {
    target[offset + i] = Math.max(-127, Math.min(127, Math.round(vector[i] * inverse)));
  }
  return scale;
}

export function dotInt8(query, bytes, offset, dims, scale) {
  let sum = 0;
  for (let i = 0; i < dims; i++) sum += query[i] * bytes[offset + i];
  return sum * scale;
}

// Orders dimensions by sample variance (descending) so the coarse prefix
// used for candidate ranking carries the most informative components. A
// permutation preserves dot products exactly and costs ~2 bytes per
// dimension in the root, unlike a full rotation matrix.
export function trainDimensionPermutation(sample, dims) {
  const count = sample.length / dims;
  const sums = new Float64Array(dims);
  const squares = new Float64Array(dims);
  for (let row = 0; row < count; row++) {
    const base = row * dims;
    for (let d = 0; d < dims; d++) {
      const value = sample[base + d];
      sums[d] += value;
      squares[d] += value * value;
    }
  }
  const variance = new Float64Array(dims);
  for (let d = 0; d < dims; d++) {
    const meanValue = sums[d] / Math.max(1, count);
    variance[d] = squares[d] / Math.max(1, count) - meanValue * meanValue;
  }
  const order = Array.from({ length: dims }, (_, d) => d);
  order.sort((a, b) => variance[b] - variance[a] || a - b);
  return Uint16Array.from(order);
}

export function applyPermutation(vector, offset, permutation, target) {
  for (let d = 0; d < permutation.length; d++) target[d] = vector[offset + permutation[d]];
  return target;
}

// Mini-batch style k-means over a training sample. Assignment uses the
// coarse dimension prefix to keep the builder fast; centroids are averaged
// at full dimensionality from the final assignment.
export function trainCentroids(sample, dims, k, { coarseDims = dims, iterations = 6 } = {}) {
  const count = sample.length / dims;
  const clusterCount = Math.max(1, Math.min(k, count));
  const centroids = new Float32Array(clusterCount * dims);
  // Deterministic spread seeding: evenly strided sample rows.
  for (let c = 0; c < clusterCount; c++) {
    const row = Math.floor((c * count) / clusterCount);
    centroids.set(sample.subarray(row * dims, row * dims + dims), c * dims);
  }
  const assignment = new Int32Array(count);
  const sums = new Float64Array(clusterCount * dims);
  const sizes = new Int32Array(clusterCount);
  for (let iteration = 0; iteration < iterations; iteration++) {
    for (let row = 0; row < count; row++) {
      assignment[row] = nearestCentroid(sample, row * dims, centroids, clusterCount, dims, coarseDims);
    }
    sums.fill(0);
    sizes.fill(0);
    for (let row = 0; row < count; row++) {
      const cluster = assignment[row];
      sizes[cluster] += 1;
      const base = cluster * dims;
      const source = row * dims;
      for (let d = 0; d < dims; d++) sums[base + d] += sample[source + d];
    }
    for (let c = 0; c < clusterCount; c++) {
      if (!sizes[c]) continue;
      const base = c * dims;
      for (let d = 0; d < dims; d++) centroids[base + d] = sums[base + d] / sizes[c];
    }
  }
  return { centroids, clusterCount };
}

export function nearestCentroid(vectors, offset, centroids, clusterCount, dims, coarseDims = dims) {
  let best = 0;
  let bestScore = -Infinity;
  for (let c = 0; c < clusterCount; c++) {
    const base = c * dims;
    let score = 0;
    for (let d = 0; d < coarseDims; d++) score += vectors[offset + d] * centroids[base + d];
    if (score > bestScore) {
      bestScore = score;
      best = c;
    }
  }
  return best;
}

function pushObjectPointer(out, entry, packIndexes) {
  pushVarint(out, packIndexes.get(entry.pack) ?? 0);
  pushVarint(out, entry.offset);
  pushVarint(out, entry.length);
  pushVarint(out, entry.physicalLength || entry.length);
  pushVarint(out, entry.logicalLength || 0);
  pushUtf8(out, entry.checksum?.algorithm || "");
  pushUtf8(out, entry.checksum?.value || "");
}

function readObjectPointer(bytes, state, packTable, target) {
  const packIndex = readVarint(bytes, state);
  target.pack = packTable[packIndex];
  target.offset = readVarint(bytes, state);
  target.length = readVarint(bytes, state);
  target.physicalLength = readVarint(bytes, state);
  target.logicalLength = readVarint(bytes, state) || null;
  const algorithm = readUtf8(bytes, state);
  const value = readUtf8(bytes, state);
  target.checksum = value ? { algorithm: algorithm || "sha256", value } : null;
  return target;
}

// Cluster page: doc ids, refine-store ordinals, per-vector scales, and int8
// coarse-dim rows. Ordinals address fixed-width rows in the refine packs, so
// no per-document pointers are stored anywhere.
export function encodeVectorClusterPage({ field, clusterIndex, coarseDims, docs, ordinals, scales, codes }) {
  const count = docs.length;
  const header = [...VECTOR_CLUSTER_PAGE_MAGIC];
  pushVarint(header, FORMAT_VERSION);
  pushUtf8(header, field);
  pushVarint(header, clusterIndex);
  pushVarint(header, coarseDims);
  pushVarint(header, count);
  const headerBytes = Buffer.from(Uint8Array.from(header));
  const fixed = Buffer.alloc(count * 12);
  for (let i = 0; i < count; i++) {
    fixed.writeUInt32LE(docs[i], i * 4);
    fixed.writeUInt32LE(ordinals[i], count * 4 + i * 4);
    fixed.writeFloatLE(scales[i], count * 8 + i * 4);
  }
  const codeBytes = Buffer.from(codes.buffer, codes.byteOffset, count * coarseDims);
  return Buffer.concat([headerBytes, fixed, codeBytes]);
}

export function decodeVectorClusterPage(buffer, expected = {}) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, VECTOR_CLUSTER_PAGE_MAGIC, "Unsupported Rangefind vector cluster page");
  const state = { pos: VECTOR_CLUSTER_PAGE_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FORMAT_VERSION) throw new Error(`Unsupported Rangefind vector cluster page version ${version}`);
  const field = readUtf8(bytes, state);
  if (expected.name && expected.name !== field) throw new Error(`Rangefind vector cluster page field mismatch: ${field}`);
  const clusterIndex = readVarint(bytes, state);
  const coarseDims = readVarint(bytes, state);
  const count = readVarint(bytes, state);
  const view = new DataView(bytes.buffer, bytes.byteOffset);
  let pos = state.pos;
  const docs = new Uint32Array(count);
  for (let i = 0; i < count; i++, pos += 4) docs[i] = view.getUint32(pos, true);
  const ordinals = new Uint32Array(count);
  for (let i = 0; i < count; i++, pos += 4) ordinals[i] = view.getUint32(pos, true);
  const scales = new Float32Array(count);
  for (let i = 0; i < count; i++, pos += 4) scales[i] = view.getFloat32(pos, true);
  const codes = new Int8Array(bytes.buffer, bytes.byteOffset + pos, count * coarseDims);
  pos += count * coarseDims;
  if (pos !== bytes.length) throw new Error("Rangefind vector cluster page has trailing bytes.");
  return { field, clusterIndex, coarseDims, count, docs, ordinals, scales, codes };
}

// Root: metric/dims metadata, int8-quantized centroids (with per-centroid
// scales), cluster page pointers, and the fixed-width refine store layout
// (refine rows are addressed as doc * rowBytes — no per-doc pointers).
export function encodeVectorRoot({
  field,
  dims,
  coarseDims,
  metric,
  total,
  permutation,
  centroids,
  clusterCount,
  clusters,
  refine,
  packTable,
  packIndexes
}) {
  const out = [...VECTOR_ROOT_MAGIC];
  pushVarint(out, FORMAT_VERSION);
  pushUtf8(out, field);
  pushUtf8(out, metric);
  pushVarint(out, dims);
  pushVarint(out, coarseDims);
  pushVarint(out, total);
  for (let d = 0; d < dims; d++) pushVarint(out, permutation ? permutation[d] : d);
  pushVarint(out, packTable.length);
  for (const pack of packTable) pushUtf8(out, pack);
  pushVarint(out, clusterCount);
  const centroidCodes = new Int8Array(clusterCount * dims);
  const centroidScales = new Float32Array(clusterCount);
  for (let c = 0; c < clusterCount; c++) {
    centroidScales[c] = quantizeVector(centroids.subarray(c * dims, c * dims + dims), centroidCodes, c * dims);
  }
  const head = Buffer.from(Uint8Array.from(out));
  const scaleBytes = Buffer.from(centroidScales.buffer.slice(0));
  const codeBytes = Buffer.from(centroidCodes.buffer.slice(0));
  const tail = [];
  for (const cluster of clusters) {
    pushVarint(tail, cluster.count);
    pushObjectPointer(tail, cluster.entry, packIndexes);
  }
  pushVarint(tail, refine.rowBytes);
  pushVarint(tail, refine.rowsPerPack);
  pushVarint(tail, refine.packs.length);
  for (const pack of refine.packs) pushUtf8(tail, pack);
  return {
    buffer: Buffer.concat([head, scaleBytes, codeBytes, Buffer.from(Uint8Array.from(tail))]),
    meta: {
      format: VECTOR_ROOT_FORMAT,
      page_format: VECTOR_CLUSTER_PAGE_FORMAT,
      field,
      metric,
      dims,
      coarse_dims: coarseDims,
      total,
      clusters: clusterCount
    }
  };
}

export function parseVectorRoot(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, VECTOR_ROOT_MAGIC, "Unsupported Rangefind vector root");
  const state = { pos: VECTOR_ROOT_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FORMAT_VERSION) throw new Error(`Unsupported Rangefind vector root version ${version}`);
  const field = readUtf8(bytes, state);
  const metric = readUtf8(bytes, state);
  const dims = readVarint(bytes, state);
  const coarseDims = readVarint(bytes, state);
  const total = readVarint(bytes, state);
  const permutation = new Uint16Array(dims);
  for (let d = 0; d < dims; d++) permutation[d] = readVarint(bytes, state);
  const packCount = readVarint(bytes, state);
  const packTable = new Array(packCount);
  for (let i = 0; i < packCount; i++) packTable[i] = readUtf8(bytes, state);
  const clusterCount = readVarint(bytes, state);
  const view = new DataView(bytes.buffer, bytes.byteOffset);
  const centroidScales = new Float32Array(clusterCount);
  let pos = state.pos;
  for (let c = 0; c < clusterCount; c++, pos += 4) centroidScales[c] = view.getFloat32(pos, true);
  const centroidCodes = new Int8Array(bytes.buffer, bytes.byteOffset + pos, clusterCount * dims);
  pos += clusterCount * dims;
  state.pos = pos;
  const clusters = new Array(clusterCount);
  for (let c = 0; c < clusterCount; c++) {
    const count = readVarint(bytes, state);
    clusters[c] = readObjectPointer(bytes, state, packTable, { index: c, count });
  }
  const refineRowBytes = readVarint(bytes, state);
  const refineRowsPerPack = readVarint(bytes, state);
  const refinePackCount = readVarint(bytes, state);
  const refinePacks = new Array(refinePackCount);
  for (let i = 0; i < refinePackCount; i++) refinePacks[i] = readUtf8(bytes, state);
  if (state.pos !== bytes.length) throw new Error("Rangefind vector root has trailing bytes.");
  return {
    format: VECTOR_ROOT_FORMAT,
    pageFormat: VECTOR_CLUSTER_PAGE_FORMAT,
    field,
    metric,
    dims,
    coarseDims,
    total,
    permutation,
    packTable,
    clusterCount,
    centroidScales,
    centroidCodes,
    clusters,
    refine: {
      rowBytes: refineRowBytes,
      rowsPerPack: refineRowsPerPack,
      packs: refinePacks
    }
  };
}
