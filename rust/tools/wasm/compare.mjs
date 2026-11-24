// Shared comparison for the wasm parity runners: the same rules as
// rust/rangefind/tests/engine_parity.rs.
export const LANE_STATS = [
  "plannerLane", "countLane", "exact", "topKProven", "totalExact", "tailExhausted", "geoLane",
  "geoDistanceSorted", "docValuePruning", "docValueChunkPruning", "docValueSortText", "typoApplied",
  "postalCodeNormalized", "rankPriorBoost", "geoBoost", "facetCountLane", "vectorLane"
];

function numbersEqual(a, b) {
  return a === b || Math.abs(a - b) <= 1e-9 * Math.max(Math.abs(a), Math.abs(b), 1);
}

export function firstDifference(path, left, right) {
  if (typeof left === "number" && typeof right === "number") {
    return numbersEqual(left, right) ? null : `${path}: js=${left} wasm=${right}`;
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    if (left.length !== right.length) return `${path}: js len ${left.length} wasm len ${right.length}`;
    for (let i = 0; i < left.length; i++) {
      const diff = firstDifference(`${path}[${i}]`, left[i], right[i]);
      if (diff) return diff;
    }
    return null;
  }
  if (left && right && typeof left === "object" && typeof right === "object") {
    for (const key of Object.keys(left)) {
      if (!(key in right)) return `${path}.${key}: wasm missing`;
      const diff = firstDifference(`${path}.${key}`, left[key], right[key]);
      if (diff) return diff;
    }
    for (const key of Object.keys(right)) if (!(key in left)) return `${path}.${key}: js missing`;
    return null;
  }
  return (left ?? null) === (right ?? null) ? null : `${path}: js=${JSON.stringify(left)} wasm=${JSON.stringify(right)}`;
}

export function compareCase(item, actual, error) {
  if (item.error) {
    if (!error) return `js threw ${JSON.stringify(item.error.message)}, wasm answered`;
    return error.message === item.error.message ? null : `error js=${item.error.message} wasm=${error.message}`;
  }
  if (error) return `wasm threw ${error.message}`;
  const expected = item.response;
  if (expected === null || typeof expected !== "object" || Array.isArray(expected)) {
    return firstDifference("response", expected ?? null, actual ?? null);
  }
  // Every top-level key except `stats`, both ways; missing ≠ null.
  if (!actual || typeof actual !== "object" || Array.isArray(actual)) return `response: js=object wasm=${JSON.stringify(actual)}`;
  for (const key of Object.keys(expected)) {
    if (key === "stats") continue;
    if (!(key in actual)) return `${key}: wasm missing`;
    const diff = firstDifference(key, expected[key], actual[key]);
    if (diff) return diff;
  }
  for (const key of Object.keys(actual)) {
    if (key !== "stats" && !(key in expected)) return `${key}: js missing`;
  }
  for (const key of LANE_STATS) {
    const diff = firstDifference(`stats.${key}`, item.response?.stats?.[key] ?? null, actual?.stats?.[key] ?? null);
    if (diff) return diff;
  }
  return null;
}
