import { postingRowCount } from "./posting_rows.js";

export function partitionInputBytes(partition) {
  if (!partition) return 0;
  if (Number.isFinite(partition.inputBytes)) return Math.max(0, Math.floor(partition.inputBytes));
  if (Number.isFinite(partition.length)) return Math.max(0, Math.floor(partition.length));
  return partition.entries?.reduce((sum, [term, rows]) => sum + String(term).length + postingRowCount(rows) * 8, 0) || 0;
}

export function partitionTermCount(partition) {
  if (!partition) return 0;
  if (Number.isFinite(partition.terms)) return Math.max(0, Math.floor(partition.terms));
  return partition.entries?.length || 0;
}

export function partitionRowCount(partition) {
  if (!partition) return 0;
  if (Number.isFinite(partition.rows)) return Math.max(0, Math.floor(partition.rows));
  return partition.entries?.reduce((sum, [, rows]) => sum + postingRowCount(rows), 0) || 0;
}

export function* partitionTermEntries(partition) {
  if (!Array.isArray(partition?.entries)) {
    throw new Error("Rangefind reducer partition is missing streaming term entries.");
  }
  yield* partition.entries;
}
