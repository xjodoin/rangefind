export const RANGE_MERGE_GAP_BYTES = 8 * 1024;

export function shardKey(term, depth) {
  return String(term || "").slice(0, depth).padEnd(depth, "_");
}

export function baseShardFor(term, config) {
  return shardKey(term, config.baseShardDepth);
}

export function entryPostingCount(entries) {
  return entries.reduce((sum, [, rows]) => sum + rows.length, 0);
}

export function partitionEntries(entries, config, depth = config.baseShardDepth) {
  if (!entries.length) return [];
  if (entryPostingCount(entries) <= config.targetShardPostings || depth >= config.maxShardDepth) {
    return [{ name: shardKey(entries[0][0], depth), entries }];
  }
  const groups = new Map();
  for (const entry of entries) {
    const key = shardKey(entry[0], depth + 1);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(entry);
  }
  return [...groups.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .flatMap(([, group]) => partitionEntries(group, config, depth + 1));
}

export function groupRanges(items, options = RANGE_MERGE_GAP_BYTES) {
  const mergeGapBytes = typeof options === "number" ? options : options.mergeGapBytes ?? RANGE_MERGE_GAP_BYTES;
  const maxMergedBytes = typeof options === "number" ? Infinity : options.maxMergedBytes ?? Infinity;
  const maxOverfetchBytes = typeof options === "number" ? Infinity : options.maxOverfetchBytes ?? Infinity;
  const maxOverfetchRatio = typeof options === "number" ? Infinity : options.maxOverfetchRatio ?? Infinity;
  const byPack = new Map();
  for (const item of items) {
    if (!byPack.has(item.entry.pack)) byPack.set(item.entry.pack, []);
    byPack.get(item.entry.pack).push(item);
  }
  const groups = [];
  for (const [pack, packItems] of byPack) {
    const sorted = packItems
      .map(item => ({ ...item, start: item.entry.offset, end: item.entry.offset + item.entry.length }))
      .sort((a, b) => a.start - b.start);
    let current = null;
    for (const item of sorted) {
      const itemBytes = item.end - item.start;
      if (!current) {
        current = { pack, start: item.start, end: item.end, items: [item] };
        Object.defineProperty(current, "exactBytes", { value: itemBytes, writable: true, enumerable: false });
        groups.push(current);
        continue;
      }
      const nextEnd = Math.max(current.end, item.end);
      const mergedBytes = nextEnd - current.start;
      const exactBytes = current.exactBytes + itemBytes;
      const overfetchBytes = mergedBytes - exactBytes;
      const shouldMerge = item.start <= current.end + mergeGapBytes
        && mergedBytes <= maxMergedBytes
        && overfetchBytes <= maxOverfetchBytes
        && mergedBytes <= exactBytes * maxOverfetchRatio;
      if (!shouldMerge) {
        current = { pack, start: item.start, end: item.end, items: [item] };
        Object.defineProperty(current, "exactBytes", { value: itemBytes, writable: true, enumerable: false });
        groups.push(current);
      } else {
        current.items.push(item);
        current.end = nextEnd;
        current.exactBytes = exactBytes;
      }
    }
  }
  return groups;
}
