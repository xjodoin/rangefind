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

export function shardFor(term, manifest, availableShards) {
  const maxDepth = manifest.stats?.max_shard_depth || 5;
  const baseDepth = manifest.stats?.base_shard_depth || 3;
  for (let depth = maxDepth; depth >= baseDepth; depth--) {
    const key = shardKey(term, depth);
    if (availableShards.has(key)) return key;
  }
  return shardKey(term, baseDepth);
}

export function groupRanges(items, mergeGapBytes = RANGE_MERGE_GAP_BYTES) {
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
      if (!current || item.start > current.end + mergeGapBytes) {
        current = { pack, start: item.start, end: item.end, items: [item] };
        groups.push(current);
      } else {
        current.items.push(item);
        current.end = Math.max(current.end, item.end);
      }
    }
  }
  return groups;
}
