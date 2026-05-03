import { copyFileSync, mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { gzipSync } from "node:zlib";

export const SEGMENT_MANIFEST_FORMAT = "rfsegmentmanifest-v1";
export const SEGMENT_MANIFEST_PATH = "segments/manifest.json.gz";

function fieldNames(items = []) {
  return items.map(item => item.name).filter(Boolean);
}

function typedFields(items = [], defaultType) {
  return items
    .filter(item => item.name)
    .map(item => ({
      name: item.name,
      type: item.type || defaultType
    }));
}

function segmentFile(file, bytes, checksum) {
  return {
    path: file,
    bytes: bytes || 0,
    checksum: checksum || null
  };
}

function segmentRow(segment, ordinal) {
  return {
    id: segment.id,
    ordinal,
    format: segment.format || "rfsegment-v1",
    docBase: segment.docBase || 0,
    docCount: segment.docCount || 0,
    termCount: segment.termCount || 0,
    postingCount: segment.postingCount || 0,
    bytes: (segment.termsBytes || 0) + (segment.postingBytes || 0),
    approxMemoryBytes: segment.approxMemoryBytes || segment.approxBytes || 0,
    flushReason: segment.flushReason || "",
    mergeTier: segment.mergeTier || 0,
    sourceSegments: segment.sourceSegments || [],
    files: {
      terms: segmentFile(segment.terms || segment.files?.terms?.path || "terms.bin", segment.termsBytes, segment.termsChecksum || segment.files?.terms?.checksum),
      postings: segmentFile(segment.postings || segment.files?.postings?.path || "postings.bin", segment.postingBytes, segment.postingsChecksum || segment.files?.postings?.checksum)
    }
  };
}

export function publishSegmentFiles(out, segments = []) {
  return segments.map((segment, ordinal) => {
    const id = segment.id || `segment-${String(ordinal).padStart(6, "0")}`;
    const directory = `s${String(ordinal).padStart(6, "0")}`;
    const relativeDirectory = `segments/${directory}`;
    const destination = resolve(out, relativeDirectory);
    mkdirSync(destination, { recursive: true });
    copyFileSync(segment.termsPath || resolve(segment.dir, segment.terms || "terms.bin"), resolve(destination, "terms.bin"));
    copyFileSync(segment.postingsPath || resolve(segment.dir, segment.postings || "postings.bin"), resolve(destination, "postings.bin"));
    return {
      ...segment,
      id,
      terms: `${relativeDirectory}/terms.bin`,
      postings: `${relativeDirectory}/postings.bin`,
      published: true
    };
  });
}

export function buildSegmentManifest({ config = {}, total = 0, segments = [], summary = null, mergeTiers = [], mergePolicy = null, published = false, storage = "" } = {}) {
  const rows = segments.map(segmentRow);
  return {
    format: SEGMENT_MANIFEST_FORMAT,
    sourceFormat: "rfsegment-v1",
    storage: storage || (published ? "static-segment-files-v1" : "builder-spool"),
    published,
    totalDocs: total || 0,
    segmentCount: rows.length,
    termCount: summary?.terms ?? rows.reduce((sum, segment) => sum + segment.termCount, 0),
    postingCount: summary?.postings ?? rows.reduce((sum, segment) => sum + segment.postingCount, 0),
    bytes: summary?.bytes ?? rows.reduce((sum, segment) => sum + segment.bytes, 0),
    peakSegmentMemoryBytes: summary?.approxMemoryBytes ?? rows.reduce((peak, segment) => Math.max(peak, segment.approxMemoryBytes), 0),
    flushReasons: summary?.flushReasons || rows.reduce((counts, segment) => {
      const reason = segment.flushReason || "unknown";
      counts[reason] = (counts[reason] || 0) + 1;
      return counts;
    }, {}),
    fields: {
      text: fieldNames(config.fields),
      facets: fieldNames(config.facets),
      numbers: typedFields(config.numbers, "number"),
      booleans: fieldNames(config.booleans)
    },
    merge: {
      policy: mergePolicy?.policy || config.segmentMergePolicy || "tiered-log",
      fanIn: config.segmentMergeFanIn || 0,
      targetSegments: mergePolicy?.targetSegments || config.finalSegmentTargetCount || config.segmentMergeFanIn || 0,
      forceMerge: Boolean(mergePolicy?.forceMerge),
      maxTempBytes: mergePolicy?.maxTempBytes || config.segmentMergeMaxTempBytes || 0,
      writeAmplification: mergePolicy?.writeAmplification || 0,
      intermediateBytes: mergePolicy?.intermediateBytes || 0,
      skippedSegments: mergePolicy?.skippedSegments || 0,
      blockedByTempBudget: Boolean(mergePolicy?.blockedByTempBudget),
      tiers: mergeTiers
    },
    segments: rows
  };
}

export function writeSegmentManifest(out, options = {}) {
  const segments = options.publishSegments ? publishSegmentFiles(out, options.segments || []) : options.segments || [];
  const manifest = buildSegmentManifest({
    ...options,
    segments,
    published: options.publishSegments || options.published || false,
    storage: options.storage || (options.publishSegments ? "static-segment-files-v1" : "")
  });
  const compressed = gzipSync(JSON.stringify(manifest), { level: 6 });
  mkdirSync(resolve(out, "segments"), { recursive: true });
  writeFileSync(resolve(out, SEGMENT_MANIFEST_PATH), compressed);
  return {
    ...manifest,
    path: SEGMENT_MANIFEST_PATH,
    compressedBytes: compressed.length
  };
}
