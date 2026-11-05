import {
  closeSync,
  copyFileSync,
  createReadStream,
  createWriteStream,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
  writeSync
} from "node:fs";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, extname, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { createInterface } from "node:readline";
import { pipeline } from "node:stream/promises";
import { createGunzip, gzipSync } from "node:zlib";
import {
  normalizeAddressAuthorityKey,
  normalizePostalCodeSpacing
} from "../../../address.js";
import { createJsonlReadStream } from "../../../jsonl.js";

export const ADDRESS_ENRICHMENT_SCHEMA_VERSION = 1;
export const ADDRESS_CANDIDATE_CACHE_SCHEMA_VERSION = 1;

const WRITE_BUFFER_BYTES = 8 * 1024 * 1024;
const SQLITE_BATCH_ROWS = 250_000;
const CANDIDATE_CACHE_BLOCK_BYTES = 8 * 1024 * 1024;
const KEY_SET_SHARDS = 64;
const require = createRequire(import.meta.url);
let DatabaseSync = null;

function clean(value) {
  return String(value ?? "").replaceAll(/[\u0000-\u001f\u0085\u2028\u2029]+/gu, " ").trim();
}

function upper(value) {
  return clean(value).toUpperCase();
}

function canonicalPostcode(value) {
  return normalizePostalCodeSpacing(clean(value)).toUpperCase();
}

function finiteCoordinate(value, minimum, maximum) {
  const number = Number(value);
  return Number.isFinite(number) && number >= minimum && number <= maximum ? number : null;
}

function sourceId(source) {
  const id = clean(source?.id);
  if (!id || !/^[a-z0-9][a-z0-9._-]*$/iu.test(id)) {
    throw new Error(`Address enrichment source needs a stable id, got ${JSON.stringify(id)}.`);
  }
  return id.toLowerCase();
}

function identityFor(path) {
  const absolute = resolve(path);
  const stat = statSync(absolute);
  return { path: absolute, bytes: stat.size, mtimeMs: Math.floor(stat.mtimeMs) };
}

function sameIdentity(left, right) {
  return left?.path === right?.path && left?.bytes === right?.bytes && left?.mtimeMs === right?.mtimeMs;
}

function sourceIdentity(source) {
  if (source.identity) return source.identity;
  if (source.path) return identityFor(source.path);
  return { id: sourceId(source), version: clean(source.version) };
}

function sameSourceIdentity(left, right) {
  if (left?.path || right?.path) return sameIdentity(left, right);
  return stableJson(left) === stableJson(right);
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function writeAll(fd, buffer) {
  let offset = 0;
  while (offset < buffer.length) offset += writeSync(fd, buffer, offset, buffer.length - offset);
}

function createAppendJsonlWriter(path) {
  const fd = openSync(path, "a");
  let parts = [];
  let bytes = 0;
  let closed = false;
  function flush() {
    if (!parts.length) return;
    writeAll(fd, Buffer.from(parts.join("")));
    parts = [];
    bytes = 0;
  }
  return {
    write(value) {
      this.writeJson(JSON.stringify(value));
    },
    writeJson(json) {
      const line = `${json}\n`;
      parts.push(line);
      bytes += Buffer.byteLength(line);
      if (bytes >= WRITE_BUFFER_BYTES) flush();
    },
    close() {
      if (closed) return;
      flush();
      closeSync(fd);
      closed = true;
    }
  };
}

/** Streaming RFC 4180 parser with a configurable one-character delimiter. */
export async function* parseDelimitedRows(stream, options = {}) {
  const delimiter = String(options.delimiter ?? ",");
  if (delimiter.length !== 1) throw new TypeError("Delimited address sources need a one-character delimiter.");
  let row = [];
  let field = "";
  let quoted = false;
  let pendingQuote = false;
  let firstField = true;
  for await (const chunk of stream) {
    const text = chunk.toString(options.encoding || "utf8");
    for (let index = 0; index < text.length; index++) {
      const char = text[index];
      if (quoted) {
        if (pendingQuote) {
          if (char === '"') {
            field += '"';
            pendingQuote = false;
            continue;
          }
          quoted = false;
          pendingQuote = false;
          index--;
          continue;
        }
        if (char === '"') pendingQuote = true;
        else field += char;
        continue;
      }
      if (char === '"' && field === "") {
        quoted = true;
      } else if (char === delimiter) {
        if (firstField) {
          field = field.replace(/^\uFEFF/u, "");
          firstField = false;
        }
        row.push(field);
        field = "";
      } else if (char === "\n") {
        if (firstField) {
          field = field.replace(/^\uFEFF/u, "");
          firstField = false;
        }
        if (field.endsWith("\r")) field = field.slice(0, -1);
        row.push(field);
        field = "";
        if (row.some(value => value !== "")) yield row;
        row = [];
      } else {
        field += char;
      }
    }
  }
  if (pendingQuote) quoted = false;
  if (quoted) throw new Error("Delimited address source ended inside a quoted field.");
  if (field || row.length) {
    if (firstField) field = field.replace(/^\uFEFF/u, "");
    row.push(field.replace(/\r$/u, ""));
    if (row.some(value => value !== "")) yield row;
  }
}

function rowObject(header, row) {
  const value = {};
  for (let index = 0; index < header.length; index++) value[header[index]] = row[index] || "";
  return value;
}

function openCompressedText(path, options = {}) {
  const compression = options.compression || (
    extname(path).toLowerCase() === ".zip" ? "zip"
      : extname(path).toLowerCase() === ".gz" ? "gzip"
        : "none"
  );
  if (compression === "none") return { stream: createReadStream(path), done: Promise.resolve() };
  if (compression === "gzip") {
    const stream = createReadStream(path).pipe(createGunzip());
    return { stream, done: new Promise((resolveDone, reject) => {
      stream.once("end", resolveDone);
      stream.once("error", reject);
    }) };
  }
  if (compression !== "zip") throw new Error(`Unsupported address source compression: ${compression}`);
  const args = ["-p", path];
  if (options.archiveEntry) args.push(options.archiveEntry);
  const child = spawn("unzip", args, { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", chunk => { stderr += chunk; });
  const done = new Promise((resolveDone, reject) => {
    child.once("error", reject);
    child.once("close", code => code === 0
      ? resolveDone()
      : reject(new Error(`Unable to read address source ${path}: ${stderr.trim()}`)));
  });
  return { stream: child.stdout, done };
}

function mappedValue(raw, selector) {
  if (selector == null) return "";
  if (typeof selector === "function") return selector(raw);
  if (Array.isArray(selector)) return selector.map(item => mappedValue(raw, item)).filter(Boolean).join(" ");
  return raw?.[selector] ?? "";
}

/**
 * Create a provider-neutral source from CSV/TSV input. Mapping keys use the
 * canonical record contract consumed by normalizeExternalAddressRecord.
 */
export function createDelimitedAddressSource(options = {}) {
  if (!options.path) throw new Error("Address source is missing: no path configured");
  const path = resolve(options.path);
  if (!existsSync(path)) throw new Error(`Address source is missing: ${path}`);
  const mapping = options.mapping || {};
  return {
    ...options,
    id: sourceId(options),
    path,
    async *records() {
      const { stream, done } = openCompressedText(path, options);
      let header = Array.isArray(options.header) ? options.header : null;
      let rowIndex = 0;
      try {
        for await (const row of parseDelimitedRows(stream, { delimiter: options.delimiter ?? "," })) {
          rowIndex++;
          if (!header && options.header !== false) {
            header = row;
            continue;
          }
          const raw = header ? rowObject(header, row) : row;
          const record = { ...options.defaults };
          for (const [key, selector] of Object.entries(mapping)) record[key] = mappedValue(raw, selector);
          record._raw = raw;
          record._row = rowIndex;
          yield record;
        }
        await done;
      } finally {
        if (!stream.destroyed) stream.destroy();
      }
    }
  };
}

/** Create a provider source from one canonical JSON object per line. */
export function createJsonlAddressSource(options = {}) {
  if (!options.path) throw new Error("Address source is missing: no path configured");
  const path = resolve(options.path);
  if (!existsSync(path)) throw new Error(`Address source is missing: ${path}`);
  return {
    ...options,
    id: sourceId(options),
    path,
    async *records() {
      const { stream, done } = openCompressedText(path, options);
      const lines = createInterface({ input: stream, crlfDelay: Infinity });
      let row = 0;
      try {
        for await (const line of lines) {
          row++;
          if (!line.trim()) continue;
          try {
            yield JSON.parse(line);
          } catch (error) {
            throw new Error(`Invalid JSONL address record at ${path}:${row}: ${error.message}`, { cause: error });
          }
        }
        await done;
      } finally {
        lines.close();
        if (!stream.destroyed) stream.destroy();
      }
    }
  };
}

/** Canonicalize a provider row without imposing a country-specific schema. */
export function normalizeExternalAddressRecord(value = {}, defaults = {}) {
  if (value.active === false || value.deleted === true) return null;
  const lat = finiteCoordinate(value.lat ?? value.latitude, -90, 90);
  const lon = finiteCoordinate(value.lon ?? value.lng ?? value.longitude, -180, 180);
  if (lat == null || lon == null) return null;
  const record = {
    id: clean(value.id),
    houseNumber: clean(value.houseNumber ?? value.house_number ?? defaults.houseNumber),
    street: clean(value.street ?? defaults.street),
    unit: clean(value.unit ?? defaults.unit),
    city: clean(value.city ?? value.locality ?? defaults.city),
    district: clean(value.district ?? value.county ?? defaults.district),
    state: clean(value.state ?? value.region ?? defaults.state),
    postcode: canonicalPostcode(value.postcode ?? value.postalCode ?? value.postal_code ?? defaults.postcode),
    country: upper(value.country ?? value.countryCode ?? value.country_code ?? defaults.country),
    lat,
    lon,
    url: clean(value.url ?? defaults.url),
    kind: value.kind === "postal_code" || value.type === "postal_code" ? "postal_code" : "address"
  };
  if (record.kind === "address" && (!record.houseNumber || !record.street)) return null;
  if (record.kind === "postal_code" && !record.postcode) return null;
  return record;
}

function addressIdentity(record) {
  return normalizeAddressAuthorityKey([
    record.houseNumber,
    record.street,
    record.city,
    record.state,
    record.country
  ].filter(Boolean).join(" "));
}

function looseAddressIdentity(record) {
  if (!record.houseNumber || !record.street || !record.city) return "";
  return normalizeAddressAuthorityKey([record.houseNumber, record.street, record.city].join(" "));
}

function addressText(record, includeUnit = false, includeCountry = true) {
  const number = [includeUnit ? record.unit : "", record.houseNumber].filter(Boolean).join("-");
  const street = [number, record.street].filter(Boolean).join(" ");
  const locality = [record.city, record.state, record.postcode, includeCountry ? record.country : ""].filter(Boolean).join(", ");
  return [street, locality].filter(Boolean).join(", ");
}

function stableRecordId(source, record, key) {
  if (record.id) return `${sourceId(source)}/${record.id}`;
  const hash = createHash("sha256").update(key).digest("hex").slice(0, 24);
  return `${sourceId(source)}/address/${hash}`;
}

export function externalAddressDocument(recordValue, source, options = {}) {
  const record = normalizeExternalAddressRecord(recordValue, source.defaults);
  if (!record || record.kind !== "address") return null;
  const key = addressIdentity(record);
  if (!key) return null;
  const address = addressText(record, options.includeUnits === true, options.includeCountry !== false);
  return {
    id: stableRecordId(source, record, key),
    url: record.url || clean(source.url),
    name: address,
    address,
    house_number: record.houseNumber,
    street: record.street,
    ...(options.includeUnits === true && record.unit ? { unit: record.unit } : {}),
    ...(record.city ? { city: record.city } : {}),
    ...(record.district ? { district: record.district } : {}),
    ...(record.state ? { state: record.state } : {}),
    ...(record.postcode ? { postcode: record.postcode } : {}),
    ...(record.country ? { country: record.country } : {}),
    source: clean(source.name) || sourceId(source),
    category: "address",
    type: "civic_address",
    lat: Number(record.lat.toFixed(7)),
    lon: Number(record.lon.toFixed(7))
  };
}

function openDedupeStore(path) {
  DatabaseSync ||= require("node:sqlite").DatabaseSync;
  rmSync(path, { force: true });
  const db = new DatabaseSync(path);
  db.exec("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-131072");
  db.exec(`
    CREATE TABLE base_addresses (key TEXT PRIMARY KEY) WITHOUT ROWID;
    CREATE TABLE source_seen (key TEXT PRIMARY KEY) WITHOUT ROWID;
    CREATE TABLE postal_areas (
      key TEXT PRIMARY KEY,
      postcode TEXT NOT NULL,
      country TEXT NOT NULL,
      sum_lat REAL NOT NULL,
      sum_lon REAL NOT NULL,
      min_lat REAL NOT NULL,
      min_lon REAL NOT NULL,
      max_lat REAL NOT NULL,
      max_lon REAL NOT NULL,
      samples INTEGER NOT NULL,
      addresses INTEGER NOT NULL
    ) WITHOUT ROWID;
    CREATE TABLE postal_places (
      area_key TEXT NOT NULL,
      place_key TEXT NOT NULL,
      city TEXT NOT NULL,
      state TEXT NOT NULL,
      samples INTEGER NOT NULL,
      PRIMARY KEY (area_key, place_key)
    ) WITHOUT ROWID;
    CREATE TABLE postal_sources (
      area_key TEXT NOT NULL,
      source_id TEXT NOT NULL,
      source_name TEXT NOT NULL,
      source_url TEXT NOT NULL,
      priority INTEGER NOT NULL,
      PRIMARY KEY (area_key, source_id)
    ) WITHOUT ROWID;
  `);
  return db;
}

function beginBatch(db) {
  let rows = 0;
  db.exec("BEGIN");
  return {
    touch() {
      rows++;
      if (rows < SQLITE_BATCH_ROWS) return;
      db.exec("COMMIT; BEGIN");
      rows = 0;
    },
    close() {
      db.exec("COMMIT");
    }
  };
}

function baseAddressKeys(doc) {
  if (doc.house_number && doc.street) {
    const full = normalizeAddressAuthorityKey([
      doc.house_number,
      doc.street,
      doc.city,
      doc.state,
      doc.country
    ].filter(Boolean).join(" "));
    const loose = doc.city ? normalizeAddressAuthorityKey([
      doc.house_number,
      doc.street,
      doc.city
    ].join(" ")) : "";
    return [...new Set([full && `f\u001f${full}`, loose && `l\u001f${loose}`].filter(Boolean))];
  }
  const key = normalizeAddressAuthorityKey(doc.address);
  return key ? [`f\u001f${key}`] : [];
}

async function indexBaseAddresses(db, osmPath) {
  const insert = db.prepare("INSERT OR IGNORE INTO base_addresses (key) VALUES (?)");
  const batch = beginBatch(db);
  const lines = createInterface({ input: createJsonlReadStream(osmPath), crlfDelay: Infinity });
  let addresses = 0;
  try {
    for await (const line of lines) {
      if (!line) continue;
      const keys = baseAddressKeys(JSON.parse(line));
      if (!keys.length) continue;
      for (const key of keys) insert.run(key);
      batch.touch();
      addresses++;
    }
  } finally {
    batch.close();
  }
  return addresses;
}

function postalAreaKey(record) {
  return `${record.country || "XX"}\u001f${record.postcode}`;
}

function postalAreaDocument(db, row) {
  const places = [...db.prepare(`
    SELECT city, state, samples
    FROM postal_places
    WHERE area_key = ?
    ORDER BY samples DESC, city, state
  `).iterate(row.key)];
  const sources = [...db.prepare(`
    SELECT source_id, source_name, source_url
    FROM postal_sources
    WHERE area_key = ?
    ORDER BY priority, source_id
  `).iterate(row.key)];
  const primary = places[0] || { city: "", state: "" };
  const aliases = places.slice(1).map(place => [row.postcode, place.city].filter(Boolean).join(", "));
  const name = [row.postcode, primary.city].filter(Boolean).join(", ");
  const country = row.country === "XX" ? "" : row.country;
  return {
    id: `postal/${country || "unknown"}/${row.postcode.replaceAll(/\s+/gu, "")}`,
    ...(sources[0]?.source_url ? { url: sources[0].source_url } : {}),
    name,
    search_name: name,
    ...(aliases.length ? { aliases } : {}),
    postal_lookup: row.postcode,
    body: ["postal code", ...places.flatMap(place => [place.city, place.state]), country].filter(Boolean).join(" "),
    ...(primary.city ? { city: primary.city } : {}),
    ...(primary.state ? { state: primary.state } : {}),
    postcode: row.postcode,
    ...(country ? { country } : {}),
    source: sources.map(source => source.source_name || source.source_id).join(", "),
    category: "boundary",
    type: "postal_code",
    ...(row.addresses ? { address_count: row.addresses } : {}),
    sample_count: row.samples,
    bbox: [
      Number(row.min_lat.toFixed(7)),
      Number(row.min_lon.toFixed(7)),
      Number(row.max_lat.toFixed(7)),
      Number(row.max_lon.toFixed(7))
    ],
    lat: Number((row.sum_lat / row.samples).toFixed(7)),
    lon: Number((row.sum_lon / row.samples).toFixed(7))
  };
}

// --- candidate cache ---------------------------------------------------------
//
// Everything enrichment does except the OSM-duplicate check depends only on
// the address sources: parsing, normalization, source-priority dedupe, civic
// documents and postal aggregates. For a country that is ~96M rows of SQLite
// work against ~1.7M OSM address keys, so a new PBF with unchanged sources
// replays the cached candidates and only re-filters them against the OSM keys.
//
// Cache lines are `c<sourceIndex>\t<keys>\t<civic doc json>` (every unique
// civic candidate, before the OSM-duplicate check) and `p\t<postal doc json>`,
// written as gzip members. <keys> are baseAddressKeys(doc) — the same fields
// addressIdentity() and looseAddressIdentity() read from the record — joined
// by \u001e, so a replay filters without parsing the doc. Keys are
// space-joined tokens; a key that ever held a tab or \u001e would fall back
// to `j<sourceIndex>\t<json>`, whose keys are recomputed from the parsed doc.

function candidateCacheKey(options, sources, sourceMeta) {
  return createHash("sha256").update(stableJson({
    schemaVersion: ADDRESS_CANDIDATE_CACHE_SCHEMA_VERSION,
    enrichmentSchemaVersion: ADDRESS_ENRICHMENT_SCHEMA_VERSION,
    identity: options.candidateCache.identity ?? sourceMeta.map(source => source.identity),
    sources: sourceMeta.map(({ identity, ...provenance }, index) => ({
      ...provenance,
      includeAddresses: sources[index].includeAddresses !== false,
      includeUnits: sources[index].includeUnits === true,
      includeCountry: sources[index].includeCountry !== false,
      defaults: sources[index].defaults ?? null
    })),
    includePostalCodes: options.includePostalCodes !== false,
    limit: Number(options.limit || 0)
  })).digest("hex");
}

function readCandidateCacheMeta(path, key) {
  if (!existsSync(path) || !existsSync(`${path}.meta.json`)) return null;
  try {
    const meta = JSON.parse(readFileSync(`${path}.meta.json`, "utf8"));
    return meta.schemaVersion === ADDRESS_CANDIDATE_CACHE_SCHEMA_VERSION
      && meta.key === key
      && meta.bytes === statSync(path).size
      ? meta
      : null;
  } catch {
    return null;
  }
}

function createCandidateCacheWriter(path) {
  mkdirSync(dirname(path), { recursive: true });
  const partial = `${path}.partial`;
  rmSync(partial, { force: true });
  const fd = openSync(partial, "w");
  let parts = [];
  let size = 0;
  let members = 0;
  let closed = false;
  const flush = () => {
    if (!size && members) return;
    writeAll(fd, gzipSync(Buffer.from(parts.join("")), { level: 1 }));
    members++;
    parts = [];
    size = 0;
  };
  return {
    write(line) {
      parts.push(line);
      size += line.length;
      if (size >= CANDIDATE_CACHE_BLOCK_BYTES) flush();
    },
    commit(meta) {
      flush();
      closeSync(fd);
      closed = true;
      renameSync(partial, path);
      writeFileSync(`${path}.meta.json`, JSON.stringify({ ...meta, bytes: statSync(path).size }, null, 2));
    },
    abort() {
      if (!closed) closeSync(fd);
      closed = true;
      rmSync(partial, { force: true });
    }
  };
}

// Exact OSM address-key set. Sharded because one Set caps at 2^24 entries.
function createKeySet() {
  const shards = Array.from({ length: KEY_SET_SHARDS }, () => new Set());
  const shardOf = key => {
    let hash = 0;
    for (let index = 0; index < key.length; index++) hash = (Math.imul(hash, 31) + key.charCodeAt(index)) | 0;
    return shards[hash & (KEY_SET_SHARDS - 1)];
  };
  return {
    add(key) { shardOf(key).add(key); },
    has(key) { return shardOf(key).has(key); }
  };
}

async function readBaseAddressKeys(osmPath) {
  const keys = createKeySet();
  let addresses = 0;
  const lines = createInterface({ input: createJsonlReadStream(osmPath), crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line) continue;
    const docKeys = baseAddressKeys(JSON.parse(line));
    if (!docKeys.length) continue;
    for (const key of docKeys) keys.add(key);
    addresses++;
  }
  return { keys, addresses };
}

// Same predicate as the SQLite lookup in the full pass:
// base has `f<key>` or (loose ? `l<loose>` : `f<key>`).
function osmDuplicate(keys, candidateKeys) {
  for (const key of candidateKeys) {
    if (keys.has(key)) return true;
  }
  return false;
}

// candidateKeys must equal baseAddressKeys(doc); the full pass passes the
// keys it already computed from the record (same fields, same normalizer).
function candidateCacheLine(sourceIndex, json, candidateKeys) {
  const keys = candidateKeys.join("\u001e");
  return candidateKeys.some(key => /[\t\n\u001e]/u.test(key))
    ? `j${sourceIndex}\t${json}\n`
    : `c${sourceIndex}\t${keys}\t${json}\n`;
}

async function replayCandidateCache({ cachePath, cacheMeta, osmPath, outputPath, outputPartial, metaPath, sourceMeta, options }) {
  const started = performance.now();
  rmSync(outputPartial, { force: true });
  if (osmPath.toLowerCase().endsWith(".gz")) {
    await pipeline(createJsonlReadStream(osmPath), createWriteStream(outputPartial));
  } else {
    copyFileSync(osmPath, outputPartial);
  }
  const fd = openSync(outputPartial, "a");
  let parts = [];
  let bytes = 0;
  const flush = () => {
    if (!parts.length) return;
    writeAll(fd, Buffer.from(parts.join("")));
    parts = [];
    bytes = 0;
  };
  const write = json => {
    const line = `${json}\n`;
    parts.push(line);
    bytes += line.length;
    if (bytes >= WRITE_BUFFER_BYTES) flush();
  };
  try {
    const { keys, addresses: osmAddresses } = await readBaseAddressKeys(osmPath);
    const written = new Array(sourceMeta.length).fill(0);
    let osmDuplicates = 0;
    let postalDocs = 0;
    const lines = createInterface({ input: createReadStream(cachePath).pipe(createGunzip()), crlfDelay: Infinity });
    for await (const line of lines) {
      if (!line) continue;
      const tab = line.indexOf("\t");
      if (line[0] === "p") {
        write(line.slice(tab + 1));
        postalDocs++;
        continue;
      }
      let json;
      let candidateKeys;
      if (line[0] === "c") {
        const keyEnd = line.indexOf("\t", tab + 1);
        json = line.slice(keyEnd + 1);
        candidateKeys = keyEnd > tab + 1 ? line.slice(tab + 1, keyEnd).split("\u001e") : [];
      } else {
        json = line.slice(tab + 1);
        candidateKeys = baseAddressKeys(JSON.parse(json));
      }
      if (osmDuplicate(keys, candidateKeys)) {
        osmDuplicates++;
        continue;
      }
      write(json);
      written[Number(line.slice(1, tab))]++;
    }
    flush();
    closeSync(fd);
    renameSync(outputPartial, outputPath);
    const addressesWritten = written.reduce((sum, count) => sum + count, 0);
    const osmDocs = Number(options.osmDocs || 0);
    const meta = {
      schemaVersion: ADDRESS_ENRICHMENT_SCHEMA_VERSION,
      sources: sourceMeta,
      osm: identityFor(osmPath),
      osmAddresses,
      ...cacheMeta.totals,
      addressesWritten,
      osmDuplicates,
      postalDocs,
      bySource: cacheMeta.bySource.map((source, index) => ({ ...source, addressesWritten: written[index] })),
      osmDocs,
      totalDocs: osmDocs + addressesWritten + postalDocs,
      bytes: statSync(outputPath).size,
      seconds: Math.round((performance.now() - started) / 100) / 10,
      candidateCache: "hit"
    };
    writeFileSync(metaPath, JSON.stringify(meta, null, 2));
    options.log?.(`[address-enrichment] replayed cached candidates: wrote ${addressesWritten.toLocaleString()} civic and ${postalDocs.toLocaleString()} postal docs in ${meta.seconds}s`);
    return { path: outputPath, meta };
  } catch (error) {
    try { closeSync(fd); } catch { /* already closed */ }
    rmSync(outputPartial, { force: true });
    throw error;
  }
}

function enrichmentSourcesMeta(sources) {
  return sources.map(source => ({
    id: sourceId(source),
    name: clean(source.name) || sourceId(source),
    ...(source.url ? { url: clean(source.url) } : {}),
    ...(source.license ? { license: clean(source.license) } : {}),
    ...(source.attribution ? { attribution: clean(source.attribution) } : {}),
    identity: sourceIdentity(source)
  }));
}

/**
 * Append multiple address authorities to one OSM corpus. Sources are applied
 * in priority order. Civic duplicates are emitted once; postcode documents
 * merge every provider into one country-scoped aggregate.
 */
export async function augmentOsmWithAddressSources(options = {}) {
  const root = resolve(options.root || ".");
  const osmPath = resolve(options.osmPath || "");
  if (!existsSync(osmPath)) throw new Error(`OSM source is missing: ${osmPath || "no path configured"}`);
  const sources = (options.sources || []).filter(Boolean);
  if (!sources.length) throw new Error("Address enrichment needs at least one source.");
  for (const source of sources) {
    sourceId(source);
    if (typeof source.records !== "function" && !source[Symbol.asyncIterator]) {
      throw new TypeError(`Address source ${source.id} needs records() or an async iterator.`);
    }
  }
  const outputPath = resolve(options.outputPath || resolve(root, "data", "osm-enriched-places.jsonl"));
  const metaPath = `${outputPath}.meta.json`;
  const outputPartial = `${outputPath}.partial`;
  const sqlitePath = resolve(root, "data", "address-enrichment.sqlite");
  const osmIdentity = identityFor(osmPath);
  const sourceMeta = enrichmentSourcesMeta(sources);
  if (!options.force && existsSync(outputPath) && existsSync(metaPath)) {
    const meta = JSON.parse(readFileSync(metaPath, "utf8"));
    const reusable = meta.schemaVersion === ADDRESS_ENRICHMENT_SCHEMA_VERSION
      && sameIdentity(meta.osm, osmIdentity)
      && meta.sources?.length === sourceMeta.length
      && meta.sources.every((source, index) => (
        source.id === sourceMeta[index].id
        && source.name === sourceMeta[index].name
        && source.url === sourceMeta[index].url
        && source.license === sourceMeta[index].license
        && source.attribution === sourceMeta[index].attribution
        && sameSourceIdentity(source.identity, sourceMeta[index].identity)
      ));
    if (reusable) return { path: outputPath, meta };
  }

  const cachePath = options.candidateCache?.path ? resolve(options.candidateCache.path) : "";
  const cacheKey = cachePath ? candidateCacheKey(options, sources, sourceMeta) : "";
  if (cachePath) {
    const cacheMeta = readCandidateCacheMeta(cachePath, cacheKey);
    if (cacheMeta) {
      mkdirSync(dirname(outputPath), { recursive: true });
      return replayCandidateCache({ cachePath, cacheMeta, osmPath, outputPath, outputPartial, metaPath, sourceMeta, options });
    }
  }

  mkdirSync(dirname(outputPath), { recursive: true });
  mkdirSync(dirname(sqlitePath), { recursive: true });
  rmSync(outputPartial, { force: true });
  if (osmPath.toLowerCase().endsWith(".gz")) {
    await pipeline(createJsonlReadStream(osmPath), createWriteStream(outputPartial));
  } else {
    copyFileSync(osmPath, outputPartial);
  }
  const writer = createAppendJsonlWriter(outputPartial);
  const cache = cachePath ? createCandidateCacheWriter(cachePath) : null;
  const db = openDedupeStore(sqlitePath);
  const started = performance.now();
  const totals = {
    rowsRead: 0,
    normalized: 0,
    uniqueCivic: 0,
    addressesWritten: 0,
    osmDuplicates: 0,
    sourceDuplicates: 0,
    invalid: 0,
    postalSamples: 0,
    postalDocs: 0
  };
  const bySource = [];
  let lastProgress = performance.now();
  try {
    const osmAddresses = await indexBaseAddresses(db, osmPath);
    const seen = db.prepare("INSERT OR IGNORE INTO source_seen (key) VALUES (?)");
    const exactBase = db.prepare("SELECT 1 AS found FROM base_addresses WHERE key IN (?, ?) LIMIT 1");
    const upsertPostal = db.prepare(`
      INSERT INTO postal_areas (
        key, postcode, country, sum_lat, sum_lon,
        min_lat, min_lon, max_lat, max_lon, samples, addresses
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
      ON CONFLICT(key) DO UPDATE SET
        sum_lat = sum_lat + excluded.sum_lat,
        sum_lon = sum_lon + excluded.sum_lon,
        min_lat = MIN(min_lat, excluded.min_lat),
        min_lon = MIN(min_lon, excluded.min_lon),
        max_lat = MAX(max_lat, excluded.max_lat),
        max_lon = MAX(max_lon, excluded.max_lon),
        samples = samples + 1,
        addresses = addresses + excluded.addresses
    `);
    const upsertPlace = db.prepare(`
      INSERT INTO postal_places (area_key, place_key, city, state, samples)
      VALUES (?, ?, ?, ?, 1)
      ON CONFLICT(area_key, place_key) DO UPDATE SET samples = samples + 1
    `);
    const insertPostalSource = db.prepare(`
      INSERT OR IGNORE INTO postal_sources (area_key, source_id, source_name, source_url, priority)
      VALUES (?, ?, ?, ?, ?)
    `);
    const batch = beginBatch(db);
    try {
      for (const [sourceIndex, source] of sources.entries()) {
        const sourceStats = { id: sourceId(source), rowsRead: 0, normalized: 0, addressesWritten: 0, invalid: 0 };
        const iterable = typeof source.records === "function" ? source.records() : source;
        for await (const raw of iterable) {
          totals.rowsRead++;
          sourceStats.rowsRead++;
          const mapped = typeof source.normalize === "function" ? source.normalize(raw) : raw;
          let record = normalizeExternalAddressRecord(mapped, source.defaults);
          if (record?.postcode && typeof source.normalizePostcode === "function") {
            record = normalizeExternalAddressRecord({
              ...record,
              postcode: source.normalizePostcode(record.postcode, record)
            }, source.defaults);
          }
          if (!record || (typeof source.filter === "function" && !source.filter(record, raw))) {
            totals.invalid++;
            sourceStats.invalid++;
            continue;
          }
          totals.normalized++;
          sourceStats.normalized++;
          let uniqueAddress = false;
          let key = "";
          if (record.kind === "address") {
            key = addressIdentity(record);
            uniqueAddress = Boolean(key && seen.run(key).changes);
            if (uniqueAddress) totals.uniqueCivic++;
            else totals.sourceDuplicates++;
          }
          if (record.postcode && (record.kind === "postal_code" || uniqueAddress)) {
            const areaKey = postalAreaKey(record);
            const isAddress = record.kind === "address" ? 1 : 0;
            upsertPostal.run(
              areaKey,
              record.postcode,
              record.country || "XX",
              record.lat,
              record.lon,
              record.lat,
              record.lon,
              record.lat,
              record.lon,
              isAddress
            );
            const placeKey = `${record.city}\u001f${record.state}`;
            upsertPlace.run(areaKey, placeKey, record.city, record.state);
            insertPostalSource.run(
              areaKey,
              sourceId(source),
              clean(source.name) || sourceId(source),
              clean(source.url),
              sourceIndex
            );
            totals.postalSamples++;
          }
          if (record.kind === "address" && uniqueAddress && source.includeAddresses !== false) {
            const looseKey = looseAddressIdentity(record);
            const duplicate = Boolean(exactBase.get(`f\u001f${key}`, looseKey ? `l\u001f${looseKey}` : `f\u001f${key}`));
            const doc = (!duplicate || cache) ? externalAddressDocument(record, source, {
              includeUnits: source.includeUnits,
              includeCountry: source.includeCountry
            }) : null;
            const json = doc ? JSON.stringify(doc) : "";
            if (doc && cache) {
              const candidateKeys = looseKey ? [`f\u001f${key}`, `l\u001f${looseKey}`] : [`f\u001f${key}`];
              cache.write(candidateCacheLine(sourceIndex, json, candidateKeys));
            }
            if (duplicate) {
              totals.osmDuplicates++;
            } else {
              if (doc) {
                writer.writeJson(json);
                totals.addressesWritten++;
                sourceStats.addressesWritten++;
              }
            }
          }
          batch.touch();
          const now = performance.now();
          if (now - lastProgress >= Number(options.progressLogMs || 30_000)) {
            lastProgress = now;
            options.log?.(`[address-enrichment] ${totals.rowsRead.toLocaleString()} rows, ${totals.addressesWritten.toLocaleString()} civic docs, ${totals.osmDuplicates.toLocaleString()} OSM duplicates`);
          }
          if (options.limit && totals.normalized >= options.limit) break;
        }
        bySource.push(sourceStats);
        if (options.limit && totals.normalized >= options.limit) break;
      }
    } finally {
      batch.close();
    }

    if (options.includePostalCodes !== false) {
      const postalRows = db.prepare("SELECT * FROM postal_areas ORDER BY country, postcode");
      for (const row of postalRows.iterate()) {
        const doc = postalAreaDocument(db, row);
        writer.write(doc);
        if (cache) cache.write(`p\t${JSON.stringify(doc)}\n`);
        totals.postalDocs++;
      }
    }
    writer.close();
    renameSync(outputPartial, outputPath);
    if (cache) {
      const { addressesWritten, osmDuplicates, ...sourceTotals } = totals;
      cache.commit({
        schemaVersion: ADDRESS_CANDIDATE_CACHE_SCHEMA_VERSION,
        key: cacheKey,
        totals: sourceTotals,
        bySource: bySource.map(({ addressesWritten: _written, ...source }) => source)
      });
    }
    const osmDocs = Number(options.osmDocs || 0);
    const meta = {
      schemaVersion: ADDRESS_ENRICHMENT_SCHEMA_VERSION,
      sources: sourceMeta,
      osm: osmIdentity,
      osmAddresses,
      ...totals,
      bySource,
      osmDocs,
      totalDocs: osmDocs + totals.addressesWritten + totals.postalDocs,
      bytes: statSync(outputPath).size,
      seconds: Math.round((performance.now() - started) / 100) / 10,
      ...(cache ? { candidateCache: "miss" } : {})
    };
    writeFileSync(metaPath, JSON.stringify(meta, null, 2));
    options.log?.(`[address-enrichment] wrote ${totals.addressesWritten.toLocaleString()} civic and ${totals.postalDocs.toLocaleString()} postal docs in ${meta.seconds}s`);
    return { path: outputPath, meta };
  } catch (error) {
    writer.close();
    cache?.abort();
    rmSync(outputPartial, { force: true });
    throw error;
  } finally {
    db.close();
    rmSync(sqlitePath, { force: true });
  }
}
