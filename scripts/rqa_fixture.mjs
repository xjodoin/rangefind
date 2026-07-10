import {
  closeSync,
  copyFileSync,
  createReadStream,
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
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { createInterface } from "node:readline";
import {
  normalizeAddressAuthorityKey,
  normalizePostalCodeSpacing
} from "../src/address.js";

export const RQA_CSV_URL = "https://diffusion.mern.gouv.qc.ca/diffusion/RGQ/Vectoriel/Theme/Local/RQA/CSV/RQA_CSV.zip";
export const RQA_DATASET_URL = "https://www.donneesquebec.ca/recherche/dataset/referentiel-quebecois-des-adresses";

const RQA_SCHEMA_VERSION = 3;
const WRITE_BUFFER_BYTES = 8 * 1024 * 1024;
const SQLITE_BATCH_ROWS = 250_000;
const require = createRequire(import.meta.url);
let DatabaseSync = null;

function clean(value) {
  return String(value || "").replaceAll(/[\u0000-\u001f\u0085\u2028\u2029]+/gu, " ").trim();
}

function postalCode(value) {
  return normalizePostalCodeSpacing(clean(value)).toUpperCase();
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
      const line = `${JSON.stringify(value)}\n`;
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

// Streaming RFC 4180 parser. RQA currently has one record per physical line,
// but keeping quote/newline state across chunks makes ingestion resilient to a
// future formatted-address or metadata field containing an embedded newline.
export async function* parseCsvRows(stream) {
  let row = [];
  let field = "";
  let quoted = false;
  let pendingQuote = false;
  let firstField = true;
  for await (const chunk of stream) {
    const text = chunk.toString("utf8");
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
          // Reprocess the delimiter following the closing quote.
          index--;
          continue;
        }
        if (char === '"') pendingQuote = true;
        else field += char;
        continue;
      }
      if (char === '"' && field === "") {
        quoted = true;
      } else if (char === ",") {
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
  if (quoted) throw new Error("RQA CSV ended inside a quoted field.");
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

function civicParts(raw) {
  // The flat file retains lifecycle metadata; ended civic addresses must not
  // reappear in search merely because an older coordinate remains published.
  if (clean(raw.date_fin)) return null;
  const number = clean(raw.numero_municipal);
  const suffix = clean(raw.numero_municipal_suffixe);
  const houseNumber = `${number}${suffix}`;
  const street = clean(raw.odonyme_recompose_normal || raw.odonyme_recompose_court || raw.odonyme_recompose_long);
  const city = clean(raw.nom_municipalite);
  const postcode = postalCode(raw.code_postal);
  const lat = Number(raw.latitude);
  const lon = Number(raw.longitude);
  if (!number || !street || !city || !Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return { houseNumber, street, city, postcode, lat, lon };
}

function civicKey(parts) {
  return normalizeAddressAuthorityKey(`${parts.houseNumber} ${parts.street} ${parts.city}`);
}

export function rqaAddressDoc(raw) {
  const parts = civicParts(raw);
  if (!parts) return null;
  const locality = [parts.city, "QC", parts.postcode].filter(Boolean).join(", ");
  const address = `${parts.houseNumber} ${parts.street}, ${locality}`;
  return {
    id: `rqa/${clean(raw.identifiant_unique_adresse)}`,
    url: RQA_DATASET_URL,
    name: address,
    address,
    house_number: parts.houseNumber,
    street: parts.street,
    city: parts.city,
    state: "QC",
    ...(parts.postcode ? { postcode: parts.postcode } : {}),
    country: "CA",
    category: "address",
    type: "civic_address",
    lat: Number(parts.lat.toFixed(7)),
    lon: Number(parts.lon.toFixed(7))
  };
}

function identityFor(path) {
  const absolute = resolve(path);
  const stat = statSync(absolute);
  return { path: absolute, bytes: stat.size, mtimeMs: Math.floor(stat.mtimeMs) };
}

function sameIdentity(left, right) {
  return left?.path === right.path && left?.bytes === right.bytes && left?.mtimeMs === right.mtimeMs;
}

function openRqaCsv({ archivePath, csvPath }) {
  if (csvPath) return { stream: createReadStream(csvPath), done: Promise.resolve() };
  const child = spawn("unzip", ["-p", archivePath, "RQA.csv"], { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", chunk => { stderr += chunk; });
  const done = new Promise((resolveDone, reject) => {
    child.once("error", reject);
    child.once("close", code => code === 0 ? resolveDone() : reject(new Error(`Unable to read RQA.csv from ${archivePath}: ${stderr.trim()}`)));
  });
  return { stream: child.stdout, done };
}

function openDedupeStore(path) {
  DatabaseSync ||= require("node:sqlite").DatabaseSync;
  rmSync(path, { force: true });
  const db = new DatabaseSync(path);
  db.exec("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-131072");
  db.exec(`
    CREATE TABLE osm_full (key TEXT PRIMARY KEY) WITHOUT ROWID;
    CREATE TABLE rqa_seen (key TEXT PRIMARY KEY) WITHOUT ROWID;
    CREATE TABLE postcodes (
      key TEXT PRIMARY KEY,
      postcode TEXT NOT NULL,
      city TEXT NOT NULL,
      municipality_code TEXT NOT NULL,
      sum_lat REAL NOT NULL,
      sum_lon REAL NOT NULL,
      addresses INTEGER NOT NULL
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

async function indexOsmAddresses(db, osmPath) {
  const insert = db.prepare("INSERT OR IGNORE INTO osm_full (key) VALUES (?)");
  const batch = beginBatch(db);
  const lines = createInterface({ input: createReadStream(osmPath), crlfDelay: Infinity });
  let addresses = 0;
  try {
    for await (const line of lines) {
      if (!line) continue;
      const doc = JSON.parse(line);
      const key = normalizeAddressAuthorityKey(doc.address);
      if (!key) continue;
      insert.run(key);
      batch.touch();
      addresses++;
    }
  } finally {
    batch.close();
  }
  return addresses;
}

function postalDoc(row) {
  const lat = row.sum_lat / row.addresses;
  const lon = row.sum_lon / row.addresses;
  const name = `${row.postcode}, ${row.city}`;
  return {
    id: `rqa/postcode/${row.postcode.replaceAll(" ", "")}/${row.municipality_code || "unknown"}`,
    url: RQA_DATASET_URL,
    name,
    search_name: name,
    postal_lookup: row.postcode,
    body: `postal code ${row.city} Quebec`,
    city: row.city,
    state: "QC",
    postcode: row.postcode,
    country: "CA",
    category: "boundary",
    type: "postal_code",
    address_count: row.addresses,
    lat: Number(lat.toFixed(7)),
    lon: Number(lon.toFixed(7))
  };
}

export async function augmentOsmWithRqa(options) {
  const root = resolve(options.root);
  const osmPath = resolve(options.osmPath);
  const archivePath = options.archivePath ? resolve(options.archivePath) : "";
  const csvPath = options.csvPath ? resolve(options.csvPath) : "";
  const sourcePath = csvPath || archivePath;
  if (!sourcePath || !existsSync(sourcePath)) throw new Error(`RQA source is missing: ${sourcePath || "no path configured"}`);
  const outputPath = resolve(options.outputPath || resolve(root, "data", "osm-rqa-places.jsonl"));
  const metaPath = `${outputPath}.meta.json`;
  const outputPartial = `${outputPath}.partial`;
  const sqlitePath = resolve(root, "data", "rqa-dedupe.sqlite");
  const sourceIdentity = identityFor(sourcePath);
  const osmIdentity = identityFor(osmPath);
  if (!options.force && existsSync(outputPath) && existsSync(metaPath)) {
    const meta = JSON.parse(readFileSync(metaPath, "utf8"));
    if (meta.schemaVersion === RQA_SCHEMA_VERSION
        && sameIdentity(meta.source, sourceIdentity)
        && sameIdentity(meta.osm, osmIdentity)) {
      console.log(`[rqa] reusing ${outputPath} (${meta.totalDocs.toLocaleString()} docs)`);
      return { path: outputPath, meta };
    }
  }

  mkdirSync(dirname(outputPath), { recursive: true });
  mkdirSync(dirname(sqlitePath), { recursive: true });
  rmSync(outputPartial, { force: true });
  copyFileSync(osmPath, outputPartial);
  const writer = createAppendJsonlWriter(outputPartial);
  const db = openDedupeStore(sqlitePath);
  const started = performance.now();
  let rowsRead = 0;
  let uniqueCivic = 0;
  let addressesWritten = 0;
  let osmDuplicates = 0;
  let invalid = 0;
  let lastProgress = performance.now();
  try {
    const osmAddresses = await indexOsmAddresses(db, osmPath);
    console.log(`[rqa] indexed ${osmAddresses.toLocaleString()} explicit OSM addresses for canonical deduplication`);
    const seen = db.prepare("INSERT OR IGNORE INTO rqa_seen (key) VALUES (?)");
    const exactOsm = db.prepare("SELECT 1 AS found FROM osm_full WHERE key = ?");
    const upsertPostal = db.prepare(`
      INSERT INTO postcodes (key, postcode, city, municipality_code, sum_lat, sum_lon, addresses)
      VALUES (?, ?, ?, ?, ?, ?, 1)
      ON CONFLICT(key) DO UPDATE SET
        sum_lat = sum_lat + excluded.sum_lat,
        sum_lon = sum_lon + excluded.sum_lon,
        addresses = addresses + 1
    `);
    const batch = beginBatch(db);
    const { stream, done } = openRqaCsv({ archivePath, csvPath });
    let header = null;
    try {
      for await (const row of parseCsvRows(stream)) {
        if (!header) {
          header = row;
          continue;
        }
        rowsRead++;
        const raw = rowObject(header, row);
        const parts = civicParts(raw);
        if (!parts) {
          invalid++;
          continue;
        }
        const key = civicKey(parts);
        if (!seen.run(key).changes) continue;
        uniqueCivic++;
        const municipalityCode = clean(raw.code_municipalite);
        if (parts.postcode) {
          const postalKey = `${parts.postcode}\u0000${municipalityCode || parts.city}`;
          upsertPostal.run(postalKey, parts.postcode, parts.city, municipalityCode, parts.lat, parts.lon);
        }
        const doc = rqaAddressDoc(raw);
        const duplicate = Boolean(doc && exactOsm.get(normalizeAddressAuthorityKey(doc.address)));
        if (duplicate) {
          osmDuplicates++;
        } else {
          if (doc) {
            writer.write(doc);
            addressesWritten++;
          }
        }
        batch.touch();
        const now = performance.now();
        if (now - lastProgress >= 30_000) {
          lastProgress = now;
          console.log(`[rqa] ${rowsRead.toLocaleString()} rows, ${addressesWritten.toLocaleString()} civic docs, ${osmDuplicates.toLocaleString()} OSM duplicates`);
        }
        if (options.limit && uniqueCivic >= options.limit) break;
      }
      await done;
    } finally {
      batch.close();
    }

    let postalDocs = 0;
    const postalRows = db.prepare("SELECT postcode, city, municipality_code, sum_lat, sum_lon, addresses FROM postcodes ORDER BY key");
    for (const row of postalRows.iterate()) {
      writer.write(postalDoc(row));
      postalDocs++;
    }
    writer.close();
    renameSync(outputPartial, outputPath);
    const osmDocs = Number(options.osmDocs || 0);
    const meta = {
      schemaVersion: RQA_SCHEMA_VERSION,
      source: sourceIdentity,
      osm: osmIdentity,
      rowsRead,
      uniqueCivic,
      addressesWritten,
      osmDuplicates,
      invalid,
      postalDocs,
      osmDocs,
      totalDocs: osmDocs + addressesWritten + postalDocs,
      bytes: statSync(outputPath).size,
      seconds: Math.round((performance.now() - started) / 100) / 10,
      license: "CC-BY-4.0",
      attribution: "Ministère des Ressources naturelles et des Forêts du Québec - Référentiel québécois des adresses"
    };
    writeFileSync(metaPath, JSON.stringify(meta, null, 2));
    console.log(`[rqa] wrote ${addressesWritten.toLocaleString()} civic and ${postalDocs.toLocaleString()} postal docs in ${meta.seconds}s`);
    return { path: outputPath, meta };
  } catch (error) {
    writer.close();
    rmSync(outputPartial, { force: true });
    throw error;
  } finally {
    db.close();
    rmSync(sqlitePath, { force: true });
  }
}
