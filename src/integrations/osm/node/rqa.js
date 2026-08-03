import { createReadStream, existsSync } from "node:fs";
import { spawn } from "node:child_process";
import { resolve } from "node:path";
import {
  augmentOsmWithAddressSources,
  externalAddressDocument,
  normalizeExternalAddressRecord,
  parseDelimitedRows
} from "./address_enrichment.js";

export const RQA_CSV_URL = "https://diffusion.mern.gouv.qc.ca/diffusion/RGQ/Vectoriel/Theme/Local/RQA/CSV/RQA_CSV.zip";
export const RQA_DATASET_URL = "https://www.donneesquebec.ca/recherche/dataset/referentiel-quebecois-des-adresses";

function clean(value) {
  return String(value ?? "").replaceAll(/[\u0000-\u001f\u0085\u2028\u2029]+/gu, " ").trim();
}

function rowObject(header, row) {
  const value = {};
  for (let index = 0; index < header.length; index++) value[header[index]] = row[index] || "";
  return value;
}

function rqaRecord(raw) {
  if (clean(raw.date_fin)) return null;
  return normalizeExternalAddressRecord({
    id: clean(raw.identifiant_unique_adresse),
    houseNumber: `${clean(raw.numero_municipal)}${clean(raw.numero_municipal_suffixe)}`,
    street: clean(raw.odonyme_recompose_normal || raw.odonyme_recompose_court || raw.odonyme_recompose_long),
    unit: clean(raw.numero_unite),
    city: clean(raw.nom_municipalite),
    state: "QC",
    postcode: clean(raw.code_postal),
    country: "CA",
    lat: raw.latitude,
    lon: raw.longitude
  });
}

export function rqaAddressDoc(raw) {
  const record = rqaRecord(raw);
  return record ? externalAddressDocument(record, {
    id: "rqa",
    name: "Référentiel québécois des adresses (RQA)",
    url: RQA_DATASET_URL
  }, { includeCountry: false }) : null;
}

// Compatibility export retained for existing consumers. The implementation
// is the provider-neutral parser used by every delimited address source.
export function parseCsvRows(stream) {
  return parseDelimitedRows(stream, { delimiter: "," });
}

function openRqaCsv({ archivePath, csvPath }) {
  if (csvPath) return { stream: createReadStream(csvPath), done: Promise.resolve() };
  const child = spawn("unzip", ["-p", archivePath, "RQA.csv"], { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", chunk => { stderr += chunk; });
  const done = new Promise((resolveDone, reject) => {
    child.once("error", reject);
    child.once("close", code => code === 0
      ? resolveDone()
      : reject(new Error(`Unable to read RQA.csv from ${archivePath}: ${stderr.trim()}`)));
  });
  return { stream: child.stdout, done };
}

function createRqaSource({ archivePath, csvPath }) {
  const sourcePath = csvPath || archivePath;
  return {
    id: "rqa",
    name: "Référentiel québécois des adresses (RQA)",
    path: sourcePath,
    url: RQA_DATASET_URL,
    license: "CC-BY-4.0",
    attribution: "Ministère des Ressources naturelles et des Forêts du Québec - Référentiel québécois des adresses",
    includeCountry: false,
    async *records() {
      const { stream, done } = openRqaCsv({ archivePath, csvPath });
      let header = null;
      try {
        for await (const row of parseCsvRows(stream)) {
          if (!header) {
            header = row;
            continue;
          }
          yield rowObject(header, row);
        }
        await done;
      } finally {
        if (!stream.destroyed) stream.destroy();
      }
    },
    normalize: rqaRecord
  };
}

/** Quebec adapter over the generic multi-provider enrichment engine. */
export async function augmentOsmWithRqa(options) {
  const root = resolve(options.root);
  const osmPath = resolve(options.osmPath);
  const archivePath = options.archivePath ? resolve(options.archivePath) : "";
  const csvPath = options.csvPath ? resolve(options.csvPath) : "";
  const sourcePath = csvPath || archivePath;
  if (!sourcePath || !existsSync(sourcePath)) {
    throw new Error(`RQA source is missing: ${sourcePath || "no path configured"}`);
  }
  const outputPath = resolve(options.outputPath || resolve(root, "data", "osm-rqa-places.jsonl"));
  const result = await augmentOsmWithAddressSources({
    ...options,
    root,
    osmPath,
    outputPath,
    sources: [createRqaSource({ archivePath, csvPath })],
    log: options.log || (line => console.log(line))
  });
  return result;
}
