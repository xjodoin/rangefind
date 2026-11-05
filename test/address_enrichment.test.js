import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { gzipSync } from "node:zlib";
import {
  augmentOsmWithAddressSources,
  createDelimitedAddressSource,
  createJsonlAddressSource
} from "../src/integrations/osm/node/address_enrichment.js";

async function* records(values) {
  yield* values;
}

test("address enrichment streams a gzip-compressed OSM base corpus", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-enrichment-gzip-"));
  try {
    const osmPath = join(root, "osm.jsonl.gz");
    const outputPath = join(root, "combined.jsonl");
    const osmDoc = { id: "node/1", name: "Compressed OSM place", lat: 45.64, lon: -73.8 };
    await writeFile(osmPath, gzipSync(`${JSON.stringify(osmDoc)}\n`));
    const result = await augmentOsmWithAddressSources({
      root,
      osmPath,
      outputPath,
      osmDocs: 1,
      sources: [{
        id: "address-authority",
        name: "Address Authority",
        records: () => records([{ id: "new", houseNumber: "12", street: "Rue Exemple", city: "Rosemère", lat: 45.641, lon: -73.801 }])
      }]
    });
    const docs = (await readFile(outputPath, "utf8")).trim().split("\n").map(JSON.parse);
    assert.equal(result.meta.totalDocs, 2);
    assert.deepEqual(docs[0], osmDoc);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("generic enrichment merges postal authorities and deduplicates civic addresses by source priority", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-enrichment-"));
  try {
    const osmPath = join(root, "osm.jsonl");
    const outputPath = join(root, "combined.jsonl");
    await writeFile(osmPath, `${JSON.stringify({
      id: "node/1",
      address: "10 Rue Exemple, Rosemère",
      house_number: "10",
      street: "Rue Exemple",
      city: "Rosemère",
      lat: 45.64,
      lon: -73.8
    })}\n`);

    const result = await augmentOsmWithAddressSources({
      root,
      osmPath,
      outputPath,
      osmDocs: 1,
      sources: [
        {
          id: "postal-authority",
          name: "Postal Authority",
          version: "2026-08",
          url: "https://postal.example/data",
          includeAddresses: false,
          records: () => records([{
            id: "J7A1V6",
            kind: "postal_code",
            postcode: "j7a1v6",
            city: "Rosemère",
            state: "QC",
            country: "CA",
            lat: 45.64,
            lon: -73.7971
          }])
        },
        {
          id: "address-authority",
          name: "Address Authority",
          version: "2026-08",
          url: "https://address.example/data",
          records: () => records([
            { id: "duplicate-osm", houseNumber: "10", street: "Rue Exemple", city: "Rosemère", state: "QC", country: "CA", postcode: "J7A 1V6", lat: 45.64, lon: -73.8 },
            { id: "new", houseNumber: "12", street: "Rue Exemple", city: "Rosemère", state: "QC", country: "CA", postcode: "J7A 1V6", lat: 45.641, lon: -73.801 },
            { id: "duplicate-source", houseNumber: "12", street: "Rue Exemple", city: "Rosemère", state: "QC", country: "CA", postcode: "J7A 1V6", lat: 45.641, lon: -73.801 }
          ])
        }
      ]
    });

    const docs = (await readFile(outputPath, "utf8")).trim().split("\n").map(JSON.parse);
    assert.equal(result.meta.addressesWritten, 1);
    assert.equal(result.meta.osmDuplicates, 1);
    assert.equal(result.meta.sourceDuplicates, 1);
    assert.equal(result.meta.postalDocs, 1);
    assert.equal(docs.length, 3);
    assert.equal(docs[1].id, "address-authority/new");
    assert.equal(docs[1].source, "Address Authority");
    assert.deepEqual(docs[2], {
      id: "postal/CA/J7A1V6",
      url: "https://postal.example/data",
      name: "J7A 1V6, Rosemère",
      search_name: "J7A 1V6, Rosemère",
      postal_lookup: "J7A 1V6",
      body: "postal code Rosemère QC CA",
      city: "Rosemère",
      state: "QC",
      postcode: "J7A 1V6",
      country: "CA",
      source: "Postal Authority, Address Authority",
      category: "boundary",
      type: "postal_code",
      address_count: 2,
      sample_count: 3,
      bbox: [45.64, -73.801, 45.641, -73.7971],
      lat: 45.6403333,
      lon: -73.7993667
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("delimited source adapters map headerless TSV rows into the canonical contract", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-delimited-"));
  try {
    const path = join(root, "postal.tsv");
    await writeFile(path, "CA\tJ7A 1V6\tRosemere\tQuebec\tQC\t45.64\t-73.7971\n");
    const source = createDelimitedAddressSource({
      id: "geonames-ca",
      path,
      delimiter: "\t",
      header: false,
      defaults: { kind: "postal_code" },
      mapping: {
        country: 0,
        postcode: 1,
        city: 2,
        state: 4,
        lat: 5,
        lon: 6
      }
    });
    const rows = [];
    for await (const row of source.records()) rows.push(row);
    assert.deepEqual(rows[0], {
      kind: "postal_code",
      country: "CA",
      postcode: "J7A 1V6",
      city: "Rosemere",
      state: "QC",
      lat: "45.64",
      lon: "-73.7971",
      _raw: ["CA", "J7A 1V6", "Rosemere", "Quebec", "QC", "45.64", "-73.7971"],
      _row: 1
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("JSONL source adapters stream canonical partition files", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-jsonl-"));
  try {
    const path = join(root, "postal.jsonl");
    await writeFile(path, `${JSON.stringify({
      kind: "postal_code",
      country: "GB",
      postcode: "SW1A 1AA",
      city: "London",
      lat: 51.501,
      lon: -0.1416
    })}\n`);
    const source = createJsonlAddressSource({ id: "partition", path });
    const rows = [];
    for await (const row of source.records()) rows.push(row);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].postcode, "SW1A 1AA");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("enrichment cache compares generic remote identities and provenance metadata", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-identity-"));
  try {
    const osmPath = join(root, "osm.jsonl");
    const outputPath = join(root, "combined.jsonl");
    await writeFile(osmPath, `${JSON.stringify({ id: "node/1", name: "Base" })}\n`);
    let reads = 0;
    const source = (etag, name = "Postal Authority") => ({
      id: "postal-authority",
      name,
      identity: { etag, config: "mapping-v1" },
      records: () => {
        reads++;
        return records([{
          kind: "postal_code",
          postcode: "12345",
          city: "Example",
          country: "US",
          lat: 40,
          lon: -75
        }]);
      }
    });

    await augmentOsmWithAddressSources({ root, osmPath, outputPath, osmDocs: 1, sources: [source("v1")] });
    await augmentOsmWithAddressSources({ root, osmPath, outputPath, osmDocs: 1, sources: [source("v1")] });
    assert.equal(reads, 1);

    await augmentOsmWithAddressSources({ root, osmPath, outputPath, osmDocs: 1, sources: [source("v2")] });
    await augmentOsmWithAddressSources({ root, osmPath, outputPath, osmDocs: 1, sources: [source("v2", "Renamed Authority")] });
    assert.equal(reads, 3);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a candidate-cache replay against a new OSM corpus equals a full enrichment", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-enrichment-cache-"));
  try {
    const osmDoc = (id, houseNumber, city = "Rosemère") => JSON.stringify({
      id, address: `${houseNumber} Rue Exemple, ${city}`, house_number: houseNumber, street: "Rue Exemple", city, lat: 45.64, lon: -73.8
    });
    const civic = Array.from({ length: 40 }, (_, i) => ({
      id: `a${i}`, houseNumber: String(i), street: "Rue Exemple", city: i % 3 ? "Rosemère" : "", state: "QC",
      country: "CA", postcode: i % 2 ? "J7A 1V6" : "J7A 2B2", lat: 45.6 + i / 1000, lon: -73.8
    }));
    let reads = 0;
    const sources = () => [
      {
        id: "postal-authority", name: "Postal Authority", version: "2026-08", includeAddresses: false,
        records: () => { reads++; return records([{ id: "p", kind: "postal_code", postcode: "j7a1v6", city: "Rosemère", state: "QC", country: "CA", lat: 45.64, lon: -73.79 }]); }
      },
      {
        id: "address-authority", name: "Address Authority", version: "2026-08",
        // Repeats exercise source-priority dedupe, which only the full pass sees.
        records: () => { reads++; return records([...civic, ...civic.slice(0, 5)]); }
      }
    ];
    const cachePath = join(root, "candidates.gz");
    const candidateCache = { path: cachePath, identity: "region-content-v1" };
    const normalize = meta => {
      const { seconds, osm, candidateCache: _mode, ...rest } = meta;
      return rest;
    };

    // Cycle 1: OSM already has #4 and #5 → cache miss, cache written.
    await writeFile(join(root, "osm1.jsonl"), `${osmDoc("node/1", "4")}\n${osmDoc("node/2", "5")}\n`);
    const first = await augmentOsmWithAddressSources({
      root, osmPath: join(root, "osm1.jsonl"), outputPath: join(root, "out1.jsonl"), osmDocs: 2, sources: sources(), candidateCache
    });
    assert.equal(first.meta.candidateCache, "miss");
    assert.equal(first.meta.osmDuplicates, 2);

    // Cycle 2: OSM drops #5, gains #7/#8 and a cityless #9 that matches nothing.
    await writeFile(join(root, "osm2.jsonl.gz"), gzipSync(`${osmDoc("node/1", "4")}\n${osmDoc("node/7", "7")}\n${osmDoc("node/8", "8")}\n${osmDoc("node/9", "9", "")}\n`));
    const full = await augmentOsmWithAddressSources({
      root, osmPath: join(root, "osm2.jsonl.gz"), outputPath: join(root, "full.jsonl"), osmDocs: 4, sources: sources()
    });
    const readsBeforeReplay = reads;
    const replay = await augmentOsmWithAddressSources({
      root, osmPath: join(root, "osm2.jsonl.gz"), outputPath: join(root, "replay.jsonl"), osmDocs: 4, sources: sources(), candidateCache
    });
    assert.equal(reads, readsBeforeReplay, "a cache hit must not read the address sources");
    assert.equal(replay.meta.candidateCache, "hit");
    assert.equal(await readFile(join(root, "replay.jsonl"), "utf8"), await readFile(join(root, "full.jsonl"), "utf8"));
    assert.deepEqual(normalize(replay.meta), normalize(full.meta));
    assert.equal(replay.meta.osmDuplicates, 3);

    // A different content identity is a miss, not a stale replay.
    const changed = await augmentOsmWithAddressSources({
      root, osmPath: join(root, "osm2.jsonl.gz"), outputPath: join(root, "changed.jsonl"), osmDocs: 4, sources: sources(),
      candidateCache: { path: cachePath, identity: "region-content-v2" }
    });
    assert.equal(changed.meta.candidateCache, "miss");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
