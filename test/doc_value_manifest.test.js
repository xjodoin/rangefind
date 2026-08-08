import assert from "node:assert/strict";
import { gunzipSync } from "node:zlib";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";
import { parseDocValueManifest } from "../src/doc_value_manifest.js";
import { inspectArtifact } from "../src/inspect.js";
import { serveStatic } from "../scripts/bench_support.mjs";

// Small chunk sizes force many chunks per field, and a lookup chunk size
// below the main size forces lookup_chunks — both paths of the binary codec.
async function buildFixture() {
  const root = await mkdtemp(join(tmpdir(), "rangefind-dvm-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  const docs = [];
  for (let i = 0; i < 40; i++) {
    docs.push(JSON.stringify({
      id: String(i),
      title: `Place ${i} ${i % 2 ? "bakery" : "museum"}`,
      body: `common tokens plus ${i % 2 ? "croissant" : "exhibit"}`,
      category: i % 2 ? "bakery" : "museum",
      open: i % 3 === 0,
      rating: (i % 10) / 2,
      lat: 45 + i * 0.01,
      lon: -73 - i * 0.01,
      url: `/${i}`
    }));
  }
  await writeFile(docsPath, docs.join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    queryBundles: false,
    typoMode: "off",
    docValueChunkSize: 8,
    docValueLookupChunkSize: 4,
    fields: [
      { name: "title", path: "title", weight: 2.0 },
      { name: "body", path: "body", weight: 1.0 }
    ],
    facets: [{ name: "category", path: "category" }],
    numbers: [{ name: "rating", path: "rating", type: "float" }],
    booleans: [{ name: "open", path: "open" }],
    geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
    display: ["title", "url", "category", "rating"]
  }));
  await build({ configPath });
  return { root, output };
}

function chunkFacts(chunk) {
  return {
    start: chunk.start,
    count: chunk.count,
    pack: chunk.pack,
    offset: chunk.offset,
    length: chunk.length,
    width: chunk.width,
    min: chunk.min ?? null,
    max: chunk.max ?? null,
    words: Array.isArray(chunk.words) ? chunk.words.map(word => word >>> 0) : null
  };
}

test("binary doc-value manifest round-trips every consumer-visible chunk fact", async () => {
  const { output } = await buildFixture();
  const minManifest = JSON.parse(await readFile(join(output, "manifest.min.json"), "utf8"));
  const binaryPath = minManifest.lazy_manifests?.doc_values_v2;
  assert.ok(binaryPath, "min manifest advertises lazy_manifests.doc_values_v2");
  assert.match(binaryPath, /^doc-values\/manifest\.[0-9a-f]{24}\.bin\.gz$/u);

  const v1 = JSON.parse(gunzipSync(await readFile(join(output, "doc-values", "manifest.json.gz"))).toString());
  const v2 = parseDocValueManifest(new Uint8Array(gunzipSync(await readFile(join(output, binaryPath)))));

  assert.equal(v2.chunk_size, v1.chunk_size);
  assert.equal(v2.lookup_chunk_size, v1.lookup_chunk_size);
  assert.deepEqual(Object.keys(v2.fields).sort(), Object.keys(v1.fields).sort());
  for (const [name, field] of Object.entries(v1.fields)) {
    const round = v2.fields[name];
    assert.equal(round.kind, field.kind, name);
    assert.equal(round.type, field.type, name);
    assert.equal(round.words | 0, field.words | 0, name);
    assert.deepEqual(round.chunks.map(chunkFacts), field.chunks.map(chunkFacts), `${name} chunks`);
    assert.equal(Boolean(round.lookup_chunks), Boolean(field.lookup_chunks), `${name} lookup presence`);
    if (field.lookup_chunks) {
      assert.deepEqual(round.lookup_chunks.map(chunkFacts), field.lookup_chunks.map(chunkFacts), `${name} lookup chunks`);
    }
    assert.ok(field.lookup_chunks, `${name} fixture exercises lookup chunks`);
  }
  // Checksum rows are sequential in emission order and advertised for the
  // runtime's lazy verification reads.
  assert.ok(v2.checksum_rows?.file.startsWith("doc-values/checksums."));
  assert.equal(v2.checksum_rows.algorithm, "sha256");
  const rows = Object.values(v2.fields).flatMap(field => [
    ...field.chunks.map(chunk => chunk.checksumRow),
    ...(field.lookup_chunks || []).map(chunk => chunk.checksumRow)
  ]);
  assert.deepEqual(rows, rows.map((_, i) => i));

  // The inspector decodes the artifact standalone.
  const report = inspectArtifact(new Uint8Array(gunzipSync(await readFile(join(output, binaryPath)))));
  assert.equal(report.artifact, "doc-value manifest (rfdvm-v1)");
  assert.equal(report.total, 40);
  assert.ok(report.fields.some(field => field.name === "rating" && field.lookupChunks > field.chunks / 2));
});

test("binary and JSON doc-value manifests serve identical queries, checksums verify from the sidecar", async (t) => {
  const { root, output } = await buildFixture();
  const server = await serveStatic(join(root, "public"));
  t.after(() => server.close());

  const requested = [];
  const nativeFetch = globalThis.fetch;
  globalThis.fetch = async (input, init) => {
    requested.push(new URL(String(input?.url || input)).pathname);
    return nativeFetch(input, init);
  };
  t.after(() => {
    globalThis.fetch = nativeFetch;
  });

  const baseUrl = new URL("rangefind/", server.url);
  const params = {
    q: "bakery",
    filters: { numbers: { rating: { min: 1 } }, booleans: { open: true } },
    geo: { near: { lat: 45.05, lon: -73.05 }, sort: "distance" },
    size: 10
  };
  const binaryEngine = await createSearch({ baseUrl });
  const binaryResponse = await binaryEngine.search(params);
  const binaryRequests = requested.splice(0);

  const legacyEngine = await createSearch({ baseUrl, docValuesBinaryManifest: false });
  const legacyResponse = await legacyEngine.search(params);
  const legacyRequests = requested.splice(0);

  assert.ok(binaryResponse.results.length > 0, "fixture query matches documents");
  assert.deepEqual(
    binaryResponse.results.map(result => [result.title, result.rating, result.distanceMeters]),
    legacyResponse.results.map(result => [result.title, result.rating, result.distanceMeters])
  );
  assert.equal(binaryResponse.total, legacyResponse.total);
  assert.ok(binaryRequests.some(path => /doc-values\/manifest\.[0-9a-f]+\.bin\.gz$/u.test(path)));
  assert.ok(!binaryRequests.some(path => path.endsWith("doc-values/manifest.json.gz")));
  assert.ok(legacyRequests.some(path => path.endsWith("doc-values/manifest.json.gz")));

  // This index declares checksummedObjects, so the engine verified each
  // fetched chunk against rows range-read from the sidecar.
  const manifest = JSON.parse(await readFile(join(output, "manifest.min.json"), "utf8"));
  assert.ok(manifest.features.checksummedObjects, "fixture verifies checksums by default");
  assert.ok(binaryRequests.some(path => /doc-values\/checksums\.[0-9a-f]+\.bin$/u.test(path)));

  // Corrupting the sidecar must fail verification loudly, not serve data.
  const checksumsPath = parseDocValueManifest(
    new Uint8Array(gunzipSync(await readFile(join(output, manifest.lazy_manifests.doc_values_v2))))
  ).checksum_rows.file;
  const sidecar = Buffer.from(await readFile(join(output, checksumsPath)));
  for (let row = 0; row < sidecar.length; row += 32) sidecar[row] ^= 0xff;
  await writeFile(join(output, checksumsPath), sidecar);
  const tamperedEngine = await createSearch({ baseUrl });
  await assert.rejects(tamperedEngine.search(params), /checksum mismatch/u);
});
