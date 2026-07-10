import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { Readable } from "node:stream";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  augmentOsmWithRqa,
  parseCsvRows,
  rqaAddressDoc
} from "../scripts/rqa_fixture.mjs";

const HEADER = [
  "identifiant_unique_adresse",
  "numero_municipal",
  "numero_municipal_suffixe",
  "numero_unite",
  "code_postal",
  "odonyme_recompose_normal",
  "code_municipalite",
  "nom_municipalite",
  "longitude",
  "latitude"
];

function csvRow(values) {
  return values.map(value => `"${String(value).replaceAll('"', '""')}"`).join(",");
}

test("RQA CSV parsing survives chunk boundaries, escaped quotes, and embedded newlines", async () => {
  const input = `\uFEFFa,b,c\r\n"one","two, too","three"\r\n"four","five ""quoted""","six\ncontinued"\r\n`;
  const chunks = [input.slice(0, 13), input.slice(13, 29), input.slice(29, 47), input.slice(47)];
  const rows = [];
  for await (const row of parseCsvRows(Readable.from(chunks))) rows.push(row);
  assert.deepEqual(rows, [
    ["a", "b", "c"],
    ["one", "two, too", "three"],
    ["four", 'five "quoted"', "six\ncontinued"]
  ]);
});

test("RQA documents canonicalize Canadian postcodes without indexing residential browse fields", () => {
  const doc = rqaAddressDoc({
    identifiant_unique_adresse: "abc",
    numero_municipal: "12",
    numero_municipal_suffixe: "A",
    code_postal: "j7a1v6",
    odonyme_recompose_normal: "Rue Exemple",
    nom_municipalite: "Rosemère",
    longitude: "-73.8",
    latitude: "45.64"
  });
  assert.equal(doc.address, "12A Rue Exemple, Rosemère, QC, J7A 1V6");
  assert.equal(doc.postcode, "J7A 1V6");
  assert.equal(doc.search_name, undefined);
  assert.equal(doc.geo_lat, undefined);
  assert.equal(rqaAddressDoc({ ...doc, date_fin: "20260101" }), null);
});

test("RQA augmentation deduplicates OSM and emits one compact postal-area document", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-rqa-"));
  try {
    const osmPath = join(root, "osm.jsonl");
    const csvPath = join(root, "rqa.csv");
    const outputPath = join(root, "combined.jsonl");
    await writeFile(osmPath, [
      {
        id: "node/1",
        name: "10 Rue Exemple, Rosemère",
        address: "10 Rue Exemple, Rosemère, QC, J7A 1V6",
        house_number: "10",
        street: "Rue Exemple",
        city: "Rosemère",
        lat: 45.64,
        lon: -73.8
      },
      {
        id: "node/2",
        name: "14 Rue Exemple",
        address: "14 Rue Exemple",
        house_number: "14",
        street: "Rue Exemple",
        lat: 45.642,
        lon: -73.802
      }
    ].map(JSON.stringify).join("\n") + "\n");
    const rows = [
      HEADER.join(","),
      csvRow(["duplicate", "10", "", "", "J7A1V6", "Rue Exemple", "73020", "Rosemère", "-73.8", "45.64"]),
      csvRow(["new", "12", "", "", "J7A1V6", "Rue Exemple", "73020", "Rosemère", "-73.801", "45.641"]),
      csvRow(["osm-incomplete", "14", "", "", "J7A1V6", "Rue Exemple", "73020", "Rosemère", "-73.802", "45.642"]),
      csvRow(["unit", "12", "", "2", "J7A1V6", "Rue Exemple", "73020", "Rosemère", "-73.801", "45.641"])
    ];
    await writeFile(csvPath, `${rows.join("\n")}\n`);
    const result = await augmentOsmWithRqa({ root, osmPath, csvPath, outputPath, osmDocs: 2 });
    const docs = (await readFile(outputPath, "utf8")).trim().split("\n").map(JSON.parse);
    assert.equal(result.meta.uniqueCivic, 3);
    assert.equal(result.meta.osmDuplicates, 1);
    assert.equal(result.meta.addressesWritten, 2);
    assert.equal(result.meta.postalDocs, 1);
    assert.equal(docs.length, 5);
    assert.equal(docs[2].address, "12 Rue Exemple, Rosemère, QC, J7A 1V6");
    assert.equal(docs[3].address, "14 Rue Exemple, Rosemère, QC, J7A 1V6");
    assert.deepEqual(
      { name: docs[4].name, lookup: docs[4].postal_lookup, count: docs[4].address_count },
      { name: "J7A 1V6, Rosemère", lookup: "J7A 1V6", count: 3 }
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
