import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  addressRangeContains,
  addressRangeLookupValues,
  addressRangeQueryCandidates,
  decodeAddressRangeGeometry,
  encodeAddressRangeGeometry,
  interpolateAddressRangePoint,
  looksLikeAddressQuery,
  normalizeAddressAuthorityKey,
  normalizeAddressKey,
  normalizePostalCodePrefixSpacing,
  normalizePostalCodeSpacing
} from "../src/address.js";
import { build } from "../src/builder.js";
import { authorityAddressRangeKey } from "../src/authority_codec.js";
import { createNodeSearch } from "../src/runtime.node.js";
import {
  addressFromTags,
  interpolationRangeDocs,
  placeDoc
} from "../src/integrations/osm/documents.js";

test("address keys normalize directions, suffixes, punctuation, and ordinal words", () => {
  assert.equal(
    normalizeAddressKey("1600 Pennsylvania Avenue Northwest, Washington"),
    "1600 pennsylvania ave nw washington"
  );
  assert.equal(
    normalizeAddressKey("1600 Pennsylvania Ave. NW Washington"),
    "1600 pennsylvania ave nw washington"
  );
  assert.equal(normalizeAddressKey("350 Fifth Avenue, New York"), "350 5th ave new york");
  assert.equal(normalizeAddressKey("21 Twenty-First Street"), "21 21st st");
  assert.equal(
    normalizeAddressAuthorityKey("Washington 1600 Pennsylvania Ave NW"),
    normalizeAddressAuthorityKey("1600 Pennsylvania Avenue Northwest Washington")
  );
  assert.equal(looksLikeAddressQuery("350 Fifth Avenue New York"), true);
  assert.equal(looksLikeAddressQuery("Pennsylvania Avenue"), false);
  assert.equal(looksLikeAddressQuery("J7A 1V6"), true);
  assert.equal(normalizePostalCodeSpacing("H4R1P8"), "H4R 1P8");
  assert.equal(normalizePostalCodeSpacing("h4r 1p8"), "h4r 1p8");
  assert.equal(normalizePostalCodePrefixSpacing("J7A1V"), "J7A 1V");
  assert.equal(normalizePostalCodePrefixSpacing("pharmacy J7A1"), "pharmacy J7A 1");
});

test("OSM address extraction retains structured and address-only features", () => {
  const tags = new Map([
    ["addr:housenumber", "350"],
    ["addr:street", "5th Avenue"],
    ["addr:city", "New York"],
    ["addr:state", "NY"],
    ["addr:postcode", "10118"]
  ]);
  const address = addressFromTags(tags);
  assert.equal(address.formatted, "350 5th Avenue, New York, NY, 10118");
  assert.equal(address.complete, true);

  const doc = placeDoc("node", 42, 40.7484, -73.9857, tags);
  assert.equal(doc.name, address.formatted);
  assert.equal(doc.address, address.formatted);
  assert.equal(doc.house_number, "350");
  assert.equal(doc.street, "5th Avenue");
  assert.equal(doc.city, "New York");
  assert.equal(doc.postcode, "10118");
  assert.equal(doc.category, "address");
  assert.equal(doc.address_authority, undefined);

  const canadian = placeDoc("node", 43, 45.64, -73.8, new Map([
    ["addr:housenumber", "12"],
    ["addr:street", "Rue Exemple"],
    ["addr:city", "Rosemère"],
    ["addr:postcode", "J7A1V6"]
  ]));
  assert.equal(canadian.postcode, "J7A 1V6");
  assert.match(canadian.address, /J7A 1V6/u);

  const incomplete = placeDoc("node", 44, 40.7, -73.9, new Map([["addr:street", "5th Avenue"]]));
  assert.equal(incomplete, null);
});

test("named OSM places expose their address without replacing their identity", () => {
  const doc = placeDoc("node", 99, 40.7484, -73.9857, new Map([
    ["name", "Chipotle"],
    ["amenity", "fast_food"],
    ["cuisine", "mexican"],
    ["addr:housenumber", "350"],
    ["addr:street", "5th Avenue"],
    ["addr:city", "New York"]
  ]));
  assert.equal(doc.name, "Chipotle");
  assert.equal(doc.address, "350 5th Avenue, New York");
  assert.equal(doc.body, "fast food amenity mexican");
  assert.equal(doc.category, "amenity");
});

test("address interpolation uses compact buckets and follows polyline distance", () => {
  const values = addressRangeLookupValues(202, 218, 2, ["Rue Libersan Sainte-Thérèse"]);
  assert.ok(values.length <= 2);
  assert.ok(addressRangeQueryCandidates("214 Rue Libersan, Sainte-Thérèse")
    .some(candidate => values.includes(candidate.lookupValue) && candidate.houseNumber === 214));
  assert.ok(values.every(value => authorityAddressRangeKey(value).startsWith("i|")));
  assert.equal(addressRangeContains(202, 218, 2, 214), true);
  assert.equal(addressRangeContains(202, 218, 2, 215), false);

  const geometry = encodeAddressRangeGeometry([
    { lat: 45.646577, lon: -73.830714 },
    { lat: 45.6472, lon: -73.8312 },
    { lat: 45.647881, lon: -73.831335 }
  ]);
  assert.equal(decodeAddressRangeGeometry(geometry).length, 3);
  const point = interpolateAddressRangePoint(geometry, 202, 218, 214);
  assert.ok(point.lat > 45.6472 && point.lat < 45.647881);
});

test("OSM interpolation ways become one compact range document", () => {
  const endpointTags = number => new Map([
    ["addr:housenumber", String(number)],
    ["addr:street", "Rue Libersan"],
    ["addr:city", "Sainte-Thérèse"]
  ]);
  const refs = [1, 2, 3];
  const docs = interpolationRangeDocs(174596216, refs, [
    { lat: 45.646577, lon: -73.830714, tags: endpointTags(202) },
    { lat: 45.6472, lon: -73.8312, tags: new Map() },
    { lat: 45.647881, lon: -73.831335, tags: endpointTags(218) }
  ], new Map([["addr:interpolation", "even"]]));
  assert.equal(docs.length, 1);
  assert.equal(docs[0]._address_range_start, 202);
  assert.equal(docs[0]._address_range_end, 218);
  assert.equal(docs[0]._address_range_step, 2);
  assert.equal(docs[0].interpolation_keys.length <= 4, true);
  assert.equal(docs[0].address, undefined);
});

test("exact, reordered, and component address forms bypass postings", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-address-"));
  const interpolation = interpolationRangeDocs(174596216, [1, 2, 3], [
    {
      lat: 45.646577,
      lon: -73.830714,
      tags: new Map([
        ["addr:housenumber", "202"],
        ["addr:street", "Rue Libersan"],
        ["addr:city", "Sainte-Thérèse"]
      ])
    },
    { lat: 45.6472, lon: -73.8312, tags: new Map() },
    {
      lat: 45.647881,
      lon: -73.831335,
      tags: new Map([
        ["addr:housenumber", "218"],
        ["addr:street", "Rue Libersan"],
        ["addr:city", "Sainte-Thérèse"]
      ])
    }
  ], new Map([["addr:interpolation", "even"]]));
  const docs = [
    {
      id: "chipotle",
      name: "Chipotle",
      address: "350 5th Avenue, New York",
      address_search: "350 5th Avenue, New York",
      house_number: "350",
      street: "5th Avenue",
      city: "New York",
      category: "amenity",
      type: "restaurant"
    },
    {
      id: "starbucks",
      name: "Starbucks",
      address: "350 5th Avenue, New York",
      address_search: "350 5th Avenue, New York",
      house_number: "350",
      street: "5th Avenue",
      city: "New York",
      category: "amenity",
      type: "cafe"
    },
    {
      id: "other",
      name: "Other Cafe",
      address: "350 5th Avenue, Seattle",
      address_search: "350 5th Avenue, Seattle",
      house_number: "350",
      street: "5th Avenue",
      city: "Seattle",
      category: "amenity",
      type: "cafe"
    },
    {
      id: "jean-coutu",
      name: "Jean Coutu",
      address: "10 Boulevard des Châteaux, J7B 1Z5",
      address_search: "10 Boulevard des Châteaux, J7B 1Z5",
      house_number: "10",
      street: "Boulevard des Châteaux",
      postcode: "J7B 1Z5",
      category: "amenity",
      type: "pharmacy"
    },
    {
      id: "postal-j7a1v6",
      name: "J7A 1V6, Rosemère",
      postal_lookup: "J7A 1V6",
      postcode: "J7A 1V6",
      city: "Rosemère",
      category: "boundary",
      type: "postal_code"
    },
    ...interpolation
  ];
  await writeFile(join(root, "docs.jsonl"), docs.map(JSON.stringify).join("\n"));
  const configPath = join(root, "rangefind.config.json");
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    targetPostingsPerDoc: 20,
    fields: [
      { name: "title", path: "name", weight: 6, b: 0.4 },
      { name: "address", path: "address_search", weight: 10, b: 0.2 }
    ],
    alwaysIndexFields: ["title", "address"],
    authority: [
      {
        name: "address",
        path: "address",
        weight: 4000000,
        surface: false,
        exact: false,
        tokens: false,
        normalizer: "address",
        addressComponents: true
      },
      {
        name: "address_interpolation",
        path: "interpolation_keys",
        weight: 3000000,
        surface: false,
        exact: true,
        tokens: false,
        normalizer: "address-range"
      },
      {
        name: "postcode",
        path: "postal_lookup",
        weight: 5000000,
        surface: false,
        exact: false,
        tokens: false,
        normalizer: "address"
      }
    ],
    suggest: [{ path: "name" }],
    facets: [
      { name: "category", path: "category" },
      { name: "type", path: "type" }
    ],
    display: [
      "id", "name", "address", "house_number", "street", "city", "postcode", "category", "type", "lat", "lon",
      "_address_range_start", "_address_range_end", "_address_range_step",
      "_address_range_geometry", "_address_range_kind", "_address_range_inclusion"
    ]
  }));
  await build({ configPath });

  const engine = await createNodeSearch({ source: join(root, "public", "rangefind") });
  try {
    const exact = await engine.search({ q: "350 Fifth Ave. New York", size: 10 });
    assert.equal(exact.stats.plannerLane, "addressAuthorityExact");
    assert.equal(exact.stats.blocksDecoded, 0);
    assert.equal(exact.stats.postingsDecoded, 0);
    assert.equal(exact.total, 2);
    assert.deepEqual(exact.results.map(result => result.id), ["chipotle", "starbucks"]);
    assert.ok(exact.results.every(result => result.address === "350 5th Avenue, New York"));

    const forcedExact = await engine.search({ q: "350 Fifth Avenue New York", size: 2, exact: true });
    assert.equal(forcedExact.stats.plannerLane, "addressAuthorityExact");

    const reordered = await engine.search({ q: "New York 350 5th Avenue", size: 2 });
    assert.deepEqual(reordered.results.map(result => result.id), ["chipotle", "starbucks"]);
    assert.equal(reordered.stats.plannerLane, "addressAuthorityExact");
    assert.equal(reordered.stats.postingsDecoded, 0);

    const partial = await engine.search({ q: "350 5th Avenue New York", size: 2 });
    assert.deepEqual(partial.results.map(result => result.id), ["chipotle", "starbucks"]);
    assert.equal(partial.stats.plannerLane, "addressAuthorityExact");
    assert.equal(partial.stats.postingsDecoded, 0);

    const named = await engine.search({ q: "Chipotle 350 5th Avenue New York", size: 1 });
    assert.equal(named.results[0].id, "chipotle");

    const filtered = await engine.search({
      q: "350 Fifth Avenue New York",
      filters: { facets: { type: ["cafe"] } },
      size: 2
    });
    assert.deepEqual(filtered.results.map(result => result.id), ["starbucks"]);
    assert.notEqual(filtered.stats.plannerLane, "addressAuthorityExact");

    const interpolated = await engine.search({ q: "214 Rue Libersan, Sainte-Thérèse", size: 2 });
    assert.equal(interpolated.stats.plannerLane, "addressInterpolationExact");
    assert.equal(interpolated.stats.blocksDecoded, 0);
    assert.equal(interpolated.stats.postingsDecoded, 0);
    assert.equal(interpolated.total, 1);
    assert.equal(interpolated.results[0].address, "214 Rue Libersan, Sainte-Thérèse");
    assert.equal(interpolated.results[0].house_number, "214");
    assert.equal(interpolated.results[0].interpolated, true);
    assert.ok(interpolated.results[0].lat > 45.6472);
    assert.equal(interpolated.results[0]._address_range_geometry, undefined);

    const wrongParity = await engine.search({ q: "213 Rue Libersan, Sainte-Thérèse", size: 2 });
    assert.equal(wrongParity.stats.plannerLane, "addressInterpolationExact");
    assert.equal(wrongParity.total, 0);
    assert.equal(wrongParity.approximate, false);
    assert.equal(wrongParity.stats.postingsDecoded, 0);

    const interpolatedSuggest = await engine.suggest({ q: "214 Rue Lib", size: 5 });
    assert.equal(interpolatedSuggest.stats.suggestLane, "address-authority");
    assert.equal(interpolatedSuggest.suggestions[0].text, "214 Rue Libersan, Sainte-Thérèse");
    assert.equal(interpolatedSuggest.suggestions[0].interpolated, true);

    const invalidSuggest = await engine.suggest({ q: "213 Rue Lib", size: 5 });
    assert.equal(invalidSuggest.suggestions.length, 0);

    const compactPostcode = await engine.search({ q: "J7B1Z5", size: 5 });
    assert.equal(compactPostcode.normalizedQuery, "J7B 1Z5");
    assert.equal(compactPostcode.stats.postalCodeNormalized, true);
    assert.equal(compactPostcode.results[0].id, "jean-coutu");
    assert.equal(compactPostcode.results[0].postcode, "J7B 1Z5");

    const compactPostcodeCount = await engine.count({ q: "J7B1Z5" });
    assert.equal(compactPostcodeCount.total, 1);
    assert.equal(compactPostcodeCount.totalExact, true);

    const postalArea = await engine.search({ q: "J7A1V6", size: 5 });
    assert.equal(postalArea.stats.plannerLane, "addressAuthorityExact");
    assert.equal(postalArea.stats.postingsDecoded, 0);
    assert.equal(postalArea.results[0].id, "postal-j7a1v6");

    const compactFullAddress = await engine.search({ q: "10 Boulevard des Châteaux J7B1Z5", size: 5 });
    assert.equal(compactFullAddress.stats.plannerLane, "addressAuthorityExact");
    assert.equal(compactFullAddress.stats.postingsDecoded, 0);
    assert.equal(compactFullAddress.results[0].id, "jean-coutu");
  } finally {
    await engine.close();
  }
});
