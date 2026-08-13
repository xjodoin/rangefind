import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  addressAuthorityQueryKeys,
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
  enrichDocLocality,
  geometryForWay,
  interpolationRangeDocs,
  osmProminence,
  placeDetails,
  placeDoc,
  wayDocTagEntries
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

test("French-Canadian abbreviated queries probe the published address keys", () => {
  // "Bd"/"Boul" are deterministic spellings of boulevard, shared with the
  // builder so both sides land on the canonical token.
  assert.equal(normalizeAddressKey("311 Bd Cartier Ouest"), normalizeAddressKey("311 Boulevard Cartier Ouest"));
  assert.equal(normalizeAddressKey("311 Boul Cartier Ouest"), normalizeAddressKey("311 Boulevard Cartier Ouest"));

  const keys = addressAuthorityQueryKeys("311 A Bd Cartier O, Laval, QC H7N 2J3");
  // Canonical reading first — single-key callers keep their behavior.
  assert.equal(keys[0], normalizeAddressAuthorityKey("311 A Bd Cartier O, Laval, QC H7N 2J3"));
  // The indexed full-formatted key (unit letter dropped, O expanded to
  // Ouest, postal code shed) and the base+city key must both be probed.
  assert.ok(keys.includes(normalizeAddressAuthorityKey("311 Boulevard Cartier Ouest, Laval, QC")));
  assert.ok(keys.includes(normalizeAddressAuthorityKey("311 Boulevard Cartier Ouest, Laval")));

  // Attached unit letters split off; detached ones also merge in.
  assert.ok(addressAuthorityQueryKeys("311A Bd Cartier O, Laval")
    .includes(normalizeAddressAuthorityKey("311 Boulevard Cartier Ouest, Laval")));
  assert.ok(addressAuthorityQueryKeys("311 A Bd Cartier O, Laval")
    .includes(normalizeAddressAuthorityKey("311A Boulevard Cartier Ouest, Laval")));

  // US-shaped tails shed the ZIP and the state abbreviation the same way.
  assert.ok(addressAuthorityQueryKeys("350 Fifth Avenue, New York, NY 10118")
    .includes(normalizeAddressAuthorityKey("350 5th Avenue, New York")));

  // Street suffixes are never shed as if they were region abbreviations.
  assert.ok(!addressAuthorityQueryKeys("100 Main St")
    .includes(normalizeAddressAuthorityKey("100 Main")));

  // Interpolation candidates carry the same readings: an abbreviated French
  // query must produce the tail the builder derived from the full spelling.
  const values = addressRangeLookupValues(202, 218, 2, ["Boulevard Cartier Ouest Laval"]);
  assert.ok(addressRangeQueryCandidates("214 Bd Cartier O, Laval")
    .some(candidate => values.includes(candidate.lookupValue) && candidate.houseNumber === 214));

  // Street type omitted and locality abbreviated at once: the interpolation
  // lane still reaches the tail the builder derived from "Rue Libersan,
  // Sainte-Thérèse".
  const libersan = addressRangeLookupValues(202, 218, 2, ["Rue Libersan Sainte-Thérèse"]);
  assert.ok(addressRangeQueryCandidates("214 libersan ste-thérèse")
    .some(candidate => libersan.includes(candidate.lookupValue) && candidate.houseNumber === 214));
});

test("abbreviated queries reach spelled-out keys across the libpostal en/fr matrix", () => {
  // Adapted from libpostal's expansion tests (test_expand.c) to this
  // matcher's contract: some probed key must equal the key the builder
  // derives from the spelled-out indexed form.
  const cases = [
    // en street types, directionals, ordinals, units
    ["123 Main St #2F, Brooklyn", "123 Main Street, Brooklyn"],
    ["120 E 96th St, New York", "120 East 96th Street, New York"],
    ["100 S St NW, Washington", "100 S Street Northwest, Washington"],
    ["1600 Pennsylvania Ave NW, Washington, DC 20500", "1600 Pennsylvania Avenue Northwest, Washington"],
    ["501 Apt 3 Elm Trl, Austin, TX 78701", "501 Elm Trail, Austin, TX"],
    ["501 Elm Trl Apt 3, Austin, TX 78701", "501 Elm Trail, Unit 3, Austin"],
    // state names spell out and contract
    ["4998 Vanderbilt Dr, Columbus, OH 43213", "4998 Vanderbilt Drive, Columbus, OH"],
    ["4998 Vanderbilt Dr, Columbus, Ohio 43213", "4998 Vanderbilt Drive, Columbus, OH"],
    // Saint/Street both ways
    ["1500 St Clair Ave W, Toronto", "1500 Saint Clair Avenue West, Toronto"],
    ["1500 Saint Clair Ave W, Toronto", "1500 St Clair Avenue West, Toronto"],
    // fr street types, saints, directionals
    ["92 Ave des Champs-Élysées, Paris", "92 Avenue des Champs-Élysées, Paris"],
    ["15 Ch de la Côte-Sainte-Catherine, Montréal", "15 Chemin de la Côte-Sainte-Catherine, Montréal"],
    ["1 Rue Ste-Catherine E, Montréal", "1 Rue Sainte-Catherine Est, Montréal"],
    ["300 Rte 117, Mont-Tremblant", "300 Route 117, Mont-Tremblant"],
    ["22 Hwy 7 E, Markham, ON", "22 Highway 7 East, Markham"],
    // de/nl/sv concatenated street suffixes: spaced, abbreviated-spaced, and
    // abbreviated-concatenated forms all reach the concatenated indexed key
    ["Markt Strasse 5, Berlin", "Marktstraße 5, Berlin"],
    ["Markt Str 5, Berlin", "Marktstraße 5, Berlin"],
    ["Marktstr 5, Berlin", "Marktstraße 5, Berlin"],
    ["Kerkstr 12, Amsterdam", "Kerkstraat 12, Amsterdam"],
    ["Kerk Straat 12, Amsterdam", "Kerkstraat 12, Amsterdam"],
    ["Stor Gatan 5, Stockholm", "Storgatan 5, Stockholm"],
    // es/ca/it/pl street types
    ["Av del Libertador 100, Madrid", "Avenida del Libertador 100, Madrid"],
    ["Avda Diagonal 200, Barcelona", "Avinguda Diagonal 200, Barcelona"],
    ["Vle Monza 100, Milano", "Viale Monza 100, Milano"],
    ["Ul Marszałkowska 1, Warszawa", "Ulica Marszałkowska 1, Warszawa"],
    // street type omitted entirely: insertion probes the common types
    ["214 libersan ste-thérèse", "214 Rue Libersan, Sainte-Thérèse"],
    ["311 cartier ouest laval", "311 Boulevard Cartier Ouest, Laval"],
    ["100 main brooklyn", "100 Main Street, Brooklyn"]
  ];
  for (const [query, indexed] of cases) {
    assert.ok(
      addressAuthorityQueryKeys(query).includes(normalizeAddressAuthorityKey(indexed)),
      `${query} should reach the key of "${indexed}"`
    );
  }

  // Probe budget: queries without ambiguous tokens must stay at one key,
  // spelled-out addresses stay at a handful ("ave"/"blvd" carry multilingual
  // readings), and even the most abbreviation-dense envelope form stays
  // within the variant cap.
  assert.equal(addressAuthorityQueryKeys("300 Rte 117, Mont-Tremblant").length, 1);
  assert.ok(addressAuthorityQueryKeys("10 Boulevard des Châteaux J7B1Z5").length <= 4);
  assert.ok(addressAuthorityQueryKeys("350 Fifth Avenue New York").length <= 4);
  assert.ok(addressAuthorityQueryKeys("311 A Bd Cartier O, Laval, QC H7N 2J3").length <= 40);

  // A lettered token is only shed as a unit when a pure house number
  // remains — "311a" alone must keep its own key.
  assert.equal(addressAuthorityQueryKeys("311a Boulevard Cartier")[0], normalizeAddressAuthorityKey("311a Boulevard Cartier"));
});

test("OSM area geometry is compact and supplies a real polygon centroid", () => {
  const shape = geometryForWay([
    { lat: 45.5, lon: -73.6 },
    { lat: 45.5, lon: -73.58 },
    { lat: 45.52, lon: -73.58 },
    { lat: 45.52, lon: -73.6 },
    { lat: 45.5, lon: -73.6 }
  ]);
  assert.equal(shape.geometry.type, "Polygon");
  assert.equal(shape.geometry.encoding, "polyline");
  assert.ok(shape.geometry.encoded.length < 80);
  assert.ok(Math.abs(shape.center.lat - 45.51) < 1e-6);
  assert.ok(Math.abs(shape.center.lon + 73.59) < 1e-6);
});

test("invalid optional OSM way geometry is dropped without aborting extraction", () => {
  assert.equal(geometryForWay([
    { lat: -89.9, lon: 10 },
    { lat: -90.000001, lon: 11 },
    { lat: -89.9, lon: 12 },
    { lat: -89.9, lon: 10 }
  ]), null);
});

test("named OSM roads publish locality-qualified street authority", () => {
  const tags = new Map([["name", "Rue Saint-Denis"], ["highway", "primary"]]);
  const doc = placeDoc("way", 101, 45.52, -73.57, tags);
  assert.equal(doc.category, "highway");
  assert.equal(doc.type, "primary");
  assert.equal(doc.street, "Rue Saint-Denis");
  assert.equal(enrichDocLocality(doc, "Montréal"), true);
  assert.equal(doc.street_authority, "Rue Saint-Denis, Montréal");
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

test("OSM place documents retain compact useful details and a prominence prior", () => {
  const tags = new Map([
    ["name", "Accessible Café"],
    ["amenity", "cafe"],
    ["brand", "Example Coffee"],
    ["cuisine", "coffee_shop"],
    ["opening_hours", "Mo-Su 07:00-21:00"],
    ["contact:phone", "+1-555-0100"],
    ["contact:website", "https://example.test"],
    ["wheelchair", "yes"],
    ["outdoor_seating", "yes"],
    ["payment:contactless", "yes"],
    ["wikidata", "Q123"]
  ]);
  assert.deepEqual(placeDetails(tags), {
    brand: "Example Coffee",
    cuisine: "coffee_shop",
    opening_hours: "Mo-Su 07:00-21:00",
    phone: "+1-555-0100",
    website: "https://example.test",
    wheelchair: "yes",
    outdoor_seating: "yes",
    payment_contactless: "yes",
    wikidata: "Q123"
  });
  const doc = placeDoc("node", 100, 45.5, -73.5, tags);
  assert.equal(doc.details.opening_hours, "Mo-Su 07:00-21:00");
  assert.equal(doc.details.wheelchair, "yes");
  assert.match(doc.body, /Example Coffee/u);
  assert.ok(doc.prominence > 0 && doc.prominence <= 1);

  const city = new Map([["name", "Montréal"], ["place", "city"], ["population", "1704694"], ["capital", "yes"]]);
  assert.ok(osmProminence(city) > doc.prominence, "a populous capital should outrank an ordinary POI");
});

test("OSM alternate names are split, prioritized, folded-deduplicated, and metadata-safe", () => {
  const tags = new Map([
    ["name", "Montréal"],
    ["name:pronunciation", "mɔ̃.ʁe.al"],
    ["name:etymology", "Mount Royal"],
    ["name:language", "fr"],
    ["old_name", "Ville-Marie"],
    ["operator", "Ville de Montréal"],
    ["name:fr", "Montréal"],
    ["name:en", "Montreal"],
    ["official_name:en", "City of Montreal"],
    ["nickname", "La Métropole"],
    ["alt_name", "Hochelaga; Montréal, QC ; Hochelaga"],
    ["short_name", "MTL"],
    ["place", "city"],
    ["population", "1704694"]
  ]);
  const doc = placeDoc("node", 624, 45.5019, -73.5674, tags);
  assert.deepEqual(doc.aliases, [
    "MTL",
    "Hochelaga",
    "Montréal, QC",
    "La Métropole",
    "City of Montreal",
    "Ville-Marie",
    "Ville de Montréal"
  ]);
  assert.ok(!doc.aliases.includes("Montreal"), "fold-equivalent names should not waste alias slots");
  assert.ok(!doc.aliases.includes("mɔ̃.ʁe.al"));
  assert.ok(!doc.aliases.includes("Mount Royal"));
  assert.ok(!doc.aliases.includes("fr"));
});

test("OSM brand-only and localized-only features retain a useful searchable identity", () => {
  const branded = placeDoc("node", 625, 45.5, -73.5, new Map([
    ["amenity", "fast_food"],
    ["brand", "Tim Hortons"],
    ["operator", "The TDL Group Corp."]
  ]));
  assert.equal(branded.name, "Tim Hortons");
  assert.deepEqual(branded.aliases, ["The TDL Group Corp."]);

  const localizedWayTags = new Map([
    ["highway", "residential"],
    ["name:fr", "Rue des Érables"],
    ["name:en", "Maple Street"],
    ["alt_name:fr", "Chemin des Érables"],
    ["name:pronunciation", "ʁy de ze.ʁabl"]
  ]);
  const retained = new Map(wayDocTagEntries(localizedWayTags));
  assert.equal(retained.get("alt_name:fr"), "Chemin des Érables");
  assert.equal(retained.has("name:pronunciation"), false);
  const localized = placeDoc("way", 626, 45.5, -73.5, retained);
  assert.equal(localized.name, "Maple Street");
  assert.equal(localized.category, "highway");
  assert.deepEqual(localized.aliases, ["Chemin des Érables", "Rue des Érables"]);
});

test("OSM alias budgets retain high-value names independently of tag order", () => {
  const tags = new Map([
    ["name", "Canonical Place"],
    ["place", "city"],
    ...Array.from({ length: 30 }, (_, index) => [
      `name:aa-${String(index).padStart(2, "0")}`,
      `Localized Place ${index}`
    ]),
    ["alt_name", "Common Place"],
    ["short_name", "CP"]
  ]);
  const doc = placeDoc("node", 627, 45.5, -73.5, tags);
  assert.equal(doc.aliases.length, 24);
  assert.deepEqual(doc.aliases.slice(0, 2), ["CP", "Common Place"]);
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
    {
      id: "cartier-ouest",
      name: "311 Boulevard Cartier Ouest, Laval, QC",
      address: "311 Boulevard Cartier Ouest, Laval, QC",
      address_search: "311 Boulevard Cartier Ouest, Laval, QC",
      house_number: "311",
      street: "Boulevard Cartier Ouest",
      city: "Laval",
      state: "QC",
      postcode: "H7N 2J3",
      category: "address",
      type: "address"
    },
    {
      id: "vanderbilt",
      name: "4998 Vanderbilt Drive, Columbus, OH",
      address: "4998 Vanderbilt Drive, Columbus, OH",
      address_search: "4998 Vanderbilt Drive, Columbus, OH",
      house_number: "4998",
      street: "Vanderbilt Drive",
      city: "Columbus",
      state: "OH",
      postcode: "43213",
      category: "address",
      type: "address"
    },
    {
      id: "st-clair",
      name: "1500 St Clair Avenue West, Toronto, ON",
      address: "1500 St Clair Avenue West, Toronto, ON",
      address_search: "1500 St Clair Avenue West, Toronto, ON",
      house_number: "1500",
      street: "St Clair Avenue West",
      city: "Toronto",
      state: "ON",
      category: "address",
      type: "address"
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

    // A pasted French-Canadian envelope form: detached unit letter,
    // abbreviated street type and directional, trailing province + postal
    // code. Every reading resolves in the authority lane, not postings.
    const frenchAbbreviated = await engine.search({ q: "311 A Bd Cartier O, Laval, QC H7N 2J3", size: 5 });
    assert.equal(frenchAbbreviated.stats.plannerLane, "addressAuthorityExact");
    assert.equal(frenchAbbreviated.stats.postingsDecoded, 0);
    assert.equal(frenchAbbreviated.results[0].id, "cartier-ouest");

    // Abbreviated English envelope forms: street type, state spelled out
    // against an abbreviated tag, and ZIP shed the same way.
    const englishAbbreviated = await engine.search({ q: "4998 Vanderbilt Dr, Columbus, Ohio 43213", size: 5 });
    assert.equal(englishAbbreviated.stats.plannerLane, "addressAuthorityExact");
    assert.equal(englishAbbreviated.stats.postingsDecoded, 0);
    assert.equal(englishAbbreviated.results[0].id, "vanderbilt");

    // "Saint" spelled out against an indexed "St" name (and the street-type
    // "St" in the same query staying a street).
    const saintSpelled = await engine.search({ q: "1500 Saint Clair Ave W, Toronto", size: 5 });
    assert.equal(saintSpelled.stats.plannerLane, "addressAuthorityExact");
    assert.equal(saintSpelled.results[0].id, "st-clair");

    // Street type omitted entirely: insertion probes recover the flat
    // address and, with the locality abbreviated too, the interpolated one.
    const typeOmitted = await engine.search({ q: "311 cartier ouest laval", size: 5 });
    assert.equal(typeOmitted.stats.plannerLane, "addressAuthorityExact");
    assert.equal(typeOmitted.results[0].id, "cartier-ouest");

    const bareInterpolated = await engine.search({ q: "214 libersan ste-thérèse", size: 2 });
    assert.equal(bareInterpolated.stats.plannerLane, "addressInterpolationExact");
    assert.equal(bareInterpolated.total, 1);
    assert.equal(bareInterpolated.results[0].house_number, "214");
  } finally {
    await engine.close();
  }

  // Production shards become generational after an incremental build. The
  // outer merge must let the owning generation hydrate the compact range and
  // synthesize the requested house number before it applies tombstones.
  await writeFile(join(root, "docs.jsonl"), JSON.stringify({
    id: "delta-cafe",
    name: "Delta Cafe",
    address: "1 Rue Nouvelle, Sainte-Thérèse",
    address_search: "1 Rue Nouvelle, Sainte-Thérèse",
    house_number: "1",
    street: "Rue Nouvelle",
    city: "Sainte-Thérèse",
    category: "amenity",
    type: "cafe"
  }));
  await build({ configPath, update: true });

  const generational = await createNodeSearch({ source: join(root, "public", "rangefind") });
  try {
    assert.equal(generational.manifest.generations.length, 2);
    const interpolated = await generational.search({ q: "214 Rue Libersan", size: 2 });
    assert.equal(interpolated.stats.plannerLane, "addressInterpolationExact");
    assert.equal(interpolated.stats.generationalAddressAuthority, true);
    assert.equal(interpolated.stats.blocksDecoded, 0);
    assert.equal(interpolated.stats.postingsDecoded, 0);
    assert.equal(interpolated.total, 1);
    assert.equal(interpolated.results[0].address, "214 Rue Libersan, Sainte-Thérèse");
    assert.equal(interpolated.results[0].house_number, "214");
    assert.equal(interpolated.results[0].interpolated, true);
    assert.equal(interpolated.results[0].generation, 0);
  } finally {
    await generational.close();
  }

  // Replacing the compact range in a later generation must suppress the base
  // range before synthesis; otherwise the same house number would be emitted
  // twice from stale and current geometry.
  const replacement = {
    ...interpolation[0],
    lat: 45.6572,
    lon: -73.8412,
    geo_lat: 45.6572,
    geo_lon: -73.8412,
    _address_range_geometry: encodeAddressRangeGeometry([
      { lat: 45.656577, lon: -73.840714 },
      { lat: 45.6572, lon: -73.8412 },
      { lat: 45.657881, lon: -73.841335 }
    ])
  };
  await writeFile(join(root, "docs.jsonl"), JSON.stringify(replacement));
  await build({ configPath, update: true });

  const replaced = await createNodeSearch({ source: join(root, "public", "rangefind") });
  try {
    const interpolated = await replaced.search({ q: "214 Rue Libersan", size: 2 });
    assert.equal(interpolated.total, 1);
    assert.equal(interpolated.results.length, 1);
    assert.equal(interpolated.results[0].generation, 2);
    assert.ok(interpolated.results[0].lat > 45.6572);
  } finally {
    await replaced.close();
  }
});
