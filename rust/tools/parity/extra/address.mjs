import { mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { build } from "../../../../src/builder.js";
import { authorityAddressRangeKey } from "../../../../src/authority_codec.js";
import {
  addressFromTags,
  enrichDocLocality,
  geometryForWay,
  interpolationRangeDocs,
  osmProminence,
  placeDetails,
  placeDoc,
  wayDocTagEntries
} from "../../../../src/integrations/osm/documents.js";


// Address authority fixture (exact keys, interpolation ranges, postal codes,
// address suggest), adapted from test/address.test.js. Writes the index to
// <root>/index.
export async function buildAddressIndex(root) {

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
    output: "index",
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
}

export function addressCases() {
  const queries = [
  ["search", { q: "350 Fifth Ave. New York", size: 10 }],
  ["search", { q: "350 Fifth Avenue New York", size: 2, exact: true }],
  ["search", { q: "New York 350 5th Avenue", size: 2 }],
  ["search", { q: "Chipotle 350 5th Avenue New York", size: 1 }],
  ["search", { q: "350 Fifth Avenue New York", filters: { facets: { type: ["cafe"] } }, size: 2 }],
  ["search", { q: "350 Fifth Avenue New York", size: 2, includeResults: false }],
  ["search", { q: "214 Rue Libersan, Sainte-Thérèse", size: 2 }],
  ["search", { q: "214 Rue Libersan, Sainte-Thérèse", size: 2, includeResults: false }],
  ["search", { q: "213 Rue Libersan, Sainte-Thérèse", size: 2 }],
  ["search", { q: "210 Rue Libersan", size: 5 }],
  ["search", { q: "J7B1Z5", size: 5 }],
  ["search", { q: "J7A1V6", size: 5 }],
  ["search", { q: "10 Boulevard des Châteaux J7B1Z5", size: 5 }],
  ["search", { q: "311 A Bd Cartier O, Laval, QC H7N 2J3", size: 5 }],
  ["search", { q: "4998 Vanderbilt Dr, Columbus, Ohio 43213", size: 5 }],
  ["search", { q: "1500 Saint Clair Ave W, Toronto", size: 5 }],
  ["search", { q: "311 cartier ouest laval", size: 5 }],
  ["search", { q: "214 libersan ste-thérèse", size: 2 }],
  ["search", { q: "Chipotle", size: 5 }],
  ["search", { q: "Starbucks New York", size: 5 }],
  ["search", { q: "Vanderbilt", size: 5 }],
  ["search", { q: "Cafe", size: 5 }],
  ["search", { q: "Starbuks", size: 5 }],
  ["search", { q: "vandrbilt drive", size: 5 }],
  ["count", { q: "J7B1Z5" }],
  ["suggest", { q: "214 Rue Lib", size: 5 }],
  ["suggest", { q: "214 Rue Lib", size: 5, hydrate: true }],
  ["suggest", { q: "213 Rue Lib", size: 5 }],
  ["suggest", { q: "350 5th", size: 5 }],
  ["suggest", { q: "Chip", size: 5 }],
  ["suggest", { q: "Chip", size: 5, hydrate: true }],
  ["suggest", { q: "J7B1", size: 5 }],
  ["suggest", { q: "c", size: 5 }],
  ["suggest", { q: "st", size: 3, hydrate: true }],
  ["searchAddressAuthority", { q: "350 5th Avenue New York", size: 3 }],
  ["searchAddressAuthority", { q: "214 Rue Libersan", size: 3 }],
  ["searchAddressAuthority", { q: "Chipotle", size: 3 }],
];
  return queries.map(([method, params]) => ({ method, params }));
}
