# Replace Google Maps search APIs with Rangefind OSM

Rangefind can replace the static-search and geocoding portion of many Google
Maps Platform integrations with a browser-only OpenStreetMap index: no API key,
query server, database, session token, per-request billing, or query data sent
to a third-party search provider.

This is a practical migration guide. It maps current Google Places and
Geocoding concepts to Rangefind, supplies a compatibility adapter, and shows
complete map-search journeys. It also identifies the boundaries honestly:
Rangefind does not calculate routes, traffic, driving times, reviews, or photos.

The Google surface names below follow the current official documentation for
[Places API (New)](https://developers.google.com/maps/documentation/places/web-service/op-overview),
[Autocomplete (New)](https://developers.google.com/maps/documentation/places/web-service/place-autocomplete),
[Text Search (New)](https://developers.google.com/maps/documentation/places/web-service/text-search),
[Nearby Search (New)](https://developers.google.com/maps/documentation/places/web-service/nearby-search),
[Place Details (New)](https://developers.google.com/maps/documentation/places/web-service/place-details),
and the [Geocoding API](https://developers.google.com/maps/documentation/geocoding/overview).

## What you can replace

| Google Maps Platform surface | Rangefind OSM replacement | Migration status |
| --- | --- | --- |
| Autocomplete (New) | `suggestOsmQuery()` | Strong replacement for places, categories, localities, streets, civic/interpolated addresses, and cursor edits. |
| Text Search (New) | `searchOsmQuery()` | Strong replacement for OSM-backed place, category, landmark, locality, street, and address search. |
| Nearby Search (New) | `searchOsmQuery()` with `near`/`geo.near` | Hard radius, nearest-first, types, constraints, and static prominence. |
| Place Details (New) | Inline Rangefind OSM result `details` | No second request; limited to fields present in OSM and the built schema. |
| Forward Geocoding | `searchOsmQuery()` | Exact civic, street/locality, postal, intersection, and compact interpolation paths. |
| Reverse Geocoding | `reverseGeocodeOsm()` | Address-first bounded lookup, accuracy/result filters, and locality fallback. |
| “Search this area” | `geo.box` or map-aware `searchOsmQuery()` | Exact viewport restriction with multi-resolution category cells. |
| Open now and place constraints | `constraints`, `evaluateOpeningHours()` | Client-side evaluation of indexed schedules plus typed OSM facets. |
| Search along an existing route | `searchAlongRouteOsm()` | Exact route corridor, progress, direction, viewport bias, and rejoin point. |
| Place/building/park geometry | Result `geometry` | Compact simplified line/polygon rendering for selected results. |
| Maps JavaScript rendering | MapLibre, Leaflet, OpenLayers, or your renderer | Rangefind supplies search data, not a basemap renderer. |
| Routes API | Keep a routing engine | Rangefind consumes a route but does not compute one. |
| Photos, reviews, ratings, traffic, live occupancy | No static OSM equivalent | Keep another provider or remove the feature. |

Google's current Nearby Search requires a circular location restriction and can
rank by distance or popularity. Rangefind supports the same hard-circle and
distance behavior; “popularity” is a static indexed OSM prominence prior, not
Google's live proprietary popularity signal.

## The important mental-model change

Google APIs execute a request against a hosted service and return fields chosen
by a request-time field mask. Rangefind moves that decision to build time:

```text
OSM/RQA data -> Rangefind builder -> immutable static index -> browser Range requests
```

- `display` and geo capsules are your field mask.
- A build determines which details, facets, geometry, and regions are available.
- Queries download only the byte ranges needed from the static index.
- Content-addressed files cache indefinitely; only small manifests change.
- Search freshness follows your indexing schedule rather than a live provider.

This makes cost, privacy, and offline behavior predictable, but missing source
data cannot be recovered at query time.

## Use the free public OSM index

Rangefind publishes a ready-generated OpenStreetMap index at
[`https://osm.rangefind.dev/`](https://osm.rangefind.dev/). It is free to
query directly from browsers and Node, allows cross-origin range requests, and
does not require an account, API key, session token, or payment method.

```js
import { createSearch } from "rangefind";
import { createRangefindMapsAdapter } from "rangefind/osm";

const engine = await createSearch({
  baseUrl: "https://osm.rangefind.dev/"
});

const maps = createRangefindMapsAdapter(engine, {
  defaults: {
    near: { lat: 45.5019, lon: -73.5674 },
    timeZone: "America/Toronto"
  }
});
```

The [public index status page](https://osm.rangefind.dev/) reports current
coverage, document count, source freshness, and rebuild progress. Root
manifests advance atomically, so clients see the last complete searchable
snapshot while the next index is being built.

The hosted index is a best-effort public service without an availability SLA.
It is suitable for evaluation, prototypes, public applications, and production
clients that can tolerate that service model. Build or mirror the index under
your own domain when you need guaranteed capacity, version pinning, custom OSM
fields, private source data, or an operational SLA. In every case, display
[`© OpenStreetMap contributors`](https://www.openstreetmap.org/copyright)
wherever OSM results or map data are shown.

## Fifteen-minute browser migration

Install Rangefind. The migration adapter is part of the browser-safe OSM API:

```sh
npm install rangefind
```

Use the free public index immediately, or substitute your own compatible index
URL when you need a controlled production data contract.

```js
import { createSearch } from "rangefind";
import { createRangefindMapsAdapter } from "rangefind/osm";

const engine = await createSearch({
  baseUrl: "https://osm.rangefind.dev/",
  multiRangeRequests: true,
  geoCapsules: true,
  geoCellIndexes: true
});

const maps = createRangefindMapsAdapter(engine, {
  defaults: {
    near: { lat: 45.5019, lon: -73.5674 },
    timeZone: "America/Toronto"
  }
});
```

The adapter is a migration aid, not a byte-for-byte clone of Google's response
types. It deliberately keeps `rangefind`, `details`, `geometry`, and `source`
metadata visible instead of hiding useful capabilities behind a lowest-common-
denominator contract.

### Before: Google Places

```js
const { places } = await Place.searchByText({
  textQuery: "cinema Laval",
  fields: ["id", "displayName", "location", "formattedAddress"]
});
```

### After: Rangefind compatibility adapter

```js
const { places, status } = await maps.textSearch({
  textQuery: "cinema Laval",
  maxResultCount: 10,
  locationBias: {
    circle: {
      center: { latitude: 45.5601, longitude: -73.7124 },
      radius: 20_000
    }
  }
});
```

### Prefer the native API in new code

The adapter makes migration small. New Rangefind-first code should use the
native API so route, constraints, shard selection, traces, and result geometry
remain explicit:

```js
import { searchOsmQuery } from "rangefind/osm";

const response = await searchOsmQuery(engine, {
  query: "cinema Laval",
  near: { lat: 45.5601, lon: -73.7124 },
  limit: 10,
  trace: true
});
```

## Adapter API

The runnable adapter lives at
[`src/integrations/osm/google_maps_adapter.js`](../src/integrations/osm/google_maps_adapter.js)
and is exported from `rangefind/osm`. The
[`examples/osm-geo/google-maps-adapter.js`](../examples/osm-geo/google-maps-adapter.js)
entry point is a copy-friendly re-export that always tracks the tested package
implementation.

| Method | Request shape | Response |
| --- | --- | --- |
| `autocomplete()` | `input`, `inputOffset`, location bias, result count | `{ suggestions, status, rangefind }` |
| `textSearch()` | `textQuery`, type, bias/restriction, open-now/constraints | `{ places, status, rangefind }` |
| `nearbySearch()` | included/excluded types, circular restriction, rank preference | `{ places, status, rangefind }` |
| `geocode()` | `address`, optional location bias | `{ results, status, rangefind }` |
| `reverseGeocode()` | `location`, radius, result/accuracy filters | `{ results, status, rangefind }` |
| `searchAlongRoute()` | route, query, corridor, direction, constraints | `{ places, status, rangefind }` |
| `placeDetails()` | cached `placeId` | `{ place, status }` |

Adapter statuses are `OK` and `ZERO_RESULTS`. Programming/validation errors
throw rather than being disguised as zero results.

### Place shape

```js
{
  id: "node/123",
  displayName: { text: "Example Cafe" },
  formattedAddress: "123 Example Street, Montreal",
  location: { latitude: 45.5, longitude: -73.6 },
  primaryType: "cafe",
  types: ["cafe"],
  openingHours: {
    osmExpression: "Mo-Fr 07:00-18:00",
    openNow: true,
    state: "open"
  },
  accessibilityOptions: { wheelchair: "yes" },
  nationalPhoneNumber: "+1-555-0100",
  websiteUri: "https://example.com",
  geometry: { type: "Polygon", encoded: "...", precision: 5, bbox: [...] },
  details: { /* compact indexed OSM tags */ },
  source: { dataset: "OpenStreetMap", osmType: "node", osmId: 123 },
  rangefind: {
    score,
    shard,
    distanceMeters,
    routeDistanceMeters,
    routeProgressMeters,
    routeProgressRatio,
    routeBearingDegrees,
    routeRank,
    rejoinPoint,
    constraintMatches
  }
}
```

Place details are returned inline. `placeDetails()` reads the adapter's result
cache and intentionally fails if the application discarded the selected place.
This catches an unnecessary Google-style second request during migration.

Autocomplete predictions carry `rangefindSelection` instead of inventing a
Google place id before a document has been selected:

```js
const response = await maps.autocomplete({
  input: "214 rue lib",
  inputOffset: 11,
  locationBias: {
    circle: {
      center: { latitude: 45.64, longitude: -73.83 },
      radius: 30_000
    }
  }
});

const prediction = response.suggestions[0].placePrediction;
// prediction.rangefindSelection = { query, shards? }
```

Pass the selection to the native API for the most efficient follow-up:

```js
const selected = prediction.rangefindSelection;
const result = await searchOsmQuery(engine, {
  query: selected.query,
  shards: selected.shards,
  near: { lat: 45.64, lon: -73.83 },
  limit: 1
});
```

Rangefind does not need `sessionToken`: there is no billing session or server
state. `includedPrimaryTypes` is supported as a prediction post-filter. A
location bias influences the search context, but the adapter does not pretend
that suggestions are hard-clipped to a Google-style autocomplete rectangle.
Debounce UI input for user experience, not quota protection.

## Use case 1: nearby restaurants, open now and accessible

### Adapter

```js
const response = await maps.nearbySearch({
  includedTypes: ["restaurant", "cafe"],
  maxResultCount: 20,
  rankPreference: "DISTANCE",
  locationRestriction: {
    circle: {
      center: { latitude: 45.5019, longitude: -73.5674 },
      radius: 2500
    }
  },
  openNow: true,
  constraints: {
    wheelchair: true,
    toiletsWheelchair: true,
    contactless: true
  },
  timeZone: "America/Toronto"
});
```

### Native natural-language form

```js
const response = await searchOsmQuery(engine, {
  query: "wheelchair-accessible restaurant open now with contactless",
  near: { lat: 45.5019, lon: -73.5674 },
  limit: 20,
  timeZone: "America/Toronto"
});
```

The native constraint engine recognizes supported English/French phrases,
pushes static requirements into typed facets, hydrates only candidates, and
verifies the returned details. Unsupported holiday/sunrise schedules are
`unknown`, never guessed open. Set `includeUnknownOpenNow: true` only if the UI
can clearly distinguish unknown from verified open.

## Use case 2: forward-geocode a civic or interpolated address

```js
const { results, status } = await maps.geocode({
  address: "214 Rue Libersan, Sainte-Thérèse",
  maxResultCount: 5
});

if (status === "OK") {
  map.flyTo({
    center: [results[0].location.longitude, results[0].location.latitude],
    zoom: results[0].rangefind.locationType === "RANGE_INTERPOLATED" ? 17 : 18
  });
}
```

Explicit address points win. When the address is not explicitly mapped, a
compact OSM interpolation record checks numeric range/parity and computes the
coordinate along the road geometry without storing one document per possible
house number.

## Use case 3: reverse-geocode a map click

```js
map.on("click", async event => {
  const { results } = await maps.reverseGeocode({
    location: {
      latitude: event.lngLat.lat,
      longitude: event.lngLat.lng
    },
    radiusMeters: 3000,
    maxResultCount: 8,
    resultTypes: ["street_address", "route", "locality"],
    locationTypes: [
      "ROOFTOP",
      "RANGE_INTERPOLATED",
      "GEOMETRIC_CENTER",
      "APPROXIMATE"
    ]
  });

  showAddressPicker(results);
});
```

The hard radius never silently expands. Address filters are post-search
semantic filters, matching the intent of Google's reverse-geocoding filters.
A separate bounded locality fallback is used only when locality-like result
types were requested.

## Use case 4: search the visible map

```js
const bounds = map.getBounds();
const response = await maps.textSearch({
  textQuery: "pharmacy",
  maxResultCount: 50,
  locationRestriction: {
    rectangle: {
      low: {
        latitude: bounds.getSouth(),
        longitude: bounds.getWest()
      },
      high: {
        latitude: bounds.getNorth(),
        longitude: bounds.getEast()
      }
    }
  }
});
```

Rangefind chooses a multi-resolution category-cell level that fits the
viewport and cell budget, opens only intersecting category blocks, and can
return map-result capsules without document-pack hydration.

Use a location **bias** while typing and a location **restriction** after the
user presses “Search this area.” Bias improves ranking without excluding a
relevant place just outside the current map; restriction is a hard boundary.

## Use case 5: search along an existing route

Rangefind consumes route geometry produced by your routing engine, a stored
GPX/GeoJSON trip, or the user's recorded path:

```js
const response = await maps.searchAlongRoute({
  route: encodedPolylineOrGeoJSON,
  query: "Tim Hortons",
  corridorMeters: 1500,
  routePositionMeters: 12_400,
  routeDirection: "forward",
  viewport: { lat: 45.56, lon: -73.66 },
  maxResultCount: 20,
  openNow: true,
  constraints: { contactless: true },
  timeZone: "America/Toronto"
});

for (const place of response.places) {
  console.log({
    name: place.displayName.text,
    offRouteMeters: place.rangefind.routeDistanceMeters,
    tripProgressMeters: place.rangefind.routeProgressMeters,
    rejoin: place.rangefind.rejoinPoint
  });
}
```

The corridor becomes multi-resolution cells without collapsing a winding route
into one large diagonal bounding box. Final points are checked against exact
segments and ranked by cross-track distance, route progress, forward/behind
direction, and viewport proximity.

This is not a replacement for Google's
[Compute Routes](https://developers.google.com/maps/documentation/routes/compute-route-over):
Rangefind does not calculate the route, ETA, traffic, tolls, turn instructions,
or the driving detour after adding a stop. A client can search a route entirely
offline once it already has the geometry.

## Use case 6: render the actual result geometry

Schema-v3 OSM results can include compact polygons/lines for useful parks,
campuses, venues, and buildings:

```js
import { decodePolyline } from "rangefind/osm";

function geometryFeature(place) {
  const geometry = place.geometry;
  if (!geometry) return null;
  const points = decodePolyline(geometry.encoded, geometry.precision)
    .map(point => [point.lon, point.lat]);
  return {
    type: "Feature",
    properties: { id: place.id, name: place.displayName.text },
    geometry: geometry.type === "Polygon"
      ? { type: "Polygon", coordinates: [points] }
      : { type: "LineString", coordinates: points }
  };
}

const feature = geometryFeature(selectedPlace);
map.getSource("selected-place").setData({
  type: "FeatureCollection",
  features: feature ? [feature] : []
});
```

Result geometry is intentionally not a vector-tile system or a polygon spatial
relation index. It makes selection/highlight rendering better while point,
viewport, radius, and corridor lookup stay fast.

## Type migration

Google and OSM taxonomies are not identical. Do not copy a large Google type
allowlist and assume every value maps one-to-one. The example adapter includes
a small explicit alias table and otherwise converts underscores to words:

| Google-style input | OSM/Rangefind search surface |
| --- | --- |
| `movie_theater` | `cinema` |
| `gas_station` | `fuel` |
| `grocery_store` | `supermarket` |
| `shopping_mall` | `mall` |
| `coffee_shop` | `cafe` |
| `tourist_attraction` | `attraction` |

Rangefind's OSM category lexicon resolves multilingual aliases to canonical
indexed types. For product-specific categories, extend the alias table and add
benchmark cases rather than silently broadening queries.

## Place Details migration

Google Place Details is an id-based second request with a field mask. Rangefind
returns the configured compact detail projection inline with search results.

Typical OSM details include:

- opening hours;
- phone, email, and website;
- brand and operator;
- cuisine;
- wheelchair and accessible toilets;
- internet access and outdoor seating;
- takeaway, delivery, and drive-through;
- payment/contactless support;
- reservations, fees, capacity, and access; and
- OSM/Wikidata/Wikipedia references.

There is no equivalent for Google ratings, editorial summaries, reviews,
photos, live business status, future opening businesses, or proprietary
transit/departure details unless you add a separately licensed source to your
own corpus.

Choose `geoCapsuleFields` carefully: include the fields required to draw a
result list/card, but keep large optional content out of every map marker
capsule.

## Production index requirements

For the complete replacement surface, build with the current OSM schema:

```js
import { buildOsmIndex } from "rangefind/osm/node";

await buildOsmIndex({
  root: "work/osm",
  region: "quebec",
  workerCount: 4
});
```

Schema v3 enables compact details, typed constraints, `opening_hours`, geo
capsules, category cells, wildcard occupancy for brand/text routes, and useful
result geometry. Older indexes remain searchable but cannot return fields they
never stored.

For a planet or multi-country service, build independently updateable regional
shards under frozen shared scoring statistics, then publish root text and
suggestion routing. This prevents autocomplete and ordinary text queries from
opening every region.

See [Sharded OSM](sharded-osm.md) and the
[OSM example](../examples/osm-geo/README.md) for build workflows.

## Static hosting and CDN checklist

Search performance depends more on byte-range caching than on JavaScript
minification.

- Return `206 Partial Content` and a correct `Content-Range` for range requests.
- Return `Content-Length`; Cloudflare needs it to serve a cached client range as
  `206` rather than the entire object as `200`.
- Do not apply a second `Content-Encoding: gzip` to `.bin.gz` objects.
- Mark content-addressed packs/directories immutable with a long browser/edge
  TTL.
- Keep manifests short-lived or revalidated with ETag/Last-Modified.
- Do not emit `Set-Cookie`, `private`, or `no-store` on immutable index files.
- Avoid putting authorization headers or per-user values in index requests.
- Make the index path/hostname explicitly eligible in your CDN cache rule.
- Preserve CORS access when the index and application use different origins.
- Keep multipart range enabled when supported; Rangefind falls back to ordinary
  ranges when it is not.

For Cloudflare, create a narrowly scoped Cache Rule matching only the Rangefind
index hostname/path and set **Cache eligibility: Eligible for cache**. Respect
the origin's immutable TTLs or configure equivalent edge/browser TTLs. A
repeated immutable request should move from `CF-Cache-Status: MISS` to `HIT`;
`BYPASS` means the response was eligible but origin headers/cookies made it
uncacheable. Cloudflare documents both
[Cache Rules](https://developers.cloudflare.com/cache/how-to/cache-rules/) and
[client range behavior](https://developers.cloudflare.com/cache/concepts/default-cache-behavior/#client-side-range-requests).

Smoke-test the deployed transport:

```sh
# Manifest is reachable and intentionally mutable/revalidated.
curl -sSI https://maps-index.example.com/manifest.min.json

# An immutable object should cache.
curl -sSI https://maps-index.example.com/terms/packs/PACK.bin.gz
curl -sSI https://maps-index.example.com/terms/packs/PACK.bin.gz

# A range must remain a 206 with a correct Content-Range.
curl -sSI -H 'Range: bytes=0-1023' \
  https://maps-index.example.com/terms/packs/PACK.bin.gz
```

Expected signs:

```text
HTTP/2 206
content-range: bytes 0-1023/TOTAL
content-length: 1024
cf-cache-status: HIT
```

## OpenStreetMap attribution and tiles

OpenStreetMap data is available under the ODbL and requires attribution. For an
interactive map or geocoding application, place a legible link near the map or
in the customary attribution location:

```html
<a href="https://www.openstreetmap.org/copyright">
  © OpenStreetMap contributors
</a>
```

Follow the official [OSMF attribution guidelines](https://osmfoundation.org/wiki/Licence/Attribution_Guidelines),
including any notices for additional datasets such as RQA. Treat the index
manifest's `meta` block as the data-provenance source for the UI.

Rangefind indexes OSM data; it does not grant unlimited use of the public
`tile.openstreetmap.org` tile service. Use a suitable tile provider, self-hosted
tiles, or another renderer/source whose terms match your traffic.

## Privacy, operations, and cost

| Concern | Google-hosted request API | Rangefind OSM |
| --- | --- | --- |
| Query processing | Provider service | User's browser/device |
| API key | Required | None for a public static index |
| Per-query billing/quota | Provider-dependent | None; pay storage/CDN egress |
| Query data exposure | Sent to provider | Stays client-side, apart from static object access logs |
| Offline use | Separate SDK/data constraints | Supported with `rangefind/mobile` or local range reads |
| Freshness | Provider-managed | Your rebuild/update schedule |
| Data completeness | Proprietary multi-source | OSM plus explicitly integrated datasets |
| Operations | Billing, quotas, key restrictions | Index builds, static publication, CDN cache health |

Do not promise “free forever”: storage, build machines, CDN egress, tiles, and
optional external datasets still have costs. The useful distinction is that
search has no query server and scales through immutable static delivery.

## Production benchmark gate

Run the same journeys users will depend on:

```sh
# Current production search/geocoding surface.
npm run bench:osm-maps:production

# Schema-v3 promotion: details, constraints/open-now, route cells, geometry.
npm run bench:osm-maps:next-index

# Reverse-geocoding contract.
npm run bench:osm-maps:reverse

# Full workload and edge ambiguities.
npm run bench:osm-maps:full
npm run bench:osm-maps:edge
```

The promotion suite validates result identity, locality, viewport/radius,
nearest order, autocomplete selection, address accuracy, reverse semantics,
constraint truth, open-now evaluation, route distance/progress/rejoin, geometry
coverage, shard fan-out, requests, transfer, and cold/warm phone budgets. See
[OSM maps workload benchmark](osm-maps-benchmark.md).

Add application-specific cases before migration. If “Tim Hortons along a
route,” “cinema Laval,” or a particular civic address matters to the product,
make it a named benchmark rather than relying only on generic categories.

## Migration plan

### Phase 1: observe

1. Inventory every Google Places/Geocoding call and requested field.
2. Capture representative queries, map centers/viewports, languages, expected
   result ids/names, and latency budgets.
3. Classify each requested field as OSM, separately sourced, or unavailable.
4. Build those journeys into `osm_maps_bench.mjs`.

### Phase 2: dual-run

1. Initialize one Rangefind engine at application startup.
2. Run Rangefind behind the existing UI without changing the rendered result.
3. Compare target rank, locality, empty-result behavior, and request bytes.
4. Inspect `trace: true` for shard/range fan-out rather than judging latency
   alone.

Do not merge Google response data into OSM and republish it unless the source
license explicitly permits that use. Compare results for quality evaluation;
keep separately licensed datasets separate.

### Phase 3: switch common surfaces

1. Replace autocomplete and selection.
2. Replace text/category/nearby search.
3. Replace forward and reverse geocoding.
4. Remove redundant Place Details calls; retain the selected inline result.
5. Add viewport repeat-search and result geometry.
6. Keep Google or another provider only for unsupported dynamic fields.

### Phase 4: optimize and remove old infrastructure

1. Confirm immutable `MISS -> HIT` and cached `206` behavior at the CDN.
2. Promote only after production/next-index benchmarks pass.
3. Remove API keys, session-token code, quota retry logic, and unused field
   masks from migrated paths.
4. Preserve OSM attribution and source metadata.
5. Monitor index age, build success, manifest publication, cache status, cold
   requests/bytes, and quality regressions.

## Troubleshooting

| Symptom | Likely cause | Check/fix |
| --- | --- | --- |
| Correct warm search, slow cold search | CDN bypass or broad shard/range fan-out | Inspect `CF-Cache-Status`, `stats.trace`, opened shards, requests, and bytes. |
| Range request returns the whole file | CDN/origin ignored Range or omitted `Content-Length` | Require cached `206` plus exact `Content-Range`. |
| Brand route search opens many geo leaves | Index lacks schema-v3 wildcard occupancy cells | Rebuild and run `bench:osm-maps:next-index`. |
| Constraints always return zero | Older index lacks typed details/facets | Inspect manifest/schema and rebuild; do not weaken verification silently. |
| Open-now place disappears | Missing/unsupported schedule is `unknown` | Show unknown separately or opt into `includeUnknownOpenNow`. |
| No polygon appears | Result/index has no schema-v3 geometry | Keep marker fallback; verify geometry coverage benchmark. |
| Autocomplete probes every shard | Root suggest routing was not published/rebuilt | Rebuild suggest-set sidecars and root suggest routing. |
| Text query probes every shard | Root text routing is absent/stale | Rebuild term-set sidecars and root text routing. |
| Search result differs from Google | Different source data/ranking/type taxonomy | Decide whether OSM is sufficient, add a licensed source, or keep that Google surface. |

## When Rangefind is the right replacement

Rangefind OSM is especially strong for:

- privacy-sensitive client applications;
- offline or intermittently connected mobile maps;
- public/community services that cannot sustain per-query fees;
- map search over a known set of countries or regions;
- self-hosted products that want deterministic data/version control;
- category, address, reverse, viewport, and along-route discovery; and
- products willing to expose OSM completeness honestly.

Keep a hosted maps provider when the product depends on live traffic, exact
driving detours/ETAs, turn-by-turn navigation, reviews, photos, real-time
business changes, proprietary popularity, or guaranteed global commercial
coverage. A hybrid migration is valid: Rangefind can own static search and
geocoding while a routing or dynamic-place provider handles only the smaller
surface that genuinely requires a service.
