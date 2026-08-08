# OSM Geo Search Example

This example builds a geo-enabled place and address index from an OpenStreetMap
regional extract. It benchmarks canonical address lookup plus Lucene-style geo
queries (bounding box, radius, nearest-neighbor distance sort, and
text-plus-geo) against exhaustive oracles.

## Integration API

OSM behavior is a reusable integration rather than demo-only code. Browser-safe
document conversion, schema generation, search intents, and autocomplete are
exported from `rangefind/osm`; bounded PBF/RQA build helpers are exported from
`rangefind/osm/node`:

```js
import {
  createOsmIndexConfig,
  reverseGeocodeOsm,
  searchAlongRouteOsm,
  searchOsmQuery,
  suggestOsmQuery
} from "rangefind/osm";
import { buildOsmIndex, augmentOsmWithRqa } from "rangefind/osm/node";
```

Both APIs produce and query the normal Rangefind pack format. There is no OSM
sidecar or parallel runtime. `scripts/osm_fixture.mjs` remains a resumable CLI
for PBF extraction and delegates document shaping, RQA ingestion, schema
generation, and index publication to these modules.

Existing Google Places/Geocoding integrations can start with the supported
`createRangefindMapsAdapter(engine)` facade exported by `rangefind/osm`. It
maps common Autocomplete, Text Search, Nearby Search, Place Details, forward/
reverse geocoding, and supplied-route search request shapes while retaining
Rangefind/OSM metadata. The full migration walkthrough, production checklist,
and parity boundaries are in
[`docs/google-maps-migration.md`](../../docs/google-maps-migration.md).

Route-corridor search accepts either an encoded polyline or GeoJSON and stays
inside the same static range-request pipeline:

```js
const response = await searchAlongRouteOsm(engine, {
  route: encodedPolylineOrGeoJSON,
  query: "wheelchair-accessible Tim Hortons open now with contactless",
  corridorMeters: 1500,
  limit: 20,
  routePositionMeters: 12_400,
  routeDirection: "forward",
  viewport: { lat: 45.56, lon: -73.66 },
  timeZone: "America/Toronto"
});
```

The route is rasterized directly into the existing multi-resolution category
cells. Adjacent cell-directory, geo-leaf, and capsule reads use Rangefind's
grouped/multipart byte-range transport. Results include
`routeDistanceMeters`, `routeProgressMeters`, `routeBearingDegrees`, and an
exact `rejoinPoint`. The input line direction defines forward travel order;
`routeDirection: "reverse"` reverses it without rewriting the polyline.

Natural constraints are also accepted by ordinary `searchOsmQuery` calls.
Supported typed predicates include wheelchair access, accessible toilets,
contactless payment, delivery, takeaway, drive-through, outdoor seating,
internet, reservations, and free admission. Indexed facets prune candidates;
the returned OSM details are verified again client-side. `open now` evaluates
common `opening_hours` rules in the requested IANA time zone. Unsupported
holiday or calendar expressions remain `unknown` and are excluded by default,
so the client never invents an open status.

Area-like OSM ways publish compact encoded polygon geometry in geo capsules.
Map clients can decode it with `decodePolyline(result.geometry.encoded,
result.geometry.precision)` and render the actual park, campus, venue, or
building instead of only its marker.

`reverseGeocodeOsm(engine, { lat, lon, radiusMeters, size })` resolves nearest
indexed addresses inside a hard radius and routes only through shards whose
bounding boxes intersect that radius. Results include `formattedAddress`,
structured `addressComponents`, `locationType`, and `reverseGeocodeAccuracy`.
`resultTypes` / `locationTypes` can filter the response; a locality-only request
uses a bounded place fallback. Coordinate text passed to `searchOsmQuery` uses
the same address-first path automatically.

`suggestOsmQuery` returns the legacy `text`, `weight`, and `shards` plus
structured `mainText`, `secondaryText`, `matchedRanges`, `types`, and a
`selection` object that can be passed into the selected search. `inputOffset`
supports mid-query cursor edits without introducing a session service.

The hosted map client queries the rolling sharded index published by the
sibling [`osm-rangefind-index`](https://github.com/xjodoin/osm-rangefind-index)
project at `https://osm.rangefind.dev/`. The fixture commands below remain
available for local builds, benchmarks, and development against one regional
extract. Its UI consumes structured autocomplete and selection shard hints,
supports click-to-reverse-geocode through **Pick map**, distinguishes exact,
interpolated, and approximate locations, and progressively displays the compact
OSM `details` fields as rebuilt shards publish them.

## Directions (static routing)

The map client includes a Directions mode backed by the rfroutegraph-v1
static routing lane (see [`docs/route-graph.md`](../../docs/route-graph.md)).
Routing runs entirely in the browser: `openRouteGraphUrl("route-graph/")`
fetches the bounded object set for each query over HTTP range reads and runs
a multilevel bidirectional Dijkstra client-side. The demo bundles the
dependency-free engine as `public/route.browser.js` (built by
`npm run build:osm-demo` alongside the runtime and OSM bundles).

The UI probes `route-graph/manifest.json` at startup. When absent, the
Directions tab explains how to publish an index:

```bash
# Build a route graph (Luxembourg matches the demo fixture region)
node scripts/osm_road_graph.mjs luxembourg-latest.osm.pbf luxembourg.graph.bin
node scripts/route_bench.mjs build luxembourg.graph.bin bench/route/luxembourg-index --peak

# Copy it into the demo (defaults to bench/route/luxembourg-index)
npm run setup:osm-demo-route
```

The route bundle and index must come from the same source revision — the
binary formats are versioned, so rebuild both together after engine changes.

Directions features:

- origin/destination/via fields with the same static autocomplete as search,
  per-field map picking, swap, and up to 8 stops;
- routes with two penalized-re-search alternatives (click a dashed line or a
  chip to promote one), snapped-point dots and snap-distance notes;
- a departure selector (`Now` / rush / free-flow) when the index was built
  with time buckets;
- 3+ stops switch to `itinerary()` — travel-time matrix, exact Held-Karp
  ordering, per-leg rendering, and optimized visit order;
- "Search along this route" feeds the computed geometry straight into the
  existing route-corridor search lane (`searchAlongRouteOsm`) against the
  public place index;
- a Route X-Ray receipt showing the object fetches, bytes, and shards each
  route touched — the routing analogue of the search Query X-Ray.

## Live traffic (PulseMesh)

The route card is followed by a **Live traffic** card that turns the
peer-to-peer traffic channel on. There is no traffic server in the path, and
the router works without any of it — that degradation is the contract, and
turning the card off proves it.

With it on, the demo:

- colours the roads it has observations for, green through red, with a
  corroborated jam drawn more strongly than a two-report hint;
- states what live traffic did to the ETA (`+2 min from live traffic · 82
  edges re-weighted from 39 observed segments`) and re-routes when the picture
  moves materially;
- pins incidents, marks the uncorroborated ones "unconfirmed", and offers
  *Still there* / *Gone* on each — which is how §8.5 gets the distinct-peer
  corroboration that turns a claim into something the router acts on;
- reports a crash, closure, road works, police or an object on the road from
  the position the demo drive is at, behind the disclosure §10.4 requires;
- shares the drive as a **45-byte capability in a URL fragment** — no server,
  here or anywhere, ever receives it — choosing what the link is worth if it
  leaks (a live locator, or stop events with no position at all), and follows
  one pasted in or opened in the address bar.

- **is a device** (§20.9): the page mints an X25519 keypair on first use and
  shows it under **This device** — a name you can change, a fingerprint, and
  the PMV1 card as a QR and a `wayfind://device#…` link. That card is what a
  dispatcher scans to be able to send this browser a job, and what this
  browser shows when it wants to be given one. The fingerprint exists to be
  read aloud and checked against the other phone's screen: a card is bytes on
  a screen, and a screen in a depot can be showing anybody's. **Enrolled
  devices** is the list a job can be sealed to — paste a card, or open a
  `wayfind://device#…` link, and confirm. The key lives in localStorage,
  which is not a Keystore: clearing site data destroys it and every job
  already sealed to it becomes unopenable by anybody. The page says so under
  the card rather than in a footnote.

  One browser plays dispatcher and driver here, and the demo does not paper
  over it: there is no hidden driver device. *Use this device's card* fills
  the enrol field with this browser's own card and stops — you enrol it
  yourself, and the roster row says "This device" so the two keys acting are
  never mistaken for two devices.

- **dispatches a job** (§20): one ticket, issued against the route on screen,
  sealed to an enrolled device, produces two artifacts that are deliberately
  unequal. The customer link is the ordinary read-only 45 bytes and exists
  *before* a driver does, so it can go out with the order confirmation. The
  job itself is **never in the clear** (§20.9): what the QR and the file
  carry is PME1 ciphertext addressed to the driver's device and to this
  dispatcher's own, so a photograph of the code on the counter is worth
  nothing to the person who took it. Whoever *can* open it still holds a
  publish capability — the protocol cannot tell two holders of one run seed
  apart — which is why it goes to one named device and no other, and why
  *Create a job ticket* with an empty roster explains enrolment instead of
  failing. Opening it (or pasting it into the follow box) shows the driver an
  accept card — stops, what the link reveals, expiry, issuer — rather than
  acting on it the way a follow link is acted on: taking a job is a decision.
  A job sealed for somebody else says exactly that, *sealed for another
  device*, with this device's fingerprint to send back — not a decode error,
  because the two have different fixes. *Hand over this job* re-seals the
  same run to a device picked from the roster, so a second courier
  resumes the run above the audience's own highest `seq` (§20.5) and the
  customer's link never dies — and if the next driver was never enrolled
  here, the panel says so and points at the card flow. No card, no transfer.
  The QR encoder is `src/qr.js`, in-repo, because
  the package has no runtime dependencies and ISO/IEC 18004 does not move.
  Enter several delivery addresses in *Directions* and the ticket carries all
  of them, in the order `itinerary()` worked out rather than the order they
  were typed — pick *End anywhere* under **Finish** so the optimizer is not
  forced to treat the last address entered as the terminus. The origin never
  becomes a stop on the ticket (it is where the vehicle leaves from, not a
  delivery), and each customer should be sent their link plus their own stop
  only: the plan holds every other customer's address.

  Each stop takes optional **delivery details** (§20.8) behind an *Add details*
  row — order reference, parcel count, instructions, contact — and the driver's
  row shows them for the stop in front of them: the reference and the count
  large, because those are what get checked against the box on a doorstep, the
  instruction under them, and the contact as a one-tap `tel:` link. None of it
  reaches the mesh: it lives in the plan, the plan lives in the ticket, and what
  goes on the wire is an 8-byte hash of the plan. What it *does* change is what
  the ticket is worth to read — a customer list rather than a route sheet —
  which is the reason the ticket is now sealed: the details are readable only
  by the devices it was addressed to, and the tile names them. An empty parcel
  box means *nobody said*, which the protocol keeps distinct from a stated `0`.

  A full day of stops is bigger than a phone camera can scan, so the job
  also downloads as a one-line `job-<id>.wayfindjob` file — the same sealed
  bytes, carrying the `wayfind://ticket#…` URL as text so it survives mail
  clients and copy-paste — and when the plan outgrows the scannable QR bound
  the page drops the symbol rather than printing an unreadable one. Sealing
  costs 130 bytes for one recipient and 194 for two, so that bound is lower
  than it was: about ten metadata-free stops rather than fifteen. *Open a
  ticket or link file* next to the follow box reads one back in, which is the
  paste path with the typing done for you; the file is ciphertext, so the
  channel carrying it cannot read the job — the device it is addressed to
  still gets a publish capability.

Following shows a card whose sentence comes from the subscriber itself, never
from the page: a stale position is never presented as a live one. Under it is
an arrival computed **here**, routing the position they broadcast to the
destination this browser cares about under this browser's live-traffic
metric — so the publisher broadcasts one position and never learns which
destination anyone asked about.

In local mode the follower is a second in-tab peer, because no transport
loops a publish back to its own sender: on a real mesh the follower is simply
another device. Joining late is the interesting case — the viewer holds
nothing but the link, and pulls the run it missed out of the *other peer's*
cache (§5.5), which is why availability grows with the audience instead of
costing more.

Two modes, both real protocol bytes:

```bash
# Default: peers inside this tab, fed by simulated vehicles on your corridor.
# Works from file:// and from any static host.
npm run serve:osm-geo

# Or join a real mesh: run a keeper and pass its multiaddr.
npm run pulsemesh:keeper
open "http://localhost:5184/?keeper=/ip4/127.0.0.1/tcp/4001/ws/p2p/12D3Koo..."

# A fleet's own seed (threads §20.10): the keeper prints its dialable
# addresses, a wayfind://seed#… card and a scannable QR of it, on stderr.
# Paste or scan that into "This fleet's seed" and every job this browser
# issues carries the address *inside the sealed ticket* — so it reaches
# the driver's phone and nobody else.
npm run pulsemesh:keeper -- --seed-card --seed-label="Depot seed"
```

A browser joins the wire **read-only** (protocol §11.6): it mints no admission
bond, joins no gossip topic, and pulls what it shows over the padded sync
path. That is the design, not a demo limitation — a tab that will never
contribute should not pay a 256 MiB mint, and an unbonded peer in the gossip
mesh would only punch holes in other peers' delivery paths.

Console access, for poking at it: `window.rangefindPulseMesh` exposes
`snapshot()`, `traffic()`, `incidents()`, `drawn()` and the session itself.

## Build

```bash
# Small region (Luxembourg, ~175k places, downloads ~450 MB PBF)
node scripts/osm_fixture.mjs all --region=luxembourg --root=examples/osm-geo

# Large region (Quebec, millions of places, downloads ~1.2 GB PBF)
# RQA civic/postal coverage is enabled by default for this region.
node scripts/osm_fixture.mjs all --region=quebec --root=examples/osm-geo

# National scale (United States, 32.8M places, downloads ~11.2 GB PBF)
RANGEFIND_OSM_US_ROOT=/Volumes/large-disk/rangefind-osm-us \
  npm run bench:osm-geo:us
```

The US driver defaults to `.cache/osm-us`; set `RANGEFIND_OSM_US_ROOT` to a
volume with at least 100 GB free for the source PBF, resumable extraction
artifacts, build scratch space, and final index. Downloads use `curl -C -`
and publish atomically after completion.

Large extraction is a four-stage bounded pipeline: candidate ways are spooled
to JSONL, their node anchors are externally sorted/deduplicated, matching node
coordinates are stored in an indexed SQLite table while node documents are
spooled, and the final corpus is materialized sequentially. Each completed
stage has PBF size/mtime metadata and is reused on restart. The extractor does
not retain all ways or coordinates in JavaScript heap.

Québec builds also download the monthly, CC BY 4.0
[Référentiel québécois des adresses (RQA)](https://www.donneesquebec.ca/recherche/dataset/referentiel-quebecois-des-adresses).
The download is resumable and can be shared across workspaces with
`RANGEFIND_RQA_ARCHIVE=/path/to/RQA_CSV.zip`; pass `--no-rqa` for an OSM-only
comparison. Ingestion streams the zipped CSV, collapses apartment/unit rows to
one civic point, and uses an indexed on-disk canonical set to suppress only
full addresses already represented identically by OSM. Spatially nearby OSM
points are retained alongside the RQA authority record when their published
components differ, ensuring the official full address remains searchable.
The RQA adapter delegates to Rangefind's generic address-enrichment engine,
which emits one country-scoped aggregate per postal code with a centroid,
coordinate bounds, primary locality, aliases, sample count, civic-address
count, and provider provenance.

The same engine accepts OpenAddresses, national address registers, GeoNames,
or another licensed provider through an async normalized-record iterator.
CSV/TSV sources can use `createDelimitedAddressSource`; provider adapters own
their mappings, filters, lifecycle rules, and country-specific postcode
normalization. Multiple providers merge in priority order and do not produce
duplicate postal results.

RQA civic records intentionally have no BM25 title/body terms, geo-browse
point, or autocomplete surface. They contribute only compressed display data
and canonical authority keys, so a complete address still uses the bounded
zero-posting exact lane without making ordinary place search, map browse, or
autocomplete proportional to the residential corpus. Postal aggregates are
searchable and suggestible; a postal-only query returns the public area
centroid and civic-address count rather than enumerating private residences.

Enrichment is streaming and uses temporary SQLite key/aggregate tables, so
heap use does not scale with the address corpus. Residential civic points do
not enter BM25, autocomplete, or geo-browse structures; the added index cost
is limited to compressed display documents and canonical authority rows.

The fixture converts named places and complete `addr:housenumber` +
`addr:street`/`addr:place` nodes and ways (anchored at their first node) into
documents. Address results expose `address`, `house_number`, `street`, `unit`,
`city`, `state`, `postcode`, and `country` alongside `name`, `category`,
`type`, and `lat`/`lon`. Useful OSM metadata is retained in one compact `details`
object (hours, contact, brand/operator, cuisine, wheelchair/accessibility,
internet, seating, takeaway/delivery/drive-through, payment, capacity, access,
and OSM knowledge references). A normalized OSM-derived `prominence` value
feeds Rangefind's generic static `rankPrior`, improving ambiguous text ranking
without a network service or a separate place database.

OSM `addr:interpolation=all|odd|even|N` ways are retained as compact range
documents. The extractor keeps their geometry and numeric address anchors,
splits at tagged intermediate anchors, rejects incompatible endpoint metadata,
and stores the polyline with 1e-6-degree delta encoding. It does **not** create
one document per inferred house. Instead, each range emits street-first
16-number bucket keys in the packed authority index. An exact query hydrates
only the matching range, verifies parity/step, and computes the coordinate by
distance along the OSM polyline. Explicitly tagged addresses are checked first
and therefore always win over an overlapping interpolation.

On the July 2026 Quebec extract, 442,979 interpolation ways produced 339,169
valid range documents covering about 9.15 million possible inferred addresses.
The compact representation grew the 5.76M-document corpus by 5.9%; naïve
materialization would have grown it by roughly 159%. `214 Rue Libersan,
Sainte-Thérèse` resolves through `addressInterpolationExact` with zero posting
blocks decoded.

Autocomplete recognizes a leading numeric house number once the active street
token has at least three characters. It completes the street/locality through
the existing lexicon, verifies a bounded candidate set through the same exact
range lane, and returns only addresses whose interpolation step actually
contains the requested number. No inferred address surfaces are added to the
lexicon.

The demo also recognizes category-plus-locality intents in either order. For
example, `Pharmacie Rosemère` resolves the exact `place=town` document for
Rosemère, maps the French category to the indexed `pharmacy` type, and performs
a distance-sorted geo text query within the town-scale radius. This finds POIs
such as Uniprix whose OSM node has coordinates and `amenity=pharmacy` but no
`addr:city`; resolved localities are cached for subsequent searches.

An exact settlement query such as `Laval` uses the same cached place resolver
before ordinary BM25. It returns the canonical city/town/village record,
disables a stale map viewport, and centers the map instead of displaying the
many POIs and addresses that merely contain the locality name.

Street-plus-locality queries use a similarly bounded plan. `Rue Hector
Rosemère` first resolves the municipality, removes the high-frequency road
designator from the text plan, and searches the distinctive street name within
the town radius. Exact OSM road segments are collapsed to one canonical street
result. This avoids exhausting the posting budget on `rue` and requires no
per-street sidecar or duplicate street index.

Autocomplete applies the same street-level presentation without changing the
index. When no house number is present, a bounded candidate window groups
civic and interpolation titles by street and municipality, so `Rue Libersan
Saint` offers `Rue Libersan, Sainte-Thérèse` instead of spending every visible
slot on individual house numbers. Numeric prefixes keep the address-level and
interpolated suggestions unchanged.

A small hierarchy of canonical address keys per document is stored in the
packed authority index: full address, house/street/locality,
house/street/postcode, and house/street. Equivalent forms such as `Fifth
Avenue`/`5th Ave`, `Northwest`/`NW`, reordered components, and those useful
partial forms use a zero-posting exact lane. Other incomplete or
place-plus-address queries fall back to the weighted inverted index.
Canadian postal codes may be entered with or without their customary space:
`J7B 1Z5` and `J7B1Z5` share the same query plan and existing index data.

The index also includes a `geo` field:

```json
{
  "geo": [{ "name": "location", "latPath": "lat", "lonPath": "lon" }]
}
```

## Benchmark

```bash
node scripts/osm_geo_bench.mjs --root=examples/osm-geo
node scripts/osm_address_bench.mjs --root=examples/osm-geo
```

Reports cold HTTP request counts, transfer KB, warm latency, and geo tree
traversal stats per query lane, then verifies bounding box, radius, nearest,
and text-plus-radius results against streaming exhaustive Haversine scans of
the corpus. The oracle retains only the densest-cell counters and bounded
top-20 nearest sets, so validation is safe at national scale.

## Query API

```js
const engine = await createSearch({ baseUrl: "./rangefind/" });

// Canonical exact-address lookup. Structured address fields are returned.
await engine.search({ q: "350 Fifth Ave. New York" });

// OSM interpolation range lookup. The result is marked `interpolated` and
// includes `address_accuracy` from addr:inclusion plus a computed lat/lon.
await engine.search({ q: "214 Rue Libersan, Sainte-Thérèse" });

// Places inside a map viewport.
await engine.search({ q: "", geo: { box: { minLat, maxLat, minLon, maxLon } } });

// Places within 5 km, unordered browse.
await engine.search({ q: "", geo: { near: { lat, lon, radiusMeters: 5000 } } });

// Exact nearest places, sorted by distance.
await engine.search({ q: "", geo: { near: { lat, lon }, sort: "distance" } });

// Text search restricted to a radius, with distance decay boost.
await engine.search({
  q: "bakery",
  geo: {
    near: { lat, lon, radiusMeters: 5000 },
    boost: { weight: 2, pivotMeters: 500 }
  }
});
```

Results carry `distanceMeters` whenever `geo.near` is present, and geo
responses expose `stats.geoLane`, `stats.geoLeavesVisited`,
`stats.geoPointsScanned`, and related counters.
