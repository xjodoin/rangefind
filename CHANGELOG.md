# Changelog

## Unreleased

### Fixed

- Country-scale route overlays no longer grow one multi-gigabyte JavaScript
  byte array or force every phone to download and inflate it. The builder
  widens large hierarchies to a bounded per-cell working set, writes exact-size
  binary columns, and publishes the root graph as independently checksummed
  range slices. Route, matrix, and itinerary searches reuse the ordinary pack
  reader and fetch/decode a slice only when their frontier reaches its cell;
  existing v5 route roots remain readable.

- Country-scale inter-region road extraction no longer serializes every
  federation portal into one giant JSON string. Source graphs persist portal
  ids and coordinates as bounded-memory binary columns (16 bytes per
  candidate), avoiding V8's string-length ceiling and the corresponding
  reader-side JSON memory spike while retaining earlier `rfroutesrc-v8`
  compatibility.

### Added

- Suggestions resolve directly to their document. Per-shard authority
  artifacts (codec v4) keep each autocomplete surface's best `[doc, score]`
  row instead of discarding it, hot lists carry the same doc, and
  `engine.suggest()` now returns `doc` whenever the owning document is
  unambiguous — always on single-index engines, on sharded fan-out when
  exactly one shard owns the winning rank. Sharded engines gained
  `hydrateRows(rows, { shard })`. The OSM layer stamps `selection.doc` on
  entity suggestions (one doc, one shard) and adds
  `resolveOsmSuggestion(engine, suggestion)`, which hydrates the selection
  into a one-result response (`plannerLane: "osmSuggestEntity"`) in a couple
  of range reads. The osm-geo demo uses it: clicking an entity suggestion
  drops the pin and opens the place card without re-running the query as a
  search, and slow searches now dim the previous answer instead of blanking
  it. Older (v2) authority sidecars keep working — suggestions just carry no
  `doc`, and selection falls back to the search path, as it also does for
  root-routed suggestions on sharded planets.

- PulseMesh protocol v1, the anonymous peer-to-peer live-traffic channel
  (`src/pulsemesh/`): wire codecs (PMC1/PMB1/PMI1/PMD1/PMG1/PMQ1/PMS1/PMF1)
  byte-identical to the specification's test vectors, dependency-free
  SHA-256, the deterministic weighted-median aggregation
  and trust ledger, the TTL store with reportId/cell/segment indexes and
  order-independent digests, the twelve-rule validation pipeline, incident
  scoring with distinct-peer capping and the speed-anchored routing gate,
  the contributor state machine and the reticent profile's four emission
  gates, PMF1 forwarding, padded/decoy/split cell fetching, topic grammar
  and rotation, signed bootstrap verification, and a `LiveTrafficProvider`
  adapter that plugs the mesh into `engine.route({ live })`. A 42-byte
  contribution carries no coordinate, trajectory, identity, or precise
  timestamp, because the codec has no field for one.
- PulseMesh wire transport (`rangefind/pulsemesh/libp2p`): a js-libp2p
  binding of the transport-agnostic mesh node — GossipSub with message
  signing disabled and sha256 message ids per the §5.1 profile, one framed
  request/response per `/rangefind/pulsemesh/1/sync` stream — plus a
  runnable §12 keeper (`scripts/pulsemesh_keeper.mjs`). Wire tests cover
  real-TCP convergence with full wire validation, contributor churn
  with zero record loss, padded late-joiner recovery, and two OS processes
  converging to byte-identical zone digests. The libp2p packages are
  optional peer dependencies; engine consumers install nothing new.
- `npm run demo:pulsemesh` — the whole loop end to end against the real
  4096-leaf OSM route graph in this repo: real libp2p peers, real GPS
  fixes through the contributor pipeline, real gossip and validation, and
  a route that changes because other drivers' phones said the road was
  slow. Also prints one contribution field by field, and finishes by
  killing every contributor to show the router degrading to the static
  metric. `npm run bench:pulsemesh:wire` measures the same transport:
  1.00 ms median publish-to-validated-and-stored across a 6-peer mesh,
  and a cold-start peer recovering 300 records in one anti-entropy round.
- PulseMesh Threads (`rangefind/pulsemesh/threads`), the second channel:
  authenticated tracking of one vehicle for a bounded audience, for
  school-bus and delivery use. A run's entire capability is one Ed25519
  public key — 45 bytes in a URL fragment — from which a holder derives
  the content key, the rotating topic tag, and the DHT rendezvous key, so
  the link is what lets you find, decrypt, and verify a thread. The
  private key signs, so no link holder can move the bus. Includes the
  publisher state machine with coarse and fine modes, the subscriber's
  nine-step validation and sequence ledger, host-free catch-up where any
  subscriber answers a late joiner, and the cross-channel rules that keep
  a signed single-source record out of a corroborated traffic aggregate
  and stop a dwelling bus from reporting 0 km/h on a flowing road. Every
  §16 test vector reproduces byte-for-byte through WebCrypto, so the same
  code runs in Node and browsers.
- `engine.locate(segment, ratio)` — the inverse of `snap()`, decoding a
  segment's canonical polyline and interpolating by arc length. Exact to
  0.00 m on round trip. Threads need it to turn a broadcast position back
  into coordinates; it is independently useful for rendering a snapped
  marker or replaying a matched trace.
- Thread benchmarks (`npm run bench:pulsemesh:threads`). The full receive
  path — decode, decrypt, parse, verify — costs about 0.09 ms, and a
  record addressed to an unguessable tag is rejected at over a million
  per second before any crypto runs. Answers two of the threads spec's open questions: catch-up
  availability depends on how many providers a joiner asks (3 → 95%,
  8 → 100%) rather than on audience size, and catch-up can never recover
  more than `THREAD_MAX_AGE` of history.
- PulseMesh benchmark and simulation harnesses
  (`npm run bench:pulsemesh:all`). Measured: per-peer bandwidth is flat in
  peer count (25 KiB/min at 10, 50 and 150 peers) and linear in contributor
  density; convergence under 15% packet loss is ~12 s regardless of mesh
  size; a 200/s hostile flood lands zero records for 461 ms of defender
  CPU; half the mesh vanishing loses no records. The M5 measurement runs a
  trajectory-reconstruction attack against the record set: over 12 seeds
  the cadence profile leaks 84.6–100% of a courier's driven route up to
  moderate density (anonymity set ~1) while the reticent profile holds at
  7.7% — median and worst case — everywhere, at 4–11× fewer emissions.
  Cadence remains the faster detector where routes are public, so the
  measurement validates the threads spec's publicity test rather than
  replacing both profiles with one. Results in
  [docs/pulsemesh-benchmarks.md](docs/pulsemesh-benchmarks.md).
- Anti-entropy digest elision: a PMG1 may carry the requester's 12-byte
  zone fold, and a responder whose fold matches answers with a 12-byte
  PMN1 instead of a full digest. Elides 100% of digest traffic in a
  converged zone and correctly never fires in an actively driven one,
  where the digest is the cost of genuine disagreement rather than waste.

- PulseMesh in the browser and on a phone. `npm run build:browser` now
  emits `pulsemesh.browser.js` (106 KB — codecs, store, aggregation,
  validation, provider), `pulsemesh-threads.browser.js` (46 KB), and the
  libp2p transport as a separate 2.5 MB entry point loaded on demand, so
  a page that consumes live traffic never pays for a transport it does
  not use. The OSM demo (`examples/osm-geo`) can run a mesh in-tab with
  no infrastructure, or join a real one with `?keeper=<multiaddr>`;
  verified live in a browser, where 45 gossiped contributions moved a
  route off a jammed stretch entirely.
- `rangefind/pulsemesh/mobile` — the app-side contributor, wired into the
  Android app through the WebView bridge, the engine interface and the
  location feed. Contribution is off unless explicitly enabled, pauses
  below 20% battery unless charging, and defaults to the reticent
  profile, because a phone's owner did not sign up to publish a
  trajectory.
- Thread discovery over the DHT (`thread_discovery.js`): `provide` and
  `findProviders` on the rendezvous key derived from the link, so a
  subscriber finds the thread's peers with no host, mailbox, or bootstrap
  address anywhere in the capability. Closes the last open row of the
  threads conformance checklist. Every DHT call is deadline-bounded — the
  underlying `provide` waits on closest peers that a small mesh may never
  supply, and a publisher cannot stop publishing because a routing table
  is thin.
- Bootstrap signature verification moved from `node:crypto` to the same
  WebCrypto Ed25519 path the thread channel uses, so one implementation
  covers Node, browsers and mobile — and neither browser bundle
  references a Node built-in.

### Added (identity bonds)

- PulseMesh identity bonds (§5.4, `src/pulsemesh/bond.js`): a per-peer,
  per-day memory-hard admission proof that moves the anti-Sybil cost off
  the record path. The misalignment it fixes: every defense punishes a
  peer — trust ledger, rate limits, `min(raw, sources)` — while
  proof-of-work charged disposable records, and peer identities were
  free, so a ban cost its target nothing. A bond (Momentum birthday
  puzzle; 256 MiB table, ~2 s desktop mint, three-hash ~0.9 µs verify at
  the default `BOND_BIRTHDAY_BITS` 44) is presented once per session as
  PMA1 on `/rangefind/pulsemesh/1/bond`, bound to the connection's
  peerId. Records from bonded peers are proofless (proofType 3):
  contributions and hazard reports publish at the tap, the record
  returns to 42 bytes, and a trust-ledger ban forfeits the bond. The
  memory-hardness refuted at record cadence (benchmarks §14.2) inverts
  at identity cadence: the once-daily table is large enough that a
  24 GiB GPU fits 96 concurrent solvers instead of ~98,000 (§14.5).
  `network.mintBond()` mines chunked/abortable and auto-presents to
  every current and future peer; validator rule 5 accepts proofType 3
  iff the delivering peer (gossip, forward, or snapshot provider) holds
  a live bond. Bonds are the ONLY wire admission: per-record
  proof-of-work is removed outright (the protocol is experimental with
  no deployed legacy) — `minePow`/`minePowChunked`/`verifyPow`,
  `POW_DIFFICULTY` and `INCIDENT_POW_MULTIPLIER` are gone, proofType 1
  is burned and never accepted, keepers mint at startup, and the §13
  wire vectors are proofless (a PMC1 is 43 bytes framed, PMA1 is 26).

- PulseMesh over LoRa (§16, `rangefind/pulsemesh/lora`): the mesh's
  first non-IP transport — Meshtastic-class radios, where the 42-byte
  record designed for privacy turns out to be exactly what a duty-cycled
  0.3–20 kbps channel needs (five contributions per 237-byte frame).
  `createLoraNetwork` implements the five-verb MeshNetwork over a duck
  radio: gossip-only by construction (the sync family cannot fit),
  topics derived from record cells (the RF footprint is the scope),
  spoofable radio senders admitted as rate-limited `lora:` pseudo-peers,
  and an airtime scheduler that buys incidents before thread updates
  before statistics inside a bytes-per-minute budget. `createLoraBridge`
  joins a radio segment to the IP mesh by §5.4 hop-vouching: it
  validates every record off the air against its own static map and
  republishes survivors under its own bond — the IP side never knows
  LoRa exists — while shuttling sealed thread frames it cannot read
  (uplink always; downlink for operator-held links), downlinking
  accepted incidents, and muting provably-lying radio senders. Tested
  against simulated radio physics; the real Meshtastic binding is a
  documented ~20-line shim (docs/pulsemesh-lora.md). MeshNode gained an
  `onRecordAccepted` tap for gateways.

- PulseMesh ban forfeiture and propagation (§8.4): a trust floor
  reached through first-hand provable evidence (rules 10–12) revokes
  the delivering peer's bond, refuses re-registration for the bond's
  remaining lifetime, and gossips a 49-byte PMX1 naming the peer by
  hash. Remote receivers treat announcements as testimony — accepted
  only from bonded deliverers, deduplicated, rate-limited, capped —
  and at `BAN_MIN_SOURCES` distinct corroborators apply one bounded
  trust penalty (`BAN_REMOTE_PENALTY`), sized so testimony alone
  down-weights but never revokes, while testimony plus a single
  first-hand violation forfeits (1000−375−500 ≤ 250). Bond rollovers
  are now phase-staggered per peer (an unphased bucket expired every
  bond on earth at the same UTC instant). Docs corrected alongside:
  forfeiture is per-receiver (the ledger is local by design) and the
  Sybil bound is throughput (~66k mints/core-day), not GPU-slot
  capacity — bonds are a toll; corroboration is the defense.

- PulseMesh read-only consumer mode (§11.6): `MeshNode({ readOnly })`
  and `createMobileMesh({ readOnly })` consume without a bond and
  without gossip membership — the browser-at-home mode. A read-only
  node tracks zones, never subscribes, never publishes (it throws), and
  converges through the padded anti-entropy pull path (PMG1 → PMQ1 →
  PMS1) against bonded peers on tick(); a real-TCP wire test proves
  bondless convergence to a byte-identical zone digest. Staying out of
  the gossip mesh is load-bearing: an unbonded relay's deliveries would
  be ignored by every bonded receiver, so joining would only punch
  holes in other peers' delivery paths. The thread channel is fully
  available in this mode: thread records are authenticated end-to-end by
  the thread key, so a read-only home viewer subscribes to the derived
  thread topics and verifies updates itself, with zero bonds in either
  direction (proven by a real-TCP test).

### Fixed

- Build sorted numeric and boolean doc-value indexes with fixed-width spill
  runs and a bounded k-way merge instead of retaining one JavaScript object per
  matching document. Brazil-scale shards no longer exhaust the V8 heap when
  entering `doc-value-sorted`; tie-aware pages and the published format remain
  unchanged. Sorted-page pack bookkeeping is append-only, and large directory
  encoders now use bounded byte chunks instead of boxed byte arrays.
  External-sort chunk and fan-in tuning is excluded from resume compatibility
  fingerprints so upgrading or retuning does not discard completed stages.

- Stream dense document-pointer tables to disk while hashing them in bounded
  batches. Very large corpora such as Brazil no longer fail after doc packing
  with Node's `ERR_OUT_OF_RANGE: data is too long`, and avoid retaining the
  entire multi-gigabyte pointer table in memory.

- (superseded) The draft per-record proof-of-work was first made
  non-blocking (`minePowChunked`, sliced mining with a wall-clock
  budget), then removed entirely with the rest of per-record PoW when
  §5.4 bonds landed; the sliced-mining technique lives on in the bond
  solver, which never blocks its host's thread.

- PulseMesh provider no longer filters its live states by the areas the
  routing engine has already fetched. The areas are the leaves fetched so
  far, so filtering to them hid every jam outside that set and defeated
  the engine's context-expansion mechanism — the one that makes jams
  exact. Areas now drive cell fetching only. An oversized leaf bbox also
  no longer rasterizes without bound.
- PulseMesh sync-stream framing no longer misparses a length prefix split
  across TCP segments. The assembler used a varint reader that cannot
  distinguish "incomplete" from "complete", so a 128-byte payload's `80
  01` prefix read as length 0 on its first byte — framing an empty
  message and silently discarding the real one. Every snapshot response
  has a multi-byte prefix, so this affected recovery on any connection
  that fragmented.
- `provider.aggregates()` is genuinely read-only. It was applying §8.4
  trust feedback on every call, so a UI polling it to draw jams walked
  every delivering peer toward a trust bound and silently changed the
  weights the next route was computed from. Feedback now happens only on
  the routing path.
- PulseMesh declared `libp2p@^3` as a peer dependency while the adapter
  is written against — and only ever tested against — the v2 duplex
  stream API. Anyone following the manifest would have installed a major
  the code cannot run on. Peer ranges now match what CI exercises.
- A sync responder can no longer be pinned by a peer that opens a stream
  and then goes quiet: reads have a deadline. Keepers, whose whole job is
  answering, were the cheapest target.
- `network.close()` is a real teardown — it unsubscribes its gossip
  topics (the host outlives the network by contract, so leaving them
  subscribed kept it in every PulseMesh mesh) and clears pending
  forwarder timers that would otherwise fire into a stopped host.
- The provider caps total cells per fetch, not just per area, and issues
  the split snapshot requests concurrently. A long route could otherwise
  serialize 60+ round trips inside a single `route()` call. A bbox
  crossing the antimeridian is no longer silently dropped.

## 0.4.7 — 2026-08-05

### Performance

- National geo builds now preload hot code-store fields instead of falling
  back to millions of tiny random reads when the complete store narrowly
  exceeds its memory budget. OSM builds use a bounded 3 GiB complete-store
  budget and a larger capsule-page cache.
- Geo leaf gzip work uses the ordered builder worker pool, capsule page indexes
  are preloaded and read in monotonic batches, and category-cell generation no
  longer allocates per-leaf dedupe maps, joined keys, or one buffer per route.
  Geo stages also report coordinate, leaf, external-sort, and merge progress.

### Added

- Static routing and itinerary planning (`rangefind/route`, `rangefind/route/node`,
  `rangefind/route/build`): a CRP/MLD route graph (`rfroutegraph-v1`) built from
  OSM road networks into range-addressed, content-addressed cell and overlay
  objects. Point-to-point driving routes with geometry and named steps,
  travel-time matrices, and multi-stop itinerary optimization (Held-Karp /
  2-opt), all exactly equal to a full-graph Dijkstra and served from static
  files with a bounded fetch set per query. The car profile honors directional
  `maxspeed`, surface and smoothness degradation, access filters, and
  single-via-node turn restrictions (compiled into the topology by via-node
  expansion at build time). Sharded builds share one top overlay and route
  identically to monolithic builds. Boundary-clique domination pruning and
  breadth-wise batched path unpacking keep per-query transfer around 1.3 MB
  and geometry unpacking to a handful of request waves on the Quebec extract.
  Single-via-way restrictions expand their via chain with path memory, the
  built-in HTTP adapter (`openRouteGraphUrl`) serves browsers with Range
  requests and 200-fallback slicing, and TypeScript declarations ship at
  `rangefind/route`. Car, bike, and foot profiles each build their own
  index, with junction penalties (signals, stops, give-way, level
  crossings) folded into edge weights. Time-of-day bucket metrics store one
  exact overlay set per bucket (per-class time factors, day/hour rules,
  `departureTime` selection) with per-bucket results equal to a reference
  Dijkstra on the scaled graph. `alternatives: k` computes diverging routes
  by penalized re-search over already-fetched objects, and `liveWeights`
  re-ranks candidates and adjusts ETAs with per-edge factors keyed by
  stable `leaf/edgeIndex` ids tied to the build epoch — the consumption
  path for CDN-published or peer-to-peer traffic deltas. Live traffic is
  consumed through a generic pluggable provider contract
  (`route({ live: provider })`): states keyed by physical directed-segment
  ids (`leaf/polyline/direction`, shared by all approach copies via the
  geometry dedup) with confidence/age blending, verified closures, and
  incident penalties; the search runs under the live metric by pulling
  referenced cells into the context set and suppressing stale overlay
  shortcuts through them, exact wherever states exist and static
  elsewhere, with graceful degradation on provider failure or epoch
  mismatch. `createStaticLiveProvider` ships as the reference
  implementation; `docs/pulsemesh.md` specifies the first planned
  network provider — a privacy-preserving P2P mesh — and
  `docs/pulsemesh-protocol.md` pins its implementable wire protocol
  (record/digest/snapshot byte layouts, topic grammar, bin tables, a
  deterministic integer aggregation algorithm, numbered validation
  rules, contributor/consumer state machines, and executable-verified
  test vectors). Turn-angle costs
  ship via full junction expansion into an edge-based graph (per-approach
  junction copies with bearing-priced turns, left costlier than right,
  u-turns penalized), with via-node restrictions as exact per-approach
  filters; multi-via-way restrictions resolve through the union of their
  via ways. An A/B-benched performance round (correct bidirectional stop,
  membership caching, copy-contiguous node order, geometry/topology split
  with canonical polyline dedup, a deeper default hierarchy, speculative
  single-wave context fetch, same-file range coalescing, progressive
  coarse geometry, and shared-context one-to-many matrices) cut Quebec
  cold queries from ~110–126 ms to ~65–76 ms at 25 ms simulated RTT,
  transfer from ~4.5 MB to ~3.3–3.6 MB, and 5-stop itineraries from
  ~750 ms to ~410 ms, with exactness re-verified at every step. The OSM
  map demo gains a full Directions experience (origin/destination
  autocomplete and map picking, alternatives, time buckets, multi-stop
  itineraries, search-along-route against the live planet index, and a
  Route X-Ray fetch receipt), computed entirely in the browser from
  static byte ranges. The Android app can preload a whole region's route index
  onto the device, refresh it, or delete it, and route from it with no network
  at all — served back over a loopback socket, since WebView's request
  interception cannot answer a Range request honestly. Route cells carry the
  posted speed limit per edge
  (`rfroutesrc-v4`, cell v5), reported on `route().edges[]` and summarized
  per step, kept separate from the travel weight so it stays a legal limit
  rather than a modelled speed — existing indexes must be re-extracted and
  rebuilt. See `docs/route-graph.md`.
- **Wayfind**, a native Android app (`android/`): Jetpack Compose UI with MapLibre Native
  rendering, backed entirely by Rangefind static indexes — worldwide search and
  autocomplete against the public OSM index, place details, reverse-geocoded
  dropped pins, directions with alternatives and maneuver steps, and
  turn-by-turn navigation with a heading chevron, a pitched follow camera,
  posted speed limits with an over-limit warning, live alternates carrying
  their ETA delta, spoken guidance, traveled-route dimming, and snap-based
  off-route rerouting. The runtime runs
  in a headless WebView on a WebViewAssetLoader https origin, so index bytes are
  fetched by the runtime itself and never cross the JS bridge, and gzip
  inflation plus SHA-256 pack verification stay native. `RangefindEngine` keeps
  the JS host swappable. It carries Rangefind's identity — the mark keeps the
  favicon's ink/muted-track/amber-range grammar, bent into a way, and amber is
  reserved for the destination. See `android/README.md`.
- Heading-aware origin snapping (`route({ fromHeading, headingPenaltySeconds })`).
  Both directions of a road snap equally well, so rerouting a moving vehicle
  could seed the opposing edge for free and return a route starting with an
  implicit U-turn back the way the driver came. A reported heading charges the
  misaligned candidates — free within 45°, full penalty past 135°, ramped
  between so a noisy compass cannot flip the decision — and the default 60 s
  is configurable, with 0 restoring the unbiased search. Since the charge is
  paid only when the route really does turn around, and a U-turn does cost
  time, it stays in the reported total.

## 0.4.6 — 2026-08-04

### Improved

- Address enrichment now streams gzip-compressed OSM base corpora, allowing
  large global pipelines to keep raw snapshots compressed between stages.

## 0.4.5 — 2026-08-04

### Fixed

- OSM extraction now drops malformed optional way geometry instead of
  aborting an entire regional corpus on an out-of-range coordinate.

## 0.4.4 — 2026-08-04

### Improved

- Builders and corpus-wide scoring-stat passes now stream `.jsonl.gz` inputs
  directly, avoiding large temporary decompressions for static planet-scale
  indexes.

## 0.4.3 — 2026-08-03

### Fixed

- OSM extraction now invalidates cached JSONL corpora when upgrading to the
  expanded alternate-name schema, ensuring every rebuilt shard receives the
  new aliases even when its source PBF is unchanged.

## 0.4.2 — 2026-08-03

### Added

- The OSM map demo renders postal-code coverage and generic bounded-result
  geometry, fits selections to their full extent, and exposes postal coverage
  metadata without changing the marker behavior for ordinary points.

### Improved

- OSM search now splits multi-value alternate names, indexes documented name
  variants and their language-qualified forms deterministically, removes
  folded duplicates and metadata-only `name:*` tags, and retains more useful
  aliases without allowing tag order to exhaust the bounded budget. Exact
  multi-word aliases receive phrase scoring and population-aware suggestions;
  brand-only and language-only features also expose a useful searchable name.

## 0.4.0 — 2026-08-02

### Added

- `searchAlongRouteOsm` accepts encoded polylines or GeoJSON and performs
  client-only route-corridor search. Results expose cross-track distance,
  forward progress, bearing, route rank, and the closest rejoin point.
- OSM queries understand reusable accessibility, service, payment, and
  open-now constraints. Typed facets prune the static index before a
  conservative client-side verification of result details and
  `opening_hours` in the caller's time zone.
- Searchable OSM areas retain simplified encoded geometry and true polygon
  centroids; the map demo renders those shapes directly from geo capsules.
- `createRangefindMapsAdapter` provides a typed, promise-based migration facade
  for common Google Places and Geocoding request shapes while retaining native
  Rangefind metadata. A complete migration guide covers real use cases,
  deployment/cache requirements, attribution, benchmarks, and parity limits.

### Performance

- Multi-resolution category-cell indexes can include a compact wildcard
  occupancy lane. Route text and brand searches use it to fetch only point
  ordinals intersecting the corridor, while typed category searches retain
  their narrower per-category routes.
- Corridor pruning keeps overlapping segment boxes separate instead of
  collapsing diagonal trips into one large bounding rectangle. Existing
  grouped and multipart byte-range reads batch the resulting cell, leaf, and
  capsule ranges without a route service or additional infrastructure.
- The production Maps benchmark now covers open-now evaluation, compound OSM
  constraints, route-corridor quality/range budgets, rejoin metadata, and
  geometry coverage.

## 0.3.24 — 2026-08-02

### Added

- OSM documents retain useful place metadata in a compact `details` object,
  including opening hours, contact, brand/operator, cuisine, accessibility,
  service, payment, capacity, access, and knowledge-reference fields.
- `rankPrior` generalizes the crawler's numeric relevance prior for any static
  corpus. OSM indexes derive a conservative normalized `prominence` signal
  from population, place hierarchy, capital status, and reference metadata.
- OSM autocomplete returns structured, cursor-aware predictions with matched
  ranges and a reusable selection/shard payload.
- `reverseGeocodeOsm` provides bounded, address-first reverse geocoding over
  the existing client-side geo index. Decimal-coordinate map searches reuse
  the same path, while callers can still request a raw coordinate marker.
  Results expose formatted address components and accuracy/location types,
  support result filters, and fall back to a bounded locality lookup on demand.
- The strict production Maps benchmark now covers urban address points,
  interpolation ranges, international and sparse rural coordinates, and an
  uncovered-ocean zero-result guard. It also measures target rank/mean
  reciprocal rank, structured autocomplete, reverse result semantics, result
  uniqueness, and cursor-edit behavior.

### Performance

- Reverse geocoding derives the intersecting shard scope from root bounding
  boxes and applies the address facet before nearest-neighbor traversal, so it
  never opens every regional shard or returns a globally distant address.
- Category-cell geo searches defer bitmap and doc-value selection until after
  exact cell membership removes covered facets. Repeated nearby and viewport
  queries therefore stay fully memory-resident instead of discovering a lazy
  filter-manifest dependency on their second execution.
- Category-plus-name-plus-locality searches prove a trailing locality through
  root authority, search only its shard for the distinctive venue name, and
  verify the result's structured locality. Queries such as `parc larochelle
  repentigny` no longer fall back to multi-region text fan-out.
- Category-looking locality names such as `Park City` and `Bar Harbor` prove
  the whole authority interpretation before starting the split locality
  search, preventing an already-losing probe from fanning out globally.
- The Maps benchmark adds a strict edge profile for ambiguity, multi-word and
  hyphenated localities, joined/spaced venue names, same-name disambiguation,
  and authority-proven empty results.
- A strict production Maps profile now exercises autocomplete-selection
  journeys, text and nearby search, forward geocoding, inline place fields,
  viewport restrictions, native script, typos, and bounded empty results.
- Unanchored exact landmarks and name-plus-locality queries use root authority
  to stay inside the relevant shards, including one-edit venue typos.
- Federated search collapses duplicate stable ids emitted by overlapping geo
  shards, and civic routing continues past valid-but-wrong locality suffixes.

## 0.3.23 — 2026-07-31

### Performance

- Mixed sharded roots use child-level facet capabilities for anchored and
  viewport category queries, so rebuilt regions immediately get exact
  category pruning without waiting for every sibling shard to upgrade.
- Single-level geo trees skip category-cell directory reads because their
  cached root already carries every leaf summary; multi-level trees retain
  the category-cell lane where it can bypass branch fan-out.
- Category-like named destinations on legacy shards use one bounded local
  relevance page after exact authority misses, avoiding global fan-out and
  province-wide coordinate reads for searches such as airports and typoed
  universities.

### Fixed

- Intersection queries prefer an existing OSM intersection document and add
  the resolved locality to street fallback searches, improving both exact
  crossing quality and regional routing.

## 0.3.22 — 2026-07-29

### Added

- Geo indexes can embed compact display capsules in their leaf pages, allowing
  viewport and nearest-result lanes to return complete map rows without
  opening document payload packs.
- Configurable multi-resolution category-cell indexes route a geo field and
  facet directly to exact matching point ordinals. The builder uses a bounded
  external sort and publishes range-addressed cell blocks and directory pages.

### Performance

- Category-cell geo queries select the finest safe grid level for the current
  viewport or radius, avoid global facet bitmap and doc-value reads, and fetch
  fewer point-pack ranges than ordinary geo-tree traversal.
- Cold category-cell and geo-tree root reads start concurrently instead of
  forming a serial network waterfall.
- OSM map intents use bounded locality/category probes, and grouped immutable
  object reads share the generic multipart range-request path.

### Fixed

- Address interpolation remains available across mixed index generations,
  including when the newest generation has no interpolation sidecar.

## 0.3.21 — 2026-07-27

### Added

- A reproducible 18-case production benchmark covers common interactive map
  searches: autocomplete, localities, landmarks, brands, categories, nearby
  searches, addresses, postal codes, viewports, typos, and Unicode names.

### Performance

- Anchored landmark and brand queries use exact root authority as a gate, map
  coverage to select the local shard, and a bounded text window whose embedded
  coordinates are ranked locally. Common POI names no longer pay global
  locality probes or scattered lat/lon doc-value reads.
- Explicit viewport category queries use their exact OSM `type` facet as the
  geo predicate. Facet-aware geo browse prunes irrelevant cells and stops when
  the requested page is full instead of materializing the viewport's entire
  text/geo intersection.
- Root autocomplete fetches bounded lexicon segments and its first ranked
  authority candidates concurrently, removing serial network waves from broad
  prefixes.
- Geo doc-set construction passes safe facet summaries into branch and leaf
  selection, avoiding spatial pages that cannot satisfy the query filters.
- Ambiguous locality authority matches route through the highest-weight exact
  bearer instead of opening every same-named region.

### Fixed

- A unique one-edit typo in the first or last category token is corrected
  before locality routing, so queries such as `cinma laval` remain bounded and
  return cinemas instead of falling into a global fuzzy fan-out.
- Anchored one-edit landmark queries use the local shard's normal typo
  correction and return the nearby corrected result.

## 0.3.20 — 2026-07-27

### Performance

- Nearby OSM category searches pass their resolved `type` facet into geo
  traversal when safe facet summaries are available. Queries such as
  "gas station near me" skip geo cells that cannot contain fuel stations
  instead of downloading every spatial page in the radius.

## 0.3.19 — 2026-07-27

### Performance

- Sharded geo routing supports wrapped coverage bounds across the
  antimeridian. Dateline-spanning regions stay narrow instead of becoming
  effectively global and joining unrelated nearby or viewport searches.

## 0.3.18 — 2026-07-27

### Performance

- Browser searches batch scattered dense document-pointer reads into exact
  multipart HTTP range requests. Supporting origins such as Cloudflare/R2 can
  return dozens of non-contiguous pointer records in one response without
  overfetching the gaps; other transports retain the single-range fallback.

## 0.3.17 — 2026-07-26

### Performance

- Geo browse and text-distance lanes no longer fetch the large doc-values
  manifest when selected facet and boolean filters are fully covered by
  filter bitmaps.

## 0.3.16 — 2026-07-26

### Performance

- Filter bitmaps can now target selected values on high-cardinality facets
  within an explicit memory budget. The OSM integration materializes its
  curated `type` categories, so exact nearest-category searches use one small
  bitmap range instead of hundreds of scattered doc-value requests.

## 0.3.15 — 2026-07-25

### Changed

- Sharded roots now preserve unsigned-facet-summary support on each shard
  descriptor. A locality-scoped OSM category query can use safe geo/facet
  pruning as soon as its shard is rebuilt, without waiting for every legacy
  shard in a mixed planet root.

## 0.3.14 — 2026-07-25

### Fixed

- Connectorless OSM category/locality queries use a successful root-authority
  miss to skip the global whole-phrase place probe, then scope the category
  search to the locality's owning shard. Queries such as `cinema laval` no
  longer open every shard manifest before searching Québec.
- Facet summary words now remain unsigned when bit 31 is set. Block, doc-value,
  and geo-cell summaries no longer serialize an entire 32-value word as zero;
  older indexes fail open instead of using potentially corrupt facet summaries.
  Rebuilt sharded roots advertise the safe encoding and use exact OSM `type`
  facets to prune sparse-category nearest searches before fetching geo pages.

## 0.3.13 — 2026-07-25

### Changed

- Sharded-root autocomplete now uses a paged `rflexicon-v2` directory and
  range-packed lexicon segments. Each segment carries direct authority-pack
  pointers, so a cold suggestion or locality lookup fetches only the small
  routing page, matching lexicon segment, and required authority ranges
  instead of downloading corpus-sized lexicon and authority-directory roots.

## 0.3.12 — 2026-07-24

### Fixed

- Suggest-routing shard prefixes no longer split astral Unicode characters at
  UTF-16 depth boundaries. The disk metadata spool now stores shard keys as
  lossless big-endian UTF-16 BLOBs, preserving embedded NULs and exact binary
  order instead of letting SQLite text conversion corrupt or reorder them.

## 0.3.11 — 2026-07-24

### Fixed

- Disk-spooled suggest-routing metadata is now externally ordered by an
  indexed UTF-16 sort key before directory and lexicon emission. Physical
  partitions emitted out of order no longer abort finalization, and sorting
  remains bounded by SQLite's file-backed page cache.

## 0.3.10 — 2026-07-23

### Fixed

- Planet-scale root suggest-routing finalization now spools physical shard
  metadata to disk and streams both directory pages and the lexicon root from
  that spool. Deep prefix partitioning no longer retains millions of directory
  and autocomplete-shard objects in the V8 heap.

## 0.3.9 — 2026-07-22

### Fixed

- Root suggest-routing finalization now reuses its partition metadata array
  when resolving content-addressed pack names instead of duplicating every
  directory entry at once. Planet-scale merges no longer exhaust the V8 heap
  after all sidecar streams and packs have already completed.

## 0.3.8 — 2026-07-20

### Added

- **Data-driven OSM category lexicon**: the query planner's category
  vocabulary now comes from the index instead of a hardcoded seven-word
  list. Sharded OSM builds merge every shard's `type` facet dictionary,
  join it with a bundled multilingual alias table (French forms, English
  synonyms, irregular plurals), and embed the result in the root manifest
  (`category_lexicon`); single indexes read their lazy `type` facet
  dictionary at query time; indexes without either fall back to a bundled
  canonical OSM vocabulary of ~180 common type values. Any type the corpus
  holds — "cinema", "bakery", "boulangerie", "dépanneur", "movie theater"
  — now gates as a category, so bare category words become nearest-first
  searches around the anchor instead of leaking into locality resolution.
  The artifact vocabulary is pruned to gateable values: a frequency floor
  (default 250) drops the freeform tail of one-off tag strings (a planet
  corpus holds ~37k distinct type values, ~1.2k of them real categories),
  and place/address types are excluded so "Quebec City" and "Miami Beach"
  keep resolving as the cities they are — backed by a whole-surface
  locality probe on every connectorless category/locality split.

### Fixed

- **Bare category words no longer teleport the map to a same-named
  village.** "cinema" was not in the old hardcoded category list, passed
  the locality gate, and resolved `osmLocalityExact` to an actual village
  named Cinema — the demo map then flew across the planet. Category words
  are now recognized via the lexicon and never enter locality resolution.
- **Category-first place names resolve as places again.** Connectorless
  category-first queries ("Bar Harbor", "Park City", "Market Harborough")
  give the whole surface one shot at resolving as a locality before being
  split into category + locality; "cinema in Nice" states its intent
  explicitly and skips straight to the split.

## 0.3.7 — 2026-07-20

### Fixed

- Suggest-set sidecars are now written as bounded concatenated gzip members,
  avoiding V8's maximum string length on multi-million-key OSM shards.

### Added

- **Location-anchored OSM search** (`params.near`): callers pass an advisory
  anchor (user location or map viewport center) and the query cascade uses
  it wherever the text itself names no place. Bare categories and near-me
  phrasing ("pharmacy near me", "restaurants", "cafés autour de moi")
  become a nearest-sorted search around the anchor — against the live
  planet index, "restaurant" drops from a ~4,700-request unroutable
  fan-out to ~95 requests / 2MB with walkable results first. Plain text
  tries a proximity-boosted search scoped to the anchor's 50 km radius
  (geo routing opens only the shards under the caller) and falls back to
  the global cascade when the local page is empty. Explicit intents —
  named localities, streets, an explicit `geo`, suggestion shard hints —
  always outrank the anchor. The map demo now feeds this anchor from
  browser geolocation (adopted silently when permission is already
  granted, one tap on the locate control otherwise) with the map center
  as fallback, and labels anchored results "near you" / "near map view".
  Dragging or zooming the map re-runs a view-anchored query for the new
  area (debounced, and only on genuine user gestures, never the app's own
  post-search recentring); resolved-place queries stay put.

## 0.3.6 — 2026-07-19

### Added

- **Root suggest routing for sharded indexes** (`rfsuggestroute-v1`): every
  shard's authority autocomplete lexicon merges into one root-level artifact
  (`writeSuggestRoutingIndex`, with `writeShardSuggestSet` sidecars for
  pipelines that reclaim shard files). A federated `suggest()` is answered
  from the root in a couple of small range reads instead of opening every
  shard's authority sidecar — on the live 310-shard planet index a cold
  keystroke was ~1,600 requests / 23MB (≈30s on a throttled 4G phone).
  Merged entries keep federation provenance: suggestion rows store shard
  ordinals (authority codec v3), so `suggestions[].shards` still names the
  region(s) that back each suggestion. Fail-open: any missing or broken
  artifact falls back to the per-shard fan-out.
- **`engine.authorityLookup(surface)`**: exact-surface lookup against the
  authority autocomplete lexicon (single or sharded root). The OSM
  integration uses it to resolve locality names ("Berlin", "montreal")
  straight from the root artifact and scope the follow-up search to the
  shards that own the name, replacing the global fan-out that opened ~200
  shards per cold locality query. Advisory: a scoped miss retries unscoped.
- **OSM locality enrichment**: extraction now stamps every document that
  lacks a mapper-provided `addr:city` with its actual municipality —
  administrative-boundary containment first (`boundary=administrative`
  relations at admin_level 8/7, assembled into polygons from a new relation
  pass in the PBF reader), nearest place node as fallback where boundaries
  are missing or clipped at extract edges. The derived name lands in the
  `city` display field and the indexed `address_search` text, so
  brand-plus-town queries ("jean coutu rosemère") match the POI even though
  the OSM node carries only name and amenity tags; the formatted address
  and the authority address lane stay untouched. Luxembourg: 99% of
  documents carry a city after enrichment. Extraction schema v9
  — cached corpora re-extract, and planet deployments should regenerate
  scoring stats alongside the rebuild (locality terms change document
  frequencies corpus-wide).

### Fixed

- **Phantom approximate totals**: early-terminated search lanes floored
  `total` at the requested page size, so a block-budget stop that found no
  eligible documents reported "5 results" with an empty page ("st hubert
  terrebonne" on a Quebec OSM index). Approximate totals now report the
  eligible documents actually seen — zero stays zero, which also lets the
  federated deferred-typo retry and the OSM locality cascade react to
  genuinely empty pages instead of trusting invented counts.

## 0.3.5 — 2026-07-18

### Added

- **Mobile browser benchmark**: `scripts/osm_mobile_bench.mjs` runs the real
  browser bundles in headless Chromium with CPU and network throttling
  against a live deployment, reporting cold/warm latency, requests, transfer,
  and JS heap per demo lane.
- **Demo-flow benchmark lanes**: `scripts/osm_remote_bench.mjs` now covers
  every OSM demo flow — category/street/civic intents, postal codes, map-area
  boxes, discovery orbit, zero-result typo, and address suggest — with a
  retrying, concurrency-capped fetch so large runs survive transient network
  failures.

### Changed

- **Sharded cold queries**: filtered text search skips the per-shard
  doc-values manifest when filter bitmaps cover the plan, and number/geo
  verification loads doc values lazily so only shards with real candidates
  pay for them; typo correction defers to a second fan-out that only runs
  when the merged page is empty; conjunction tails resolve by candidate-doc
  lookup once minShouldMatch can no longer be reached by unseen documents;
  the expanding nearest front queries wider batches and prefetches shard
  engines. On a live 310-shard planet index, locality queries dropped from
  19.2s/109MB to ~3s/23MB, two-term city queries from 8.6s to 1.5s cold
  (736ms to 9ms warm), and nearest-first from 9.3s to under 1s.
- **OSM locality and address resolution**: common locality names resolve to
  their populous bearer ("laval" → Laval, Québec) through a population-gated
  retry; street and civic-address queries resolve inside the locality's own
  shard instead of a full-index fan-out, and civic addresses match through
  their structured house-number and street fields (13.2s → ~2s cold).

### Fixed

- **Range-ignoring CDN responses**: `fetchRange` now tolerates servers that
  answer a Range request with 200 and the full object — small bodies are
  sliced, large ones aborted and retried — instead of failing the query or
  downloading a multi-hundred-MB file onto a phone.
- **Doc-range top-k proof bound**: the doc-range-aware stability proof capped
  remaining potential with the current block's max impact; doc-id-ordered
  postings could hide a higher-impact posting in a later block and wrongly
  prove a top-k missing the best documents. The bound now uses the
  remaining-suffix maximum.

## 0.3.4 — 2026-07-17

### Fixed

- **Byte-stable static hosting**: range-addressed packs and dense pointer
  tables now use a `.bin.gz` suffix so GitHub Pages and similar static hosts
  do not transparently gzip `.bin` responses and invalidate byte offsets.
  Existing manifests and `.bin` indexes remain readable.

## 0.3.3 — 2026-07-17

### Added

- **Prefix-aware sharded routing**: root text routing carries prefix matches
  into autocomplete and propagates shard hints through OSM locality search,
  avoiding unnecessary shard fan-out while preserving fail-open behavior.
- **OSM discovery diagnostics**: the map demo exposes query trace receipts and
  discovery-orbit status alongside improved geo intent parsing and navigation.

### Changed

- **Bounded routing finalization**: gzipped shard term sets are streamed and
  merged through a min-heap with bounded reusable buffers instead of eagerly
  inflating every shard vocabulary into memory. Large routing rebuilds now
  finalize without making heap use grow with the combined term count.
- **Recurring query plans**: short plans are admitted to a 128-entry LRU only
  after their second use, sharing in-flight multilingual analysis between
  concurrent searches. A 30,000-document benchmark improved 1,000 recurring
  searches by 16% (57.4 ms to 48.2 ms) while cold result-bearing queries stayed
  flat; long and one-off queries do not retain full plans.
- **Builder scoring metadata**: stable `alwaysIndexFields` metadata is reused
  across document analysis instead of rebuilding a set per field. OSM-shaped
  analysis benchmarks reduced lookup time and retained heap without changing
  full-build performance.

## 0.3.2 — 2026-07-16

### Added

- **Federated text routing for sharded roots**: `writeTextRoutingIndex`
  (`rangefind/shards`) builds a root-level term → shard-set directory
  (`text-routing/`, format `rftextroute-v1`) by enumerating every shard's
  term directory; `writeShardedRootManifest({ textRouting })` embeds it and
  `buildOsmShardedIndex` builds it automatically. The federated engine then
  opens only the shards that can satisfy `minShouldMatch` of a query plan's
  terms instead of fanning text queries out to every shard — on a 67-shard
  planet index this turns a ~1 200-request, 12–18 MB cold text query into a
  handful of shard opens. Fail-open by design: unknown shard ids are always
  searched, unroutable queries fall back to the full fan-out (per-shard typo
  correction keeps working), and routing errors never break a query.
  Responses report `stats.textRouting`.

### Changed

- **Suggest CPU**: autocomplete candidates are parsed once per authority
  shard and binary-searched per call (previously every suggest call re-walked
  and re-normalized every entry), comparisons no longer allocate, and the
  top-k pool is pruned as it grows. Warm federated suggest on a 67-shard OSM
  index dropped from ~2.5 s to ~0.1 s per call.
- **Text top-k selection**: early-terminated search lanes and the repeated
  top-k stability proofs now select the best k rows with a bounded heap
  instead of materializing and fully sorting every scored document.
- **Geo-filtered text transfer**: the geo doc-set prune now prices itself
  against the doc-value chunks the text lane would otherwise fetch (exact
  candidate leaf-page bytes vs estimated chunk bytes per verified match)
  instead of a fixed candidate-point cap, so city-radius text+near queries
  ride the well-merged geo tree pages — a live text+near+boost query dropped
  from 15.5 MB / 370 requests to 5.3 MB / 193 requests cold with identical
  results. Doc-value chunk fetches for filtered text queries also coalesce
  across each decoded posting-block batch instead of going out per block.

- **Mobile runtime** (`rangefind/mobile`): the full query engine on embedded
  JS hosts — React Native/Hermes, QuickJS, JavaScriptCore. Local indexes
  (bundled with the app or downloaded to device storage) are searched fully
  offline through positional reads on a caller-provided io adapter; http(s)
  indexes get the caching a browser would provide (bytes-bounded memory LRU
  plus an optional persistent cache adapter for content-addressed objects).
  `docs/mobile.md` covers React Native, Expo, WebView/Capacitor, Flutter, and
  native Swift/Kotlin integration paths.
- **`setInflateImplementation(fn)`** in the core runtime: injectable gzip
  inflation for hosts without `DecompressionStream` (e.g. `pako.ungzip` on
  Hermes), mirroring the existing injectable fetch transport.

## 0.3.1 — 2026-07-14

### Added

- **Per-query hook on `<rangefind-search>`**: `element.searchOptions.transform`
  — an async function receiving the params about to be sent and returning the
  params to use. Built for hybrid semantic search (embed the query, set
  `params.vector`); stale transforms are dropped when a newer keystroke wins.
- **Crawler enrichment**: `buildFromCrawl({ enrich })` runs an async hook on
  the crawled documents before indexing (embeddings, external metadata), and
  `buildFromCrawl({ config })` merges overrides into the generated config
  (e.g. a `vectors` declaration for enriched embeddings).
- **Uniform enrichment across every integration**: `enrich` accepts a
  function or a path to an ES module (default export = the hook, optional
  `config` export = overrides), so the CLI (`rangefind build <dir>
  --enrich ./enrich.mjs`), the Eleventy, Astro, and Docusaurus plugins
  (`config` + `enrich` options), and mkdocs-rangefind (`enrich:` setting)
  all expose the same capability. The Eleventy plugin also no longer warns
  on Nunjucks' internal `__keywords` shortcode marker.

- **Query CLI**: the `rangefind` binary now queries indexes as well as
  building them — `search` (facets, `--filter` facet/range/boolean filters,
  `--sort`, `--near`/`--box` geo, `--shards` scoping, `--json`), `suggest`,
  `count`, and `info` (totals, provenance, features, shard tree) against any
  local directory or http(s) index URL. Query-command failures print
  one-line messages with a failing exit code.
- **MCP server** (`rangefind-mcp`, new package): exposes any rangefind
  index as Model Context Protocol tools — `rangefind_search` (text + geo +
  facets + shard scoping), `rangefind_suggest`, `rangefind_count`,
  `rangefind_info`, and `rangefind_list_indexes` — over stdio via the
  official SDK, with structured content, read-only annotations, cached
  engines, and configured/open index access modes. Lives in its own package
  so the core keeps zero runtime dependencies.
- Crawled sites now build a title autocomplete lexicon by default, powering
  the component's `suggest` attribute and `rangefind suggest` on every
  plugin-indexed site.

### Changed

- Search params `filters` documentation and TypeScript types now describe
  the engine's actual shape (`{ facets, numbers: {field: {min, max}},
  booleans }`).
- Site crawls now index deep body vocabulary: the crawler's generated config
  sets `targetPostingsPerDoc: 128` (the corpus-scale default of 12 dropped
  most body terms on long pages, breaking multi-word site search).

## 0.3.0 — 2026-07-14

### Added

- **Release structure**: conditional package exports — node-only entries
  (`./node`, `./builder`, `./crawler`, `./config`, `./shards`,
  `./scoring-stats`, `./osm/node`, `./osm/extract`) resolve to a clear
  import-time error under browser bundler conditions instead of failing on
  `node:` built-ins. TypeScript declarations for the public surface
  (search params/responses including `shards` scoping and geo, builder,
  config, sharded roots, scoring stats, OSM integration). New
  `rangefind/osm/extract` entry exposes PBF → places JSONL extraction as a
  stable API (`extractOsmPlaces`) instead of a script path inside
  `node_modules`. `prepublishOnly` runs the full bundle + test + smoke
  pipeline.

- **Geographic index sharding** (`docs/sharded-osm.md`): a corpus can now be
  built as independently updated per-region shards that federate into one
  engine at query time. A frozen scoring-stats artifact
  (`rangefind/scoring-stats`, new `scoringStats` build config) collects
  corpus-wide document totals, average field lengths, and per-term document
  frequencies (sorted on-disk `rfdf-v1` table with spill-and-merge
  collection, resolved lazily inside parallel reduce workers), so every
  shard bakes exactly comparable BM25 impacts — a sharded index reproduces
  the monolithic build's rankings exactly. A tiny sharded root manifest
  (`rangefind/shards`) lists shard paths and coverage bboxes; the runtime
  opens shard engines lazily, routes radius/box queries to intersecting
  shards, answers nearest-first queries through an expanding shard front
  with an early-stop proof, and merges every lane (text, distance and sorted
  browse, facets, suggest, vector, hybrid). Shards compose with generational
  updates. OSM corpora get a one-call orchestrator
  (`buildOsmShardedIndex` in `rangefind/osm/node`) and a Québec-scale
  benchmark (`npm run bench:osm-shards`). Stats-frozen shards accept
  generational deltas (`build --update` with the same `scoringStats`
  artifact): impacts bake from the shared df table — no per-generation
  term-directory scan, parallel reducers retained — and a delta-updated
  shard is proven identical to a full rebuild of the final corpus, so
  region refreshes publish one small generation instead of re-shipping the
  shard. Queries scope to named shards with `shards: ["quebec"]`
  (search/count/suggest/vectorSearch; unknown names throw), resolve
  multi-level group labels from the shard entries' `groups` hierarchy
  (`shards: ["canada"]` expands to every province shard), or bypass
  federation entirely by opening a shard directory as a standalone index.
  Hierarchical roots compose: a shard entry may itself point at a sharded
  root — routing recurses, merges nest, results carry hierarchical shard
  paths, and rankings match the flat topology.

- **Manifest provenance** (`meta` config option): a free-form provenance
  block carried verbatim into every manifest (full, minimal, generational
  root, sharded root — the root defaults to the first shard's meta) next to
  the existing `built_at`. The OSM integration ships ODbL-compliant defaults
  (`© OpenStreetMap contributors`, `ODbL-1.0`, license URL, plus the RQA
  CC-BY-4.0 source when enabled) and merges caller fields — generator
  identity, source URL, upstream data version — on top via
  `createOsmIndexConfig({ meta })`.

### Changed

- **Reusable OSM integration**: OSM document normalization, compact address
  interpolation, index schema generation, map query intents, and autocomplete
  now live under the browser-safe `rangefind/osm` export. Node-only RQA
  ingestion and index publication are exposed through `rangefind/osm/node`.
  Fixture scripts and the map demo are thin consumers of those APIs, while the
  underlying Rangefind pack format stays unchanged and no OSM sidecar is added.

- **Québec civic and postal coverage**: Québec OSM fixtures now merge the
  monthly CC BY 4.0 Référentiel québécois des adresses through a resumable,
  zipped-CSV stream. A disk-indexed canonical pass collapses units and removes
  only full-address-identical OSM duplicates. Civic records stay out of BM25,
  geo browse, and autocomplete, while canonical addresses and one compact
  aggregate per postal-code/municipality pair use the zero-posting authority
  lane. A measured full run emitted 3.64M civic and 221.7k postal records into
  a 9.95M-document, 8.39 GiB index in 15m25s; posting segments and geo build
  time remained effectively unchanged from the OSM-heavy baseline.

- **Map locality intent and autocomplete overlay**: exact settlement queries
  such as `Laval` resolve the cached `place=city` record globally instead of
  ranking every address that contains the city name inside a stale viewport.
  The demo centers the locality and returns it alone. Street-plus-locality
  queries such as `Rue Hector Rosemère` resolve the town, search only the
  distinctive street token inside its radius, and collapse OSM road segments
  to one street result, avoiding common `rue` posting-budget exhaustion.
  Autocomplete overlays can escape the rounded panel without horizontal
  overflow, long labels wrap, and pending suggestion work is cancelled on
  Enter or Escape. Non-numeric street prefixes now group civic candidates by
  street and municipality, promoting canonical street suggestions without a
  new sidecar; numeric address autocomplete remains unchanged.

- **Canadian postal-code query normalization**: compact forms such as
  `J7B1Z5` are canonicalized to the already-indexed `J7B 1Z5` token form
  before search, count, autocomplete, and exact-address planning. This fixes
  postal-only, category-plus-postal, and full-address searches without an
  index rebuild, duplicate postings, or additional range requests.

- **Compact OSM address interpolation**: numeric `addr:interpolation` ways now
  become one range document per compatible anchor segment instead of millions
  of inferred documents. Street-first 16-number authority buckets locate a
  candidate with zero posting decodes; the runtime verifies range/parity and
  computes the inferred point by distance along a compact 1e-6-degree polyline.
  Explicit address objects always take precedence. On Quebec, 442,979 ways
  produced 339,169 range docs covering about 9.15M possible addresses, growing
  the corpus only 5.9%. The authority shard encoder now uses bounded byte chunks
  rather than a giant JavaScript array, and address-like demo searches bypass a
  stale map viewport so exact addresses cannot be hidden. Address autocomplete
  now resolves a typed house number against lexicon-completed street/locality
  tails after three street-token characters, reusing the compact range lane
  instead of publishing millions of inferred suggestion strings.

- **OSM category-locality search**: the map demo recognizes pharmacy queries
  such as `Pharmacie Rosemère`, `Rosemère pharmacie`, and `pharmacy rosemere`.
  It resolves the exact settlement through the place facet, maps the localized
  category to the indexed OSM term, and runs a distance-sorted geo query. This
  finds POIs whose OSM records omit `addr:city` without fabricating locality
  tags or rebuilding the index; locality resolutions are cached.

- **National-scale address search and posting reduction**: the OSM fixture now
  retains complete address-only nodes and ways, publishes structured address
  fields, and builds canonical full, locality, postcode, and street authority
  keys. Normalized, reordered, and useful partial address forms use a bounded
  zero-posting exact lane before BM25. A 66.8M-document / 700.8M-posting US
  build also exposed two reducer limits: final segment directories now merge
  through 64 KiB streaming cursors instead of decoding roughly 2 GiB into V8,
  and posting blocks are zero-copy typed-array views instead of allocating one
  JavaScript pair per posting. Large posting headers now grow through bounded
  64 KiB byte chunks instead of one giant JavaScript number array. The
  zero-copy change produced byte-identical output 2.17x faster with 62% lower
  RSS on a 1M-posting encoder benchmark.

- **National-scale OSM builds**: the geo fixture now supports the full United
  States Geofabrik extract and uses resumable downloads, disk-spooled candidate
  ways, externally sorted/deduplicated anchors, and an indexed on-disk
  coordinate store. Entity-selective PBF decoding skips unused node or way
  payloads. Quebec extraction dropped from 113.7 s / 2.71 GB peak RSS to
  77.5 s / 462 MiB with byte-identical JSONL. The exhaustive benchmark oracle
  is now a bounded two-pass stream instead of retaining every point and token
  set, and geo root bounding boxes no longer use argument-spread operations
  that can overflow at national leaf counts. Posting reducers share only the
  block-filter code columns they read; on the 32.8M-place US corpus this
  changed reduction from more than 29m35s with no output to 2m41s. Dense
  document pointers use 65,536-row sequential reads instead of two reads per
  document (4m33s to 2m44s for US document packing), and 10M+ builds may
  preload a 2.25 GiB code store so geo summaries and doc-value writers avoid
  random chunk reads. Large sidecars checkpoint independently, so an
  interrupted geo/vector phase no longer repeats authority, document, and
  doc-value work. Unified autocomplete now publishes bounded, adaptive hot
  lists for broad two- and three-letter Latin prefixes in addition to every
  one-character prefix; on the full-US index this reduced mean keystroke
  latency 69% and transfer 22% while adding 54 KiB to the one-time root.

- **Unified authority autocomplete**: `suggest` fields now stream into bounded
  authority runs and are encoded directly in `rfauth-v2` packs with exact
  weights, counts, display strings, token suffixes, per-shard max-rank
  proofs, and lazy one-character hot lists. This removes the scan-wide surface
  map, the second writer-wide map, the `suggest/` pack family, suggestion
  page/branch codecs, `manifest.suggest`, and the old runtime page lane. The
  public `engine.suggest()` and build configuration remain unchanged; legacy
  title-only `rfauth-v1` indexes retain a bounded compatibility path.

### Added

- **Full-Wikipedia build path**: the wiki fixture can place its complete
  workspace on another volume with `--root`, discover and concurrently extract
  Wikimedia's ordered multistream shards, retry throttled downloads, preserve
  completed shard work across retries, and concatenate the result
  deterministically. The full English/French npm scripts use multistream
  extraction and the bounded unified authority autocomplete path.

- **Incremental publishing (Phases 3 & 4 — complete)**: every query lane now
  merges across generations. Sorted browse and text + sort merge by the real
  doc-value keys (a new `loadDocValues` helper on the engine); geo merges in
  all three shapes (box browse, nearest-first by exact distance, radius or
  boosted text search); vector search merges by absolute similarity; and
  hybrid text + vector fuses reciprocal ranks at the *merged* level, so a
  small delta generation can never hand its documents inflated per-generation
  ranks. `rangefind build --compact` folds a generational index back into a
  single index — a full rebuild that verifies every live document id from the
  old generations is present in the input before deleting the `gen-NNNN/`
  directories (and cleans up leftovers from previously failed compactions).
  `build --update` now recommends compaction once an index crosses 8
  generations or 25% tombstoned documents.

- **Static site generator adapters**: real, independently installable
  packages for [Astro](packages/rangefind-astro) (`astro:build:done` +
  `<RangefindSearch />`), [Eleventy](packages/eleventy-plugin-rangefind)
  (`eleventy.after` + a universal `{% rangefindSearch %}` shortcode), and
  [Docusaurus](packages/docusaurus-plugin-rangefind) (`postBuild` +
  `injectHtmlTags`), each running the crawler against the generator's own
  build output and copying the search component's assets in automatically.
  [Hugo](integrations/hugo) (no plugin system — a documented
  `hugo && rangefind build public` recipe plus a `relURL`-based partial) and
  [MkDocs](integrations/mkdocs-rangefind) (a real pip-installable Python
  plugin on `on_post_build`/`on_post_page` that shells out to the Node CLI)
  round out the five. Every adapter is verified end to end against the real
  tool — a real Astro/Eleventy/Docusaurus build, a Homebrew-installed Hugo
  binary, and a pip-installed MkDocs — crawling a fixture site and confirming
  the index is actually searchable through Rangefind's own runtime.

- **Multilingual analysis** (`analysis` config block, `multi-v1` profile):
  per-document language via `languageField` or script + stopword detection;
  per-language light stemmers (en, fr, de, es, it, pt, nl, sv, no, da, fi,
  ru, el, ar, hi) and stopword lists (those plus tr, pl, cs, hu, ro, id);
  script-aware folding (ß→ss, ø→o, Greek tonos + final sigma, Arabic
  harakat/alef/alef-maqsura, Hebrew niqqud, ё→е, width folding); and
  dictionary-free CJK bigram tokenization (Han/Kana/Hangul/Thai-class),
  deterministic across Node and every browser by construction — no
  `Intl.Segmenter`, no ICU dictionaries. Queries analyze in every configured
  language: the detected language drives phrases/proximity/typo, all
  languages' stems join the retrieval union under the skip-search term
  budget, and the runtime swaps to an alternate base plan when the primary
  language's stems have no postings. The profile is stored in the manifest,
  so the browser reconstructs the exact builder analyzer; `build --update`
  refuses deltas whose profile differs from the existing generations.
  Highlighting matches across languages and marks exact CJK bigram spans.
  This is now the only analyzer; a config with no `analysis` block uses the
  default profile (English plus French). The previous Latin-only analyzer
  and its module (`src/analyzer.js`) were removed, and the language-agnostic
  phrase/proximity/bundle term helpers moved to `src/terms.js`.

### Fixed

- Auto posting-codec sampling no longer loops forever when a term spans more
  blocks than the sample budget. Wikipedia extraction now honors writable
  stream backpressure, and capped body storage retains the article's true
  `bodyLength` and length-derived tags.
- Short shard keys are no longer underscore-padded. Padding made a short term
  such as `ai` collide with the real expansion term `ai_`, producing duplicate
  directory keys that could hide one posting segment on large vocabularies.
- Large document-layout merges now use a k-way heap instead of scanning every
  sorted chunk for every document. Layout order uses a compact `Uint32Array`,
  and the document-pack preload fast path is capped at 256 MiB by default to
  avoid multi-gigabyte RSS spikes. The wiki profile restores linear-time
  impact-bucket posting order plus auto block/codec selection for substantially
  earlier broad-query top-k proofs. Posting gzip level is now configurable; the
  measured Wikipedia profile uses level 3 to reduce compression CPU with a
  small transfer-size tradeoff while the library default remains level 6.
  Multi-gigabyte document preloads are chunked to stay below Node's 2 GiB
  Buffer limit. A new `doc-id` document layout packs through bounded sequential
  read windows; the full Wikipedia profile uses it to avoid millions of tiny
  random reads and swap-heavy multi-gigabyte preloads on external volumes.
- Runtime top-k proof is now adaptively bounded to 128 decoded blocks for
  indexes with at least one million documents. This turns pathological broad
  multi-term queries into bounded approximate searches while preserving
  `topKBlockBudget: 0` and exact search for exhaustive callers.
- Authority run spooling now has its own `authorityRunFlushRecords` budget
  (100,000 by default). The old condition referenced a removed posting-run
  option and therefore retained every authority record in heap on large builds.

- Term order in posting segments, shard payloads, and the range directory
  now uses code-unit comparison instead of `localeCompare`. ICU collation
  disagrees with the runtime's binary-search key order outside ASCII, which
  made any index containing CJK terms unsearchable, and it made pack bytes
  depend on the build machine's ICU version. Resume schema bumped to v5 so
  stale collation-ordered intermediate stages cannot mix into new builds.
- Main-index typo correction now accepts candidate tokens in any script
  (previously Latin-only).

## 0.2.0 — 2026-07-06

The "full search product" release: geo, autocomplete, semantic hybrid,
facet counts, highlighting, and incremental publishing — all static, all
over HTTP range requests.

### Added

- **Geo queries** (Lucene `LatLonPoint`-class, adapted to range requests):
  `geo` config fields build a static KD tree with a branch-paged root.
  Bounding-box and radius filters (exact, Haversine-verified), exact
  nearest-neighbor distance sort with early-stop proofs — with or without a
  text query — text+geo filtering, per-cell filter summaries, and distance
  boosts. Verified against exhaustive oracles at 175k and 4.27M points;
  cold nearest on 4.27M points ≈ 92 KB. (`docs/osm-geo-benchmarks.md`)
- **Search-as-you-type autocomplete**: `suggest` config fields build a
  prefix-sorted suggestion sidecar with per-page max-weight proofs and
  precomputed per-character hot pages. `engine.suggest({ q })` answers a
  first keystroke in ~2 requests / ~13 KB at 4.27M docs; later keystrokes
  are mostly cache hits. (`docs/suggest-benchmarks.md`)
- **Hybrid semantic search**: `vectors` config fields build an int8 IVF
  index (variance-permuted dimensions, coarse-prefix candidate pages, a
  fixed-width full-dimension refine store). `engine.vectorSearch()` and
  `search({ q, vector })` with reciprocal-rank fusion; filters apply to
  both lanes. Full-probe recall@10 is 0.98 against brute force.
  (`docs/vector-benchmarks.md`)
- **Per-query facet counts**: `search({ facets: [...] })` returns top
  values + counts with exact-or-flagged semantics (dictionary-backed
  global counts, exact counts over budgeted match sets, bounded
  chunk-sampled estimates).
- **Snippets and highlighting**: `search({ highlight })` returns
  analyzer-consistent match ranges over raw display text ("montreal"
  marks "Montréal"), with best-window passage selection.
- **Incremental publishing (Phase 1)**: `rangefind build --update` adds
  delta generations over an existing index. Unchanged pack bytes keep
  their content-addressed names (and CDN cache entries); replaced
  documents tombstone their old version; delta builds replicate the base
  generations' frozen statistics so scores stay exactly comparable across
  generations. (`docs/incremental-publishing-plan.md`)
- **Examples**: an OpenStreetMap map demo (`examples/osm-geo`, MapLibre +
  autocomplete + viewport search) and semantic search in the wiki example
  (`build.mjs --embed`, browser-side query embedding via transformers.js).
- New benchmarks: `bench:osm-geo`, `bench:suggest`, `bench:vectors`.

### Fixed

- The minimal example page now loads the bundled runtime instead of a
  source path that does not exist on static deployments.
- Posting shard parsing without a manifest could misalign on block-filter
  summaries; document frequency and physical row count are now cleanly
  separated in the posting encoder.

### Changed

- `count()` rejects `geo` parameters explicitly (previously ignored).
- Search results include the internal `index` of each hit.

## 0.1.0

Initial extraction: BM25F text search over range-packed static files,
typo correction, authority sidecar, facets, typed doc values, sorted
browse, query bundles, and the frwiki scalability fixture.
