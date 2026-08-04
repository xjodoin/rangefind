# Wayfind

<img src="brand/wayfind-mark.svg" width="72" align="right" alt="">

**Maps on static byte ranges.** A native Android app — Jetpack Compose UI,
MapLibre Native rendering — whose search, geocoding, and routing all come from
Rangefind's static indexes over HTTP Range requests. There is no search server,
no geocoding API, and no routing service: the app reads byte ranges out of
immutable, content-addressed files on a CDN.

The name is the sibling of *Rangefind*: a **way** is OpenStreetMap's own
primitive for a road, and *wayfinding* is the discipline of navigating space.

## What it does

- **Search and autocomplete** worldwide against the public OSM index at
  `https://osm.rangefind.dev/`, anchored to the user's location so bare
  categories ("cafe", "pharmacy") come back nearest-first.
- **Place details** with category, address, and distance, plus long-press
  anywhere to drop a pin and reverse-geocode it.
- **Directions** open a route overview, not just a line: every candidate is
  framed at once, each carries a duration bubble on the map, and tapping a
  bubble or a grey alternate selects it — the camera deliberately holds still
  while you compare. Plus maneuver steps, ETA and arrival clock, and a fetch
  receipt showing how many range requests the route cost.
- **Turn-by-turn navigation**: a chevron puck aimed where the car is actually
  heading, a pitched follow camera with speed-banded zoom, maneuver banner,
  traveled-route dimming, current-road pill, speed readout against the posted
  speed limit (the readout turns when you are over it), spoken guidance,
  and automatic rerouting when you leave the line. Alternates you could still
  take stay on the map with their ETA delta, and one tap re-tracks onto the
  other line from where the car already is — they disappear once you commit to
  a branch, because by then they are no longer an option.
- **Day and night** basemaps, edge-to-edge layout, and full attribution.

## Architecture

The one real decision is how Kotlin talks to Rangefind, which is JavaScript.
This app runs the **standard browser runtime inside a headless WebView**:

```
Compose UI  ──  MapLibre Native (map rendering)
     │
MapsViewModel (StateFlow)  ·  RouteTracker (map-matching, progress)
     │
RangefindEngine  ←── WebViewRangefindEngine
     │
headless WebView on https://appassets.androidplatform.net
     │  fetch() straight to the index — binary never crosses the bridge
     ▼
osm.rangefind.dev (search)   ·   route graph base URL (directions)
```

Why a WebView rather than an embedded JS engine:

- **Binary never crosses the JS bridge.** The runtime issues its own Range
  requests from inside Chromium's network stack; only small JSON results come
  back through `@JavascriptInterface`. Marshalling megabytes of route index as
  base64 would dwarf the query itself.
- **No polyfills, and checksums stay on.** `WebViewAssetLoader` serves the host
  page from a virtual `https://` origin, which is a secure context, so
  `DecompressionStream` and `crypto.subtle` are both present — gzip inflation
  and SHA-256 pack verification run natively and stay enabled.
- **It runs the exact bundles the web demo ships**, synced from the repo's
  `dist/` by the `syncRangefindBundles` Gradle task.

`RangefindEngine` is an interface precisely so this stays swappable: an
embedded V8/Hermes host (zero-copy `ArrayBuffer`, native zlib) or a
`nodejs-mobile` runtime can replace it without touching anything above it.

### Layout

| Path | Role |
| --- | --- |
| `engine/RangefindEngine.kt` | Interface and domain models — the app's whole dependency on Rangefind |
| `engine/WebViewRangefindEngine.kt` | Headless WebView host, request-id bridge with cancellation |
| `assets/rangefind/bridge.js` | JS side of the bridge: search, suggest, route, snap, reverse |
| `ui/MapsViewModel.kt` | State machine: search, directions, navigation, rerouting |
| `ui/map/MapCanvas.kt` | MapLibre sources/layers, camera behavior, hit testing |
| `ui/map/MapIcons.kt` | Procedurally drawn pins and road-sign symbols |
| `nav/RouteTracker.kt` | Along-route distances, nearest-point matching, line splitting |
| `nav/Geo.kt` | Haversine, bearings, point-to-segment projection |

## Running it

Search works out of the box against the public index. **Directions need a route
graph**, and no public one is hosted yet, so debug builds point at a local one:

```bash
npm run build:browser && npm run setup:osm-demo-route
```

```bash
node scripts/serve.mjs examples/osm-geo/public 5184
```

`BuildConfig.ROUTE_BASE_URL` defaults to `http://10.0.2.2:5184/route-graph/`
in debug builds — `10.0.2.2` is how the emulator reaches the host loopback.
The bundled test index covers **Luxembourg**, so set the emulator there to try
routing:

```bash
adb emu geo fix 6.1319 49.6116
```

Then build and install:

```bash
cd android && ./gradlew installDebug
```

With no reachable route index the app degrades exactly like the web demo:
search keeps working and the Directions button explains why it is disabled.

For release builds, set `ROUTE_BASE_URL` to an https base that serves the route
graph with `Accept-Ranges` and permissive CORS (a `Range` header is not
CORS-safelisted, so the origin must answer the preflight).

## Brand

Wayfind is a Rangefind-family product and shares its identity rather than
inventing a parallel one.

The Rangefind favicon is a muted track with a highlighted sub-range inside it —
a byte range, drawn literally. The Wayfind mark keeps that exact three-value
grammar and bends the track into a way with two turns, so the resemblance is
structural rather than merely a shared palette. Source: `brand/wayfind-mark.svg`.

| Token | Value | Used for |
| --- | --- | --- |
| Ink | `#14161D` | Mark field, dark background |
| Paper | `#FAF9F6` | Light background |
| Marker (amber) | `#FFC940` | The highlighted range — and the destination pin |
| Marker soft / ink | `#FFE9AD` / `#7A5200` | Amber containers and their text |
| Pine | `#0E6F63` | Primary actions, route line (light) |
| Pine bright | `#35C2AC` | Primary actions, route line (dark) |
| Muted | `#5B6370` / `#9AA1AD` | Secondary text, alternate routes |
| Surface (dark) | `#21242E` | Sheets and cards at night |

Two deliberate rules:

- **Amber is reserved for the destination.** It is the highlighted range in the
  mark, so in the app it marks the one place you asked for — never a generic
  accent.
- **The location puck stays a conventional blue.** "You are here" is a
  functional signal drivers read at a glance, not a place to spend brand equity.

## Notes

- Location comes from the platform `LocationManager`, not Play Services — it
  works on any emulator image and `adb emu geo fix` drives navigation testing
  directly.
- Debug builds restrict native ABIs to `arm64-v8a` and `x86_64`; MapLibre ships
  ~11 MB of native code per ABI and all four make a 59 MB debug APK.
- Basemap tiles are **not** Rangefind's job — they come from CARTO's public
  styles. Attribution for both OpenStreetMap and CARTO is displayed in-app, as
  ODbL and CARTO's terms require.
