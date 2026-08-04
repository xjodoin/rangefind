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
- **Adapts to the screen**: phones get a bottom sheet, tablets, foldables and
  landscape get a floating panel beside a full-bleed map, and the map's camera
  frames routes clear of whichever one is showing.
- **Android Auto and Android Automotive OS**: search, route preview and
  turn-by-turn on the head unit, with a map drawn by the app itself.
- **Offline regions**: preload a route index onto the device, refresh it, or
  delete it, and route from it with no network at all.

## Offline regions

Chromium's HTTP cache would keep *some* of an index around, but offers no
guarantee about what survives, and Range responses are the first thing it
declines to store. A region the user asked to keep has to be bytes the app
owns.

Tap the cloud button to open the sheet. Each entry preloads, refreshes, or
deletes, and one of them can be marked in use. The download list is exact
rather than guessed: the index root enumerates its own shards, packs and names
file, so the JS side hands Kotlin the precise file list. Files land in a
staging directory and are swapped in only once every one has arrived — a
half-downloaded index would fail deep inside a route instead of simply being
absent.

Stored regions are served back over a **loopback HTTP socket**, not through
`shouldInterceptRequest`. WebView's interception cannot answer a Range request
honestly: Chromium applies its own range handling to whatever the interceptor
returns, so a 206 is rejected outright and a 200 gets sliced twice — once by
the browser, again by the runtime — surfacing as a checksum mismatch deep
inside a route. A real socket on `127.0.0.1` sidesteps all of it.

Two honest limits: **search still needs the network** (the OSM index is
remote), so offline covers routing only; and a local read costs more bytes
than the same query over HTTP, because multi-range requests fall back to a
full-file response, which is cheap from disk but shows up in the fetch
receipt.

The source host is editable in the sheet. `10.0.2.2` is the emulator's alias
for your machine's loopback; a phone on Wi-Fi needs your machine's LAN
address instead. Serve the built indexes with:

```bash
node scripts/serve.mjs bench/route 5185
```

which exposes `luxembourg-index/` and `quebec-index/` at the paths the
catalogue expects.

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

## Cars

Two build variants, because projected Android Auto and Android Automotive OS
ship mutually exclusive car-app artifacts:

```bash
./gradlew installMobileDebug       # phone, projects to Android Auto
./gradlew installAutomotiveDebug   # runs natively on Automotive OS
```

The head unit gets its own screens — a `SearchTemplate` for the destination, a
`RoutePreviewNavigationTemplate` for the options, and a `NavigationTemplate`
whose maneuver card and travel estimate come from the same route the phone
would draw. Screens are lean by design: driving is not the time for a place
detail card.

**The driving itself is not a second implementation.** `nav/NavigationCore`
owns the rules — what counts as off-route, when a phrase is due, which
alternates are still reachable — and both the phone and the car drive it. Side
effects stay with the caller: the core decides *that* a reroute is needed or
*that* something should be said, and each surface does it with its own engine
and speaker. So the car speaks the same guidance, reroutes on the same
evidence, and detects arrival the same way, without the logic existing twice.

The car also calls `NavigationManager.updateTrip()`, which is what feeds the
instrument cluster and the host's own notification surface — without it the
maneuver only exists inside our template.

Guidance audio goes through `nav/GuidanceSpeaker`, which tags the stream
`USAGE_ASSISTANCE_NAVIGATION_GUIDANCE` and takes transient focus for the
length of each phrase. A bare `TextToSpeech.speak()` is media audio: a head
unit may play it on the handset rather than the car speakers, and it talks
over the music instead of ducking it.

### Driving with a sideloaded build

Android Auto only lists apps installed from Play, so a debug build is
invisible to the car until you allow unknown sources:

1. **Settings → Connected devices → Android Auto**
2. Tap **Version** about ten times to unlock developer mode
3. **⋮ → Developer settings → Unknown sources**

`HostValidator.ALLOW_ALL_HOSTS_VALIDATOR` is already active for debuggable
builds, which is what lets a real head unit bind to an unpublished app.

Routing needs no network once a region is preloaded, but **search and the
car's basemap tiles do**. Tiles are disk-cached, so roads already seen redraw
offline; a fresh area with no signal shows the route line on an empty
background.

**The map on the car surface is drawn by the app.** MapLibre exposes no
renderer that targets an arbitrary `Surface` — only `MapView` — so rather than
fork a large native codebase for one class, the car view composes itself:
raster basemap tiles underneath, then the route with its casing, the junctions
ahead, the destination and the car. It is a real map, not a schematic.

Three things make it behave in a vehicle:

- **One render thread owns the surface.** Tiles land on a pool thread and car
  state arrives on main; two threads inside `lockCanvas` is a crash, not a
  glitch.
- **The camera eases toward each fix** instead of snapping. Location arrives
  about once a second, and a map that jumps once a second reads as broken
  however correct the geometry is. Frames stop when the camera catches up, so
  a parked car costs nothing.
- **Everything scales off the reported visible area**, not the raw surface.
  Head units vary enormously in size and aspect, and the host covers part of
  the display with its own chrome.

`androidx.car.app.ACCESS_SURFACE` is required to draw at all — without it the
host kills the app the moment it asks for the surface.

### Testing on an Automotive emulator

```bash
sdkmanager "system-images;android-33;android-automotive;arm64-v8a"
avdmanager create avd -n WayfindCar \
  -k "system-images;android-33;android-automotive;arm64-v8a" \
  -d automotive_1024p_landscape
emulator -avd WayfindCar
```

Two things that look like app bugs but are not:

- **Automotive runs apps as user 10, not user 0.** `pm grant <pkg>
  android.permission.ACCESS_FINE_LOCATION` silently grants to the wrong user
  and the app never registers a location request. Use `pm grant --user 10`,
  and launch with `am start --user 10`.
- **Location services start switched off.** `cmd location set-location-enabled
  true` before the app subscribes, or its provider query comes back empty and
  it waits forever.

Confirm the app is actually listening before blaming `geo fix`:

```bash
adb shell "dumpsys location | grep -i wayfind"
```

A registration line with a rising `locations = N` means fixes are arriving.

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
