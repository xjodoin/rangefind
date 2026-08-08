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
  and automatic rerouting when you leave the line. Panning the map to look up
  the road is honoured rather than fought, and raises a recenter control that
  puts the camera back on the car — the follow is dropped by a deliberate pan,
  and before this there was no way back short of ending the trip. Alternates you could still
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
- **Live traffic (PulseMesh)**: anonymous peer-to-peer speed observations
  colour the roads, move the ETA, and can re-route around a jam — with no
  traffic server anywhere in the path. Reporting a crash or a closure is one
  tap, incidents other drivers reported can be confirmed or refuted while
  passing them, and a drive can be shared as a single 45-byte capability link
  that no server ever sees. Reading traffic and publishing your own speeds are
  separate decisions, and publishing is off until you turn it on.

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
| `assets/rangefind/bridge.js` | JS side of the bridge: search, suggest, route, snap, reverse, live traffic |
| `ui/MapsViewModel.kt` | State machine: search, directions, navigation, rerouting |
| `ui/map/MapCanvas.kt` | MapLibre sources/layers, camera behavior, hit testing |
| `ui/components/TrafficUi.kt` | Live-traffic settings, the incident report sheet, the confirm/refute prompt |
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

### Live traffic

Live traffic is off until you turn it on, in the offline-regions sheet. What
happens then depends on `BuildConfig.PULSEMESH_BOOTSTRAP`:

- **Empty (the default)** — no keeper is deployed yet, so the app runs the
  mesh over a loopback transport with three simulated vehicles crawling the
  corridor you routed. Every byte still goes through the codec, the twelve
  validation rules against this device's own copy of the index, and the
  weighted-median aggregate the router consumes; only the drivers are
  invented, and both the sheet and the map say so on screen — the map
  carries a **Demo traffic** label for as long as anything invented is being
  drawn, mid-drive included. The vehicles are a switch of their own
  (*Simulated traffic*, in the traffic tab) and turning them off leaves live
  traffic running: the mesh keeps whatever it already validated and the map
  then shows only real Wayfind drivers nearby, which may be nothing at all.
  The choice is remembered, so it is not re-imposed on the next launch.
- **A keeper multiaddr** (`/dns4/keeper.example/tcp/443/wss/p2p/12D3Koo…`) —
  the app joins the real mesh over WebSockets. `pulsemesh-libp2p.browser.js`
  is in the assets and loads on demand, so a build with no keeper configured
  never executes it.

Run a keeper with `npm run pulsemesh:keeper`. Contribution over a real
transport needs a §5.4 admission bond, which is a memory-hard mint; that path
is implemented but **unexercised on real phone hardware**.

### Sharing a drive

A shared drive is a **thread**: one vehicle, a bounded audience, and a
capability that is 45 bytes in a URL fragment. Nothing else is in the link —
no host, no mailbox, no bootstrap address — and because it lives in the
fragment, no server receives it on either path: browsers never transmit a
fragment, and Android hands it to the app intact.

Sharing asks what the link should be worth if it leaks, because that is a
harm decision rather than a bandwidth one:

- **Live position** — a locator. Right for a courier, whose position is the
  point.
- **Stops only** — no position at all, so a leaked link is worth roughly what
  a printed timetable is. Right for anything carrying children.

The link is sent through the ordinary share sheet, from two places: the
traffic tab, which starts and stops a plain single-destination drive, and the
delivery-progress card, which is where a driver mid-day actually is. The card
shares the run already publishing — a run taken from a dispatcher's ticket
included — and when the app holds a plan but no run, which is what a process
death leaves behind, it publishes the stored plan again first and says on the
card that the new link supersedes the old one.

A dispatch run publishes **the whole plan**, in plan order and with its plan
positions intact, not the stops still to come: the wire keys its outcome map
by position in the published list, so renumbering it would attribute a
delivery to the wrong doorstep with nothing on either side to reveal it.

Ending a run publishes one final record, and **which** record it is is not a
button's choice. `COMPLETED` is a claim — every customer holding the link and
the dispatcher watching are told the run arrived — so it is emitted only when
every stop in the plan has an outcome the driver asserted. A day stopped with
stops still unmarked closes as `CANCELED` whichever control ended it
(`finalRecordFor` in `engine/RangefindEngine.kt` is the single place that
decides, and is unit-tested). *Can't finish this job*, in the progress card's
overflow menu, is the deliberate path: it confirms who hears the cancellation
and takes the driver's reason as the record's ≤64-byte note.

Handing the job to another van is in the same menu, gated on holding a ticket
rather than on a live run — the ticket is on disk, so a process death that took
the run with it leaves the capability intact. Since threads §20.9 it is gated
on one more thing, and that is the headline: **the job is sealed to a device
you have already enrolled**, so the menu asks *which* device before it draws
anything. With an empty roster there is no key to address the ciphertext to,
and the app says exactly that — the other driver has to show their device card
in Wayfind and this phone has to open it — instead of drawing a QR nobody in
the world could open.

What the QR and the `.wayfindjob` file carry is the **sealed PME1 envelope**.
A photograph of either by anyone else is ciphertext, which is why the dialog no
longer warns that whoever photographs it can publish the vehicle: it names the
one device that can open it, and says that when they do, they become the
publisher.

The dialog then offers the one exit in the app that publishes **nothing**:
after §20.5 both phones hold the same seed and the wire cannot tell them apart,
so a goodbye from the old device would tell every customer the run had ended
while the new driver is still delivering. That action is reachable from nowhere
else; an ordinary stop always says goodbye.

### Device identity and enrolment

Every install mints one X25519 **device key** on first run (§20.9). It is not
the run seed, which travels with a ticket, and not an issuer seed, which signs:
it never signs anything, and its only job is to be the address a ticket can be
sealed to. It lives in `SharedPreferences` as AES-256-GCM ciphertext under a
key generated inside `AndroidKeyStore`, so a backup or a read of the
preferences file is ciphertext with no key beside it. A phone whose Keystore
refuses stores the key in the clear and says so on the settings card rather
than pretending otherwise.

**Losing the key is unrecoverable, by construction.** Reinstalling the app or
clearing its data destroys it, and every job already sealed to that device
stops opening; the dispatcher has to enrol the phone again. The settings card
states this where the key is.

*Settings → Traffic → This device* holds the card: an editable name (≤ 32
bytes, what the other driver reads), the fingerprint in a size two people can
read to each other across a counter, the card as a QR, and the same card as a
shareable `wayfind://device#…` link. Below it is the roster — the devices this
phone can hand a job to — with a remove action.

There is no scanner in this app, by the same design as the ticket path: the
other phone's **system camera** fires `ACTION_VIEW` on the card URL it decodes,
and the `wayfind://device` filter is what catches it. Wayfind then opens a
confirmation showing the name and the fingerprint, and says plainly that the
fingerprint is there to be checked against what the other phone is showing —
four bytes is not a cryptographic binding and §20.9 does not claim it is; it is
eight characters two people compare in three seconds, which is the check that
actually happens in a doorway.

```bash
adb shell 'am start -a android.intent.action.VIEW -d "wayfind://device\#<card>"'
```

Receiving one opens the app already following: live traffic comes up on its
own (threads need no admission bond and work read-only), and the card states
only what the subscriber can support — a stale position is never presented as
a live one. The arrival is computed **on the receiving device**, routing the
position they broadcast to the destination *you* care about under *your*
live-traffic metric, so the publisher never learns which destination anyone
asked about.

Two intent filters carry the link:

```bash
# Works today, no domain verification needed.
adb shell 'am start -a android.intent.action.VIEW -d "wayfind://thread\#<capability>"'
```

The `https://rangefind.dev/t#…` filter is declared with `autoVerify`, so it
opens the app **only once `/.well-known/assetlinks.json` is published for the
signing certificate** — until then Android routes it to the browser, which is
the correct fallback (the web demo follows the same link). Enable it on one
device for testing with `adb shell pm set-app-links --package
dev.rangefind.wayfind 1 rangefind.dev`.

The mesh's clock lives in Kotlin, not in the page: a headless WebView is a
hidden page and Chromium clamps a hidden page's timers, so `MapsViewModel`
calls `tickMesh()` on its own cadence and the JS side schedules nothing.

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

There is no Android Auto launcher on a phone. Phone-screen mode was
discontinued, so the Android Auto app on a handset is settings only — the
projected car UI exists on a head unit or on the Desktop Head Unit (DHU),
and nowhere else. An app that never appears "in Android Auto" on the phone
is not misconfigured; there is no list there to appear in.

The **Unknown sources** developer toggle does not help either. Google's
testing documentation states that it covers media, messaging and parked
apps, and explicitly *not* apps built with the Android for Cars App Library.
A sideloaded template app will not be listed by a production head unit no
matter what that toggle says.

Two supported paths:

- **DHU, for local development.** Install *Android Auto Desktop Head Unit
  Emulator* from the SDK Manager's SDK Tools tab, unlock developer mode in
  the Android Auto app (About → tap the version 10×), turn on **Add new cars
  to Android Auto**, launch the phone app once so it leaves the stopped
  state and holds its location grant, then **⋮ → Start head unit server**
  and, with the phone unlocked over USB:

  ```
  adb forward tcp:5277 tcp:5277
  "$ANDROID_SDK_ROOT/extras/google/auto/desktop-head-unit"
  ```

  If the launcher comes up blank or without Wayfind, restart the head unit
  server and relaunch the DHU; a stale host is the usual cause.

- **A real vehicle** needs the build to come from a trusted source: Play
  Internal App Sharing or an internal test track. `adb install` is not
  enough.

`HostValidator.ALLOW_ALL_HOSTS_VALIDATOR` is already active for debuggable
builds, which is what lets an unpublished app bind once it is reachable.

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
