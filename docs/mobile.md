# Mobile

Rangefind runs on iOS and Android. The core runtime is dependency-free
JavaScript that needs exactly two platform capabilities — a `fetch` that
honors `Range` headers and gzip inflation — and both are injectable, so the
same query engine works in every mobile host. Pick the tier that matches your
app:

| App type | What to use | Work required |
| --- | --- | --- |
| WebView / hybrid (Capacitor, Ionic, PWA, React Native WebView) | The standard browser runtime | None — works today |
| React Native / Expo (Hermes or JSC) | `rangefind/mobile` | Adapter + polyfills below |
| Flutter | Headless WebView + local range server | Recipe below |
| Native Swift/Kotlin | Local range server + WebView, or an embedded JS engine with `rangefind/mobile` | Recipe below |

The differentiated capability on mobile is offline search at scale: because
the runtime reads byte ranges instead of loading the index, a bundled or
downloaded multi-hundred-MB index searches in milliseconds with tiny memory —
no server, no SQLite port, no loading the corpus into RAM.

## Tier 1 — WebView and hybrid apps (zero changes)

WKWebView (iOS 16.4+) and Android System WebView both provide everything the
stock browser runtime uses: `fetch` + Range, `DecompressionStream`, and the
HTTP disk cache (so content-addressed packs cache forever for free). If your
search UI renders in a WebView and the index is served over `http(s)`, use
[the normal browser setup](reference.md) unchanged.

One caveat: app-local asset schemes (`capacitor://`, `file://`,
`ionic://`) do not implement Range requests. For an offline index inside a
hybrid app, either use a local-server plugin that supports Range headers, or
drive the search from the app's JS side with `rangefind/mobile` (Tier 2) and
post results into the WebView.

## Tier 2 — React Native / Expo: `rangefind/mobile`

`rangefind/mobile` runs the full query engine (`search`, `count`, `suggest`,
`vectorSearch`, facets, sorting, geo, generational indexes) inside Hermes,
JavaScriptCore, or any embedded JS engine. You provide the platform
primitives; it wires them into the runtime:

```js
import RNFS from "react-native-fs";
import { ungzip } from "pako";
import { Buffer } from "buffer";
import { createMobileSearch } from "rangefind/mobile";

// Positional reads over the device file system (any module works —
// react-native-fs shown; expo-file-system's byte APIs work the same way).
const io = {
  async read(path, offset, length) {
    const base64 = await RNFS.read(path, length, offset, "base64");
    return Buffer.from(base64, "base64");
  },
  async size(path) {
    return Number((await RNFS.stat(path)).size);
  }
};

const engine = await createMobileSearch({
  source: `${RNFS.DocumentDirectoryPath}/rangefind`, // bundled or downloaded index
  io,
  inflate: ungzip // Hermes has no DecompressionStream
});

const { results } = await engine.search({ q: "sparse inverted index" });
const suggestions = await engine.suggest({ q: "spar" });
```

For a remote index, point `source` at the deployed URL instead. React
Native's `fetch` passes Range headers through, but provides none of the
browser's HTTP caching, so the transport adds it back: content-addressed
objects are held in a bytes-bounded memory LRU and, if you pass a `cache`
adapter, persisted on device:

```js
const engine = await createMobileSearch({
  source: "https://example.com/rangefind/",
  inflate: ungzip,
  cache: {
    // Any persistent K/V works; keys are URL#range strings, values bytes.
    get: key => readCachedBytes(key),      // Uint8Array | null
    set: (key, bytes) => writeCachedBytes(key, bytes)
  }
});
```

### Polyfills

Hermes and mobile JavaScriptCore lack some Web APIs the runtime uses. Install
these before importing `rangefind/mobile` (all standard React Native
polyfills):

| Missing API | Polyfill |
| --- | --- |
| `TextEncoder` / `TextDecoder` | `fast-text-encoding` (or `text-encoding`) |
| `URL` | `react-native-url-polyfill/auto` |
| `DecompressionStream` | Not needed — pass `inflate` (e.g. `pako`'s `ungzip`) |

### Options

| Option | Default | Meaning |
| --- | --- | --- |
| `source` | — (required) | Absolute device path, `file://` URL, or `http(s)://` base URL. |
| `io` | — (required for local sources) | `{ read(path, offset, length), size(path), readFile?(path) }` returning `Uint8Array`/`ArrayBuffer`. |
| `inflate` | `DecompressionStream` if present | Gzip inflation, e.g. `pako.ungzip`. Required on Hermes/QuickJS/JSC. |
| `fetch` | `globalThis.fetch` | Fetch used for `http(s)` sources. |
| `cache` | none | Persistent cache adapter `{ get(key), set(key, bytes) }` for immutable objects. |
| `memoryCacheBytes` | 16 MiB | Process-wide memory LRU budget. |

All other options (`manifestName`, `trace`, `verifyChecksums`, ...) pass
through to the core runtime. The returned engine adds `cacheStats()`;
`resetMobileRuntimeCaches()` clears the process-wide memory cache.

### Threading note

Queries run on the app's JS thread. Rangefind queries are I/O-bound with
short decode bursts, so this is normally fine; if you need full isolation,
the runtime has no DOM dependencies and runs unchanged inside a second engine
instance (`react-native-worklets-core`, worker-thread packages).

## Tier 3 — Flutter

Don't port the runtime to Dart. Two proven shapes:

- **Headless WebView (recommended).** Run the stock browser runtime inside a
  hidden `flutter_inappwebview`; call `search()` with `evaluateJavascript`
  and receive JSON results through a JS handler. Remote indexes work with
  zero Rangefind changes. For offline indexes, serve the index directory from
  a tiny Dart `shelf` server that implements `Range` (a ~50-line handler:
  parse `bytes=a-b`, respond `206` with the slice).
- **Embedded JS engine.** `flutter_js` (QuickJS on Android, JavaScriptCore on
  iOS) runs `rangefind/mobile` directly: bridge `io.read`/`io.size` to Dart
  file reads and inflate with Dart's `GZipCodec` or bundle pako. Note the
  bridge marshals binary as base64, which adds per-read overhead the WebView
  path doesn't have.

## Native Swift / Kotlin apps

Same trade-off as Flutter: don't port the codec surface. Either serve the
index from an embedded HTTP server with Range support (GCDWebServer on iOS,
NanoHTTPD/Ktor on Android) into a WebView running the standard runtime, or
embed a JS engine (JavaScriptCore is built into iOS) and run
`rangefind/mobile` with `io` bridged to native file reads.

## Shipping an offline index

1. Build the index as usual (`rangefind build ...`).
2. Bundle the output directory as an app asset, or download it on first
   launch (a zip of the index directory works well; unpack to app storage).
3. Point `source` at the unpacked directory.

Incremental publishing (`build --update`) composes with downloaded indexes:
unchanged packs keep their content-addressed names, so an updated index
re-downloads only what changed if you sync file-by-file.

## Live traffic on a phone (`rangefind/pulsemesh/mobile`)

The design's whole platform premise is that browsers read and apps write:
a backgrounded tab loses `watchPosition` and a screen-off phone stops
reporting, so the realistic sustained contributor is an app. The mobile
entry point is that side of it, and it is the same session the web demo
uses — the difference is the defaults, not the pipeline.

```js
import { createMobileMesh } from "rangefind/pulsemesh/mobile";

const mesh = await createMobileMesh({
  engine,                       // an open route graph
  network,                      // a MeshNetwork, or null to run consume-only
  batteryLevel: () => level,    // §10.1 rule 5: contribution pauses below 20%
  charging: () => plugged       // unless charging
});

await engine.route({ from, to, live: mesh.provider() });
await mesh.followRoute(candidates);
await mesh.onLocation({ lat, lon, speedMps, courseDeg });   // publishes nothing
mesh.setContributing(true);                                  // until asked
```

Three things a host has to get right, all learned the hard way in
the Wayfind Android app (a separate product repository):

- **Drive the clock yourself.** `mesh.start()` schedules its own
  maintenance, which is right in a page and wrong in an app host. A
  headless WebView is a hidden page and Chromium clamps a hidden page's
  timers: a mesh that scheduled its own anti-entropy ticked once and
  never again. Call `mesh.tick()` on the host's cadence instead.
- **Offer every fix, even when not contributing.** `onLocation` publishes
  nothing unless contribution is on, but it keeps the snapped position
  fresh — and §10.4 only files an incident report for a *recently
  snapped* position. Skipping the call when contribution is off silently
  turns "I don't share my speed" into "I can't report a crash".
- **Say which mesh you are on.** With no keeper reachable, a host can run
  the corridor simulator (`createCorridorTraffic`) so the feature is
  testable before peers exist. Everything about those records is real
  except the drivers, and a screen that does not say so is claiming
  something it cannot support. Make it refusable, separately from live
  traffic, and say so **on the map** rather than only in a settings sheet:
  the label is read at the moment a jam is being believed. Stopping the
  simulator withdraws the source and nothing else — records already
  validated stay, and age out on TTL like any others.

`rangefind/pulsemesh/threads` adds the second channel — one vehicle, a
bounded audience, a 78-byte capability-separated link in a URL fragment. It needs no
admission bond and works in read-only mode, because thread records are
authenticated end to end by the thread key rather than by membership.
See [pulsemesh.md](pulsemesh.md) and
[pulsemesh-threads.md](pulsemesh-threads.md).
