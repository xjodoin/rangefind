#!/usr/bin/env node

/**
 * Reads a Wayfind trip trace and says what happened.
 *
 * A road test comes back as a few hundred JSON lines, and the questions asked
 * of them are always the same: did the fixes arrive steadily, did the drawn
 * arrow move like a vehicle, did the map keep up, and what did the driver flag.
 * Answering those by eye is slow enough that it does not get done, so a drive
 * that felt wrong turns into an impression rather than a number.
 *
 * The measurement that matters most here is the *excess*: how much further the
 * drawn position moved than the vehicle could possibly have travelled since
 * the last fix. Near zero, the arrow is following the car. Consistently
 * positive, it is teleporting, and no amount of render smoothing will hide it.
 * Split by which source drew it — snapped to the route, the raw fix, or the
 * update where it changed from one to the other — because those fail for
 * different reasons and need different fixes.
 *
 *   node scripts/trip_report.mjs trip-1785957392819.jsonl [more.jsonl...]
 */

import { readFileSync } from "node:fs";
import { basename } from "node:path";

/** Metres between two points. */
function haversine(a, b) {
  const R = 6371000;
  const p1 = (a.lat * Math.PI) / 180;
  const p2 = (b.lat * Math.PI) / 180;
  const dp = p2 - p1;
  const dl = ((b.lon - a.lon) * Math.PI) / 180;
  const h =
    Math.sin(dp / 2) ** 2 +
    Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(h)));
}

function percentile(values, fraction) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))];
}

function readTrace(path) {
  const rows = [];
  for (const line of readFileSync(path, "utf8").split("\n")) {
    if (!line.trim()) continue;
    try {
      rows.push(JSON.parse(line));
    } catch {
      // A trace is appended to while driving; a torn last line is normal.
    }
  }
  return rows;
}

/** Below this the vehicle is stopped, and nothing about motion is measurable. */
const MOVING_MPS = 2.0;

function report(path) {
  const rows = readTrace(path);
  const header = rows.find((r) => r.kind === "route");
  const fixes = rows.filter((r) => r.kind === "fix");
  const marks = rows.filter((r) => r.kind === "mark");
  const events = rows.filter((r) => r.kind === "event");
  const render = rows.findLast?.((r) => r.kind === "render") ??
    [...rows].reverse().find((r) => r.kind === "render");

  console.log(`\n\x1b[1m${basename(path)}\x1b[0m`);
  if (header?.environment) {
    const e = header.environment;
    console.log(
      `  ${e.device ?? "?"} · Android ${e.android ?? "?"} · ${e.routingProfile ?? "?"} · ` +
        `${(header.distanceMeters / 1000).toFixed(1)} km planned · ${header.stepCount ?? "?"} steps`
    );
  }

  const gaps = [];
  const groups = { snapped: [], raw: [], flip: [] };
  let previous = null;

  for (const fix of fixes) {
    if (previous?.nav && fix.nav) {
      const dt = (fix.at - previous.at) / 1000;
      const speed = Math.max(fix.speedMps ?? 0, previous.speedMps ?? 0);
      if (dt > 0 && dt < 5) {
        gaps.push(dt);
        if (speed >= MOVING_MPS) {
          // The drawn position is the raw fix exactly when the code chose not
          // to snap it, which is how the branch taken is recovered here.
          const wasRaw = (r) =>
            haversine({ lat: r.lat, lon: r.lon }, { lat: r.nav.shownLat, lon: r.nav.shownLon }) < 0.5;
          const before = wasRaw(previous);
          const now = wasRaw(fix);
          const moved = haversine(
            { lat: previous.nav.shownLat, lon: previous.nav.shownLon },
            { lat: fix.nav.shownLat, lon: fix.nav.shownLon }
          );
          const key = before !== now ? "flip" : now ? "raw" : "snapped";
          groups[key].push(moved - speed * dt);
        }
      }
    }
    previous = fix;
  }

  if (gaps.length) {
    const late = gaps.filter((g) => g > 1.5).length;
    console.log(
      `  fixes        ${fixes.length}, interval p50 ${percentile(gaps, 0.5).toFixed(2)}s ` +
        `p95 ${percentile(gaps, 0.95).toFixed(2)}s, ${late} over 1.5s`
    );
  }

  const total = Object.values(groups).reduce((n, g) => n + g.length, 0);
  if (total) {
    console.log(`  drawn position, excess over what the car could travel:`);
    for (const [name, values] of Object.entries(groups)) {
      if (!values.length) continue;
      console.log(
        `    ${name.padEnd(8)} n=${String(values.length).padStart(5)}  ` +
          `p50 ${percentile(values, 0.5).toFixed(1).padStart(6)}m  ` +
          `p95 ${percentile(values, 0.95).toFixed(1).padStart(6)}m  ` +
          `max ${Math.max(...values).toFixed(1).padStart(6)}m`
      );
    }
  }

  if (render?.render) {
    const r = render.render;
    console.log(
      `  map          ${r.frames} frames / ${r.movingSeconds ?? r.seconds}s moving = ${r.fps} fps, ` +
        `gap p50 ${r.gapP50Ms}ms p95 ${r.gapP95Ms}ms, ${r.droppedPercent}% over 33ms`
    );
    console.log(`               ${r.vehicleUpdates} position updates over the same window`);
  } else {
    console.log(`  map          no render summary (trace predates the frame instrumentation)`);
  }

  for (const event of events) console.log(`  event        ${event.event}${event.detail ? ` · ${event.detail}` : ""}`);
  for (const mark of marks) {
    const at = new Date(mark.at).toISOString().slice(11, 19);
    console.log(`  flagged      #${mark.ordinal} at ${at} · ${mark.nav?.stepName ?? "?"} · ${mark.screenshot}`);
  }
}

const paths = process.argv.slice(2);
if (!paths.length) {
  console.error("usage: node scripts/trip_report.mjs <trip-*.jsonl> [...]");
  process.exit(1);
}
for (const path of paths) report(path);
console.log();
