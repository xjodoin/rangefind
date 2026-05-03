import assert from "node:assert/strict";
import test from "node:test";
import { createBuildTelemetry, timeBuildPhase } from "../src/build_telemetry.js";

test("build progress logs stay disabled when buildProgressLogMs is zero", async () => {
  const lines = [];
  const telemetry = createBuildTelemetry({
    progressLogMs: 0,
    progressLogger: line => lines.push(line)
  });

  await timeBuildPhase(telemetry, "quiet-phase", async () => {});

  assert.deepEqual(lines, []);
});

test("build progress logs phase boundaries when enabled", async () => {
  const lines = [];
  const telemetry = createBuildTelemetry({
    progressLogMs: 1000,
    progressLogger: line => lines.push(line)
  });

  await timeBuildPhase(telemetry, "visible-phase", async () => {});

  assert.equal(lines.length, 2);
  assert.match(lines[0], /Rangefind build: visible-phase started/);
  assert.match(lines[1], /Rangefind build: visible-phase done/);
});
