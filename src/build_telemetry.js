import { performance } from "node:perf_hooks";

function memorySnapshot() {
  const memory = process.memoryUsage();
  return {
    rss: memory.rss,
    heapUsed: memory.heapUsed,
    heapTotal: memory.heapTotal,
    external: memory.external,
    arrayBuffers: memory.arrayBuffers
  };
}

export function createBuildTelemetry() {
  const startedAt = performance.now();
  return {
    format: "rfbuildtelemetry-v1",
    phases: [],
    counters: {},
    startMemory: memorySnapshot(),
    peakRss: process.memoryUsage().rss,
    startedAt
  };
}

export function addBuildCounter(telemetry, name, value) {
  if (!telemetry || !name) return;
  telemetry.counters[name] = (telemetry.counters[name] || 0) + (Number(value) || 0);
}

export async function timeBuildPhase(telemetry, name, fn) {
  if (!telemetry) return fn();
  const start = performance.now();
  const startMemory = memorySnapshot();
  try {
    return await fn();
  } finally {
    const endMemory = memorySnapshot();
    telemetry.peakRss = Math.max(telemetry.peakRss || 0, endMemory.rss);
    telemetry.phases.push({
      name,
      ms: performance.now() - start,
      startMemory,
      endMemory
    });
  }
}

export function finishBuildTelemetry(telemetry) {
  if (!telemetry) return null;
  const endMemory = memorySnapshot();
  telemetry.peakRss = Math.max(telemetry.peakRss || 0, endMemory.rss);
  return {
    format: telemetry.format,
    total_ms: performance.now() - telemetry.startedAt,
    peak_rss: telemetry.peakRss,
    start_memory: telemetry.startMemory,
    end_memory: endMemory,
    counters: telemetry.counters,
    phases: telemetry.phases
  };
}
