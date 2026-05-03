import { performance } from "node:perf_hooks";
import { existsSync, readdirSync, statSync } from "node:fs";
import { resolve } from "node:path";

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

function maxMemory(left, right) {
  return {
    rss: Math.max(left?.rss || 0, right?.rss || 0),
    heapUsed: Math.max(left?.heapUsed || 0, right?.heapUsed || 0),
    heapTotal: Math.max(left?.heapTotal || 0, right?.heapTotal || 0),
    external: Math.max(left?.external || 0, right?.external || 0),
    arrayBuffers: Math.max(left?.arrayBuffers || 0, right?.arrayBuffers || 0)
  };
}

function cpuDelta(start, end) {
  return {
    user_us: Math.max(0, (end?.user || 0) - (start?.user || 0)),
    system_us: Math.max(0, (end?.system || 0) - (start?.system || 0))
  };
}

function safeStat(path) {
  try {
    return statSync(path);
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

function directoryBytes(path) {
  const stat = safeStat(path);
  if (!stat) return 0;
  if (stat.isFile()) return stat.size;
  if (!stat.isDirectory()) return 0;
  let total = 0;
  for (const entry of readdirSync(path, { withFileTypes: true })) {
    const child = resolve(path, entry.name);
    if (entry.isDirectory()) total += directoryBytes(child);
    else if (entry.isFile()) total += safeStat(child)?.size || 0;
  }
  return total;
}

function normalizeDiskByteGroups(groups) {
  const normalized = {};
  for (const [name, paths] of Object.entries(groups || {})) {
    const list = Array.isArray(paths) ? paths : [paths];
    normalized[name] = list.filter(Boolean).map(path => resolve(String(path)));
  }
  return normalized;
}

function diskSnapshot(groups) {
  const snapshot = {};
  for (const [name, paths] of Object.entries(groups || {})) {
    snapshot[name] = paths.reduce((sum, path) => sum + (existsSync(path) ? directoryBytes(path) : 0), 0);
  }
  return snapshot;
}

function diskDelta(start, end) {
  const keys = new Set([...Object.keys(start || {}), ...Object.keys(end || {})]);
  return Object.fromEntries([...keys].sort().map(key => [key, (end?.[key] || 0) - (start?.[key] || 0)]));
}

function formatBytes(bytes) {
  const value = Math.abs(Number(bytes) || 0);
  const sign = Number(bytes) < 0 ? "-" : "";
  if (value >= 1024 * 1024 * 1024) return `${sign}${(value / 1024 / 1024 / 1024).toFixed(2)} GiB`;
  if (value >= 1024 * 1024) return `${sign}${(value / 1024 / 1024).toFixed(1)} MiB`;
  if (value >= 1024) return `${sign}${(value / 1024).toFixed(1)} KiB`;
  return `${sign}${value} B`;
}

function formatDuration(ms) {
  const seconds = Math.max(0, Number(ms) || 0) / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  const rest = Math.floor(seconds % 60);
  return `${minutes}m${String(rest).padStart(2, "0")}s`;
}

function phaseProgressLine(name, kind, elapsedMs, memory, disk = null, startDisk = null, extra = "") {
  const parts = [
    `Rangefind build: ${name} ${kind}`,
    `elapsed ${formatDuration(elapsedMs)}`,
    `rss ${formatBytes(memory.rss)}`,
    `heap ${formatBytes(memory.heapUsed)}`
  ];
  if (disk && startDisk) {
    const delta = diskDelta(startDisk, disk);
    parts.push(`temp ${formatBytes(delta.build || 0)}`);
    parts.push(`packs ${formatBytes(delta.final_packs || 0)}`);
    parts.push(`sidecars ${formatBytes(delta.sidecars || 0)}`);
  }
  if (extra) parts.push(extra);
  return parts.join(", ");
}

function logBuildProgress(telemetry, line) {
  if (!telemetry?.progressLogger || telemetry.progressLogMs <= 0 || !line) return;
  telemetry.progressLogger(line);
}

export function createBuildTelemetry(options = {}) {
  const startedAt = performance.now();
  const startMemory = memorySnapshot();
  return {
    format: "rfbuildtelemetry-v1",
    phases: [],
    workers: [],
    counters: {},
    diskByteGroups: normalizeDiskByteGroups(options.diskByteGroups || options.diskRoots),
    memorySamples: [],
    maxMemorySamples: Math.max(0, Math.floor(Number(options.maxMemorySamples ?? 4096))),
    sampleIntervalMs: Math.max(0, Math.floor(Number(options.sampleIntervalMs ?? 1000))),
    progressLogMs: Math.max(0, Math.floor(Number(options.progressLogMs || 0))),
    progressLogger: typeof options.progressLogger === "function" ? options.progressLogger : null,
    startMemory,
    peakMemory: startMemory,
    peakRss: startMemory.rss,
    startedAt
  };
}

export function addBuildCounter(telemetry, name, value) {
  if (!telemetry || !name) return;
  telemetry.counters[name] = (telemetry.counters[name] || 0) + (Number(value) || 0);
}

export function recordBuildWorkers(telemetry, phase, workers, details = {}) {
  if (!telemetry || !phase) return;
  const rows = (workers || []).map(worker => ({
    worker: worker.worker,
    tasks: worker.tasks || 0,
    docs: worker.docs || 0,
    batches: worker.batches || 0,
    input_bytes: worker.inputBytes || 0,
    analysis_ms: worker.analysisMs || 0,
    reduce_ms: worker.reduceMs || 0,
    finish_ms: worker.finishMs || 0,
    mode: worker.mode || ""
  }));
  telemetry.workers.push({
    phase,
    count: rows.length,
    ...details,
    workers: rows
  });
}

function recordMemorySample(telemetry, phase, memory) {
  if (!telemetry || telemetry.memorySamples.length >= telemetry.maxMemorySamples) return;
  telemetry.memorySamples.push({
    phase,
    ms: performance.now() - telemetry.startedAt,
    ...memory
  });
}

export async function timeBuildPhase(telemetry, name, fn) {
  if (!telemetry) return fn();
  const start = performance.now();
  const startMemory = memorySnapshot();
  const startCpu = process.cpuUsage();
  const startDisk = diskSnapshot(telemetry.diskByteGroups);
  logBuildProgress(telemetry, phaseProgressLine(name, "started", 0, startMemory));
  let peakMemory = startMemory;
  let samples = 0;
  recordMemorySample(telemetry, name, startMemory);
  const sample = () => {
    const current = memorySnapshot();
    peakMemory = maxMemory(peakMemory, current);
    telemetry.peakMemory = maxMemory(telemetry.peakMemory, current);
    telemetry.peakRss = Math.max(telemetry.peakRss || 0, current.rss);
    recordMemorySample(telemetry, name, current);
    samples++;
  };
  const timer = telemetry.sampleIntervalMs > 0 ? setInterval(sample, telemetry.sampleIntervalMs) : null;
  timer?.unref?.();
  const progressTimer = telemetry.progressLogMs > 0 ? setInterval(() => {
    const current = memorySnapshot();
    peakMemory = maxMemory(peakMemory, current);
    telemetry.peakMemory = maxMemory(telemetry.peakMemory, current);
    telemetry.peakRss = Math.max(telemetry.peakRss || 0, current.rss);
    const disk = diskSnapshot(telemetry.diskByteGroups);
    logBuildProgress(telemetry, phaseProgressLine(name, "running", performance.now() - start, current, disk, startDisk));
  }, telemetry.progressLogMs) : null;
  progressTimer?.unref?.();
  try {
    return await fn();
  } finally {
    if (timer) clearInterval(timer);
    if (progressTimer) clearInterval(progressTimer);
    const endMemory = memorySnapshot();
    const endCpu = process.cpuUsage();
    const endDisk = diskSnapshot(telemetry.diskByteGroups);
    recordMemorySample(telemetry, name, endMemory);
    peakMemory = maxMemory(peakMemory, endMemory);
    telemetry.peakMemory = maxMemory(telemetry.peakMemory, endMemory);
    telemetry.peakRss = Math.max(telemetry.peakRss || 0, endMemory.rss);
    telemetry.phases.push({
      name,
      ms: performance.now() - start,
      cpu: cpuDelta(startCpu, endCpu),
      startMemory,
      endMemory,
      peakMemory,
      samples,
      disk: {
        start: startDisk,
        end: endDisk,
        delta: diskDelta(startDisk, endDisk)
      }
    });
    logBuildProgress(
      telemetry,
      phaseProgressLine(name, "done", performance.now() - start, peakMemory, endDisk, startDisk, `peak ${formatBytes(peakMemory.rss)}`)
    );
  }
}

export function finishBuildTelemetry(telemetry) {
  if (!telemetry) return null;
  const endMemory = memorySnapshot();
  telemetry.peakMemory = maxMemory(telemetry.peakMemory, endMemory);
  telemetry.peakRss = Math.max(telemetry.peakRss || 0, endMemory.rss);
  return {
    format: telemetry.format,
    total_ms: performance.now() - telemetry.startedAt,
    peak_rss: telemetry.peakRss,
    peak_memory: telemetry.peakMemory,
    start_memory: telemetry.startMemory,
    end_memory: endMemory,
    counters: telemetry.counters,
    workers: telemetry.workers,
    memory_samples: telemetry.memorySamples,
    phases: telemetry.phases
  };
}
