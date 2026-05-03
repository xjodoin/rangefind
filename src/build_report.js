function number(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

function positive(value) {
  return Math.max(0, number(value));
}

function sum(values) {
  return values.reduce((total, value) => total + number(value), 0);
}

function max(values) {
  return values.reduce((current, value) => Math.max(current, number(value)), 0);
}

function bytesPerDoc(bytes, docs) {
  return docs > 0 ? bytes / docs : 0;
}

function phaseDiskValue(phase, group, key = "delta") {
  return number(phase?.disk?.[key]?.[group]);
}

function summarizePhase(phase) {
  const tempDeltaBytes = phaseDiskValue(phase, "build");
  const finalPackDeltaBytes = phaseDiskValue(phase, "final_packs");
  const sidecarDeltaBytes = phaseDiskValue(phase, "sidecars");
  return {
    name: phase.name,
    ms: number(phase.ms),
    cpuUserMs: number(phase.cpu?.user_us) / 1000,
    cpuSystemMs: number(phase.cpu?.system_us) / 1000,
    samples: number(phase.samples),
    peakRss: number(phase.peakMemory?.rss),
    peakHeapUsed: number(phase.peakMemory?.heapUsed),
    peakHeapTotal: number(phase.peakMemory?.heapTotal),
    peakExternal: number(phase.peakMemory?.external),
    peakArrayBuffers: number(phase.peakMemory?.arrayBuffers),
    tempDeltaBytes,
    finalPackDeltaBytes,
    sidecarDeltaBytes,
    outputDeltaBytes: finalPackDeltaBytes + sidecarDeltaBytes,
    tempEndBytes: phaseDiskValue(phase, "build", "end"),
    finalPackEndBytes: phaseDiskValue(phase, "final_packs", "end"),
    sidecarEndBytes: phaseDiskValue(phase, "sidecars", "end")
  };
}

function workerSummary(workers = []) {
  return workers.map(group => ({
    phase: group.phase,
    count: group.count || 0,
    tasks: sum((group.workers || []).map(worker => worker.tasks)),
    docs: sum((group.workers || []).map(worker => worker.docs)),
    inputBytes: sum((group.workers || []).map(worker => worker.input_bytes)),
    analysisMs: sum((group.workers || []).map(worker => worker.analysis_ms)),
    reduceMs: sum((group.workers || []).map(worker => worker.reduce_ms)),
    finishMs: sum((group.workers || []).map(worker => worker.finish_ms)),
    mode: [...new Set((group.workers || []).map(worker => worker.mode).filter(Boolean))].join(",")
  }));
}

function maxSample(samples = [], field) {
  return max(samples.map(sample => sample?.[field]));
}

export function createBuildBenchmarkReport({
  telemetry,
  index = {},
  docs = 0,
  mode = "build",
  generatedAt = new Date().toISOString(),
  meta = null
} = {}) {
  const phases = (telemetry?.phases || []).map(summarizePhase);
  const samples = telemetry?.memory_samples || [];
  const indexBytes = number(index.bytes);
  const docCount = number(docs || meta?.docs || meta?.limit);
  const tempPeakBytes = max(phases.map(phase => phase.tempEndBytes));
  const tempWrittenBytes = sum(phases.map(phase => positive(phase.tempDeltaBytes)));
  const outputWrittenBytes = sum(phases.map(phase => positive(phase.outputDeltaBytes)));
  const finalPackBytes = max(phases.map(phase => phase.finalPackEndBytes));
  const sidecarBytes = max(phases.map(phase => phase.sidecarEndBytes));
  const peakMemory = telemetry?.peak_memory || {};

  return {
    format: "rfbuilderbench-v1",
    mode,
    generatedAt,
    docs: docCount,
    index: {
      files: number(index.files),
      bytes: indexBytes,
      bytesPerDoc: bytesPerDoc(indexBytes, docCount)
    },
    builder: {
      totalMs: number(telemetry?.total_ms),
      peakRss: number(telemetry?.peak_rss),
      peakHeapUsed: number(peakMemory.heapUsed),
      peakHeapTotal: number(peakMemory.heapTotal),
      peakExternal: number(peakMemory.external),
      peakArrayBuffers: number(peakMemory.arrayBuffers),
      memorySampleCount: samples.length,
      memorySamplePeakRss: maxSample(samples, "rss"),
      memorySamplePeakHeapUsed: maxSample(samples, "heapUsed"),
      phaseCount: phases.length,
      phaseSampleCount: sum(phases.map(phase => phase.samples)),
      tempPeakBytes,
      tempWrittenBytes,
      outputWrittenBytes,
      finalPackBytes,
      sidecarBytes,
      writeAmplification: indexBytes > 0 ? outputWrittenBytes / indexBytes : 0
    },
    counters: telemetry?.counters || {},
    workers: workerSummary(telemetry?.workers || []),
    phases,
    meta
  };
}
