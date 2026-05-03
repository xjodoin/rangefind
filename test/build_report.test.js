import assert from "node:assert/strict";
import test from "node:test";
import { createBuildBenchmarkReport } from "../src/build_report.js";

test("build benchmark report summarizes telemetry for builder baselines", () => {
  const report = createBuildBenchmarkReport({
    mode: "builder-only",
    docs: 100,
    index: { files: 12, bytes: 4096 },
    meta: { docs: 100 },
    generatedAt: "2026-05-03T00:00:00.000Z",
    telemetry: {
      total_ms: 1250,
      peak_rss: 9000,
      peak_memory: {
        rss: 9000,
        heapUsed: 3000,
        heapTotal: 5000,
        external: 700,
        arrayBuffers: 600
      },
      memory_samples: [
        { phase: "scan-and-spool", rss: 8000, heapUsed: 2500 },
        { phase: "reduce-postings", rss: 8800, heapUsed: 2700 }
      ],
      counters: { segment_files: 2 },
      workers: [
        {
          phase: "scan-and-spool",
          count: 2,
          workers: [
            { tasks: 2, docs: 60, input_bytes: 1200, analysis_ms: 7, mode: "worker-thread" },
            { tasks: 1, docs: 40, input_bytes: 900, analysis_ms: 5, mode: "worker-thread" }
          ]
        }
      ],
      phases: [
        {
          name: "scan-and-spool",
          ms: 500,
          cpu: { user_us: 400000, system_us: 50000 },
          samples: 3,
          peakMemory: { rss: 8000, heapUsed: 2500, heapTotal: 4000, external: 200, arrayBuffers: 100 },
          disk: {
            delta: { build: 2048, final_packs: 0, sidecars: 0 },
            end: { build: 2048, final_packs: 0, sidecars: 0 }
          }
        },
        {
          name: "reduce-postings",
          ms: 750,
          cpu: { user_us: 600000, system_us: 70000 },
          samples: 4,
          peakMemory: { rss: 8800, heapUsed: 2700, heapTotal: 4200, external: 300, arrayBuffers: 150 },
          disk: {
            delta: { build: -512, final_packs: 2500, sidecars: 700 },
            end: { build: 1536, final_packs: 2500, sidecars: 700 }
          }
        }
      ]
    }
  });

  assert.equal(report.format, "rfbuilderbench-v1");
  assert.equal(report.mode, "builder-only");
  assert.equal(report.docs, 100);
  assert.equal(report.index.bytesPerDoc, 40.96);
  assert.equal(report.builder.totalMs, 1250);
  assert.equal(report.builder.memorySampleCount, 2);
  assert.equal(report.builder.memorySamplePeakRss, 8800);
  assert.equal(report.builder.phaseSampleCount, 7);
  assert.equal(report.builder.tempPeakBytes, 2048);
  assert.equal(report.builder.tempWrittenBytes, 2048);
  assert.equal(report.builder.outputWrittenBytes, 3200);
  assert.equal(report.builder.writeAmplification, 3200 / 4096);
  assert.deepEqual(report.counters, { segment_files: 2 });
  assert.deepEqual(report.workers, [
    {
      phase: "scan-and-spool",
      count: 2,
      tasks: 3,
      docs: 100,
      inputBytes: 2100,
      analysisMs: 12,
      reduceMs: 0,
      finishMs: 0,
      mode: "worker-thread"
    }
  ]);
  assert.equal(report.phases[1].cpuUserMs, 600);
  assert.equal(report.phases[1].outputDeltaBytes, 3200);
});
