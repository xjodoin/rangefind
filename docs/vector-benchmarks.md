# Vector Search Benchmarks

`scripts/vector_bench.mjs` builds a synthetic clustered corpus (anisotropic
within-cluster noise with a decaying spectrum, mimicking real embedding
models), indexes it, and measures recall@10 against an in-memory brute-force
oracle plus per-query latency and transfer over localhost HTTP. Recorded
2026-07-06 on an Apple Silicon laptop.

Reproduce with:

```bash
npm run bench:vectors
node scripts/vector_bench.mjs --docs=200000 --dims=384 --centers=1024
```

## 100,000 vectors × 256 dims (512 true clusters)

Index: 31 MB vector sidecar (int8, ~310 B/vector including the refine
store), 196 IVF clusters, 64 coarse dims, 56 KB root. Vector build phase:
**2.25 s** (spool streaming, permutation + k-means training, quantization,
packing) inside a 4.9 s total build from a 139 MB JSONL.

| nprobe | recall@10 | cold transfer | mean latency |
| ------ | --------- | ------------- | ------------ |
| 1      | 0.68      | 119 KB        | 8.8 ms |
| 4      | 0.78      | 229 KB        | 8.2 ms |
| 8      | 0.82      | 366 KB        | 8.7 ms |
| 16     | 0.87      | 668 KB        | 9.5 ms |
| 32     | 0.92      | 1.3 MB        | 11.8 ms |

Loss decomposition (measured): with every cluster probed, recall is 0.95 at
the default shortlist (`refineFactor: 8`) and 0.98 at `refineFactor: 32` —
the int8 quantization and coarse-prefix shortlist are nearly lossless, and
the remaining gap at small nprobe is standard IVF cluster-boundary misses.

## How it stays cheap

- **Coarse-then-refine**: cluster pages store only a 64-dim int8 prefix per
  vector for candidate ranking; the top candidates re-score against
  full-dimension int8 rows fetched from a fixed-width refine store
  (addressed as `ordinal × rowBytes` — no per-document pointers exist).
- **Variance-descending dimension permutation** (learned from the training
  sample, ~2 bytes/dim in the root) makes the coarse prefix carry the most
  informative components of any embedding model while preserving dot
  products exactly.
- **One root fetch per session**: int8 centroids plus cluster pointers;
  cluster pages and refine rows arrive through coalesced range requests.

## Hybrid retrieval

`engine.search({ q, vector })` runs the text and vector lanes in parallel
and fuses them with reciprocal rank fusion (`hybrid.rrfK`, default 60).
Filters apply to both lanes (vector candidates verify against doc values
before fusion). `engine.vectorSearch({ vector, k, nprobe })` is the pure
lane with real cosine scores. Query embedding generation happens in the
host (e.g. transformers.js in the browser); the engine consumes float
arrays, Float32Arrays, or base64-encoded float32 payloads.
