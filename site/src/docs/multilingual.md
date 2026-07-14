---
title: Multilingual analysis
lede: One index, twenty-plus languages, and byte-identical analysis everywhere — Node and every browser.
description: Rangefind's multilingual analysis profile — per-document language detection, light stemmers and stopwords, script-aware folding, deterministic CJK bigrams, and cross-language query expansion.
order: 5
---

Turn on the multilingual profile in the build config:

```json
{ "analysis": { "profile": "multi-v1" } }
```

or pin languages explicitly:

```json
{ "analysis": { "profile": "multi-v1", "languages": ["en", "fr", "de"], "languageField": "lang" } }
```

## What the profile does

- **Per-document language** — detected from content, or read from
  `languageField` when your data already knows.
- **Light stemmers and stopwords** for 20+ languages, tuned for recall
  without aggressive over-stemming.
- **Script-aware folding** — Latin, Greek, Cyrillic, Arabic, Hebrew, and
  Devanagari diacritics and forms normalize predictably, so `Émile` matches
  `emile`.
- **CJK bigram tokenization** — dictionary-free and deterministic, so
  Chinese, Japanese, and Korean text is searchable without shipping
  segmentation dictionaries.
- **Cross-language query expansion** — a query analyzed under several
  plausible languages picks the plan with the best index evidence, so a
  French query against a mixed corpus finds French stems without an
  explicit language switch.

## The determinism rule

The entire analysis profile is **frozen into the manifest** at build time,
and the runtime reconstructs the exact same analyzer from it. Nothing
depends on ICU or platform locale tables — the same text tokenizes to the
same terms in Node 22, Chrome, Safari, and Firefox. This is what makes a
static index safe: query analysis can never drift from index analysis, even
across browser versions and years.

The practical consequence: [incremental updates](../incremental-updates/)
and [shards](../sharded-indexes/) must be built with the same `analysis`
config as their base — the builder enforces this and refuses mismatched
profiles rather than producing silently incomparable terms.
