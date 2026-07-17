---
title: Hosting the index
lede: If your host can serve a file and honor a Range header, it can serve rangefind. Here's how to do it well — and what it costs.
description: Static hosting requirements for rangefind indexes, cache-control guidance, CDN behavior, and cost comparisons for large indexes on object storage.
order: 3
---

## The one requirement

The runtime issues `Range: bytes=start-end` requests and expects
`206 Partial Content` responses. Netlify, Vercel, GitHub Pages, Cloudflare
Pages, S3, R2, GCS, nginx, Caddy, and virtually every CDN support this out
of the box. If a host answers `200` with the full file instead, queries still
work — they just transfer more than needed.

Two things to verify on exotic setups:

- **No compression middleware on packs.** The packs are already compressed
  internally; a proxy that gzips them on the fly breaks range math. Current
  Rangefind builds use a `.bin.gz` suffix for every range-addressed object so
  static hosts such as GitHub Pages do not apply transparent compression. For
  older `.bin` indexes, serve the files without transformation.
- **CORS, if the index lives on another origin.** Allow `GET` with the
  `Range` header and expose `Content-Range`.

## Cache headers that make it fast

Everything except the manifests is immutable and content-addressed:

```
/rangefind/manifest*.json      Cache-Control: max-age=60, must-revalidate
/rangefind/**/*.bin.gz         Cache-Control: public, max-age=31536000, immutable
/rangefind/**/*.bin            Cache-Control: public, max-age=31536000, immutable # legacy
```

With those headers, [incremental updates](../incremental-updates/) are
CDN-friendly by construction: unchanged packs keep their filenames, so a
site update invalidates only the small manifests.

## Small sites

For a docs or marketing site the index is usually a few megabytes — host it
with the site itself and stop thinking about it. This site does exactly that
on GitHub Pages; the `.bin.gz` object suffix keeps byte ranges stable through
Pages' automatic content encoding.

## Large indexes on object storage

Multi-gigabyte indexes (Wikipedia-scale corpora, [geo
datasets](../geo-search/)) belong on object storage with a CDN in front:

- **Cloudflare R2** — native range support, zero egress fees. A ~750 GB
  planet-scale OSM index costs about **$11/month** in storage; a country-
  scale index (~10 GB) fits in the free tier. Front it with a custom domain
  and the cache rules above.
- **Backblaze B2 + Cloudflare CDN** — ~$6/TB-month with free egress through
  the bandwidth alliance; the cheapest legitimate setup at scale.
- **Avoid** standard S3/GCS egress pricing (~$0.09/GB) — per-query transfer
  is small, but bulk downloads and misses add up; eliminating egress cost is
  the point of this architecture.

For comparison: hosted search APIs charge per request (commonly ~$5 per
1,000). A million queries a month against a static index is a few dollars
of CDN traffic — or zero on free tiers.

## Offline and embedded

Because the index is plain files, it also works where servers can't:
download a region's index directory and query it locally with
[`rangefind/node`](../node-runtime/), bundle one into an Electron or mobile
webview app, or mirror it wherever your users are. No key management, no
rate limits, no phoning home.
