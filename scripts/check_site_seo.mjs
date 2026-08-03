#!/usr/bin/env node

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const siteRoot = resolve(process.argv[2] || "site/_site");
const mapFile = resolve(process.argv[3] || "examples/osm-geo/public/index.html");

function read(path) {
  return readFileSync(path, "utf8");
}

function decodeHtml(value) {
  return String(value)
    .replaceAll("&amp;", "&")
    .replaceAll("&quot;", "\"")
    .replaceAll("&#39;", "'")
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">");
}

function title(html) {
  const value = html.match(/<title>([^<]+)<\/title>/iu)?.[1];
  assert.ok(value, "page must have a title");
  return decodeHtml(value.trim());
}

function meta(html, attribute, key) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const first = new RegExp(`<meta\\s+${attribute}="${escaped}"\\s+content="([^"]*)"`, "iu");
  const reverse = new RegExp(`<meta\\s+content="([^"]*)"\\s+${attribute}="${escaped}"`, "iu");
  const value = html.match(first)?.[1] ?? html.match(reverse)?.[1];
  assert.ok(value, `page must have ${attribute}=${key}`);
  return decodeHtml(value);
}

function canonical(html) {
  const value = html.match(/<link\s+rel="canonical"\s+href="([^"]+)"/iu)?.[1];
  assert.ok(value, "page must have a canonical URL");
  return value;
}

function structuredData(html) {
  const blocks = [...html.matchAll(/<script\s+type="application\/ld\+json">([\s\S]*?)<\/script>/giu)];
  assert.ok(blocks.length, "page must have JSON-LD structured data");
  return blocks.map(([, value]) => JSON.parse(value));
}

function assertSingleH1(html, label) {
  const count = (html.match(/<h1(?:\s[^>]*)?>/giu) || []).length;
  assert.equal(count, 1, `${label} must have exactly one h1`);
}

const home = read(resolve(siteRoot, "index.html"));
const landing = read(resolve(siteRoot, "google-maps-api-alternative/index.html"));
const sitemap = read(resolve(siteRoot, "sitemap.xml"));
const robots = read(resolve(siteRoot, "robots.txt"));
const map = read(mapFile);

const landingTitle = title(landing);
assert.match(landingTitle, /Free Google Maps API Alternative/iu);
assert.ok(landingTitle.length >= 35 && landingTitle.length <= 65, "landing title should remain SERP-sized");
const landingDescription = meta(landing, "name", "description");
assert.ok(
  landingDescription.length >= 110 && landingDescription.length <= 170,
  "landing description should remain useful in search snippets"
);
assert.equal(canonical(landing), "https://rangefind.dev/google-maps-api-alternative/");
assert.equal(meta(landing, "property", "og:url"), canonical(landing));
assert.match(meta(landing, "name", "robots"), /index,follow/iu);
assertSingleH1(landing, "Google Maps alternative landing page");
assert.match(landing, /https:\/\/osm\.rangefind\.dev\//u);
assert.match(landing, /best-effort public service/iu);
assert.match(landing, /© OpenStreetMap contributors/u);
assert.match(landing, /href="\/map\/"/u);

const landingGraph = structuredData(landing).flatMap(value => value["@graph"] || [value]);
assert.ok(landingGraph.some(value => value["@type"] === "SoftwareApplication"));
assert.ok(landingGraph.some(value => value["@type"] === "BreadcrumbList"));

assert.match(home, /href="\/google-maps-api-alternative\/"/u);
assert.match(home, /free Google Maps API alternative/iu);
assert.match(sitemap, /<loc>https:\/\/rangefind\.dev\/google-maps-api-alternative\/<\/loc>/u);
assert.match(sitemap, /<loc>https:\/\/rangefind\.dev\/map\/<\/loc>/u);
assert.match(robots, /Sitemap:\s+https:\/\/rangefind\.dev\/sitemap\.xml/iu);

assert.match(title(map), /Google Maps Search Alternative/iu);
assert.equal(canonical(map), "https://rangefind.dev/map/");
assert.equal(meta(map, "property", "og:url"), canonical(map));
assert.match(meta(map, "name", "robots"), /index,follow/iu);
assertSingleH1(map, "map demo");
assert.match(map, /href="https:\/\/rangefind\.dev\/google-maps-api-alternative\/"/u);
const mapStructuredData = structuredData(map).flatMap(value => value["@graph"] || [value]);
assert.ok(mapStructuredData.some(value => value["@type"] === "WebApplication"));

console.log("[seo] landing page, public index copy, map metadata, structured data, robots, and sitemap passed");
