import { evaluateOpeningHours } from "./opening_hours.js";

const DEFINITIONS = Object.freeze({
  toiletsWheelchair: {
    facet: "toilets_wheelchair",
    detail: "toilets_wheelchair",
    values: ["yes", "designated"],
    phrases: [/\bwheelchair[- ]accessible toilets?\b/giu, /\btoilettes? accessibles? en fauteuil roulant\b/giu]
  },
  wheelchair: {
    facet: "wheelchair",
    values: ["yes", "designated"],
    phrases: [/\bwheelchair[- ]accessible\b/giu, /\baccessible to wheelchairs\b/giu, /\baccessible en fauteuil roulant\b/giu]
  },
  contactless: {
    facet: "payment_contactless",
    detail: "payment_contactless",
    values: ["yes"],
    phrases: [/\b(?:with )?contactless(?: payment)?\b/giu, /\btap(?: payment)?\b/giu, /\bpaiement sans contact\b/giu]
  },
  delivery: { facet: "delivery", detail: "delivery", values: ["yes", "only"], phrases: [/\bwith delivery\b/giu, /\bdelivery available\b/giu, /\bavec livraison\b/giu] },
  takeaway: { facet: "takeaway", detail: "takeaway", values: ["yes", "only"], phrases: [/\b(?:with )?takeaway\b/giu, /\btakeout\b/giu, /\bpour emporter\b/giu] },
  driveThrough: { facet: "drive_through", detail: "drive_through", values: ["yes"], phrases: [/\bdrive[- ]through\b/giu, /\bservice au volant\b/giu] },
  outdoorSeating: { facet: "outdoor_seating", detail: "outdoor_seating", values: ["yes"], phrases: [/\boutdoor seating\b/giu, /\bpatio\b/giu, /\bavec terrasse\b/giu] },
  internet: { facet: "internet_access", detail: "internet_access", values: ["yes", "wlan", "terminal", "wired"], phrases: [/\b(?:with )?(?:wi-fi|wifi|internet)\b/giu] },
  reservation: { facet: "reservation", detail: "reservation", values: ["yes", "required", "recommended"], phrases: [/\b(?:takes?|accepts?) reservations?\b/giu] },
  free: { facet: "fee", detail: "fee", values: ["no"], phrases: [/\bfree(?: admission| entry)?\b/giu, /\bno fee\b/giu] }
});

const OPEN_PHRASES = [/\bopen now\b/giu, /\bcurrently open\b/giu, /\bouvert(?:e|s|es)? maintenant\b/giu];

function normalizeExplicit(value) {
  if (!value || typeof value !== "object") return {};
  const out = {};
  for (const key of Object.keys(DEFINITIONS)) {
    if (value[key] != null) out[key] = Boolean(value[key]);
  }
  if (value.openNow != null) out.openNow = Boolean(value.openNow);
  return out;
}

export function parseOsmConstraints(query, explicit = null) {
  let text = String(query || "");
  const constraints = normalizeExplicit(explicit);
  for (const [key, definition] of Object.entries(DEFINITIONS)) {
    for (const phrase of definition.phrases) {
      phrase.lastIndex = 0;
      if (phrase.test(text)) constraints[key] = true;
      phrase.lastIndex = 0;
      text = text.replace(phrase, " ");
    }
  }
  for (const phrase of OPEN_PHRASES) {
    phrase.lastIndex = 0;
    if (phrase.test(text)) constraints.openNow = true;
    phrase.lastIndex = 0;
    text = text.replace(phrase, " ");
  }
  return {
    query: text.replace(/\s+/gu, " ").replace(/^\s*(?:with|and)\s+|\s+(?:with|and)\s*$/giu, "").trim(),
    constraints
  };
}

export function compileOsmConstraintFilters(constraints, availableFacets = new Set()) {
  const facets = {};
  for (const [key, enabled] of Object.entries(constraints || {})) {
    const definition = DEFINITIONS[key];
    if (!enabled || !definition || !availableFacets.has(definition.facet)) continue;
    facets[definition.facet] = definition.values.slice();
  }
  return facets;
}

function normalizedDetailValues(value) {
  return new Set(String(value || "").toLowerCase().split(";").map(item => item.trim()).filter(Boolean));
}

export function evaluateOsmConstraints(result, constraints, options = {}) {
  const details = result?.details || {};
  const checks = {};
  for (const [key, enabled] of Object.entries(constraints || {})) {
    if (!enabled || key === "openNow") continue;
    const definition = DEFINITIONS[key];
    if (!definition) continue;
    const values = normalizedDetailValues(details[definition.detail || definition.facet]);
    checks[key] = definition.values.some(value => values.has(value));
  }
  let openingHours = null;
  if (constraints?.openNow) {
    openingHours = evaluateOpeningHours(details.opening_hours, {
      at: options.at,
      timeZone: options.timeZone,
      requireComplete: options.requireCompleteOpeningHours
    });
    checks.openNow = openingHours.isOpen === true
      || (openingHours.isOpen == null && options.includeUnknownOpenNow === true);
  }
  return {
    matches: Object.values(checks).every(Boolean),
    checks,
    ...(openingHours ? { openingHours } : {})
  };
}

export function annotateConstraintResult(result, evaluation) {
  if (!evaluation) return result;
  return {
    ...result,
    constraintMatches: evaluation.checks,
    ...(evaluation.openingHours ? {
      openNow: evaluation.openingHours.isOpen,
      openingHoursState: evaluation.openingHours.state,
      openingHoursEvaluation: evaluation.openingHours
    } : {})
  };
}

export const OSM_CONSTRAINT_DEFINITIONS = DEFINITIONS;
