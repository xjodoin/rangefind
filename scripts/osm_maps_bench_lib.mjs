function fold(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/\p{M}+/gu, "")
    .toLowerCase();
}

function resultRows(response) {
  return response?.results || response?.suggestions || [];
}

function resultText(row) {
  return row?.name || row?.title || row?.text || "";
}

function addCheck(checks, name, pass, actual) {
  checks.push({ name, pass: Boolean(pass), actual });
}

export function evaluateExpectations(response, expect = {}) {
  const rows = resultRows(response);
  const top = rows[0] || null;
  const lane = response?.stats?.plannerLane || response?.stats?.suggestLane || response?.stats?.geoLane || null;
  const checks = [];
  const metrics = {};
  if (expect.minResults != null) {
    addCheck(checks, "minResults", rows.length >= expect.minResults, rows.length);
  }
  if (expect.maxResults != null) {
    addCheck(checks, "maxResults", rows.length <= expect.maxResults, rows.length);
  }
  if (expect.topTextAny?.length) {
    const actual = resultText(top);
    const folded = fold(actual);
    addCheck(checks, "topTextAny", expect.topTextAny.some(value => folded.includes(fold(value))), actual);
  }
  if (expect.anyTextAny?.length) {
    const actual = rows.slice(0, expect.checkTop || 8).map(resultText);
    addCheck(
      checks,
      "anyTextAny",
      actual.some(text => expect.anyTextAny.some(value => fold(text).includes(fold(value)))),
      actual
    );
  }
  if (expect.targetTextAny?.length) {
    const rank = rows.findIndex(row => expect.targetTextAny.some(value => fold(resultText(row)).includes(fold(value)))) + 1;
    const maxRank = Math.max(1, Number(expect.targetMaxRank || rows.length || 1));
    addCheck(checks, "targetRank", rank > 0 && rank <= maxRank, rank || null);
    metrics.targetRank = rank || null;
    metrics.reciprocalRank = rank > 0 ? 1 / rank : 0;
  }
  if (expect.topTypes?.length) {
    const actual = top?.type || top?.category || "";
    addCheck(checks, "topTypes", expect.topTypes.map(fold).includes(fold(actual)), actual);
  }
  if (expect.topHasCoordinates) {
    const actual = { lat: top?.lat, lon: top?.lon };
    addCheck(
      checks,
      "topHasCoordinates",
      Number.isFinite(Number(top?.lat)) && Number.isFinite(Number(top?.lon)),
      actual
    );
  }
  if (expect.topHasAddress) {
    const actual = top?.address || "";
    addCheck(checks, "topHasAddress", Boolean(String(actual).trim()), actual);
  }
  if (expect.topHasId) {
    const actual = top?.id || "";
    addCheck(checks, "topHasId", Boolean(String(actual).trim()), actual);
  }
  if (expect.structuredSuggestion) {
    const actual = top ? {
      mainText: top.mainText,
      secondaryText: top.secondaryText,
      matchedRanges: top.matchedRanges,
      selection: top.selection
    } : null;
    addCheck(
      checks,
      "structuredSuggestion",
      Boolean(top?.mainText && Array.isArray(top?.matchedRanges) && top?.selection?.query),
      actual
    );
  }
  if (expect.topLocationTypes?.length) {
    addCheck(checks, "topLocationTypes", expect.topLocationTypes.includes(top?.locationType), top?.locationType || "");
  }
  if (expect.topReverseAccuracy?.length) {
    addCheck(
      checks,
      "topReverseAccuracy",
      expect.topReverseAccuracy.includes(top?.reverseGeocodeAccuracy),
      top?.reverseGeocodeAccuracy || ""
    );
  }
  if (expect.topAddressComponentTypes?.length) {
    const actual = (top?.addressComponents || []).flatMap(component => component.types || []);
    addCheck(
      checks,
      "topAddressComponentTypes",
      expect.topAddressComponentTypes.every(type => actual.includes(type)),
      actual
    );
  }
  if (expect.topDetailFields?.length) {
    const actual = Object.keys(top?.details || {});
    addCheck(
      checks,
      "topDetailFields",
      expect.topDetailFields.every(field => top?.details?.[field] != null),
      actual
    );
  }
  if (expect.anyDetailFields?.length) {
    const actual = rows.slice(0, expect.checkTop || 18)
      .flatMap(row => Object.keys(row?.details || {}));
    addCheck(
      checks,
      "anyDetailFields",
      expect.anyDetailFields.some(field => actual.includes(field)),
      [...new Set(actual)].sort()
    );
  }
  if (expect.detailCoverageMin != null) {
    const sample = rows.slice(0, expect.checkTop || 18);
    const covered = sample.filter(row => row?.details && Object.keys(row.details).length).length;
    const ratio = sample.length ? covered / sample.length : 0;
    addCheck(checks, "detailCoverageMin", ratio >= expect.detailCoverageMin, ratio);
    metrics.detailCoverage = ratio;
  }
  if (expect.topPostcodeAny?.length) {
    const actual = top?.postcode || "";
    addCheck(checks, "topPostcodeAny", expect.topPostcodeAny.map(fold).includes(fold(actual)), actual);
  }
  if (expect.topShard) {
    addCheck(checks, "topShard", top?.shard === expect.topShard, top?.shard || "");
  }
  if (expect.topLocalityAny?.length) {
    const actual = [top?.city, top?.district, top?.suburb, top?.county, top?.state].filter(Boolean);
    addCheck(
      checks,
      "topLocalityAny",
      actual.some(value => expect.topLocalityAny.some(expected => fold(value) === fold(expected))),
      actual
    );
  }
  if (expect.allTopShards?.length) {
    const actual = rows.slice(0, expect.checkTop || 8).map(row => row.shard || "");
    addCheck(checks, "allTopShards", actual.every(shard => expect.allTopShards.includes(shard)), actual);
  }
  if (expect.firstDistanceMax != null) {
    const actual = Number(top?.distanceMeters);
    addCheck(checks, "firstDistanceMax", Number.isFinite(actual) && actual <= expect.firstDistanceMax, actual);
  }
  if (expect.distanceAscending) {
    const actual = rows.slice(0, expect.checkTop || 8)
      .map(row => Number(row.distanceMeters))
      .filter(Number.isFinite);
    const ordered = actual.length >= 2 && actual.every((value, index) => index === 0 || value >= actual[index - 1]);
    addCheck(checks, "distanceAscending", ordered, actual);
  }
  if (expect.allDistancesMax != null) {
    const actual = rows.slice(0, expect.checkTop || 18)
      .map(row => Number(row.distanceMeters))
      .filter(Number.isFinite);
    addCheck(
      checks,
      "allDistancesMax",
      actual.length > 0 && actual.every(value => value <= expect.allDistancesMax),
      actual
    );
  }
  if (expect.uniqueIds) {
    const actual = rows.slice(0, expect.checkTop || 18).map(row => row.id).filter(Boolean);
    addCheck(
      checks,
      "uniqueIds",
      actual.length === Math.min(rows.length, expect.checkTop || 18) && new Set(actual).size === actual.length,
      actual
    );
  }
  if (expect.uniqueTexts) {
    const actual = rows.slice(0, expect.checkTop || 18).map(resultText).map(fold).filter(Boolean);
    addCheck(checks, "uniqueTexts", new Set(actual).size === actual.length, actual);
  }
  if (expect.viewportBox) {
    const box = expect.viewportBox;
    const positioned = rows.slice(0, expect.checkTop || 18)
      .filter(row => Number.isFinite(Number(row.lat)) && Number.isFinite(Number(row.lon)));
    const inside = positioned.length > 0 && positioned.every(row => (
      Number(row.lat) >= box.minLat
      && Number(row.lat) <= box.maxLat
      && (box.minLon <= box.maxLon
        ? Number(row.lon) >= box.minLon && Number(row.lon) <= box.maxLon
        : Number(row.lon) >= box.minLon || Number(row.lon) <= box.maxLon)
    ));
    addCheck(checks, "viewportBox", inside, `${positioned.length}/${Math.min(rows.length, expect.checkTop || 18)} positioned`);
  }
  if (expect.lanes?.length) {
    addCheck(checks, "lanes", expect.lanes.includes(lane), lane);
  }
  if (expect.maxShardsQueried != null) {
    const actual = Number(response?.stats?.shardsQueried || 0);
    addCheck(checks, "maxShardsQueried", actual <= expect.maxShardsQueried, actual);
  }
  return {
    passed: checks.every(check => check.pass),
    checks,
    metrics
  };
}

export function evaluateBudgets(cold, warm, budget = {}) {
  const checks = [];
  if (budget.coldMs != null) addCheck(checks, "coldMs", cold.ms <= budget.coldMs, Math.round(cold.ms));
  if (budget.coldRequests != null) addCheck(checks, "coldRequests", cold.requests <= budget.coldRequests, cold.requests);
  if (budget.coldBytes != null) addCheck(checks, "coldBytes", cold.bytes <= budget.coldBytes, cold.bytes);
  if (budget.warmMs != null) addCheck(checks, "warmMs", warm.ms <= budget.warmMs, Math.round(warm.ms));
  if (budget.warmRequests != null) addCheck(checks, "warmRequests", warm.requests <= budget.warmRequests, warm.requests);
  return {
    passed: checks.every(check => check.pass),
    checks
  };
}

function percentile(values, quantile) {
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * quantile) - 1));
  return sorted[index];
}

function weightedAverage(rows, value) {
  const totalWeight = rows.reduce((sum, row) => sum + Number(row.weight || 1), 0);
  if (!totalWeight) return null;
  return rows.reduce((sum, row) => sum + Number(value(row) || 0) * Number(row.weight || 1), 0) / totalWeight;
}

function aggregate(rows) {
  const usable = rows.filter(row => !row.error);
  const totalWeight = usable.reduce((sum, row) => sum + Number(row.weight || 1), 0);
  const qualityWeight = usable.reduce((sum, row) => sum + (row.quality?.passed ? Number(row.weight || 1) : 0), 0);
  const budgetWeight = usable.reduce((sum, row) => sum + (row.budget?.passed ? Number(row.weight || 1) : 0), 0);
  return {
    cases: rows.length,
    completed: usable.length,
    errors: rows.length - usable.length,
    weight: totalWeight,
    qualityPassRate: totalWeight ? qualityWeight / totalWeight : null,
    budgetPassRate: totalWeight ? budgetWeight / totalWeight : null,
    meanReciprocalRank: weightedAverage(
      usable.filter(row => row.quality?.metrics?.reciprocalRank != null),
      row => row.quality.metrics.reciprocalRank
    ),
    cold: {
      weightedMeanMs: weightedAverage(usable, row => row.cold.ms),
      p50Ms: percentile(usable.map(row => row.cold.ms), 0.5),
      p95Ms: percentile(usable.map(row => row.cold.ms), 0.95),
      weightedMeanRequests: weightedAverage(usable, row => row.cold.requests),
      weightedMeanBytes: weightedAverage(usable, row => row.cold.bytes)
    },
    warm: {
      weightedMeanMs: weightedAverage(usable, row => row.warm.ms),
      p50Ms: percentile(usable.map(row => row.warm.ms), 0.5),
      p95Ms: percentile(usable.map(row => row.warm.ms), 0.95),
      weightedMeanRequests: weightedAverage(usable, row => row.warm.requests)
    }
  };
}

export function summarizeCases(cases) {
  const families = {};
  for (const row of cases) {
    const family = row.family || "other";
    if (!families[family]) families[family] = [];
    families[family].push(row);
  }
  return {
    overall: aggregate(cases),
    families: Object.fromEntries(
      Object.entries(families).map(([family, rows]) => [family, aggregate(rows)])
    ),
    failures: cases
      .filter(row => row.error || !row.quality?.passed || !row.budget?.passed)
      .map(row => ({
        id: row.id,
        error: row.error || null,
        quality: row.quality?.checks?.filter(check => !check.pass) || [],
        budget: row.budget?.checks?.filter(check => !check.pass) || []
      }))
  };
}
