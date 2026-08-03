const DAY_INDEX = new Map([
  ["Su", 0], ["Mo", 1], ["Tu", 2], ["We", 3], ["Th", 4], ["Fr", 5], ["Sa", 6]
]);

function zonedParts(at, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    weekday: "short",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23"
  });
  const parts = Object.fromEntries(formatter.formatToParts(at).map(part => [part.type, part.value]));
  const weekday = DAY_INDEX.get(parts.weekday?.slice(0, 2));
  return {
    weekday,
    minutes: Number(parts.hour) * 60 + Number(parts.minute),
    date: `${parts.year}-${parts.month}-${parts.day}`
  };
}

function daySet(surface) {
  if (!surface) return null;
  const days = new Set();
  for (const item of surface.split(",")) {
    const token = item.trim();
    const range = /^(Mo|Tu|We|Th|Fr|Sa|Su)-(Mo|Tu|We|Th|Fr|Sa|Su)$/u.exec(token);
    if (range) {
      let current = DAY_INDEX.get(range[1]);
      const end = DAY_INDEX.get(range[2]);
      for (let guard = 0; guard < 7; guard++) {
        days.add(current);
        if (current === end) break;
        current = (current + 1) % 7;
      }
      continue;
    }
    if (DAY_INDEX.has(token)) days.add(DAY_INDEX.get(token));
    else return null;
  }
  return days;
}

function clockMinutes(hour, minute) {
  const value = Number(hour) * 60 + Number(minute);
  return value >= 0 && value <= 24 * 60 ? value : null;
}

function parseRule(surface) {
  const clean = surface.replace(/"[^"]*"/gu, " ").trim();
  if (!clean) return null;
  if (/\b(?:PH|SH|week|sunrise|sunset|dawn|dusk|easter)\b/iu.test(clean)
      || /\d{4}|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/iu.test(clean)) {
    return { unsupported: true, surface: clean };
  }
  const dayMatch = /^(?:(Mo|Tu|We|Th|Fr|Sa|Su)(?:-(?:Mo|Tu|We|Th|Fr|Sa|Su))?(?:,(?:Mo|Tu|We|Th|Fr|Sa|Su)(?:-(?:Mo|Tu|We|Th|Fr|Sa|Su))?)*)\s+/u.exec(clean);
  const days = dayMatch ? daySet(dayMatch[0].trim()) : new Set([0, 1, 2, 3, 4, 5, 6]);
  if (!days) return { unsupported: true, surface: clean };
  const body = dayMatch ? clean.slice(dayMatch[0].length).trim() : clean;
  if (/^(?:off|closed)$/iu.test(body)) return { days, closed: true, surface: clean };
  if (/^(?:open|24\/7)$/iu.test(body)) return { days, intervals: [[0, 1440]], surface: clean };
  const intervals = [];
  for (const item of body.split(",")) {
    const match = /^(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})(?:\+)?$/u.exec(item.trim());
    if (!match) return { unsupported: true, surface: clean };
    const start = clockMinutes(match[1], match[2]);
    const end = clockMinutes(match[3], match[4]);
    if (start == null || end == null) return { unsupported: true, surface: clean };
    intervals.push([start, end]);
  }
  return { days, intervals, surface: clean };
}

function ruleState(rule, weekday, minutes) {
  if (rule.unsupported) return null;
  if (rule.closed) return rule.days.has(weekday) ? false : null;
  for (const [start, end] of rule.intervals || []) {
    if (end > start && rule.days.has(weekday) && minutes >= start && minutes < end) return true;
    if (end <= start) {
      if (rule.days.has(weekday) && minutes >= start) return true;
      if (rule.days.has((weekday + 6) % 7) && minutes < end) return true;
    }
  }
  return rule.days.has(weekday) ? false : null;
}

/**
 * Conservative client-side evaluator for the common OSM opening_hours
 * grammar. Unsupported calendar/holiday constructs return "unknown" rather
 * than claiming a business is open. Later rules override earlier ones, as in
 * the OSM specification's exception style.
 */
export function evaluateOpeningHours(value, options = {}) {
  const raw = String(value || "").trim();
  if (!raw) return { state: "unknown", isOpen: null, reason: "missing" };
  if (/^24\/7$/u.test(raw)) return { state: "open", isOpen: true, reason: "24/7", raw };
  const at = options.at instanceof Date ? options.at : new Date(options.at ?? Date.now());
  if (!Number.isFinite(at.getTime())) throw new RangeError("Opening-hours evaluation requires a valid date.");
  let local;
  try {
    local = zonedParts(at, options.timeZone);
  } catch {
    return { state: "unknown", isOpen: null, reason: "invalid-time-zone", raw };
  }
  const rules = raw.split(";").map(parseRule).filter(Boolean);
  if (!rules.length) return { state: "unknown", isOpen: null, reason: "unsupported", raw };
  let state = null;
  let matchedRule = null;
  let unsupported = false;
  for (const rule of rules) {
    if (rule.unsupported) {
      unsupported = true;
      continue;
    }
    const next = ruleState(rule, local.weekday, local.minutes);
    if (next != null) {
      state = next;
      matchedRule = rule.surface;
    }
  }
  if (state == null && !unsupported) {
    return { state: "closed", isOpen: false, reason: "outside-schedule", raw, localDate: local.date };
  }
  if (state == null || (unsupported && options.requireComplete !== false)) {
    return { state: "unknown", isOpen: null, reason: unsupported ? "unsupported-rule" : "no-applicable-rule", raw, localDate: local.date };
  }
  return {
    state: state ? "open" : "closed",
    isOpen: state,
    reason: "evaluated",
    rule: matchedRule,
    raw,
    localDate: local.date
  };
}
