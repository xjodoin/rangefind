// Date strings for the Date.parse parity fixture (rust/rangefind/tests/
// date_parse_parity.rs): the ES date-time format, V8's legacy fallback
// (month names, separators, RFC 2822, AM/PM, zone names and offsets,
// comments) and malformed input. Recorded with TZ=UTC: V8 reads zone-less
// date-times and legacy dates as local time, the Rust port as UTC.

function rng(seed) {
  let state = seed >>> 0 || 1;
  return () => {
    state ^= state << 13;
    state >>>= 0;
    state ^= state >>> 17;
    state ^= state << 5;
    state >>>= 0;
    return state / 0x100000000;
  };
}

const FIXED = [
  "", " ", "nope", "2026", "1970-01-01", "2010-01-01", "2010-01-01T01:00:00Z", "2010-01-01T01:00:00+01:00",
  "20100101", "12345678901", "201001", "2010010", "1", "12", "13", "32", "49", "50", "99", "100", "999", "0", "00",
  "000", "0000", "00000", "000000", "20260", "123456", "275760", "275761", "300000", "-000000", "-0000000", "-000001",
  "+000000", "+002010-06-15", "-002010-06-15", "+275760-09-13T00:00:00Z", "+275760-09-13T00:00:00.001Z",
  "-271821-04-20T00:00:00Z", "-271821-04-19T23:59:59.999Z", "-1", "-2010", "- 2010", "--2010", "2010-", "2010--06",
  "2010-6-15", "2010-06-5", "2010-13-01", "2010-00-10", "2010-06-00", "2010-06-32", "2010-02-31", "2010-02-29",
  "2012-02-29", "2010-06-15Z", "2010-06-15z", "2010-06-15T", "2010-06-15T10", "2010-06-15T10:00", "2010-06-15t10:00",
  "2010-06-15T10:00Z", "2010-06-15T10:00:00", "2010-06-15T10:00:00Z", "2010-06-15T10:00:00.5", "2010-06-15T10:00:00.05Z",
  "2010-06-15T10:00:00.123456Z", "2010-06-15T10:00:00.1234567890123Z", "2010-06-15T10:00:00.Z", "2010-06-15T24:00",
  "2010-06-15T24:00:00.000Z", "2010-06-15T24:00:01", "2010-06-15T24:01", "2010-06-15T25:00", "2010-06-15T10:60",
  "2010-06-15T10:00:60", "2010-06-15T10:00+0530", "2010-06-15T10:00+05:30", "2010-06-15T10:00-08:00",
  "2010-06-15T10:00+24:00", "2010-06-15T10:00+0560", "2010-06-15T10:00:00+01", "2010-06-15T10:00:00Z ",
  "2010-06-15T10:00:00 Z", "2010-06-15T10:00Z junk", "2010T10:00", "2010-06T10:00Z", "T10:00", "2010-06-15 10:00",
  "2010-06-15 10:00Z", "2010-06-15 10:00 GMT", "2010-06-15 10:00 junk", "10:00 2010-06-15", "2010/06/15",
  "2010/6/15 10:30", "6/15/2010", "12/31/1999", "1/2/3", "1/2/2003 4:05:06 PM", "15.06.2010", "1.2.2010", "1.5",
  "15 June 2010", "June 15, 2010", "June 15, 2010 3:00 PM", "June 15, 2010 3:00 PM EST", "Jun 15 2010 10:00 PDT",
  "Jan 5", "Jan 2010", "2010 Jan", "Jan 1 49", "Jan 1 50", "Jan 1 0000000002010", "Jan 1 2010", "Jan 1 2010",
  "Jan 1 2010 (comment) 10:00", "2010 (a(b)c) 1", "2010 (unclosed", "2010)", "2010 )", "Tue, 15 Jun 2010 10:00:00 GMT",
  "Tue, 15 Jun 2010 10:00:00 +0200", "Tue, 15 Jun 2010 10:00:00 -0830", "Thu, 01 Jan 1970 00:00:00 GMT",
  "Sat Mar 20 2021 12:34:56 GMT+0100 (Central European Standard Time)", "2010-06-15 GMT+2", "2010-06-15 GMT+0230",
  "2010-06-15 GMT+02:30", "2010-06-15 UTC-8", "2010-06-15 UTC+12345", "2010 Z", "2010 Zulu", "2010 +01", "10:00 +01",
  "1Z", "Z", "a", "a 1", "1 a", "AM 2010", "2010 10 PM", "2010 13:00 PM", "2010 12:00 AM", "2010 12:00 PM",
  "2010 1:2", "2010 1:2:3.4", "2010 1:2:3.45678", "2010 1:2:3:4", "2010 1:2:3:4:5", "2010 1::", "2010 1::2",
  "10:00.5 2010", "2010 1 2 3", "2010 1 2 3 4", "jun 2010 15", "jun 15 2010 10", "Jan 1 2010 10:00:00.1234",
  "2010 1 junk", "junk 2010", "junkx2010", "December 25, 1995 13:30", "Dec 25 95", "25 Dec 1995 13:30 UTC",
  "1995-12-25T13:30:00.000-05:00", "Mon Dec 25 1995", "Monday, December 25, 1995", " 2010", " \t2010", "\n2010",
  "2010\u00002011", "2010 ﻿ 06", "true", "false", "null", "[object Object]", "2010,6", "2010,6,15", "1e21",
  "１９７０", "2010-06-15T10:00:00.000Ｚ", "2010年6月15日"
];

const MONTHS = ["Jan", "February", "mar", "APR", "May", "June", "jul", "Aug", "Sept", "October", "nov", "Dec"];
const WEEKDAYS = ["Mon", "Tue", "Wednesday", "thu", "Fri", "Sat", "Sun"];
const ZONES = ["", "Z", "z", " UTC", " GMT", " UT", " EST", " pdt", " CST", " +0100", " -0530", " GMT+1", " GMT-0800", "+05:30", "-08:00", " junk"];
const pad = (n, width) => String(n).padStart(width, "0");

export function dateStrings() {
  const rand = rng(4242);
  const pick = list => list[Math.floor(rand() * list.length)];
  const int = (lo, hi) => lo + Math.floor(rand() * (hi - lo + 1));
  const out = new Set(FIXED);
  while (out.size < FIXED.length + 600) {
    const year = pick([int(1900, 2100), int(0, 99), int(-5000, 10000), int(100000, 300000)]);
    const month = pick([int(1, 12), int(0, 13)]);
    const day = pick([int(1, 28), int(0, 32)]);
    const hour = pick([int(0, 23), 24, int(0, 30)]);
    const minute = pick([int(0, 59), 60]);
    const second = int(0, 61);
    const ms = pick(["", `.${int(0, 9)}`, `.${pad(int(0, 999), 3)}`, `.${int(0, 999999)}`]);
    const iso = year >= 0 && year <= 9999 ? pad(year, 4) : `${year < 0 ? "-" : "+"}${pad(Math.abs(year), 6)}`;
    const forms = [
      () => `${iso}-${pad(month, 2)}-${pad(day, 2)}`,
      () => `${iso}-${pad(month, 2)}`,
      () => `${iso}-${pad(month, 2)}-${pad(day, 2)}T${pad(hour, 2)}:${pad(minute, 2)}${pick(["", `:${pad(second, 2)}${ms}`])}${pick(["", "Z", "+01:00", "-0830", "+1", "z"])}`,
      () => `${iso}-${pad(month, 2)}-${pad(day, 2)} ${hour}:${pad(minute, 2)}${pick(["", `:${pad(second, 2)}`])}${pick(ZONES)}`,
      () => `${month}/${day}/${year}${pick(["", ` ${hour}:${pad(minute, 2)}`, ` ${int(1, 12)}:${pad(minute, 2)} ${pick(["AM", "PM", "am", "pm"])}`])}`,
      () => `${year}/${month}/${day}`,
      () => `${pick(MONTHS)} ${day}${pick([",", ""])} ${year}${pick(["", ` ${hour}:${pad(minute, 2)}:${pad(second, 2)}`])}${pick(ZONES)}`,
      () => `${day} ${pick(MONTHS)} ${year}${pick(["", ` ${hour}:${pad(minute, 2)}`])}${pick(ZONES)}`,
      () => `${pick(WEEKDAYS)}, ${pad(day, 2)} ${pick(MONTHS)} ${year} ${pad(hour, 2)}:${pad(minute, 2)}:${pad(second, 2)}${pick(ZONES)}`,
      () => `${year}${pad(month, 2)}${pad(day, 2)}`,
      () => String(year),
      () => `${day}.${month}.${year}`,
      () => `${pick(MONTHS)}-${day}-${year}`,
      () => `${year} (${pick(["note", "a(b)", ""])}) ${pick(MONTHS)} ${day}`
    ];
    out.add(pick(forms)());
  }
  return [...out];
}

// The JS side of the fixture: every string with its Date.parse result
// (null for NaN).
export function dateParseCases() {
  return dateStrings().map(text => {
    const time = Date.parse(text);
    return [text, Number.isNaN(time) ? null : time];
  });
}
