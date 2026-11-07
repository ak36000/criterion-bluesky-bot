/**
 * Criterion Channel Bluesky Bot — AWS Lambda port
 *
 * Trigger: EventBridge Scheduler, rate(1 minute), targeting this function.
 * State: a single DynamoDB item (table name from TABLE_NAME env var). See
 *   loadState() for the attributes.
 *
 * Posting (since 2026-11): the primary source is Criterion's own 24/7
 * programming schedule (SCHEDULE_URL), which gives exact start/end times for
 * about a week ahead. The bot posts ~1 minute before each scheduled start,
 * then checks the "now playing" page a few minutes after the start to
 * confirm the right film began. If the schedule can't be read, the original
 * scrape-and-estimate logic (runFallbackMode) takes over until it's back.
 * Secrets: BSKY_HANDLE / BSKY_APP_PASSWORD as Lambda environment variables.
 *
 * This is a straight port of the Cloudflare Worker version's scraping/
 * posting logic (same reasons for every heuristic — see inline comments),
 * with only the platform-specific plumbing swapped: KV -> DynamoDB, the
 * scheduled()/fetch() dual export -> a single handler that branches on
 * whether the invocation came from EventBridge Scheduler or the Function
 * URL (used the same way the old ?dryRun=true / ?filmpage= HTTP routes
 * were used for manual testing).
 */

import * as cheerio from 'cheerio';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TABLE_NAME = process.env.TABLE_NAME || 'CriterionBotState';
const STATE_PK = 'criterion-bot-state';

// Plain fetch() doesn't send the headers a real browser would, and
// Criterion's site rejects that (whatsonnow returns 403 without these).
const BROWSER_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.9',
};

// The Criterion 24/7 channel's own page — used as the link when we can't
// find a dedicated film page for whatever is currently playing.
const GENERIC_LINK = 'https://www.criterionchannel.com/live/1emmgvqX/criterion-24-7';

// The whatsonnow page sometimes renders with its <h1> as the channel's own
// branding instead of an actual film title — seen with no film link present
// either, so it looks like a station-ID/bumper state (or a not-yet-resolved
// server response) rather than a real "now playing" value. Treat these the
// same as a fetch error: not a real title, don't post it, don't let it
// clobber lastTitle.
const PLACEHOLDER_TITLES = new Set(['Criterion 24/7', 'Criterion Channel']);

// --- Polling schedule tuning ---
const FAST_INTERVAL_MS = 60 * 1000;
const SLOW_INTERVAL_MS = 5 * 60 * 1000;

// `now` is sampled after the EventBridge tick lands (plus cold start), so
// now + 60s is usually slightly AFTER the next tick, which then gets skipped
// by the nextCheckAt gate and silently halves the poll rate. Scheduling a
// little early avoids that; the worst case is one extra cheap no-op read.
const SCHEDULE_SLACK_MS = 20 * 1000;
const scheduleIn = (now, ms) => now + ms - SCHEDULE_SLACK_MS;

// Margin = minutes before the *estimated* end of a film that we start polling.
const MARGIN_TIGHT = 3;    // used once an early wake has caught a change in fast mode
const MARGIN_START = 10;   // default, and the floor after a first-try detection
const MARGIN_MAX = 20;     // cap on escalation
const FAST_GRACE_POLLS = 5; // fast polls to keep going after the margin window

function isGenericLink(href) {
  if (!href) return true;
  return href.replace(/\/$/, '') === GENERIC_LINK;
}

// --- Deterministic record key, used to make duplicate posts impossible ---
// Two concurrent invocations for the same film compute the same key, and the
// PDS refuses to create a second record at an existing key.
const B32 = '234567abcdefghijklmnopqrstuvwxyz';

function makeTid(micros, clockId) {
  let v = (BigInt(micros) << 10n) | BigInt(clockId & 1023);
  let out = '';
  for (let i = 0; i < 13; i++) { out = B32[Number(v & 31n)] + out; v >>= 5n; }
  return out;
}

function hash10(str) {
  let h = 2166136261;
  for (const c of str) { h ^= c.charCodeAt(0); h = Math.imul(h, 16777619); }
  return (h >>> 0) & 1023;
}

// Same title within the same 30-minute bucket => same key.
function dedupeRkey(title, nowMs) {
  const BUCKET_MS = 30 * 60 * 1000;
  const bucketMicros = Math.floor(nowMs / BUCKET_MS) * BUCKET_MS * 1000;
  return makeTid(bucketMicros, hash10(title));
}

// Read pixel dimensions from a WebP file's header. Criterion's image URLs
// don't carry w/h query params, so the downloaded bytes are the only
// reliable source. Returns null for anything that isn't a recognizable WebP.
function getWebpDimensions(buf) {
  const b = new Uint8Array(buf);
  if (b.length < 30) return null;
  const tag = (o) => String.fromCharCode(b[o], b[o + 1], b[o + 2], b[o + 3]);
  if (tag(0) !== 'RIFF' || tag(8) !== 'WEBP') return null;

  const chunk = tag(12);
  if (chunk === 'VP8X') {
    return {
      width: 1 + (b[24] | (b[25] << 8) | (b[26] << 16)),
      height: 1 + (b[27] | (b[28] << 8) | (b[29] << 16)),
    };
  }
  if (chunk === 'VP8 ') {
    if (b[23] !== 0x9d || b[24] !== 0x01 || b[25] !== 0x2a) return null;
    return {
      width: (b[26] | (b[27] << 8)) & 0x3fff,
      height: (b[28] | (b[29] << 8)) & 0x3fff,
    };
  }
  if (chunk === 'VP8L') {
    if (b[20] !== 0x2f) return null;
    const bits = (b[21] | (b[22] << 8) | (b[23] << 16) | (b[24] << 24)) >>> 0;
    return { width: (bits & 0x3fff) + 1, height: ((bits >>> 14) & 0x3fff) + 1 };
  }
  return null;
}

// Parse a release year and runtime out of text like "1 hr 50 min" or "50 min". Criterion's
// film pages render the release year glued directly to the front of this
// string with no separator (e.g. a 2000 release that runs 1hr50 shows up in
// scraped text as "20001 hr 50 min"), so we strip a plausible leading year
// before parsing the numbers.

function validYear(y) {
  const n = parseInt(y, 10);
  return n >= 1888 && n <= new Date().getFullYear() + 1 ? String(n) : null;
}

function parseReleaseYear(rawText) {
  if (!rawText) return null;
  const m = rawText.match(/((?:18|19|20)\d{2})\s*\d+\s*(?:hr|min)\b/i);
  return m ? validYear(m[1]) : null;
}

function parseRuntimeMinutes(rawText) {
  if (!rawText) return null;
  const stripYear = (digits) => (digits.length >= 5 ? digits.replace(/^(19|20)\d{2}/, '') : digits);

  let m = rawText.match(/(\d+)\s*hr\s*(\d{1,2})\s*min/i);
  if (m) {
    const hours = parseInt(stripYear(m[1]), 10);
    const minutes = parseInt(m[2], 10);
    if (!Number.isNaN(hours) && !Number.isNaN(minutes)) return hours * 60 + minutes;
  }

  m = rawText.match(/(\d+)\s*hr\b/i);
  if (m) {
    const hours = parseInt(stripYear(m[1]), 10);
    if (!Number.isNaN(hours)) return hours * 60;
  }

  m = rawText.match(/(\d+)\s*min\b/i);
  if (m) {
    const minutes = parseInt(stripYear(m[1]), 10);
    if (!Number.isNaN(minutes)) return minutes;
  }

  return null;
}

// A crude "does this look like a person's name, not nav/boilerplate text"
// check, used only as a fallback when JSON-LD isn't available.
function looksLikeName(text) {
  if (!text) return false;
  const t = text.trim();
  if (!t || t.length > 60) return false;
  if (/^(home|new|all films|subscribe|log in|search|criterion|watch|details|account)\b/i.test(t)) return false;
  return /^[A-ZÀ-Ý][\p{L}.'-]*(?:\s+[A-ZÀ-Ý][\p{L}.'-]*){0,4}$/u.test(t);
}

// Parse an ISO 8601 duration like "PT1H50M58S" into whole minutes (seconds
// dropped, to match the "1 hr 50 min" the site shows users elsewhere).
function parseISO8601DurationMinutes(iso) {
  if (!iso) return null;
  const m = iso.match(/^PT(?:(\d+)H)?(?:(\d+)M)?(?:\d+(?:\.\d+)?S)?$/i);
  if (!m) return null;
  const hours = m[1] ? parseInt(m[1], 10) : 0;
  const minutes = m[2] ? parseInt(m[2], 10) : 0;
  if (!hours && !minutes) return null;
  return hours * 60 + minutes;
}

// Film pages embed schema.org JSON-LD for SEO (VideoObject/Movie), which
// gives us exact director/cast/runtime with no guessing. This is the
// primary data source; parseRuntimeMinutes/looksLikeName above are only a
// fallback for if Criterion ever drops this markup.
function extractFromJsonLd($doc) {
  const nodes = [];
  $doc('script[type="application/ld+json"]').each((_, el) => {
    try {
      const parsed = JSON.parse($doc(el).contents().text());
      for (const item of Array.isArray(parsed) ? parsed : [parsed]) {
        if (item && Array.isArray(item['@graph'])) nodes.push(...item['@graph']);
        else if (item) nodes.push(item);
      }
    } catch {
      // Malformed or non-JSON script tag matching the selector - ignore it.
    }
  });

  const videoNode = nodes.find((n) => n && n['@type'] === 'VideoObject');
  const movieNode = nodes.find((n) => n && n['@type'] === 'Movie');

  const names = (value) => (Array.isArray(value) ? value : value ? [value] : [])
    .map((p) => p?.name)
    .filter(Boolean);

  const director = names(videoNode?.director).join(', ');
  const cast = names(movieNode?.actor ?? videoNode?.actor).join(', ');
  const runtimeMinutes = parseISO8601DurationMinutes(movieNode?.duration ?? videoNode?.duration);
    // Only Movie-node fields: VideoObject dates are likely the upload date.
  const rawDate = movieNode?.datePublished ?? movieNode?.dateCreated ?? movieNode?.copyrightYear;
  const yearMatch = String(rawDate ?? '').match(/\d{4}/);
  const releaseYear = yearMatch ? validYear(yearMatch[0]) : null;

  return { director, cast, runtimeMinutes, releaseYear };
}

// Positional fallback for when JSON-LD isn't present or is missing a field.
function extractPositionally($doc) {
  let runtimeMinutes = null;
  let releaseYear = null; 
  let director = '';
  let $scope = $doc('h1').first();
  for (let i = 0; i < 3 && $scope.length; i++) {
    if (runtimeMinutes === null) runtimeMinutes = parseRuntimeMinutes($scope.text());
	if (releaseYear === null) releaseYear = parseReleaseYear($scope.text()); 
    if (!director) {
      const prevText = $scope.prev().text().trim();
      if (looksLikeName(prevText)) director = prevText;
    }
    if (runtimeMinutes !== null && director && releaseYear) break;
    $scope = $scope.parent();
  }

  const bodyText = $doc('body').text();
  const starringMatch = bodyText.match(
    /Starring\s+([^]*?)(?:\s*(?:Supplements|Categories|My List|Trailer|Commentary)|$)/i,
  );
  const filmInfo = [
    director ? `Directed by ${director}` : '',
    starringMatch ? `Starring ${starringMatch[1].trim()}` : '',
  ].filter(Boolean).join('\n');

  return { runtimeMinutes, filmInfo, releaseYear };
}

// Scrape a dedicated film page (https://www.criterionchannel.com/films/...)
// for the director, poster image, runtime, and cast list.
async function scrapeFilmPage(url) {
  const res = await fetch(url, { headers: BROWSER_HEADERS, signal: AbortSignal.timeout(15_000) });
  if (!res.ok) throw new Error(`Film page fetch failed: HTTP ${res.status}`);
  const html = await res.text();
  const $film = cheerio.load(html);

  const imageUrl = $film('meta[property="og:image"]').attr('content') ?? null;

  const { director, cast, runtimeMinutes: jsonLdRuntime, releaseYear: jsonLdYear } = extractFromJsonLd($film);
  let runtimeMinutes = jsonLdRuntime;
  let releaseYear = jsonLdYear;
  let filmInfo = [director ? `Directed by ${director}` : '', cast ? `Starring ${cast}` : ''].filter(Boolean).join('\n');

  if (runtimeMinutes === null || !filmInfo || !releaseYear) {
    const fallback = extractPositionally($film);
    if (runtimeMinutes === null) runtimeMinutes = fallback.runtimeMinutes;
    if (!filmInfo) filmInfo = fallback.filmInfo;
    if (!releaseYear) releaseYear = fallback.releaseYear;
  }

  return { imageUrl, runtimeMinutes, filmInfo, releaseYear };
}

// Repairs text whose UTF-8 bytes were mis-decoded as a single-byte charset
// (e.g. "Fassbinder‚Äôs Women" -> "Fassbinder’s Women"). Returns the input
// untouched if it doesn't look like mojibake.
const REVERSE_MAPS = ['macintosh', 'windows-1252'].map((enc) => {
  const dec = new TextDecoder(enc);
  const map = new Map();
  for (let b = 0x80; b <= 0xff; b++) {
    map.set(dec.decode(new Uint8Array([b])), b);
  }
  return map;
});

function fixMojibake(str) {
  if (!str || !/[^\x00-\x7f]/.test(str)) return str; // pure ASCII: nothing to fix
  for (const map of REVERSE_MAPS) {
    const bytes = [];
    let ok = true;
    for (const ch of str) {
      const cp = ch.codePointAt(0);
      if (cp < 0x80) bytes.push(cp);
      else if (map.has(ch)) bytes.push(map.get(ch));
      else { ok = false; break; }
    }
    if (!ok) continue;
    try {
      const fixed = new TextDecoder('utf-8', { fatal: true }).decode(new Uint8Array(bytes));
      if (fixed !== str) return fixed;
    } catch { /* not valid UTF-8 after reversal, so not mojibake from this charset */ }
  }
  return str;
}

// --- DynamoDB state (replaces the Worker's KV namespace) ---
// Everything lives in one item so a read/write is a single request instead
// of four. loadState() always returns all four fields (null if unset, same
// as KV.get() would have).
async function loadState() {
  const res = await ddb.send(new GetCommand({ TableName: TABLE_NAME, Key: { pk: STATE_PK } }));
  const item = res.Item || {};
  return {
    lastTitle: item.lastTitle ?? null,
    nextCheckAtStr: item.nextCheckAt ?? null,
    pollMode: item.pollMode ?? null,
    fastPollCountStr: item.fastPollCount ?? null,
    margin: item.margin ?? null,
    // Schedule mode:
    lastPostedKey: item.lastPostedKey ?? null,     // mediaid|startTime of the last showing posted
    verifyKey: item.verifyKey ?? null,             // showing awaiting the after-start check
    verifyAt: item.verifyAt ?? null,
    verifyAttempts: item.verifyAttempts ?? 0,
    scheduleRetryAt: item.scheduleRetryAt ?? null, // set while the schedule feed is unavailable
  };
}

async function saveState(fields) {
  const names = {};
  const values = {};
  const sets = [];
  let i = 0;
  for (const [k, v] of Object.entries(fields)) {
    const nk = `#k${i}`;
    const vk = `:v${i}`;
    names[nk] = k;
    values[vk] = v;
    sets.push(`${nk} = ${vk}`);
    i += 1;
  }
  await ddb.send(new UpdateCommand({
    TableName: TABLE_NAME,
    Key: { pk: STATE_PK },
    UpdateExpression: `SET ${sets.join(', ')}`,
    ExpressionAttributeNames: names,
    ExpressionAttributeValues: values,
  }));
}

function jsonResponse(statusCode, obj) {
  return {
    statusCode,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(obj, null, 2),
  };
}

// --- Criterion 24/7 programming schedule (JW Player media feed) ---
// The 24/7 player page loads this JSON client-side. Its `schedule` array has
// exact UTC start/end times for roughly the next week of programming. Each
// entry's `mediaid` is also the film page id (/films/{mediaid} redirects to
// the full /films/{mediaid}/{slug} URL).
const SCHEDULE_URL = 'https://www.criterionchannel.com/api/media/1emmgvqX';

// Criterion's schedule is cut into daily blocks at 11:00 UTC, so a film that
// spans that boundary appears as two back-to-back entries with the same
// mediaid. Join those back into one entry.
function mergeSplitEntries(entries) {
  const out = [];
  for (const e of entries) {
    const prev = out[out.length - 1];
    if (prev && prev.mediaid === e.mediaid && prev.endTime === e.startTime) {
      prev.endTime = e.endTime;
      prev.mergedParts = (prev.mergedParts ?? 1) + 1;
    } else {
      out.push({
        mediaid: e.mediaid,
        title: e.title ?? e.episodeTitle ?? '',
        startTime: e.startTime,
        endTime: e.endTime,
        programType: e.programType ?? null,
        contentType: e.contentType ?? null,
      });
    }
  }
  return out;
}

// Given merged entries, return the one playing at `nowMs` and the one after.
function findNowAndNext(entries, nowMs) {
  const i = entries.findIndex(
    (e) => Date.parse(e.startTime) <= nowMs && nowMs < Date.parse(e.endTime),
  );
  if (i === -1) {
    const upcoming = entries.find((e) => Date.parse(e.startTime) > nowMs) ?? null;
    return { current: null, next: upcoming };
  }
  return { current: entries[i], next: entries[i + 1] ?? null };
}

// ?probe=schedule — read-only check that this Lambda can reach the schedule
// feed. Tries a few request variants one after another so we learn which
// headers/params are actually needed. Never touches DynamoDB or Bluesky.
async function probeSchedule() {
  const variants = [
    { name: 'browser headers, ?fresh=1', url: `${SCHEDULE_URL}?fresh=1`, headers: BROWSER_HEADERS },
    { name: 'browser headers, no param', url: SCHEDULE_URL, headers: BROWSER_HEADERS },
    { name: 'browser headers, JSON accept', url: SCHEDULE_URL, headers: { ...BROWSER_HEADERS, Accept: 'application/json' } },
    { name: 'plain fetch, no headers', url: SCHEDULE_URL, headers: {} },
  ];

  const results = [];
  let summary = null;

  for (const v of variants) {
    const started = Date.now();
    const r = { variant: v.name, url: v.url };
    try {
      const res = await fetch(v.url, { headers: v.headers, signal: AbortSignal.timeout(15_000) });
      const body = await res.text();
      r.status = res.status;
      r.ms = Date.now() - started;
      r.bytes = body.length;
      r.contentType = res.headers.get('content-type');
      r.cacheControl = res.headers.get('cache-control');
      r.age = res.headers.get('age');
      r.server = res.headers.get('server');
      r.cfRay = res.headers.get('cf-ray');
      try {
        const json = JSON.parse(body);
        const raw = Array.isArray(json.schedule) ? json.schedule : [];
        r.scheduleEntries = raw.length;
        if (raw.length) {
          r.scheduleFrom = raw[0].startTime;
          r.scheduleTo = raw[raw.length - 1].endTime;
        }
        if (!summary && raw.length) {
          const merged = mergeSplitEntries(raw);
          const now = Date.now();
          const { current, next } = findNowAndNext(merged, now);
          summary = {
            checkedAt: new Date(now).toISOString(),
            rawEntries: raw.length,
            mergedEntries: merged.length,
            splitsMerged: raw.length - merged.length,
            programTypes: [...new Set(raw.map((e) => `${e.programType}/${e.contentType}`))],
            current,
            next,
            secondsUntilNext: next ? Math.round((Date.parse(next.startTime) - now) / 1000) : null,
            nextFilmPage: next ? `https://www.criterionchannel.com/films/${next.mediaid}` : null,
          };
        }
      } catch {
        r.notJson = true;
        r.bodyStart = body.slice(0, 300);
      }
    } catch (e) {
      r.error = e.message;
      r.ms = Date.now() - started;
    }
    results.push(r);
    await new Promise((res) => setTimeout(res, 500)); // don't burst
  }

  // Baseline: the page the bot already scrapes, fetched the same way.
  let whatsonnow = null;
  try {
    const started = Date.now();
    const res = await fetch('https://whatsonnow.criterionchannel.com/', {
      headers: BROWSER_HEADERS,
      signal: AbortSignal.timeout(15_000),
    });
    const html = await res.text();
    whatsonnow = {
      status: res.status,
      ms: Date.now() - started,
      finalUrl: res.url,
      h1: cheerio.load(html)('h1').first().text().trim() || null,
    };
  } catch (e) {
    whatsonnow = { error: e.message };
  }

  return {
    region: process.env.AWS_REGION ?? null,
    ok: results.some((r) => r.status === 200 && r.scheduleEntries > 0),
    summary,
    variants: results,
    whatsonnowBaseline: whatsonnow,
  };
}

// --- Schedule-mode tuning ---
// Post at the first once-a-minute tick that lands within this long before a
// film's scheduled start. Ticks arrive once a minute at an arbitrary second,
// so posts land 30-90 seconds before the start (about a minute on average).
const POST_LEAD_MS = 90 * 1000;
// Wake this long before the posting tick to re-read the schedule, so a
// last-minute programming change is caught before we post.
const REFRESH_BEFORE_MS = 3 * 60 * 1000;
// Never sleep longer than this between schedule reads, so changes made
// mid-film are still picked up.
const MAX_SLEEP_MS = 30 * 60 * 1000;
// After a schedule-driven post, check the "now playing" page this long after
// the film's start to confirm the right film actually began.
const VERIFY_AFTER_START_MS = 3 * 60 * 1000;
const VERIFY_RETRY_MS = 2 * 60 * 1000;
const VERIFY_MAX_ATTEMPTS = 4;
// If the schedule can't be read, use the old method and only retry the
// schedule this often (it's ~250 KB, so don't refetch it every minute).
const SCHEDULE_RETRY_MS = 10 * 60 * 1000;

const RETRYABLE_STATUSES = new Set([429, 500, 502, 503, 504, 520, 521, 522, 523, 524]);

// fetch() with one retry on a transient 5xx/429 (Cloudflare/Vercel-fronted
// origins hand these back occasionally) or a network error/timeout.
async function fetchWithRetry(url, console, label) {
  let res = null;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      res = await fetch(url, { headers: BROWSER_HEADERS, signal: AbortSignal.timeout(15_000) });
      if (res.ok || !RETRYABLE_STATUSES.has(res.status) || attempt === 2) return res;
      console.warn(`${label} returned HTTP ${res.status} on attempt ${attempt}, retrying once...`);
    } catch (e) {
      if (attempt === 2) throw e;
      console.warn(`${label} fetch failed on attempt ${attempt} (${e.message}), retrying once...`);
    }
    await new Promise((r) => setTimeout(r, 2_000));
  }
  return res;
}

// Read and sanity-check the schedule. Returns merged entries, or null if the
// feed is unreachable, malformed, or doesn't cover the current time (a stale
// or empty schedule is treated the same as no schedule).
async function fetchSchedule(console, now) {
  let res;
  try {
    res = await fetchWithRetry(SCHEDULE_URL, console, 'Schedule feed');
  } catch (e) {
    console.warn(`SCHEDULE unavailable: ${e.message}`);
    return null;
  }
  if (!res.ok) {
    console.warn(`SCHEDULE unavailable: HTTP ${res.status}`);
    return null;
  }
  let json;
  try {
    json = await res.json();
  } catch (e) {
    console.warn(`SCHEDULE unavailable: response was not JSON (${e.message})`);
    return null;
  }
  const raw = Array.isArray(json?.schedule) ? json.schedule : [];
  const valid = raw.filter((e) => e && e.mediaid && (e.title || e.episodeTitle)
    && !Number.isNaN(Date.parse(e.startTime)) && !Number.isNaN(Date.parse(e.endTime)));
  if (!valid.length) {
    console.warn('SCHEDULE unavailable: no usable entries');
    return null;
  }
  const entries = mergeSplitEntries(valid);
  const { current } = findNowAndNext(entries, now);
  if (!current) {
    console.warn(`SCHEDULE unavailable: nothing scheduled at ${new Date(now).toISOString()} (feed covers ${entries[0].startTime} to ${entries[entries.length - 1].endTime})`);
    return null;
  }
  console.log(`SCHEDULE ok: ${raw.length} entries (${entries.length} after joining splits), now playing "${current.title}" until ${current.endTime}`);
  return entries;
}

const scheduleKey = (e) => `${e.mediaid}|${e.startTime}`;
const filmPageFor = (e) => `https://www.criterionchannel.com/films/${e.mediaid}`;

// Deterministic record key for a schedule-driven post: one per scheduled
// showing (mediaid + start time). The timestamp part is the start time
// floored to the minute minus 2 minutes, which is always slightly before the
// post goes out (posts go out at most 90 seconds before the start).
function scheduleRkey(entry) {
  const startMs = Date.parse(entry.startTime);
  const stampMs = Math.floor(startMs / 60_000) * 60_000 - 2 * 60_000;
  return makeTid(stampMs * 1000, hash10(entry.mediaid));
}

// "114 minutes (2:02 PM EST/11:02 AM PST)", measured from `fromMs`. Clock
// times drop the seconds (2:02:36 shows as 2:02), as Criterion's own pages do.
function formatNextStart(nextStartMs, fromMs) {
  const rounded = new Date(Math.floor(nextStartMs / 60_000) * 60_000);
  const minutes = Math.max(0, Math.round((nextStartMs - fromMs) / 60_000));
  const time = (tz) => rounded.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: tz });
  const zone = (tz, fallback) => rounded.toLocaleDateString('en-US', { timeZone: tz, timeZoneName: 'short' }).split(', ')[1] ?? fallback;
  return `${minutes} minute${minutes === 1 ? '' : 's'} (${time('America/New_York')} ${zone('America/New_York', 'ET')}/${time('America/Los_Angeles')} ${zone('America/Los_Angeles', 'PT')})`;
}

// Scrape the film page for poster, director/cast, year (and runtime, which
// only the fallback path needs). Never throws.
async function getFilmDetails(filmHref, console) {
  if (!filmHref || isGenericLink(filmHref)) {
    console.log('No dedicated film page; posting without extra metadata.');
    return { imageUrl: null, filmInfo: '', runtimeMinutes: null, releaseYear: null };
  }
  try {
    const d = await scrapeFilmPage(filmHref);
    console.log(`Film page: ${filmHref}`);
    console.log(`Image URL: ${d.imageUrl}`);
    console.log(`Runtime: ${d.runtimeMinutes ?? 'unknown'} min`);
    console.log(`Release year: ${d.releaseYear ?? 'unknown'}`);
    console.log(`Film info: ${d.filmInfo}`);
    return d;
  } catch (e) {
    console.warn('Could not fetch/parse film page:', e.message);
    return { imageUrl: null, filmInfo: '', runtimeMinutes: null, releaseYear: null };
  }
}

// Build the post text + link facet. `nextLine` is the full "Next film starts
// ..." line.
function buildPost({ title, releaseYear, filmInfo, nextLine, filmLink }) {
  const linkText = 'Watch on Criterion Channel';
  const BSKY_LIMIT = 300;

  function truncateFilmInfo(info, budget) {
    if (!info) return '';
    const lines = info.split('\n');
    if ([...info].length <= budget) return info;
    if (lines.length > 1 && [...lines[0]].length <= budget) return lines[0];
    return [...lines[0]].slice(0, budget - 1).join('') + '…';
  }

  const titleLine = releaseYear ? `${title} (${releaseYear})` : title;
  const basePost = `🎬 Now streaming on Criterion Channel 24/7:\n\n${titleLine}\n\n${nextLine}\n\n${linkText}`;
  const filmInfoBudget = Math.max(0, BSKY_LIMIT - [...basePost].length - 1);
  const filmInfoTrimmed = truncateFilmInfo(fixMojibake(filmInfo), filmInfoBudget);
  const postText = filmInfoTrimmed
    ? `🎬 Now streaming on Criterion Channel 24/7:\n\n${titleLine}\n${filmInfoTrimmed}\n\n${nextLine}\n\n${linkText}`
    : basePost;

  const encoder = new TextEncoder();
  const beforeLink = postText.slice(0, postText.lastIndexOf(linkText));
  const byteStart = encoder.encode(beforeLink).length;
  const byteEnd = byteStart + encoder.encode(linkText).length;
  const facets = [{
    index: { byteStart, byteEnd },
    features: [{ $type: 'app.bsky.richtext.facet#link', uri: filmLink }],
  }];
  return { postText, facets };
}

// Log in and create the post (with poster image if available). Retries the
// whole thing up to 3 times; the deterministic rkey makes retries and
// concurrent invocations safe. Throws if every attempt fails.
async function publishPost({ title, postText, facets, imageUrl, rkey, console }) {
  async function postToBluesky() {
    console.log('Logging in to Bluesky...');
    const loginRes = await fetch('https://bsky.social/xrpc/com.atproto.server.createSession', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        identifier: process.env.BSKY_HANDLE,
        password: process.env.BSKY_APP_PASSWORD,
      }),
      signal: AbortSignal.timeout(30_000),
    });

    if (!loginRes.ok) throw new Error(`Login failed: ${loginRes.status} ${await loginRes.text()}`);
    const { accessJwt, did } = await loginRes.json();
    console.log('Logged in.');

    let embed;
    if (imageUrl) {
      const imageStepStart = Date.now();
      let stage = 'starting';
      try {
        stage = 'fetching image from imgix';
        const fetchStart = Date.now();
        const imgRes = await fetch(imageUrl, { signal: AbortSignal.timeout(30_000) });
        console.log(`Image fetch: HTTP ${imgRes.status} in ${Date.now() - fetchStart}ms`);

        if (!imgRes.ok) {
          console.warn(`Image fetch returned HTTP ${imgRes.status}, skipping image.`);
        } else {
          stage = 'reading image bytes';
          const imgBuffer = await imgRes.arrayBuffer();
          const contentType = imgRes.headers.get('content-type') ?? 'image/jpeg';
          console.log(`Image downloaded: ${imgBuffer.byteLength} bytes (${contentType})`);

          stage = 'uploading blob to Bluesky';
          const uploadStart = Date.now();
          const uploadRes = await fetch('https://bsky.social/xrpc/com.atproto.repo.uploadBlob', {
            method: 'POST',
            headers: {
              'Authorization': `Bearer ${accessJwt}`,
              'Content-Type': contentType,
            },
            body: imgBuffer,
            signal: AbortSignal.timeout(30_000),
          });
          console.log(`Blob upload: HTTP ${uploadRes.status} in ${Date.now() - uploadStart}ms`);

          if (uploadRes.ok) {
            stage = 'parsing upload response';
            const { blob } = await uploadRes.json();

            const dims = getWebpDimensions(imgBuffer);
            const aspectRatio = dims ? { width: dims.width, height: dims.height } : undefined;
            console.log(dims
              ? `Image dimensions: ${dims.width}x${dims.height}`
              : 'Could not read image dimensions; posting without aspectRatio.');

            embed = {
              $type: 'app.bsky.embed.images',
              images: [{ image: blob, alt: `Film poster for ${title}`, aspectRatio }],
            };
          } else {
            console.warn('Blob upload failed:', await uploadRes.text());
          }
        }
      } catch (e) {
        console.warn(`Image step failed while ${stage} (${Date.now() - imageStepStart}ms elapsed): ${e.message}`);
      }
    }

    const postRes = await fetch('https://bsky.social/xrpc/com.atproto.repo.createRecord', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessJwt}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        repo: did,
        collection: 'app.bsky.feed.post',
        rkey,
        record: {
          $type: 'app.bsky.feed.post',
          text: postText,
          facets,
          embed,
          createdAt: new Date().toISOString(),
        },
      }),
      signal: AbortSignal.timeout(30_000),
    });

    const postBody = await postRes.text();
    if (!postRes.ok) {
      const check = await fetch(
        `https://bsky.social/xrpc/com.atproto.repo.getRecord?repo=${encodeURIComponent(did)}&collection=app.bsky.feed.post&rkey=${rkey}`,
        { signal: AbortSignal.timeout(15_000) },
      );
      if (check.ok) {
        console.log('DEDUPED: record already exists', rkey, 'original error:', postBody);
        return 'deduped';
      }
      throw new Error(`Post failed: ${postRes.status} ${postBody}`);
    }
    const created = JSON.parse(postBody);
    console.log('POST_CREATED', JSON.stringify({ uri: created.uri, cid: created.cid, rkey }));
    return 'posted';
  }

  let lastError;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const outcome = await postToBluesky();
      console.log(outcome === 'deduped'
        ? `Already posted by another invocation: ${title}`
        : `Posted: ${title}`);
      return outcome;
    } catch (e) {
      lastError = e;
      console.warn(`Attempt ${attempt} failed: ${e.message}`);
      if (attempt < 3) await new Promise((r) => setTimeout(r, 10_000));
    }
  }
  throw lastError;
}

// Scrape the "now playing" page. Returns { title, filmHref } with title ''
// when the page is unreachable or showing the channel's own branding.
async function scrapeNowPlaying(console) {
  let res = null;
  try {
    res = await fetchWithRetry('https://whatsonnow.criterionchannel.com/', console, 'whatsonnow.criterionchannel.com');
  } catch (e) {
    console.warn(`whatsonnow.criterionchannel.com fetch failed: ${e.message}. Treating as no title change.`);
    return { title: '', filmHref: null };
  }
  if (!res.ok) {
    console.warn(`whatsonnow.criterionchannel.com returned HTTP ${res.status}. Treating as no title change.`);
    return { title: '', filmHref: null };
  }
  const $ = cheerio.load(await res.text());
  const title = fixMojibake($('h1').first().text().trim());
  const filmHrefRaw = $('a[href*="/films/"]').first().attr('href') || null;
  const filmHref = filmHrefRaw ? new URL(filmHrefRaw, 'https://www.criterionchannel.com').toString() : null;
  if (PLACEHOLDER_TITLES.has(title)) {
    console.warn(`whatsonnow.criterionchannel.com's <h1> was the channel's own branding ("${title}"), not a film title. Treating as no title change.`);
    return { title: '', filmHref: null };
  }
  console.log(`Now playing (whatsonnow): ${title}`);
  return { title, filmHref };
}

async function runBot({ dryRun = false, invocationId = 'manual' } = {}) {
  // Shadow console so every log line (including inside helpers) is tagged
  // with this invocation's id.
  const console = {
    log: (...a) => globalThis.console.log(`[${invocationId}]`, ...a),
    warn: (...a) => globalThis.console.warn(`[${invocationId}]`, ...a),
  };

  const state = await loadState();
  const now = Date.now();
  const nextCheckAt = state.nextCheckAtStr ? Date.parse(state.nextCheckAtStr) : 0;

  console.log('STATE_READ', JSON.stringify({ ...state, now: new Date(now).toISOString() }));

  // --- Respect the scheduled wait (skip this gate during a dry run) ---
  if (nextCheckAt && now < nextCheckAt && !dryRun) {
    console.log(`Skipping — next check scheduled for ${new Date(nextCheckAt).toISOString()}`);
    return;
  }
  if (nextCheckAt && now < nextCheckAt && dryRun) {
    console.log(`(Dry run ignoring schedule gate — normally would skip until ${new Date(nextCheckAt).toISOString()})`);
  }

  // --- Primary: the published schedule. Fallback: scrape "now playing". ---
  const scheduleRetryAt = state.scheduleRetryAt ? Date.parse(state.scheduleRetryAt) : 0;
  let entries = null;
  if (dryRun || !scheduleRetryAt || now >= scheduleRetryAt) {
    entries = await fetchSchedule(console, now);
    if (!entries && !dryRun) {
      await saveState({ scheduleRetryAt: new Date(now + SCHEDULE_RETRY_MS).toISOString() });
    }
  } else {
    console.log(`Schedule unavailable earlier; next retry at ${state.scheduleRetryAt}. Using fallback.`);
  }

  if (entries) {
    return runScheduleMode({ entries, state, now, dryRun, console });
  }
  console.log('MODE fallback (scraping now-playing page)');
  return runFallbackMode({ state, now, dryRun, console });
}

// --- Schedule mode ---
async function runScheduleMode({ entries, state, now, dryRun, console }) {
  console.log('MODE schedule');
  const { current, next } = findNowAndNext(entries, now);

  // The showing to post right now, if any: the next one once we're within
  // POST_LEAD_MS of its start, otherwise whatever is on.
  const due = next && now >= Date.parse(next.startTime) - POST_LEAD_MS ? next : current;
  const dueKey = due ? scheduleKey(due) : null;
  const dueStarted = due ? now >= Date.parse(due.startTime) : false;

  let action = 'none';
  if (due && dueKey !== state.lastPostedKey) {
    // Already posted under its title (by the fallback, or by the version of
    // the bot before this one): adopt it instead of posting it again.
    action = dueStarted && due.title === state.lastTitle ? 'adopt' : 'post';
  }

  const updates = {
    // Reset the fallback's polling so it starts fresh if it's ever needed.
    pollMode: 'waiting',
    fastPollCount: '0',
    scheduleRetryAt: null,
  };
  let lastPostedKey = state.lastPostedKey;
  let verifyKey = state.verifyKey ?? null;
  let verifyAt = state.verifyAt ? Date.parse(state.verifyAt) : 0;
  let verifyAttempts = state.verifyAttempts ?? 0;
  let preview = null;

  if (action === 'adopt') {
    console.log(`"${due.title}" (${dueKey}) was already posted by title; adopting it without posting.`);
    lastPostedKey = dueKey;
    updates.lastPostedKey = dueKey;
  } else if (action === 'post') {
    const after = entries[entries.indexOf(due) + 1];
    const nextStartMs = Date.parse(after?.startTime ?? due.endTime);
    const filmLink = filmPageFor(due);
    const details = await getFilmDetails(filmLink, console);
    const nextLine = `Next film starts in: ${formatNextStart(nextStartMs, now)}`;
    const { postText, facets } = buildPost({
      title: due.title, releaseYear: details.releaseYear, filmInfo: details.filmInfo, nextLine, filmLink,
    });
    const rkey = scheduleRkey(due);
    const secondsEarly = Math.round((Date.parse(due.startTime) - now) / 1000);
    console.log(`POSTING "${due.title}" ${secondsEarly >= 0 ? `${secondsEarly}s before` : `${-secondsEarly}s after`} its scheduled start (${due.startTime}). RKEY ${rkey}`);
    preview = { title: due.title, scheduledStart: due.startTime, secondsBeforeStart: secondsEarly, filmLink, imageUrl: details.imageUrl, releaseYear: details.releaseYear, filmInfo: details.filmInfo, postText, facets, rkey };

    if (!dryRun) {
      try {
        await publishPost({ title: due.title, postText, facets, imageUrl: details.imageUrl, rkey, console });
      } catch (e) {
        // Leave lastPostedKey alone so the next tick tries again.
        await saveState({ ...updates, nextCheckAt: new Date(now + 60 * 1000).toISOString() })
          .catch((err) => console.warn('Could not save retry state after failure:', err.message));
        throw e;
      }
    }
    lastPostedKey = dueKey;
    verifyKey = dueKey;
    verifyAt = Math.max(now, Date.parse(due.startTime)) + VERIFY_AFTER_START_MS;
    verifyAttempts = 0;
    Object.assign(updates, {
      lastTitle: due.title,
      lastPostedKey: dueKey,
      verifyKey,
      verifyAt: new Date(verifyAt).toISOString(),
      verifyAttempts: 0,
    });
  } else if (verifyKey && now >= verifyAt) {
    // --- Backup check: did the film we announced actually start? ---
    const result = await verifyNowPlaying({ entries, state, verifyKey, verifyAttempts, now, dryRun, console });
    if (result.done) {
      verifyKey = null;
      Object.assign(updates, { verifyKey: null, verifyAt: null, verifyAttempts: 0 });
      if (result.postedTitle) updates.lastTitle = result.postedTitle;
    } else {
      verifyAttempts += 1;
      verifyAt = now + VERIFY_RETRY_MS;
      Object.assign(updates, { verifyAttempts, verifyAt: new Date(verifyAt).toISOString() });
    }
    preview = result;
  }

  // --- Plan the next wake-up ---
  const upcoming = entries.find((e) => scheduleKey(e) !== lastPostedKey
    && Date.parse(e.startTime) - POST_LEAD_MS > now);
  const wakes = [now + MAX_SLEEP_MS];
  let postAt = null;
  if (upcoming) {
    postAt = Date.parse(upcoming.startTime) - POST_LEAD_MS;
    const refreshAt = postAt - REFRESH_BEFORE_MS;
    // Re-read the schedule a few minutes ahead; once that's passed, wake
    // exactly at the posting time.
    wakes.push(refreshAt > now + 30 * 1000 ? refreshAt : postAt);
  }
  if (verifyKey) wakes.push(Math.max(verifyAt, now + 30 * 1000));
  const wakeAt = Math.min(...wakes);
  updates.nextCheckAt = new Date(wakeAt).toISOString();

  console.log(`PLAN next wake ${updates.nextCheckAt}${upcoming ? `; next post "${upcoming.title}" at ${new Date(postAt).toISOString()} (starts ${upcoming.startTime})` : ''}${verifyKey ? `; verify pending at ${new Date(verifyAt).toISOString()}` : ''}`);

  if (dryRun) {
    console.log('--- DRY RUN: not posting to Bluesky, not saving state ---');
    // Always preview a post, even when nothing is due, so dry runs stay useful.
    if (!preview && (due || next)) {
      const show = due ?? next;
      const after = entries[entries.indexOf(show) + 1];
      const filmLink = filmPageFor(show);
      const details = await getFilmDetails(filmLink, console);
      const { postText, facets } = buildPost({
        title: show.title, releaseYear: details.releaseYear, filmInfo: details.filmInfo, filmLink,
        nextLine: `Next film starts in: ${formatNextStart(Date.parse(after?.startTime ?? show.endTime), now)}`,
      });
      preview = { title: show.title, scheduledStart: show.startTime, filmLink, imageUrl: details.imageUrl, releaseYear: details.releaseYear, filmInfo: details.filmInfo, postText, facets, rkey: scheduleRkey(show), note: 'Preview only: this showing is not due to be posted right now.' };
    }
    return {
      dryRun: true,
      mode: 'schedule',
      action,
      current,
      next,
      preview,
      plan: {
        nextCheckAt: updates.nextCheckAt,
        nextPost: upcoming ? { title: upcoming.title, startTime: upcoming.startTime, postAt: new Date(postAt).toISOString() } : null,
        verifyPending: verifyKey ? new Date(verifyAt).toISOString() : null,
      },
    };
  }

  await saveState(updates);
  if (action === 'none' && !preview) console.log('Nothing to post. Done.');
  return undefined;
}

// After a schedule-driven post, confirm on the "now playing" page that the
// announced film is really what started. If something else is on, post that
// film using the old method. Returns { done, postedTitle? }.
async function verifyNowPlaying({ entries, state, verifyKey, verifyAttempts, now, dryRun, console }) {
  const idx = entries.findIndex((e) => scheduleKey(e) === verifyKey);
  const expected = idx >= 0 ? entries[idx] : null;
  const previous = idx > 0 ? entries[idx - 1] : null;
  const giveUp = verifyAttempts + 1 >= VERIFY_MAX_ATTEMPTS;

  const { title, filmHref } = await scrapeNowPlaying(console);
  if (!title) {
    console.warn(`VERIFY could not read the now-playing page (attempt ${verifyAttempts + 1}/${VERIFY_MAX_ATTEMPTS}).`);
    return { verify: 'unreadable', done: giveUp };
  }
  if (title === state.lastTitle || (expected && title === expected.title)) {
    console.log(`VERIFY ok: now playing "${title}" as scheduled.`);
    return { verify: 'ok', title, done: true };
  }
  if (previous && title === previous.title) {
    // The page hasn't caught up with the change yet.
    console.log(`VERIFY page still shows the previous film "${title}" (attempt ${verifyAttempts + 1}/${VERIFY_MAX_ATTEMPTS}).`);
    if (giveUp) console.warn('VERIFY giving up: page never showed the scheduled film.');
    return { verify: 'lagging', title, done: giveUp };
  }

  // Something other than what we announced is playing: post it, old-style.
  console.warn(`VERIFY mismatch: schedule said "${expected?.title ?? verifyKey}", but now playing is "${title}". Posting it with the fallback method.`);
  const details = await getFilmDetails(filmHref, console);
  const nextLine = details.runtimeMinutes
    ? `Next film starts around: ${formatNextStart(now + details.runtimeMinutes * 60 * 1000, now)}`
    : 'Next film starts around: unknown';
  const filmLink = filmHref && !isGenericLink(filmHref) ? filmHref : GENERIC_LINK;
  const { postText, facets } = buildPost({ title, releaseYear: details.releaseYear, filmInfo: details.filmInfo, nextLine, filmLink });
  const rkey = dedupeRkey(title, now);
  if (!dryRun) {
    await publishPost({ title, postText, facets, imageUrl: details.imageUrl, rkey, console });
  }
  return { verify: 'mismatch', title, postText, rkey, done: true, postedTitle: title };
}

// --- Fallback mode: the original scrape-and-estimate logic ---
// Used only when the schedule can't be read. The "next film" time here is an
// estimate from the film's runtime, so the post says "around".
async function runFallbackMode({ state, now, dryRun, console }) {
  const { lastTitle, pollMode, fastPollCountStr, margin: marginState } = state;
  const fastPollCount = fastPollCountStr ? parseInt(fastPollCountStr, 10) : 0;

  const { title, filmHref } = await scrapeNowPlaying(console);
  const titleChanged = Boolean(title) && title !== lastTitle;

  let details = { imageUrl: null, filmInfo: '', runtimeMinutes: null, releaseYear: null };
  if (titleChanged || dryRun) details = await getFilmDetails(filmHref, console);
  const { imageUrl, filmInfo, runtimeMinutes, releaseYear } = details;
  const filmLink = filmHref && !isGenericLink(filmHref) ? filmHref : GENERIC_LINK;

  // --- Determine next check time, poll mode, and wake-up margin ---
  const currentMargin = marginState ?? MARGIN_START;
  let newPollMode = pollMode ?? 'waiting';
  let newFastPollCount = fastPollCount;
  let newMargin = currentMargin;
  let nextCheckMs;

  if (titleChanged) {
    // How we detected the change tells us how far off our end-of-film
    // estimate was, which decides how early to wake next time.
    if (pollMode === 'fast') {
      newMargin = MARGIN_TIGHT;
      console.log(`DETECTION fast: caught after ${fastPollCount} unchanged poll(s); end estimate was off by ~${(currentMargin - fastPollCount + 0.5).toFixed(1)} min (positive = estimated too late). Margin ${currentMargin} -> ${newMargin}.`);
    } else if (pollMode === 'slow') {
      newMargin = MARGIN_START;
      console.log(`DETECTION slow: caught during slow polling. Margin ${currentMargin} -> ${newMargin}.`);
    } else {
      newMargin = Math.min(Math.max(currentMargin + 5, MARGIN_START), MARGIN_MAX);
      console.log(`DETECTION first-try: end estimate was off by >= ${currentMargin} min. Margin ${currentMargin} -> ${newMargin}.`);
    }

    newPollMode = 'waiting';
    newFastPollCount = 0;
    if (runtimeMinutes !== null && runtimeMinutes > 1) {
      const sleepMin = Math.max(runtimeMinutes - newMargin, 1);
      nextCheckMs = scheduleIn(now, sleepMin * 60 * 1000);
      console.log(`New film posted. Sleeping ~${sleepMin} min (runtime ${runtimeMinutes} - margin ${newMargin}).`);
    } else {
      nextCheckMs = scheduleIn(now, SLOW_INTERVAL_MS);
    }
  } else if (newPollMode === 'waiting') {
    newPollMode = 'fast';
    newFastPollCount = 1;
    nextCheckMs = scheduleIn(now, FAST_INTERVAL_MS);
    console.log('Entering fast poll mode (1 min intervals).');
  } else if (newPollMode === 'fast') {
    newFastPollCount += 1;
    const fastLimit = currentMargin + FAST_GRACE_POLLS;
    if (newFastPollCount >= fastLimit) {
      newPollMode = 'slow';
      nextCheckMs = scheduleIn(now, SLOW_INTERVAL_MS);
      console.log(`Fast poll limit reached (${newFastPollCount}/${fastLimit}). Switching to slow (5 min) mode.`);
    } else {
      nextCheckMs = scheduleIn(now, FAST_INTERVAL_MS);
      console.log(`Fast poll ${newFastPollCount}/${fastLimit}. Next check in 1 minute.`);
    }
  } else {
    nextCheckMs = scheduleIn(now, SLOW_INTERVAL_MS);
    console.log('Slow poll mode. Next check in 5 minutes.');
  }

  // Don't sleep past the next schedule retry, so we switch back promptly.
  const retryAt = Date.parse(state.scheduleRetryAt ?? '') || now + SCHEDULE_RETRY_MS;
  nextCheckMs = Math.min(nextCheckMs, Math.max(retryAt, now + 30 * 1000));

  if (!dryRun) {
    await saveState({
      nextCheckAt: new Date(nextCheckMs).toISOString(),
      pollMode: newPollMode,
      fastPollCount: String(newFastPollCount),
      margin: newMargin,
    });
  }

  if (!titleChanged && !dryRun) {
    console.log('No new film. Done.');
    return undefined;
  }

  const nextLine = runtimeMinutes !== null
    ? `Next film starts around: ${formatNextStart(now + runtimeMinutes * 60 * 1000, now)}`
    : 'Next film starts around: unknown';
  const { postText, facets } = buildPost({ title, releaseYear, filmInfo, nextLine, filmLink });
  const rkey = dedupeRkey(title, now);
  console.log('RKEY', rkey);

  if (dryRun) {
    console.log('--- DRY RUN: not posting to Bluesky, not saving state ---');
    return {
      dryRun: true,
      mode: 'fallback',
      title, titleChanged, filmHref, filmLink, imageUrl, runtimeMinutes, releaseYear, filmInfo,
      postText, facets, rkey,
      schedule: {
        pollMode, newPollMode,
        margin: currentMargin, newMargin,
        nextCheckAt: new Date(nextCheckMs).toISOString(),
      },
    };
  }

  try {
    await publishPost({ title, postText, facets, imageUrl, rkey, console });
  } catch (e) {
    try {
      await saveState({
        nextCheckAt: new Date(Date.now() + 60 * 1000).toISOString(),
        pollMode: 'waiting',
        fastPollCount: '0',
      });
    } catch (err) {
      console.warn('Could not reset schedule after failure:', err.message);
    }
    throw e;
  }
  // lastPostedKey is cleared so schedule mode re-checks by title on return.
  await saveState({ lastTitle: title, lastPostedKey: null });
  return undefined;
}

// Single entry point for both trigger types:
//   - EventBridge Scheduler invocations have no requestContext/rawQueryString.
//   - Function URL invocations carry the API Gateway v2 HTTP payload shape,
//     used the same way ?dryRun=true / ?filmpage= worked on the Worker.
export const handler = async (event) => {
  const invocationId = crypto.randomUUID().slice(0, 8);
  const isHttp = Boolean(event && (event.requestContext?.http || event.rawQueryString !== undefined));

  globalThis.console.log(`[${invocationId}] INVOKE`, JSON.stringify({
    isHttp,
    startedAt: new Date().toISOString(),
  }));

  if (isHttp) {
    // Setting names and values are matched case-insensitively, since browsers
    // sometimes autocomplete a differently-capitalized URL from history.
    const params = new Map();
    for (const [k, v] of new URLSearchParams(event.rawQueryString || '')) params.set(k.toLowerCase(), v);
    const flag = (name) => (params.get(name) ?? '').toLowerCase();

    const filmPageUrl = params.get('filmpage');
    if (filmPageUrl) {
      try {
        const result = await scrapeFilmPage(filmPageUrl);
        return jsonResponse(200, result);
      } catch (e) {
        return jsonResponse(500, { error: e.message });
      }
    }

    if (flag('probe') === 'schedule') {
      try {
        return jsonResponse(200, await probeSchedule());
      } catch (e) {
        return jsonResponse(500, { error: e.message });
      }
    }

    if (flag('dryrun') === 'true') {
      const result = await runBot({ dryRun: true, invocationId });
      if (!result) {
        return jsonResponse(500, { error: 'runBot returned nothing — check CloudWatch logs for details.' });
      }
      return jsonResponse(200, result);
    }

    return {
      statusCode: 200,
      headers: { 'content-type': 'text/plain' },
      body: 'Criterion Bluesky Bot. Add ?dryRun=true to preview without posting, or ?probe=schedule to test access to the 24/7 schedule feed.',
    };
  }

  // EventBridge Scheduler invocation — the real thing.
  await runBot({ dryRun: false, invocationId });
  return { ok: true };
};
