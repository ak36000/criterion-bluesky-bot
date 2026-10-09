/**
 * Criterion Channel Bluesky Bot — AWS Lambda port
 *
 * Trigger: EventBridge Scheduler, rate(1 minute), targeting this function.
 * State: a single DynamoDB item (table name from TABLE_NAME env var) with
 *   attributes: lastTitle, nextCheckAt, pollMode, fastPollCount.
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

async function runBot({ dryRun = false, invocationId = 'manual' } = {}) {
  // Shadow console so every log line (including inside postToBluesky) is
  // tagged with this invocation's id, same as the Worker version.
  const console = {
    log: (...a) => globalThis.console.log(`[${invocationId}]`, ...a),
    warn: (...a) => globalThis.console.warn(`[${invocationId}]`, ...a),
  };

  // --- Load state ---
  const { lastTitle, nextCheckAtStr, pollMode, fastPollCountStr, margin: marginState } = await loadState();
  const now = Date.now();
  const nextCheckAt = nextCheckAtStr ? new Date(nextCheckAtStr).getTime() : 0;
  const fastPollCount = fastPollCountStr ? parseInt(fastPollCountStr, 10) : 0;

  console.log('STATE_READ', JSON.stringify({
    lastTitle, nextCheckAtStr, pollMode, fastPollCountStr, marginState, 
    now: new Date(now).toISOString(),
  }));

  // --- Respect the scheduled wait (skip this gate during a dry run) ---
  if (nextCheckAt && now < nextCheckAt && !dryRun) {
    console.log(`Skipping — next check scheduled for ${new Date(nextCheckAt).toISOString()}`);
    return;
  }
  if (nextCheckAt && now < nextCheckAt && dryRun) {
    console.log(`(Dry run ignoring schedule gate — normally would skip until ${new Date(nextCheckAt).toISOString()})`);
  }

  // --- Scrape What's On Now, with a status-aware retry for transient
  // 5xx/429s (Cloudflare-fronted origins hand these back occasionally,
  // unrelated to our scraping logic). ---
  const RETRYABLE_STATUSES = new Set([429, 500, 502, 503, 504, 520, 521, 522, 523, 524]);
  let nowRes = null;
  try {
    for (let attempt = 1; attempt <= 2; attempt++) {
      nowRes = await fetch('https://whatsonnow.criterionchannel.com/', {
        headers: BROWSER_HEADERS,
        signal: AbortSignal.timeout(15_000),
      });
      if (nowRes.ok || !RETRYABLE_STATUSES.has(nowRes.status) || attempt === 2) break;
      console.warn(`whatsonnow.criterionchannel.com returned HTTP ${nowRes.status} on attempt ${attempt}, retrying once...`);
      await new Promise((r) => setTimeout(r, 2_000));
    }
  } catch (e) {
    console.warn(`whatsonnow.criterionchannel.com fetch failed: ${e.message}. Treating as no title change.`);
    nowRes = null;
  }

  let title = '';
  let filmHref = null;
  if (!nowRes || !nowRes.ok) {
    if (nowRes) {
      console.warn(`whatsonnow.criterionchannel.com returned HTTP ${nowRes.status}. Treating as no title change.`);
    }
  } else {
    const nowHtml = await nowRes.text();
    const $ = cheerio.load(nowHtml);
    title = fixMojibake($('h1').first().text().trim());
    const filmHrefRaw = $('a[href*="/films/"]').first().attr('href') || null;
    filmHref = filmHrefRaw ? new URL(filmHrefRaw, 'https://www.criterionchannel.com').toString() : null;

    if (PLACEHOLDER_TITLES.has(title)) {
      console.warn(`whatsonnow.criterionchannel.com's <h1> was the channel's own branding ("${title}"), not a film title. Treating as no title change.`);
      title = '';
      filmHref = null;
    } else {
      console.log(`Now playing: ${title}`);
    }
  }

  // --- Determine whether the film changed ---
  const titleChanged = Boolean(title) && title !== lastTitle;

  // --- If the film changed, scrape its page now for runtime/image/info ---
  let imageUrl = null;
  let filmInfo = '';
  let filmLink = GENERIC_LINK;
  let runtimeMinutes = null;
  let releaseYear = null;

  if (titleChanged) {
    if (filmHref && !isGenericLink(filmHref)) {
      filmLink = filmHref;
      try {
        const scraped = await scrapeFilmPage(filmHref);
        imageUrl = scraped.imageUrl;
        filmInfo = scraped.filmInfo;
        runtimeMinutes = scraped.runtimeMinutes;
        releaseYear = scraped.releaseYear; 
        console.log(`Film page: ${filmHref}`);
        console.log(`Image URL: ${imageUrl}`);
        console.log(`Runtime: ${runtimeMinutes ?? 'unknown'} min`);
        console.log(`Release year: ${releaseYear ?? 'unknown'}`);
        console.log(`Film info: ${filmInfo}`);
      } catch (e) {
        console.warn('Could not fetch/parse film page:', e.message);
      }
    } else {
      console.log('No dedicated film link found on the live page; posting without extra metadata.');
    }
  }

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
      // We woke early and caught the change within one 1-minute poll.
      newMargin = MARGIN_TIGHT;
      console.log(`DETECTION fast: caught after ${fastPollCount} unchanged poll(s); end estimate was off by ~${(currentMargin - fastPollCount + 0.5).toFixed(1)} min (positive = estimated too late). Margin ${currentMargin} -> ${newMargin}.`);
    } else if (pollMode === 'slow') {
      // Slow polling can be up to 5 min late, so don't trust it as a precise signal.
      newMargin = MARGIN_START;
      console.log(`DETECTION slow: caught during slow polling. Margin ${currentMargin} -> ${newMargin}.`);
    } else {
      // First poll after waking: the film had already changed, so lag is unknown (>= currentMargin).
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
    // The fast window must outlast the early-wake margin, plus some grace.
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

  // --- Save scheduling state (always, except during a dry run) ---
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
    return;
  }
  if (!titleChanged && dryRun) {
    console.log('No new film (dry run continues anyway, to preview the current film).');
  }

  // --- Build the post text ---
  let nextText = 'unknown';
  if (runtimeMinutes !== null) {
    const nextTime = new Date(now + runtimeMinutes * 60 * 1000);
    const etTime = nextTime.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' });
    const ptTime = nextTime.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/Los_Angeles' });
    const etZone = nextTime.toLocaleDateString('en-US', { timeZone: 'America/New_York', timeZoneName: 'short' }).split(', ')[1] ?? 'ET';
    const ptZone = nextTime.toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles', timeZoneName: 'short' }).split(', ')[1] ?? 'PT';
    nextText = `${runtimeMinutes} minutes (${etTime} ${etZone}/${ptTime} ${ptZone})`;
  }

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

  const basePost = `🎬 Now streaming on Criterion Channel 24/7:\n\n${titleLine}\n\nNext film starts around: ${nextText}\n\n${linkText}`;
  const baseCost = [...basePost].length;
  const filmInfoBudget = Math.max(0, BSKY_LIMIT - baseCost - 1);

  const filmInfoTrimmed = truncateFilmInfo(fixMojibake(filmInfo), filmInfoBudget);
  const postText = filmInfoTrimmed
    ? `🎬 Now streaming on Criterion Channel 24/7:\n\n${titleLine}\n${filmInfoTrimmed}\n\nNext film starts around: ${nextText}\n\n${linkText}`
    : basePost;

  const encoder = new TextEncoder();
  const beforeLink = postText.slice(0, postText.lastIndexOf(linkText));
  const byteStart = encoder.encode(beforeLink).length;
  const byteEnd = byteStart + encoder.encode(linkText).length;

  const facets = [{
    index: { byteStart, byteEnd },
    features: [{ $type: 'app.bsky.richtext.facet#link', uri: filmLink }],
  }];

  const rkey = dedupeRkey(title, now);
  console.log('RKEY', rkey);

  // --- Post to Bluesky ---
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

  if (dryRun) {
    console.log('--- DRY RUN: not posting to Bluesky, not saving state ---');
    return {
      dryRun: true,
      title,
      titleChanged,
      filmHref,
      filmLink,
      imageUrl,
      runtimeMinutes,
	  releaseYear,
      filmInfo,
      postText,
      facets,
	  schedule: {
        pollMode, newPollMode,
        margin: currentMargin, newMargin,
        nextCheckAt: new Date(nextCheckMs).toISOString(),
      },
      rkey,
    };
  }

  // Retry up to 3 times
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const outcome = await postToBluesky();
      console.log(outcome === 'deduped'
        ? `Already posted by another invocation: ${title}`
        : `Posted: ${title}`);
      lastError = null;
      break;
    } catch (e) {
      lastError = e;
      console.warn(`Attempt ${attempt} failed: ${e.message}`);
      if (attempt < 3) await new Promise((r) => setTimeout(r, 10_000));
    }
  }
  if (lastError) {
    try {
      await saveState({
        nextCheckAt: new Date(Date.now() + 60 * 1000).toISOString(),
        pollMode: 'waiting',
        fastPollCount: '0',
      });
    } catch (e) {
      console.warn('Could not reset schedule after failure:', e.message);
    }
    throw lastError;
  }

  await saveState({ lastTitle: title });
}

function jsonResponse(statusCode, obj) {
  return {
    statusCode,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(obj, null, 2),
  };
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
    const params = new URLSearchParams(event.rawQueryString || '');

    const filmPageUrl = params.get('filmpage');
    if (filmPageUrl) {
      try {
        const result = await scrapeFilmPage(filmPageUrl);
        return jsonResponse(200, result);
      } catch (e) {
        return jsonResponse(500, { error: e.message });
      }
    }

    if (params.get('dryRun') === 'true') {
      const result = await runBot({ dryRun: true, invocationId });
      if (!result) {
        return jsonResponse(500, { error: 'runBot returned nothing — check CloudWatch logs for details.' });
      }
      return jsonResponse(200, result);
    }

    return {
      statusCode: 200,
      headers: { 'content-type': 'text/plain' },
      body: 'Criterion Bluesky Bot. Add ?dryRun=true to preview without posting.',
    };
  }

  // EventBridge Scheduler invocation — the real thing.
  await runBot({ dryRun: false, invocationId });
  return { ok: true };
};
