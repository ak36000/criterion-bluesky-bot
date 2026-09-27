/**
 * Criterion Channel Bluesky Bot — Cloudflare Worker
 *
 * Cron: every 1 minute (* * * * *)
 * KV namespace: CRITERION_STATE  (bind in wrangler.toml)
 *
 * State keys:
 *   lastTitle      — title of the last film posted
 *   nextCheckAt    — ISO timestamp: don't do anything before this time
 *   pollMode       — "waiting" | "fast" | "slow"
 *   fastPollCount  — how many fast (1-min) polls have fired since film changed
 *
 * NOTE (2026-09): Criterion redesigned whatsonnow.criterionchannel.com and the
 * film pages. The old scraper looked for an <a> with text "What's on now: ..."
 * and an <a> with text "More", neither of which exist anymore, and film URLs
 * changed from flat slugs (/the-film-title) to /films/{opaqueId}/{slug} —
 * so slug-guessing is no longer possible. This version scrapes the new
 * markup instead. The site also no longer exposes a "Next film starts in: X
 * minutes" countdown in the static HTML (it looks like that's rendered
 * client-side now), so we estimate it from the film's own runtime instead —
 * see parseRuntimeMinutes() and the "starts around" post copy.
 *
 * Director is no longer labeled "Directed by ..." anywhere on the page, so
 * scrapeFilmPage() finds it positionally (the unlabeled text right above the
 * film's <h1>) rather than by a text marker — see looksLikeName().
 */

import * as cheerio from 'cheerio';

// The Criterion 24/7 channel's own page — used as the link when we can't
// find a dedicated film page for whatever is currently playing.
const GENERIC_LINK = 'https://www.criterionchannel.com/live/1emmgvqX/criterion-24-7';

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
// The title hash goes in the 10-bit clock id, so a different film in the same
// bucket gets a different key (a 1-in-1024 collision chance).
function dedupeRkey(title, nowMs) {
  const BUCKET_MS = 30 * 60 * 1000;
  const bucketMicros = Math.floor(nowMs / BUCKET_MS) * BUCKET_MS * 1000;
  return makeTid(bucketMicros, hash10(title));
}

// Parse a runtime out of text like "1 hr 50 min" or "50 min". Criterion's
// film pages render the release year glued directly to the front of this
// string with no separator (e.g. a 2000 release that runs 1hr50 shows up in
// scraped text as "20001 hr 50 min"), so we strip a plausible leading year
// before parsing the numbers. We only strip when the digit run is long
// enough that it couldn't just be a real runtime (>=5 digits), so a lone
// "20 min" short isn't mistaken for a bare year.
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
// check, used when we're pulling text based on its position on the page
// rather than a label. Rejects anything too long, empty, or obviously not
// name-shaped (full sentences, nav links, etc).
function looksLikeName(text) {
  if (!text) return false;
  const t = text.trim();
  if (!t || t.length > 60) return false;
  if (/^(home|new|all films|subscribe|log in|search|criterion|watch|details|account)\b/i.test(t)) return false;
  // Expect 1-5 capitalized words, e.g. "Karyn Kusama" or "Jean-Luc Godard".
  return /^[A-ZÀ-Ý][\p{L}.'-]*(?:\s+[A-ZÀ-Ý][\p{L}.'-]*){0,4}$/u.test(t);
}

// Scrape a dedicated film page (https://www.criterionchannel.com/films/...)
// for the director, poster image, runtime, and cast list.
//
// None of this is based on confirmed class names (only cleaned/rendered
// text), so it's positional: director and runtime are found by walking up
// from the <h1> a couple of parent levels and checking each level's
// previous sibling (director) or own text (runtime) against a recognizer.
// That's a heuristic, not a confirmed selector — verify it with
// `?filmpage=<url>` (see fetch handler below) after deploying, and tighten
// it if either field doesn't show up correctly in the logs.
async function scrapeFilmPage(url) {
  const res = await fetch(url, { signal: AbortSignal.timeout(15_000) });
  if (!res.ok) throw new Error(`Film page fetch failed: HTTP ${res.status}`);
  const html = await res.text();
  const $film = cheerio.load(html);

  const imageUrl = $film('meta[property="og:image"]').attr('content') ?? null;

  let runtimeMinutes = null;
  let director = '';
  let $scope = $film('h1').first();
  for (let i = 0; i < 3 && $scope.length; i++) {
    if (runtimeMinutes === null) runtimeMinutes = parseRuntimeMinutes($scope.text());
    if (!director) {
      const prevText = $scope.prev().text().trim();
      if (looksLikeName(prevText)) director = prevText;
    }
    if (runtimeMinutes !== null && director) break;
    $scope = $scope.parent();
  }

  // "Starring ..." still appears as its own text run, ending before the
  // "Supplements" section (or one of a few other section labels, as a
  // fallback boundary, or the end of the text as a last resort).
  const bodyText = $film('body').text();
  const starringMatch = bodyText.match(
    /Starring\s+([^]*?)(?:\s*(?:Supplements|Categories|My List|Trailer|Commentary)\b|$)/i,
  );
  const starring = starringMatch ? `Starring ${starringMatch[1].trim()}` : '';

  const filmInfo = [director ? `Directed by ${director}` : '', starring].filter(Boolean).join('\n');

  return { imageUrl, runtimeMinutes, filmInfo };
}

// Fetch or upload with one retry on timeout/network error. Only retries on
// timeout/abort or a thrown network error — not on a clean non-2xx response,
// since that's a server telling us something's wrong, not just slow.
async function withRetry(label, attemptFn) {
  const start = Date.now();
  try {
    return await attemptFn(30_000);
  } catch (e) {
    console.warn(`${label} failed on attempt 1 (${Date.now() - start}ms): ${e.message}. Retrying once...`);
    await new Promise(r => setTimeout(r, 1_000));
    const retryStart = Date.now();
    try {
      const result = await attemptFn(15_000);
      console.log(`${label} succeeded on retry (${Date.now() - retryStart}ms).`);
      return result;
    } catch (e2) {
      console.warn(`${label} failed on retry too (${Date.now() - retryStart}ms): ${e2.message}. Giving up on this step.`);
      throw e2;
    }
  }
}


export default {
  async scheduled(event, env, ctx) {
    const invocationId = crypto.randomUUID().slice(0, 8);
    console.log(`[${invocationId}] INVOKE`, JSON.stringify({
      cron: event.cron,
      scheduledTime: new Date(event.scheduledTime).toISOString(),
      startedAt: new Date().toISOString(),
    }));
    ctx.waitUntil(runBot(env, { invocationId, scheduledTime: event.scheduledTime }));
  },

  // Manual HTTP triggers, for testing only.
  //   ?dryRun=true        — run the full pipeline and return what it *would*
  //                         post, without touching KV state or Bluesky.
  //   ?filmpage=<url>     — scrape a single film page directly, to verify
  //                         runtime/cast extraction against real Criterion
  //                         markup without waiting for a live transition.
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const filmPageUrl = url.searchParams.get('filmpage');
    if (filmPageUrl) {
      try {
        const result = await scrapeFilmPage(filmPageUrl);
        return new Response(JSON.stringify(result, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }, null, 2), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }
    if (url.searchParams.get('dryRun') === 'true') {
      const result = await runBot(env, { dryRun: true });
      if (!result) {
        return new Response(
          JSON.stringify({ error: 'runBot returned nothing — check wrangler tail logs for details.' }, null, 2),
          { status: 500, headers: { 'Content-Type': 'application/json' } },
        );
      }
      return new Response(JSON.stringify(result, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      });
    }
    return new Response('Criterion Bluesky Bot. Add ?dryRun=true to preview without posting.');
  },
};

async function runBot(env, { dryRun = false, invocationId = 'manual', scheduledTime = null } = {}) {
  // Shadow console so every log line inside runBot (including the nested
  // postToBluesky) is tagged with this invocation's id. No other console.log
  // edits are needed.
  const console = {
    log: (...a) => globalThis.console.log(`[${invocationId}]`, ...a),
    warn: (...a) => globalThis.console.warn(`[${invocationId}]`, ...a),
  };
  const KV = env.CRITERION_STATE;

  // --- Load state ---
  const [lastTitle, nextCheckAtStr, pollMode, fastPollCountStr] = await Promise.all([
    KV.get('lastTitle'),
    KV.get('nextCheckAt'),
    KV.get('pollMode'),
    KV.get('fastPollCount'),
  ]);

  const now = Date.now();
  const nextCheckAt = nextCheckAtStr ? new Date(nextCheckAtStr).getTime() : 0;
  const fastPollCount = fastPollCountStr ? parseInt(fastPollCountStr) : 0;

  console.log('STATE_READ', JSON.stringify({
    lastTitle, nextCheckAtStr, pollMode, fastPollCountStr,
    now: new Date(now).toISOString(),
    scheduledTime: scheduledTime ? new Date(scheduledTime).toISOString() : null,
  }));

  // --- Respect the scheduled wait (skip this gate during a dry run) ---
  if (nextCheckAt && now < nextCheckAt && !dryRun) {
    console.log(`Skipping — next check scheduled for ${new Date(nextCheckAt).toISOString()}`);
    return;
  }
  if (nextCheckAt && now < nextCheckAt && dryRun) {
    console.log(`(Dry run ignoring schedule gate — normally would skip until ${new Date(nextCheckAt).toISOString()})`);
  }

  // --- Scrape What's On Now ---
  // New markup: the title is a plain <h1> under a "Now Playing On Criterion
  // 24/7" heading, and the dedicated film page is whichever <a> points into
  // /films/... (its visible label, e.g. "Film Page", isn't load-bearing —
  // matching on the href is more robust if Criterion tweaks the label).
  const nowRes = await fetch('https://whatsonnow.criterionchannel.com/');
  const nowHtml = await nowRes.text();
  const $ = cheerio.load(nowHtml);

  const title = $('h1').first().text().trim();
  const filmHref = $('a[href*="/films/"]').first().attr('href') || null;

  console.log(`Now playing: ${title}`);

  // --- Determine whether the film changed ---
  const titleChanged = title && title !== lastTitle;

  // --- If the film changed, scrape its page now so we have runtime info
  // available both for the schedule below and for the post text later. ---
  let imageUrl = null;
  let filmInfo = '';
  let filmLink = GENERIC_LINK;
  let runtimeMinutes = null;

  if (titleChanged) {
    if (filmHref && !isGenericLink(filmHref)) {
      filmLink = filmHref;
      try {
        const scraped = await scrapeFilmPage(filmHref);
        imageUrl = scraped.imageUrl;
        filmInfo = scraped.filmInfo;
        runtimeMinutes = scraped.runtimeMinutes;
        console.log(`Film page: ${filmHref}`);
        console.log(`Image URL: ${imageUrl}`);
        console.log(`Runtime: ${runtimeMinutes ?? 'unknown'} min`);
        console.log(`Film info: ${filmInfo}`);
      } catch (e) {
        console.warn('Could not fetch/parse film page:', e.message);
      }
    } else {
      console.log('No dedicated film link found on the live page; posting without extra metadata.');
    }
  }

  // --- Determine next check time and poll mode ---
  let newPollMode = pollMode ?? 'waiting';
  let newFastPollCount = fastPollCount;
  let nextCheckMs;

  if (titleChanged) {
    // New film detected — reset to "waiting" mode. We no longer get a
    // live countdown from the site, so we estimate the sleep from the
    // film's own runtime (assuming "now" is close to when it started,
    // which fast/slow polling below is meant to guarantee).
    newPollMode = 'waiting';
    newFastPollCount = 0;
    if (runtimeMinutes !== null && runtimeMinutes > 1) {
      nextCheckMs = now + (runtimeMinutes - 1) * 60 * 1000;
      console.log(`New film posted. Sleeping ~${runtimeMinutes - 1} minutes (estimated from runtime).`);
    } else {
      // Couldn't determine a runtime — check again in 5 minutes.
      nextCheckMs = now + 5 * 60 * 1000;
    }
  } else {
    // No new film yet
    if (newPollMode === 'waiting') {
      // First time waking up near a transition — switch to fast polling
      newPollMode = 'fast';
      newFastPollCount = 1;
      nextCheckMs = now + 60 * 1000; // check again in 1 minute
      console.log('Entering fast poll mode (1 min intervals).');
    } else if (newPollMode === 'fast') {
      newFastPollCount += 1;
      if (newFastPollCount >= 5) {
        // After 5 fast checks with no change, slow down
        newPollMode = 'slow';
        nextCheckMs = now + 5 * 60 * 1000;
        console.log(`Fast poll limit reached (${newFastPollCount}). Switching to slow (5 min) mode.`);
      } else {
        nextCheckMs = now + 60 * 1000;
        console.log(`Fast poll ${newFastPollCount}/5. Next check in 1 minute.`);
      }
    } else {
      // slow mode — keep checking every 5 minutes
      nextCheckMs = now + 5 * 60 * 1000;
      console.log('Slow poll mode. Next check in 5 minutes.');
    }
  }

  // --- Save scheduling state (always, except during a dry run) ---
  if (!dryRun) {
    await Promise.all([
      KV.put('nextCheckAt', new Date(nextCheckMs).toISOString()),
      KV.put('pollMode', newPollMode),
      KV.put('fastPollCount', String(newFastPollCount)),
    ]);
  }

  if (!titleChanged && !dryRun) {
    console.log('No new film. Done.');
    return;
  }
  if (!titleChanged && dryRun) {
    console.log('No new film (dry run continues anyway, to preview the current film).');
  }

  // --- Build the post text ---
  // We're estimating this from runtime rather than reading it off the site,
  // so the copy says "starts around" instead of "starts in".
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

  // Bluesky's limit is 300 graphemes
  const BSKY_LIMIT = 300;

  function truncateFilmInfo(info, budget) {
    if (!info) return '';
    const lines = info.split('\n');
    // Try both lines
    if ([...info].length <= budget) return info;
    // Try just the first line
    if (lines.length > 1 && [...lines[0]].length <= budget) return lines[0];
    // Last resort: truncate with ellipsis
    return [...lines[0]].slice(0, budget - 1).join('') + '…';
  }

  // Calculate budget: measure the base post (without filmInfo) and see what's left
  const basePost = `🎬 Now streaming on Criterion Channel 24/7:\n\n${title}\n\nNext film starts around: ${nextText}\n\n${linkText}`;
  const baseCost = [...basePost].length;
  const filmInfoBudget = Math.max(0, BSKY_LIMIT - baseCost - 1); // -1 for the extra \n separator

  const filmInfoTrimmed = truncateFilmInfo(filmInfo, filmInfoBudget);
  const postText = filmInfoTrimmed
    ? `🎬 Now streaming on Criterion Channel 24/7:\n\n${title}\n${filmInfoTrimmed}\n\nNext film starts around: ${nextText}\n\n${linkText}`
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
        identifier: env.BSKY_HANDLE,
        password: env.BSKY_APP_PASSWORD,
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

            // Parse width/height from URL params (e.g. w=1280&h=720) for correct aspect ratio
            const imgUrlParams = new URL(imageUrl).searchParams;
            const imgWidth = parseInt(imgUrlParams.get('w') ?? '0');
            const imgHeight = parseInt(imgUrlParams.get('h') ?? '0');
            const aspectRatio = (imgWidth && imgHeight)
              ? { width: imgWidth, height: imgHeight }
              : undefined;

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
      // Failed. Did another invocation (or an earlier attempt of ours) already create it?
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
      filmInfo,
      postText,
      facets,
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
      if (attempt < 3) await new Promise(r => setTimeout(r, 10_000));
    }
  }
  if (lastError) {
    // Every attempt failed. The schedule we saved earlier says "sleep until the
    // next film", so undo that and retry on the next tick. lastTitle was never
    // updated, so the next run will see this film as new and try again.
    try {
      await Promise.all([
        KV.put('nextCheckAt', new Date(Date.now() + 60 * 1000).toISOString()),
        KV.put('pollMode', 'waiting'),
        KV.put('fastPollCount', '0'),
      ]);
    } catch (e) {
      console.warn('Could not reset schedule after failure:', e.message);
    }
    throw lastError;
  }

  // --- Persist new lastTitle ---
  await KV.put('lastTitle', title);
}
