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
 */

import * as cheerio from 'cheerio';

// The generic link Criterion falls back to when there's no dedicated film page.
const GENERIC_LINK = 'https://www.criterionchannel.com/events/criterion-24-7';

function isGenericLink(href) {
  if (!href) return true;
  return href.replace(/\/$/, '') === GENERIC_LINK;
}

// Criterion film page URLs are just the title, lowercased and hyphenated,
// e.g. "The Tit and the Moon" -> criterionchannel.com/the-tit-and-the-moon
function slugify(str) {
  return str
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '') // strip accents/diacritics
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '')    // strip punctuation
    .trim()
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-');
}

// Try to guess a film's Criterion Channel page from its title, and scrape
// image + director/cast info from it, in the same shape the moreHref path
// already produces (imageUrl, filmInfo as "Directed by ...\nStarring ...").
// Returns null if we can't confidently find the right page - callers should
// treat that as "no extra info available" and continue gracefully.
async function guessAndScrapeFilmPage(title) {
  const slug = slugify(title);
  if (!slug) return null;

  const url = `https://www.criterionchannel.com/${slug}`;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(15_000) });
    if (!res.ok) return null;

    const html = await res.text();
    const $page = cheerio.load(html);

    // Sanity check: make sure the guessed page is actually about this film,
    // not a 404 that returns 200, a redirect to the homepage, etc.
    const ogTitle = $page('meta[property="og:title"]').attr('content') ?? '';
    const cleanOgTitle = ogTitle.replace(/\s*-\s*The Criterion Channel\s*$/i, '').trim().toLowerCase();
    if (cleanOgTitle !== title.trim().toLowerCase()) {
      console.log(`Guessed URL ${url} didn't match ("${ogTitle}"), skipping.`);
      return null;
    }

    const imageUrl = $page('meta[property="og:image"]').attr('content') ?? null;
    const desc = $page('meta[property="og:description"]').attr('content') ?? '';
    const descLines = desc.split('\n').map(l => l.trim()).filter(Boolean);
    const dirLine = descLines.find(l => /^Directed by /i.test(l)) ?? '';
    const starLine = descLines.find(l => /^Starring /i.test(l)) ?? '';
    const filmInfo = [dirLine, starLine].filter(Boolean).join('\n');

    return { filmLink: url, imageUrl, filmInfo };
  } catch (e) {
    console.warn(`Guess-and-scrape failed for "${title}":`, e.message);
    return null;
  }
}

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runBot(env));
  },

  // Manual HTTP trigger, for testing only. Visiting the worker's URL with
  // ?dryRun=true runs the full scrape/guess pipeline and returns what it
  // *would* post, without touching KV state or Bluesky. Safe to hit anytime.
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.searchParams.get('dryRun') === 'true') {
      const result = await runBot(env, { dryRun: true });
      return new Response(JSON.stringify(result, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      });
    }
    return new Response('Criterion Bluesky Bot. Add ?dryRun=true to preview without posting.');
  },
};

async function runBot(env, { dryRun = false } = {}) {
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

  // --- Respect the scheduled wait ---
  if (nextCheckAt && now < nextCheckAt) {
    console.log(`Skipping — next check scheduled for ${new Date(nextCheckAt).toISOString()}`);
    return;
  }

  // --- Scrape What's On Now ---
  const nowRes = await fetch('https://whatsonnow.criterionchannel.com/');
  const nowHtml = await nowRes.text();
  const $ = cheerio.load(nowHtml);

  const moreHref = $('a')
    .filter((_, el) => $(el).text().trim() === 'More')
    .first()
    .attr('href');

  const nowText = $('a')
    .filter((_, el) => /what'?s on now/i.test($(el).text()))
    .first()
    .text()
    .trim();
  let title = nowText.replace(/^What'?s on now:\s*/i, '').trim();

  if (!title && moreHref) {
    const slug = moreHref.replace(/.*criterionchannel\.com\//, '');
    title = slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  }

  const nextRaw = $('body').text().match(/Next film starts in:\s*(\d+)\s*minute/i)?.[1];
  const minutesUntilNext = nextRaw ? parseInt(nextRaw) : null;

  console.log(`Now playing: ${title}`);
  console.log(`Minutes until next: ${minutesUntilNext ?? 'unknown'}`);

  // --- Determine next check time and poll mode ---
  const titleChanged = title && title !== lastTitle;

  let newPollMode = pollMode ?? 'waiting';
  let newFastPollCount = fastPollCount;
  let nextCheckMs;

  if (titleChanged) {
    // New film detected — reset to "waiting" mode using site's own countdown
    newPollMode = 'waiting';
    newFastPollCount = 0;
    if (minutesUntilNext !== null && minutesUntilNext > 1) {
      // Wake up 1 minute before the next film is due
      nextCheckMs = now + (minutesUntilNext - 1) * 60 * 1000;
      console.log(`New film posted. Sleeping for ${minutesUntilNext - 1} minutes.`);
    } else {
      // Site doesn't know, check again in 5 minutes
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
  let nextText = 'unknown';
  if (minutesUntilNext !== null) {
    const nextTime = new Date(now + minutesUntilNext * 60 * 1000);
    const etTime = nextTime.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' });
    const ptTime = nextTime.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/Los_Angeles' });
    const etZone = nextTime.toLocaleDateString('en-US', { timeZone: 'America/New_York', timeZoneName: 'short' }).split(', ')[1] ?? 'ET';
    const ptZone = nextTime.toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles', timeZoneName: 'short' }).split(', ')[1] ?? 'PT';
    nextText = `${minutesUntilNext} minutes (${etTime} ${etZone}/${ptTime} ${ptZone})`;
  }

  // --- Fetch og:image / director / cast ---
  let imageUrl = null;
  let filmInfo = '';
  let filmLink = moreHref ?? GENERIC_LINK;
  let usedGuessedPage = false;

  if (moreHref && !isGenericLink(moreHref)) {
    // Normal case: Criterion gave us a real, dedicated film link.
    try {
      const filmRes = await fetch(moreHref, { signal: AbortSignal.timeout(15_000) });
      const filmHtml = await filmRes.text();
      const $film = cheerio.load(filmHtml);
      imageUrl = $film('meta[property="og:image"]').attr('content') ?? null;
      console.log(`Image URL: ${imageUrl}`);

      // Extract director/year/country and cast from og:description
      const desc = $film('meta[property="og:description"]').attr('content') ?? '';
      const descLines = desc.split('\n').map(l => l.trim()).filter(Boolean);
      const dirLine = descLines.find(l => /^Directed by /i.test(l)) ?? '';
      const starLine = descLines.find(l => /^Starring /i.test(l)) ?? '';
      filmInfo = [dirLine, starLine].filter(Boolean).join('\n');
      console.log(`Film info: ${filmInfo}`);
    } catch (e) {
      console.warn('Could not fetch film page:', e.message);
    }
  } else {
    // No dedicated film link on the page - try guessing the URL from the title.
    console.log('No dedicated film link found; attempting to guess the film page URL...');
    const scraped = await guessAndScrapeFilmPage(title);
    if (scraped) {
      console.log(`Guessed film page: ${scraped.filmLink}`);
      console.log(`Image URL: ${scraped.imageUrl}`);
      console.log(`Film info: ${scraped.filmInfo}`);
      imageUrl = scraped.imageUrl;
      filmInfo = scraped.filmInfo;
      filmLink = scraped.filmLink;
      usedGuessedPage = true;
    } else {
      console.log('Could not confirm a guessed film page; posting without extra metadata.');
    }
  }

  const linkText = 'Watch on Criterion Channel';

  // Bluesky's limit is 300 graphemes
  const BSKY_LIMIT = 300;

  function truncateFilmInfo(info, budget) {
    if (!info) return '';
    const lines = info.split('\n');
    // Try both lines
    if ([...info].length <= budget) return info;
    // Try just the directed-by line
    if (lines.length > 1 && [...lines[0]].length <= budget) return lines[0];
    // Last resort: truncate with ellipsis
    return [...lines[0]].slice(0, budget - 1).join('') + '…';
  }

  // Calculate budget: measure the base post (without filmInfo) and see what's left
  const basePost = `🎬 Now streaming on Criterion Channel 24/7:\n\n${title}\n\nNext film starts in: ${nextText}\n\n${linkText}`;
  const baseCost = [...basePost].length;
  const filmInfoBudget = Math.max(0, BSKY_LIMIT - baseCost - 1); // -1 for the extra \n separator

  const filmInfoTrimmed = truncateFilmInfo(filmInfo, filmInfoBudget);
  const postText = filmInfoTrimmed
    ? `🎬 Now streaming on Criterion Channel 24/7:\n\n${title}\n${filmInfoTrimmed}\n\nNext film starts in: ${nextText}\n\n${linkText}`
    : basePost;
	
  const encoder = new TextEncoder();
  const beforeLink = postText.slice(0, postText.lastIndexOf(linkText));
  const byteStart = encoder.encode(beforeLink).length;
  const byteEnd = byteStart + encoder.encode(linkText).length;

  const facets = [{
    index: { byteStart, byteEnd },
    features: [{ $type: 'app.bsky.richtext.facet#link', uri: filmLink }],
  }];

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
      try {
        const imgRes = await fetch(imageUrl, { signal: AbortSignal.timeout(30_000) });
        const imgBuffer = await imgRes.arrayBuffer();
        const contentType = imgRes.headers.get('content-type') ?? 'image/jpeg';

        const uploadRes = await fetch('https://bsky.social/xrpc/com.atproto.repo.uploadBlob', {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${accessJwt}`,
            'Content-Type': contentType,
          },
          body: imgBuffer,
          signal: AbortSignal.timeout(30_000),
        });

        if (uploadRes.ok) {
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
          console.warn('Image upload failed:', await uploadRes.text());
        }
      } catch (e) {
        console.warn('Image upload failed, posting without image:', e.message);
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

    if (!postRes.ok) throw new Error(`Post failed: ${postRes.status} ${await postRes.text()}`);
  }

  if (dryRun) {
    console.log('--- DRY RUN: not posting to Bluesky, not saving state ---');
    return {
      dryRun: true,
      title,
      titleChanged,
      moreHref,
      usedGuessedPage,
      filmLink,
      imageUrl,
      filmInfo,
      postText,
      facets,
    };
  }

  // Retry up to 3 times
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      await postToBluesky();
      console.log(`Posted: ${title}`);
      lastError = null;
      break;
    } catch (e) {
      lastError = e;
      console.warn(`Attempt ${attempt} failed: ${e.message}`);
      if (attempt < 3) await new Promise(r => setTimeout(r, 10_000));
    }
  }
  if (lastError) throw lastError;

  // --- Persist new lastTitle ---
  await KV.put('lastTitle', title);
}