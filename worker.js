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

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runBot(env));
  },
};

async function runBot(env) {
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

  // --- Save scheduling state (always) ---
  await Promise.all([
    KV.put('nextCheckAt', new Date(nextCheckMs).toISOString()),
    KV.put('pollMode', newPollMode),
    KV.put('fastPollCount', String(newFastPollCount)),
  ]);

  if (!titleChanged) {
    console.log('No new film. Done.');
    return;
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

  // --- Fetch og:image ---
  let imageUrl = null;
  if (moreHref) {
    try {
      const filmRes = await fetch(moreHref, { signal: AbortSignal.timeout(15_000) });
      const filmHtml = await filmRes.text();
      const $film = cheerio.load(filmHtml);
      imageUrl = $film('meta[property="og:image"]').attr('content') ?? null;
      console.log(`Image URL: ${imageUrl}`);
    } catch (e) {
      console.warn('Could not fetch film page:', e.message);
    }
  }

  const filmLink = moreHref ?? 'https://www.criterionchannel.com/events/criterion-24-7';
  const linkText = 'Watch on Criterion Channel';
  const postText = `🎬 Now streaming on Criterion Channel 24/7:\n\n${title}\n\nNext film starts in: ${nextText}\n\n${linkText}`;

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