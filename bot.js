import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { AtpAgent, BlobRef } from '@atproto/api';
import fs from 'fs';

const STATE_FILE = 'state.json';

// --- Load previous state ---
let state = { lastTitle: null };
if (fs.existsSync(STATE_FILE)) {
  state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
}

// --- Scrape What's On Now ---
const nowRes = await fetch('https://whatsonnow.criterionchannel.com/');
const nowHtml = await nowRes.text();
const $ = cheerio.load(nowHtml);

// The page lists "What's on now: TITLE" and a More link
const nowText = $('a').filter((_, el) => /what'?s on now/i.test($(el).text())).first().text().trim();
let title = nowText.replace(/^What'?s on now:\s*/i, '').trim();

// Fallback: derive title from the More link URL slug
if (!title && moreHref) {
  const slug = moreHref.replace(/.*criterionchannel\.com\//, '');
  title = slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

const moreHref = $('a').filter((_, el) => $(el).text().trim() === 'More').first().attr('href');
const nextRaw = $('body').text().match(/Next film starts in:\s*(\d+)\s*minute/i)?.[1];
const minutesUntilNext = nextRaw ? parseInt(nextRaw) : null;

let nextText = 'unknown';
if (minutesUntilNext !== null) {
  const nextTime = new Date(Date.now() + minutesUntilNext * 60 * 1000);
  const etTime = nextTime.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' });
  const ptTime = nextTime.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/Los_Angeles' });
  const etZone = nextTime.toLocaleDateString('en-US', { timeZone: 'America/New_York', timeZoneName: 'short' }).split(', ')[1] ?? 'ET';
  const ptZone = nextTime.toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles', timeZoneName: 'short' }).split(', ')[1] ?? 'PT';
  nextText = `${minutesUntilNext} minutes (${etTime} ${etZone}/${ptTime} ${ptZone})`;
}

console.log(`Now playing: ${title}`);
console.log(`More link: ${moreHref}`);
console.log(`Next film: ${nextText}`);

if (!title || title === state.lastTitle) {
  console.log('No change, skipping.');
  process.exit(0);
}

// --- Fetch film page for og:image ---
let imageUrl = null;
if (moreHref) {
  const filmRes = await fetch(moreHref);
  const filmHtml = await filmRes.text();
  const $film = cheerio.load(filmHtml);
  imageUrl = $film('meta[property="og:image"]').attr('content') ?? null;
  console.log(`Image URL: ${imageUrl}`);
}

// --- Post to Bluesky ---
const agent = new AtpAgent({ service: 'https://bsky.social' });
await agent.login({
  identifier: process.env.BSKY_HANDLE,
  password: process.env.BSKY_APP_PASSWORD,
});

// Build post text
const filmLink = moreHref ?? 'https://www.criterionchannel.com/events/criterion-24-7';
const linkText = 'Watch on Criterion Channel';
const postText = `🎬 Now streaming on Criterion Channel 24/7:\n\n${title}\n\nNext film starts in: ${nextText}\n\n${linkText}`;

// Calculate byte positions of the link text (Bluesky facets use UTF-8 byte offsets)
const encoder = new TextEncoder();
const beforeLink = postText.slice(0, postText.lastIndexOf(linkText));
const byteStart = encoder.encode(beforeLink).length;
const byteEnd = byteStart + encoder.encode(linkText).length;

const facets = [{
  index: { byteStart, byteEnd },
  features: [{ $type: 'app.bsky.richtext.facet#link', uri: filmLink }],
}];

// Upload image if available
let embed = undefined;
if (imageUrl) {
  try {
    const imgRes = await fetch(imageUrl);
    const imgBuffer = await imgRes.arrayBuffer();
    const contentType = imgRes.headers.get('content-type') ?? 'image/jpeg';
    const uploadRes = await agent.uploadBlob(Buffer.from(imgBuffer), { encoding: contentType });
    embed = {
      $type: 'app.bsky.embed.images',
      images: [{ image: uploadRes.data.blob, alt: `Film poster for ${title}` }],
    };
  } catch (e) {
    console.warn('Image upload failed, posting without image:', e.message);
  }
}

await agent.post({ text: postText, facets, embed, createdAt: new Date().toISOString() });
console.log(`Posted: ${title}`);

// --- Save new state ---
state.lastTitle = title;
fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
