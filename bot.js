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
const nowText = $('a[href*="criterion-24-7"]').first().text().trim();
const title = nowText.replace(/^What's on now:\s*/i, '').trim();

const moreHref = $('a').filter((_, el) => $(el).text().trim() === 'More').first().attr('href');
const nextText = $('body').text().match(/Next film starts in:\s*(.+)/i)?.[1]?.trim() ?? 'unknown';

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
const postText = `🎬 Now streaming on Criterion Channel 24/7:\n\n${title}\n\nNext film starts in: ${nextText}\n\n${filmLink}`;

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
      images: [{
        image: uploadRes.data.blob,
        alt: `Film poster for ${title}`,
      }],
    };
  } catch (e) {
    console.warn('Image upload failed, posting without image:', e.message);
  }
}

await agent.post({
  text: postText,
  embed,
  createdAt: new Date().toISOString(),
});

console.log(`Posted: ${title}`);

// --- Save new state ---
state.lastTitle = title;
fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
