# criterion-bluesky-bot

A bot that posts to Bluesky whenever a new film starts on the Criterion Channel's 24/7 linear stream.

Runs on **AWS Lambda**, triggered once a minute by an EventBridge Scheduler rule. It reads Criterion's published 24/7 schedule and posts to Bluesky about a minute before each film starts, falling back to scraping [whatsonnow.criterionchannel.com](https://whatsonnow.criterionchannel.com/) if the schedule is unavailable.

> **Note:** this project originally ran on Cloudflare Workers. It was migrated to AWS Lambda to fix an issue where Cloudflare's Cron Triggers execute on whichever data center has free capacity globally (not necessarily the US), which was intermittently hitting Criterion's region/bot-detection blocking. Lambda's region is pinned at deploy time, so every invocation — scheduled or manual — consistently runs from the same place.

## Architecture

| Piece | What it does |
|---|---|
| **Lambda function** (`index.mjs`) | Reads the 24/7 schedule (or, as a fallback, scrapes the "now playing" page), scrapes the film's own page for director/cast/year/poster, and posts to Bluesky. Single handler serves both trigger types (see below). |
| **DynamoDB table** (`CriterionBotState`) | One item holding the bot's state: `lastTitle`, `nextCheckAt`, the schedule-mode fields (`lastPostedKey`, `verifyKey`, `verifyAt`, `verifyAttempts`, `scheduleRetryAt`), and the fallback's polling fields (`pollMode`, `fastPollCount`, `margin`). |
| **EventBridge Scheduler** (`criterion-bot-schedule`) | Fires the Lambda every minute (`rate(1 minute)`). The function itself self-throttles via the `nextCheckAt` state — most ticks are a no-op "skip, not due yet" read. |
| **Lambda Function URL** | A public HTTPS endpoint for manual testing (`?dryRun=true`, `?filmpage=<url>`) — the AWS equivalent of what used to be the Worker's own `fetch()` handler. |

### Posting behavior

**Primary: Criterion's published schedule.** The 24/7 player page loads a JSON feed (`https://www.criterionchannel.com/api/media/1emmgvqX`) whose `schedule` array lists every showing for about the next week, with exact UTC start/end times and each film's id.

- The bot posts at the first once-a-minute tick within 90 seconds of a film's scheduled start, so posts go out 30–90 seconds before the film begins.
- It re-reads the schedule ~3 minutes before each post (and at least every 30 minutes), so last-minute programming changes are picked up.
- "Next film starts in: X minutes (time)" uses the next showing's exact scheduled start.
- Title and film-page link come straight from the schedule (`/films/{mediaid}` redirects to the full film URL). Director, cast, year and poster still come from the film page's JSON-LD.
- Criterion's schedule is cut into daily blocks at 11:00 UTC (6 AM ET), so a film spanning that boundary appears as two entries; these are joined back into one.
- About 3 minutes after each start, the bot checks the "now playing" page. If a different film is on than the schedule said, it posts that film using the fallback method. If the page still shows the previous film, it waits and checks again.
- Duplicate posts are prevented by a record key derived from the showing (film id + start time), and by remembering the last showing posted.

**Fallback: the original scrape-and-estimate logic.** If the schedule feed is unreachable, malformed, or doesn't cover the current time, the bot scrapes [whatsonnow.criterionchannel.com](https://whatsonnow.criterionchannel.com/) as before: it sleeps until a margin (3–20 minutes, adaptive) before the film's estimated end based on its runtime, then polls. Posts made this way say "Next film starts around:", since the time is an estimate. The schedule is retried every 10 minutes, and the bot switches back as soon as it's available, without reposting the film already announced.

Transient errors from Criterion's side (403, 429, 5xx) are retried once in-request and otherwise treated as "no change, try again on the normal schedule" rather than crashing or posting garbage.

### Testing URLs (Function URL)

Setting names and values are not case-sensitive.

- `?dryRun=true` — shows what the bot would do right now (mode, the post it would make, and when it plans to wake next) without posting or saving state.
- `?probe=schedule` — checks that the Lambda can read the schedule feed, and reports what's playing now and next.
- `?filmpage=<url>` — scrapes one film page and shows the extracted metadata.

### Title/metadata scraping notes

- The "now playing" title comes from the page's `<h1>`. A couple of known non-film placeholder values (e.g. `"Criterion 24/7"`, seen during what looks like a bumper/station-ID state) are explicitly filtered out so they're never mistaken for a real title.
- Director/cast/runtime/release year are read primarily from the film page's embedded `schema.org` JSON-LD (`VideoObject`/`Movie`), which gives exact values with no guessing. A positional fallback (walking up from the `<h1>`, text-pattern matching) only kicks in if that JSON-LD is ever missing.
- The poster image's aspect ratio is read directly from the downloaded WebP file's binary header (`getWebpDimensions`), not from CDN URL query params — Criterion's image URLs don't actually carry width/height params, so relying on them silently produced no aspect ratio hint at all and Bluesky would letterbox the image.
- A deterministic record key (`rkey`, derived from the title + a 30-minute time bucket) makes duplicate posts from concurrent/retried invocations impossible — the Bluesky PDS rejects a second write to the same key.

## Repo layout

```
index.mjs       — the Lambda handler and all bot logic
package.json    — dependencies (cheerio, @aws-sdk/client-dynamodb, @aws-sdk/lib-dynamodb)
```

Deployment scratch files (IAM policy JSON, the zipped deployment package, `environment.json` containing secrets) are intentionally **not** committed — see `.gitignore`.

## Environment variables (set on the Lambda function, not in this repo)

| Variable | Purpose |
|---|---|
| `BSKY_HANDLE` | Bluesky account handle to post as |
| `BSKY_APP_PASSWORD` | Bluesky [app password](https://bsky.app/settings/app-passwords) (not your main account password) |
| `TABLE_NAME` | DynamoDB table name (defaults to `CriterionBotState` if unset) |

## Deploying a code change

There's no CI/CD here — deployment is a manual zip-and-upload:

```powershell
# from a folder containing index.mjs, package.json, and node_modules (npm install first if needed)
Compress-Archive -Path index.mjs, package.json, node_modules -DestinationPath lambda-deploy.zip -Force

aws lambda update-function-code `
  --function-name criterion-bluesky-bot `
  --zip-file fileb://lambda-deploy.zip `
  --region us-east-1

aws lambda wait function-updated --function-name criterion-bluesky-bot --region us-east-1
```

## Testing without posting

The Function URL supports two read-only debug routes, mirroring what used to be Cloudflare's `fetch()` handler:

- `?dryRun=true` — runs the full pipeline (scrape, scheduling decision, post-text construction) and returns what it *would* do as JSON, without touching DynamoDB or Bluesky.
- `?filmpage=<url>` — scrapes a single film page directly (runtime/director/cast/image), useful for checking the scraper against a specific film without waiting for a live transition.

```powershell
Invoke-RestMethod -Uri "https://<your-function-url>/?dryRun=true"
```

## Logs

```powershell
aws logs tail /aws/lambda/criterion-bluesky-bot --region us-east-1 --since 1h
```

Every log line is tagged with an invocation ID (e.g. `[1f3f6690]`) so concurrent or overlapping runs can be told apart.

## Cost

Everything here — Lambda invocations, DynamoDB reads/writes, EventBridge Scheduler — runs comfortably within AWS's Always Free monthly limits at this bot's volume (roughly one real check every few minutes, most other ticks a cheap no-op read). A zero-spend budget alert has been setup so a notification will be provided automatically if that ever changed.
