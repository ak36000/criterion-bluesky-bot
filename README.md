# criterion-bluesky-bot

A bot that posts to Bluesky whenever a new film starts on the Criterion Channel's 24/7 linear stream.

Runs on **AWS Lambda**, checking [whatsonnow.criterionchannel.com](https://whatsonnow.criterionchannel.com/) once a minute via an EventBridge Scheduler rule, and posting to Bluesky when the title changes.

> **Note:** this project originally ran on Cloudflare Workers. It was migrated to AWS Lambda to fix an issue where Cloudflare's Cron Triggers execute on whichever data center has free capacity globally (not necessarily the US), which was intermittently hitting Criterion's region/bot-detection blocking. Lambda's region is pinned at deploy time, so every invocation — scheduled or manual — consistently runs from the same place.

## Architecture

| Piece | What it does |
|---|---|
| **Lambda function** (`index.mjs`) | Scrapes the "now playing" page, detects title changes, scrapes the film's own page for runtime/director/cast/poster, and posts to Bluesky. Single handler serves both trigger types (see below). |
| **DynamoDB table** (`CriterionBotState`) | One item holding four attributes: `lastTitle`, `nextCheckAt`, `pollMode`, `fastPollCount`. Replaces what was a Cloudflare KV namespace. |
| **EventBridge Scheduler** (`criterion-bot-schedule`) | Fires the Lambda every minute (`rate(1 minute)`). The function itself self-throttles via the `nextCheckAt` state — most ticks are a no-op "skip, not due yet" read. |
| **Lambda Function URL** | A public HTTPS endpoint for manual testing (`?dryRun=true`, `?filmpage=<url>`) — the AWS equivalent of what used to be the Worker's own `fetch()` handler. |

### Polling behavior

- After a title change, the bot sleeps until a margin (3-20 minutes, adaptive) before the estimated end, based on the film's runtime (scraped from the film page - the site no longer exposes a next start time).
- The margin widens when a change is found on the first poll after waking, and shrinks once an early wake catches it in fast mode.
- Transient errors from Criterion's side (403, 429, 5xx) are retried once in-request and otherwise treated as "no change, try again on the normal schedule" rather than crashing or posting garbage.

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
