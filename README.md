# ozel-gpt ingestion service

Vercel-ready Python backend that collects multi-exchange market data, aggregates crypto news without official APIs, cross-confirms events, and stores every artifact as JSON in Blob/S3 for later GPT Action use.

## Features
- `POST /api/market_fetch`: Bybit/Binance/CMC OHLC + tickers with retries and normalization.
- `POST /api/news_fetch`: Robots-aware RSS/HTML crawler for top crypto publications, rate limited with dedupe and content extraction.
- `POST /api/analyze_news`: Rapidfuzz clustering, lightweight NER heuristics, scoring with source weighting, GPT Action compatible output.
- `GET /api/store_list`: Debug listing of stored JSON blobs.
- `POST /api/admin_cleanup`: Weekly cron purging JSON past TTL (news, markets, clusters, logs, GPT action payloads).
- `GET /api/health`: Embedded health probe.

All POST endpoints require `X-Signature` header (`HMAC-SHA256(body, HMAC_SHARED_SECRET)`).

## Quick Start
1. **Install deps**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. **Configure**
   - Copy `.env.example` to `.env`, fill secrets (API keys, Blob endpoint, GPT shared secret).
   - Adjust `ALLOWED_ORIGINS` for allowed GPT Action callers.

3. **Local invocation**
   ```bash
   vercel dev
   ```
   or run handlers via tests/mocks.

4. **Run tests**
   ```bash
   pytest
   ```

## Environment Variables
See `.env.example` for the full list. Key settings:
- Market API keys (`BYBIT_*`, `BINANCE_*`, `CMC_API_KEY`).
- Storage config (`BLOB_BASE_URL`, credentials, bucket).
- Security (`HMAC_SHARED_SECRET`, `ALLOWED_ORIGINS`).
- Optional `HTTP_PROXY` for outbound traffic.

## Deployment
1. Push repo to Vercel project.
2. Configure project Environment Variables to match `.env.example`.
3. Deploy (`vercel --prod`). Cron in `vercel.json` registers weekly cleanup (`/api/admin_cleanup` Monday 04:00 UTC).

## Storage Layout
- `news/raw/{source}/{yyyymmdd}/{uuid}.json`
- `news/clustered/{yyyymmdd}/{cluster_id}.json`
- `market/{exchange}/{symbol}/{yyyymmddHH}/snapshot.json`
- `logs/{yyyy-mm-dd}/{request_id}.json`
- `gpt/actions/{yyyy-mm-dd}/{request_id}.json`

Objects include `created_at`, `ttl_days`, `schema_version`.

## GPT Action Manifest Snippet
```json
{
  "schema_version": "v1",
  "name_for_human": "Ozel Crypto Intel",
  "name_for_model": "ozel_crypto_intel",
  "description_for_human": "Fetch market snapshots and verified crypto news events.",
  "description_for_model": "Use POST endpoints with HMAC signature. Stick to allowed origins.",
  "auth": {
    "type": "custom",
    "authorization_type": "HMAC_SHA256",
    "verification_tokens": {
      "shared_secret": "{{HMAC_SHARED_SECRET}}"
    },
    "request_header": "X-Signature"
  },
  "api": {
    "type": "openapi",
    "url": "https://your-domain/.well-known/openapi.json"
  }
}
```

## Compliance Notes
- All scrapers check `robots.txt`, respect low QPS, and favour RSS.
- Identified as `ozel-gpt-collector/1.0 (+contact@your-domain)`.
- No bypassing auth, no heavy crawling; retry and timeout defaults at 12s.

## Cron & Cleanup
- Weekly cron (`0 4 * * 1`) hits `/api/admin_cleanup`.
- Cleanup removes objects older than their `ttl_days` (default 7) across news, markets, GPT actions, and logs.

## Tests
`tests/test_smoke.py` covers:
- `/api/health` JSON structure.
- Mocked `market_fetch` and `news_fetch` happy paths.
