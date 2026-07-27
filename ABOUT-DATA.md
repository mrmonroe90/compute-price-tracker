# About this data collector

**If you are a provider engineer who found this URL in your access logs — this page
is for you. The contact address below is monitored, and a request to stop will be
honoured without argument.**

- **Project:** compute-price-tracker
- **User-Agent:** `compute-price-tracker/0.1 (+https://github.com/mrmonroe90/compute-price-tracker; mrmonroe12@gmail.com)`
- **Contact:** mrmonroe12@gmail.com
- **Status:** pre-launch. Collection has not started as of 2026-07-27.

---

## What this is

A daily collector that records published GPU and inference prices from official,
public pricing APIs, and derives normalized indices from them. The intent is a
historical time series for economic analysis, since providers publish current prices
but generally do not publish history.

It is not a price comparison service, a reseller, or a procurement tool.

## What it collects, and how often

| Source | Endpoint | Cadence | Auth |
|---|---|---|---|
| Azure Retail Prices | `prices.azure.com/api/retail/prices` | daily | none — public API |
| OpenRouter Models | `openrouter.ai/api/v1/models` | daily | none — public API |
| AWS EC2 Spot History | `DescribeSpotPriceHistory` via boto3 | daily | our own AWS account |
| AWS EC2 Price List | public price list files | weekly | none |

Azure region coverage is currently `eastus` only.

## How it behaves

- **Official APIs only.** No HTML scraping, no headless browsers, no parsing of
  rendered pricing pages. If a source has no documented API, it is not collected.
- **Identifies itself** on every request via the User-Agent above.
- **Respects robots.txt**, including for API endpoints.
- **Rate limits conservatively** — 1 request/second per host by default, with
  exponential backoff on 429 and 5xx, and a hard daily request cap per source.
  These are daily snapshots, not real-time polling; there is no reason to be
  aggressive.
- **No anti-bot circumvention.** No CAPTCHA solving, no proxy rotation, no
  user-agent spoofing.
- **No third-party accounts.** We do not create accounts or accept clickwrap terms
  to reach pricing data. AWS credentials, where used, belong to our own account.
- **Honours opt-outs.** Any provider asking to be excluded is removed, and the
  removal is recorded publicly in `docs/compliance-log.md`.

Sources that prohibit automated access in their terms are permanently denylisted in
code. This currently includes CoreWeave and RunPod.

Full rules: [COMPLIANCE.md](./COMPLIANCE.md).

## Disclaimers

**Informational purposes only. This is not an offer, quote, or solicitation.**

Prices are as published by providers and **may be stale, incomplete, or wrong**.
They are collected automatically and are not verified against invoices. Actual
prices depend on region, commitment, negotiated terms, and eligibility that this
project does not model. Do not use this data for procurement decisions without
confirming against the provider directly.

Published figures are **derived metrics** — normalized indices, spreads, medians,
and percentiles. This project is not a mirror or republication of any provider's
price list.

Any modeled cost figures are **modeled floors** under stated assumptions, not
measured costs. Real serving costs land above them.

No affiliation with, or endorsement by, any provider named here is claimed or
implied. All trademarks belong to their respective owners.

## Provenance

Every record carries its source, the exact request URL, a UTC fetch timestamp, and
a SHA-256 hash of the originating payload. Raw payloads are archived immutably, so
any published number can be traced to the response it came from.

Collection uptime and per-run status will be published once collection begins.

## Reuse

Derived series will be published under CC BY 4.0. Raw provider data remains subject
to each provider's own terms.
