# Week 1 Ingestion — Design

**Date:** 2026-07-27
**Scope:** Milestone 1 of `compute-price-tracker-BUILD-SPEC_1.md` (Week 1 only)
**Status:** Approved for planning

---

## 1. Goal

Get a compliant daily collector running unattended, writing GPU price observations
with full provenance, for the sources that need no third-party credentials.

Nothing else in the project matters until this is true. Layers 1–3, index
construction, the roofline model, and the site each get their own design cycle.

**Done when:** the daily GitHub Actions job has run unattended for 48 hours with
Azure Retail and OpenRouter prices committed to the repo, with provenance, and a
`collection_log` entry per run.

---

## 2. Corrections to the build spec

Verified live against each API on 2026-07-27 per BUILD-SPEC §15. Findings that
contradict the spec are recorded here and must be reflected in
`docs/source-verification.md`.

### 2.1 Urgency is inverted from §0

BUILD-SPEC §0 asserts AWS spot is the urgent source because of its 90-day
retention. That is backwards.

- `DescribeSpotPriceHistory` returns a trailing 90-day window, which means it
  offers a **90-day grace period**. Starting collection any time within ~90 days
  loses nothing.
- Azure Retail, GCP Catalog, and OpenRouter return **current prices only, with no
  history and no backfill of any kind**. Their grace period is **zero**. Every day
  not collected is permanently gone.

The sources that are actually bleeding today are the unauthenticated ones, which
are also the ones with no blockers. Build order follows the data loss, not the
spec's stated ordering.

Consequence: **OpenRouter moves from Week 2 into Week 1.** It is unauthenticated,
trivial to fetch, and losing a day of it is unrecoverable.

### 2.2 Azure Retail API — six deltas from §3.3

| # | Spec says | Reality |
|---|---|---|
| 1 | Capture `priceType` | Response field is **`type`**. `priceType` is a valid OData *filter* name but appears nowhere in the payload. |
| 2 | Spot vs consumption | `type` has **four** values: `Consumption`, `DevTestConsumption`, `Reservation`, and Spot/Low-Priority ride *inside* `Consumption` via `meterName`. |
| 3 | — | **`Reservation` rows report term totals under `unitOfMeasure: "1 Hour"`.** Observed: `ND96isrH100v5`, `retailPrice: 551221.0`, `unitOfMeasure: "1 Hour"`, `reservationTerm: "1 Year"`. Trusting the unit field publishes an H100 at $551,221/hour. Exclude on `type`, never on unit. |
| 4 | — | `DevTestConsumption` is a **discounted** rate (ND96asr A100: $5.439 vs $12.645). Folding it into on-demand biases the index down. |
| 5 | — | **"Low Priority" is a third pricing mode** (24 meters observed) — Azure's deprecated preemptible tier, distinct from Spot (36 meters). Must not be merged into the spot series. |
| 6 | Capture `reservationTerm` | Present **only** on `Reservation` rows; absent from all `Consumption` rows. A required-field schema rejects every on-demand row. |

Additional observations:

- `armSkuName` is not consistently an ARM SKU. Observed both
  `Standard_ND96asr_v4` and `NDasrA100v4_Type1`. SKU mapping must tolerate this
  and log misses rather than guess.
- `Standard_ND128isr_NDR_GB200_v6` is already listed, ahead of the §6.2 catalog.
  Confirms §6.2's rule that unmapped SKUs must be logged, never dropped.
- Query used returned `Count: 128` with no `NextPageLink`, but pagination is still
  required for broader filters.

### 2.3 OpenRouter — three deltas from §3.6

| # | Spec says | Reality |
|---|---|---|
| 7 | Pricing returned as strings | Values are **strings *or* lists**. 13 distinct pricing keys observed across models; `overrides` is list-valued. `Decimal(v)` over all values raises. |
| 8 | ~400 models | **341 models** on the date verified. |
| 9 | — | **18 models are priced at zero.** They must be captured but excluded from any median or index. |

The `pricing` dict is heterogeneous per model — keys observed: `prompt`,
`completion`, `input_cache_read`, `input_cache_write`, `input_cache_write_1h`,
`web_search`, `image`, `audio`, `input_audio_cache`, `internal_reasoning`,
`overrides`, `image_output`, `audio_output`. It must be stored as key/value rows,
not as a fixed-column schema.

Confirms BUILD-SPEC §6.1: observed input/output/cached ratio is exactly 1 : 5 : 0.1.

Models carry an `expiration_date` field, so the model universe has entry and exit.
Index survivorship policy is deferred to the index design cycle, but the field is
captured now.

### 2.4 robots.txt behaviour

- `openrouter.ai/robots.txt` → HTTP 200, allows our path (`Disallow: /seo/` only).
- `prices.azure.com/robots.txt` → **HTTP 404**.
- `pricing.us-east-1.amazonaws.com/robots.txt` → **HTTP 404**.

Under RFC 9309, absent robots.txt means unrestricted crawling. The checker must
**fail open on 404** and fail closed only on an explicit `Disallow` match. A naive
"no robots.txt means don't fetch" gate would block every source in the project.

---

## 3. Decisions taken

| # | Decision | Rationale |
|---|---|---|
| D1 | Scope this cycle to Week 1 ingestion only | BUILD-SPEC §15. Layers 2–3 and the site get their own cycles. |
| D2 | Sources: Azure Retail + OpenRouter live; AWS spot built dark | Zero-grace-period sources first (§2.1). No AWS account exists yet. |
| D3 | GCP deferred to Week 2 | Needs an account that does not exist, and §3.4's two pricing shapes deserve dedicated attention. |
| D4 | Store as gzipped raw JSON + daily Parquet in-repo; DuckDB for query | Deviates from §4/§5 (Postgres). No signup, no secrets, no free-tier pause risk. Date-partitioned files are append-only and git-friendly; a committed SQLite binary would be rewritten wholesale daily. DuckDB reads Parquet natively, which §4 already wants. Postgres migration in Week 2 is a straight load. |
| D5 | Persist raw payload archive **and** parsed observations | A parser bug found in Week 3 is repairable by re-parsing the archive. Without it, weeks of unrecoverable data are silently corrupt. |
| D6 | UA points at a public GitHub repo until the domain exists | Resolves the §9 circular dependency (`/about-data` must exist before the first fetch, but the site is Week 4). |
| D7 | Pin Python 3.12 via `uv` | Machine has 3.14.6; pyarrow/pandera support on 3.14 is not dependable yet. |

### User-Agent string

```
compute-price-tracker/0.1 (+https://github.com/mrmonroe90/compute-price-tracker; mrmonroe12@gmail.com)
```

Config-driven (`config/sources.yaml`), single value, swapped for the real domain in
Week 4.

**Setup prerequisite:** a **public** GitHub repo `mrmonroe90/compute-price-tracker`
containing `ABOUT-DATA.md` (sources, cadence, contact, disclaimers per §2.11) must
exist **before the first fetch**, because that URL is what a provider's ops team
will read. The local working directory is `~/ai-economics`; directory name and repo
name intentionally differ.

---

## 4. Architecture

Each module has one responsibility, a narrow interface, and is testable alone.
Compliance logic is deliberately separated from HTTP mechanics so the hard rules in
§2 can be tested with no network.

| Module | Responsibility | Depends on |
|---|---|---|
| `src/compliance.py` | Denylist, tier gate, UA construction, robots decision. Pure functions. | — |
| `src/fetchers/base.py` | Session, rate limit, backoff, daily request cap. Calls the compliance gate before every request. | `compliance` |
| `src/fetchers/azure_retail.py` | OData filter + paging, four-way `type` classification | `base` |
| `src/fetchers/openrouter.py` | Heterogeneous pricing dict → key/value rows | `base` |
| `src/fetchers/aws_spot.py` | boto3 spot history; built dark (`enabled: false`) | `base` |
| `src/store/archive.py` | `data/raw/{source}/{date}.json.gz` + sha256 | — |
| `src/store/observations.py` | Parquet writer, idempotency key | — |
| `src/store/collection_log.py` | Per-run status, row counts, errors | — |

### Data flow

```
compliance gate → fetch → archive raw (+sha256) → parse → Parquet → collection_log
```

**Observations are never written unless the raw archive write succeeded.** Every
published number is re-derivable from an archived payload.

### Layout

```
config/
  sources.yaml          # tier, endpoint, cadence, rate limit, daily cap, enabled
  regions.yaml
data/
  raw/{source}/{YYYY-MM-DD}.json.gz
  observations/{source}/{YYYY-MM-DD}.parquet
  collection_log.parquet
src/compliance.py
src/fetchers/{base,azure_retail,openrouter,aws_spot}.py
src/store/{archive,observations,collection_log}.py
scripts/backfill_aws_spot.py
tests/{test_compliance,test_azure_parse,test_openrouter_parse,test_idempotency}.py
tests/fixtures/{azure_retail_eastus_nd.json,openrouter_models.json}
docs/{source-verification,compliance-log,backlog-sources}.md
.github/workflows/ingest-daily.yml
COMPLIANCE.md
ABOUT-DATA.md
```

---

## 5. Schema

### `observations` (one Parquet file per source per day)

| Column | Notes |
|---|---|
| `source_id` | e.g. `azure_retail` |
| `observed_at_utc` | collection date |
| `fetched_at_utc` | precise fetch timestamp |
| `provider` | `azure` \| `aws` \| `openrouter` |
| `sku` | `armSkuName` / instance type / model id |
| `region` | `armRegionName`; null for OpenRouter |
| `az` | availability zone; AWS spot only |
| `price_type` | enum, see below |
| `price` | Decimal |
| `currency` | `USD` |
| `unit` | verbatim `unitOfMeasure` |
| `reservation_term` | nullable; Reservation rows only |
| `meter_id`, `meter_name`, `sku_name`, `product_name` | Azure passthrough |
| `source_url` | exact request URL |
| `raw_hash` | sha256 of the archived payload |
| `raw_ref` | archive file path |

**Idempotency:** unique on
`(source_id, sku, region, az, price_type, observed_at_utc)`.

The two stores have different mutability rules, and the distinction matters:

- **`data/raw/` is immutable and append-only.** A re-run on the same date writes a
  new file (`{date}.{n}.json.gz`), never overwriting the earlier payload. This is
  the §12 "never delete or update raw rows" guarantee, and it is what makes a
  Week-3 parser fix able to repair history.
- **`data/observations/` is a derived, regenerable projection.** Re-running a day
  rewrites that day's Parquet atomically from the archived payload, so it never
  accumulates duplicates. Losing it is not data loss; it can always be rebuilt.

Corrections to *published* series arrive as new rows with a `supersedes`
reference rather than in-place edits (§12).

### `price_type` enum — explicit classification, never inferred

| Value | Azure rule | Index-eligible |
|---|---|---|
| `ondemand` | `type == Consumption` and meterName has neither "Spot" nor "Low Priority" | yes |
| `spot` | `type == Consumption` and meterName contains "Spot" | yes |
| `lowpriority` | `type == Consumption` and meterName contains "Low Priority" | no |
| `devtest` | `type == DevTestConsumption` | no |
| `reservation` | `type == Reservation` | no — **structurally barred from hourly math** |

All five are captured. Only `ondemand` and `spot` are index-eligible, satisfying
§13's "never blend spot and on-demand".

### OpenRouter

Stored as key/value rows: `(model_id, price_key, price_value)`. Non-scalar values
(lists) are skipped and logged, never coerced. Values parsed to `Decimal`, never
`float`. `is_free` flag set where price is zero. `context_length`, `created`, and
`expiration_date` captured.

---

## 6. Compliance enforcement

Enforced in code, not documentation, per §2.

- **Denylist** — `coreweave.com`, `runpod.io` hard-coded in `compliance.py`, not
  read from config, so a careless config commit cannot add them.
- **Tier gate** — any source not `tier: safe` raises loudly and refuses to run.
- **Robots** — fetch once per host per run, cache for the run. 404 → allow (§2.4).
  Explicit `Disallow` match → refuse. Decision logged either way.
- **Rate limit** — 1 req/sec per host default, exponential backoff on 429/5xx, hard
  daily request cap per source from config.
- **UA** — single source of truth in config; `base.py` cannot issue a request
  without it.
- **No secrets** — CI check for accidental key commits.

`COMPLIANCE.md` carries §2 verbatim and is referenced from the fetcher module
docstring.

---

## 7. Testing

TDD: tests written red first. The live payloads captured during verification become
golden fixtures.

**`test_compliance.py`**
- CoreWeave and RunPod are blocked even when injected into `sources.yaml`
- A source with `tier: medium` or `tier: avoid` refuses to run
- robots 404 → allow; explicit `Disallow` on our path → refuse
- UA string is well-formed and always present

**`test_azure_parse.py`** (fixture: real 128-record `eastus` ND response)
- `Reservation` rows never emit an hourly price (the $551,221 case)
- `DevTestConsumption` never classifies as `ondemand`
- "Low Priority" never classifies as `spot`
- Spot meter count and on-demand meter count match hand-verified expectations
- Missing `reservationTerm` on Consumption rows does not fail validation

**`test_openrouter_parse.py`** (fixture: real 341-model response)
- List-valued pricing keys are skipped, not coerced or crashed on
- Values parse to `Decimal`, never `float`
- Zero-priced models flagged `is_free`, captured, excluded from index-eligible set

**`test_idempotency.py`**
- Re-running a day produces identical row counts, no duplicates

---

## 8. Error handling and ops

- One source failing does not block the others. Each writes its own
  `collection_log` row with status and error text.
- Any failure exits nonzero, so GitHub Actions sends its built-in failure email at
  no cost. Satisfies §12's alerting requirement for Week 1; richer alerting deferred.
- A raw-archive write failure aborts that source before any observation is written.
- `collection_log` is the uptime record published on `/about-data` later (§12).

---

## 9. AWS spot — dark build

Built and fully unit-tested in this cycle against a recorded fixture, shipped with
`enabled: false` in `sources.yaml`. It starts collecting the moment read-only IAM
credentials exist, with no code change.

`scripts/backfill_aws_spot.py` runs the one-time 90-day backfill on first
activation. Per §2.1 this is safe to run any time within ~90 days at no data cost.

Required IAM permissions when the account exists: `ec2:DescribeSpotPriceHistory`,
`pricing:GetProducts`. Read-only.

---

## 10. Out of scope for this cycle

Normalization Layers 1–2, the roofline model, index construction, the crack spread,
article extracts, and the site. Also deferred: GCP Catalog, Vantage cross-check,
Vast.ai account-requirement verification, `hardware_catalog.yaml`, and Postgres.

BUILD-SPEC §14 items 1–6 (roofline parameters) are not blocking for this cycle.

---

## 11. Open items for Mike

| # | Item | Blocking? |
|---|---|---|
| 1 | ~~Create public GitHub repo with `ABOUT-DATA.md`~~ | **Done 2026-07-27.** Live at https://github.com/mrmonroe90/compute-price-tracker; UA URL verified to return 200 to an unauthenticated request. |
| 2 | ~~`gh auth login`~~ | **Done 2026-07-27** as `mrmonroe90`. |
| 3 | Create AWS account, then a read-only IAM user | No — ~90-day grace period, but start it soon |
| 4 | Create GCP account + Billing Catalog API key | No — Week 2, but its data loss is ongoing |
| 5 | ~~Which Azure regions to collect~~ | **Resolved 2026-07-27: `eastus` only for Week 1.** Accepted consequence: other regions are permanently absent from history before the date they are added. Revisit in Week 2 before the catalog work. |
| 6 | Domain name | No — Week 4 |
