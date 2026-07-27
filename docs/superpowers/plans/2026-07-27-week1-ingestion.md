# Week 1 Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A compliant daily collector that archives raw pricing payloads to GCS and commits parsed GPU price observations to the repo, running unattended on GitHub Actions.

**Architecture:** Compliance rules live in a pure, network-free module so the hard guardrails are testable in isolation; `BaseFetcher` owns HTTP mechanics and calls the compliance gate before every request. Each source is a thin fetcher/parser pair. Storage is three-tier: raw payloads to GCS (write-only in steady state), parsed observations to Parquet in git, published series generated later at site-build time. No database.

**Tech Stack:** Python 3.12 (pinned via `uv`), `httpx`, `pyarrow`, `PyYAML`, `google-cloud-storage`, `boto3`, `pytest`.

## Global Constraints

- **Python 3.12** pinned via `uv` — the machine has 3.14.6, which pyarrow support does not reliably cover.
- **User-Agent on every outbound request**, exactly: `compute-price-tracker/0.1 (+https://github.com/mrmonroe90/compute-price-tracker; mrmonroe12@gmail.com)`
- **Denylist is hard-coded in `src/compliance.py`**, never read from config: `coreweave.com`, `runpod.io`.
- **Tier gate:** any source not `tier: safe` refuses to run with a loud error.
- **robots.txt fails OPEN on 404** (RFC 9309), closed only on explicit `Disallow` match.
- **Rate limit** 1 req/sec per host default; exponential backoff on 429/5xx; hard daily request cap per source.
- **Never parse a price with `float()`.** All prices are `Decimal`.
- **No secrets in the repo.** `GCP_BILLING_API_KEY`, `GCP_SA_KEY` via env / Actions secrets.
- **Observations are never written unless the raw archive write succeeded.**
- **Raw archive is immutable** — a re-run writes `{date}.{n}.json.gz`, never overwrites.
- Azure region scope: `eastus` only.
- Price types are an explicit enum, never inferred: `ondemand | spot | lowpriority | devtest | reservation`. Only `ondemand` and `spot` are index-eligible.

---

## File Structure

| File | Responsibility |
|---|---|
| `pyproject.toml` | uv project, Python 3.12 pin, deps |
| `config/sources.yaml` | per-source tier, endpoint, cadence, rate limit, caps, enabled |
| `config/regions.yaml` | Azure region list |
| `src/cpt/compliance.py` | Denylist, tier gate, UA, robots. Pure. No network. |
| `src/cpt/fetchers/base.py` | Session, rate limit, backoff, daily cap, compliance gate |
| `src/cpt/fetchers/azure_retail.py` | OData paging + four-way `type` classification |
| `src/cpt/fetchers/openrouter.py` | Heterogeneous pricing dict → key/value rows |
| `src/cpt/fetchers/gcp_catalog.py` | Billing Catalog paging; archive only, no parsing |
| `src/cpt/fetchers/aws_spot.py` | boto3 spot history; built dark |
| `src/cpt/store/archive.py` | gzip + sha256 + immutable put (local or GCS backend) |
| `src/cpt/store/observations.py` | Parquet writer, idempotency key |
| `src/cpt/store/collection_log.py` | Per-run status, row counts, errors |
| `src/cpt/cli.py` | Orchestrator: run enabled sources, isolate failures |
| `.github/workflows/ingest-daily.yml` | Daily cron |

---

## Task 1: Scaffolding and the compliance module

**Files:**
- Create: `pyproject.toml`, `config/sources.yaml`, `config/regions.yaml`
- Create: `src/cpt/__init__.py`, `src/cpt/compliance.py`
- Test: `tests/test_compliance.py`

**Interfaces:**
- Consumes: nothing
- Produces: `USER_AGENT: str`, `DENYLIST: frozenset[str]`, `ComplianceError(RuntimeError)`, `assert_host_allowed(url: str) -> None`, `assert_tier_safe(source_id: str, tier: str) -> None`, `robots_allows(robots_txt: str | None, url: str, user_agent: str = USER_AGENT) -> bool`, `load_sources(path: Path) -> dict`

- [ ] **Step 1: Install uv and initialise the project**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
cd /Users/mrmonroe/ai-economics
uv init --package --name compute-price-tracker --python 3.12
uv add httpx pyarrow pyyaml google-cloud-storage boto3
uv add --dev pytest
```

- [ ] **Step 2: Write `pyproject.toml` requires-python pin**

Confirm `pyproject.toml` contains:

```toml
[project]
requires-python = ">=3.12,<3.13"
```

- [ ] **Step 3: Write `config/sources.yaml`**

```yaml
user_agent: "compute-price-tracker/0.1 (+https://github.com/mrmonroe90/compute-price-tracker; mrmonroe12@gmail.com)"

sources:
  - id: azure_retail
    tier: safe
    enabled: true
    endpoint: "https://prices.azure.com/api/retail/prices"
    api_version: "2023-01-01-preview"
    cadence: daily
    rate_limit_per_sec: 1.0
    daily_request_cap: 200

  - id: openrouter
    tier: safe
    enabled: true
    endpoint: "https://openrouter.ai/api/v1/models"
    cadence: daily
    rate_limit_per_sec: 1.0
    daily_request_cap: 10

  - id: gcp_catalog
    tier: safe
    enabled: true
    endpoint: "https://cloudbilling.googleapis.com/v1/services/6F81-5844-456A/skus"
    cadence: daily
    rate_limit_per_sec: 1.0
    daily_request_cap: 50
    archive_only: true

  - id: aws_spot
    tier: safe
    enabled: false
    cadence: daily
    rate_limit_per_sec: 1.0
    daily_request_cap: 500
```

- [ ] **Step 4: Write `config/regions.yaml`**

```yaml
azure:
  - eastus
```

- [ ] **Step 5: Write the failing tests**

Create `tests/test_compliance.py`:

```python
import pytest
import yaml
from pathlib import Path

from cpt.compliance import (
    USER_AGENT,
    DENYLIST,
    ComplianceError,
    assert_host_allowed,
    assert_tier_safe,
    robots_allows,
    load_sources,
)

CONFIG = Path(__file__).parent.parent / "config" / "sources.yaml"


def test_user_agent_has_url_and_contact():
    assert USER_AGENT.startswith("compute-price-tracker/0.1")
    assert "+https://github.com/mrmonroe90/compute-price-tracker" in USER_AGENT
    assert "mrmonroe12@gmail.com" in USER_AGENT


@pytest.mark.parametrize(
    "url",
    [
        "https://api.coreweave.com/v1/pricing",
        "https://www.runpod.io/api/graphql",
        "https://coreweave.com/pricing",
    ],
)
def test_denylisted_hosts_are_blocked(url):
    with pytest.raises(ComplianceError, match="denylist"):
        assert_host_allowed(url)


def test_allowed_hosts_pass():
    assert_host_allowed("https://prices.azure.com/api/retail/prices") is None


def test_denylist_cannot_be_overridden_by_config(tmp_path):
    """A careless commit adding CoreWeave to sources.yaml must still be blocked."""
    rogue = tmp_path / "sources.yaml"
    rogue.write_text(
        yaml.safe_dump(
            {
                "user_agent": USER_AGENT,
                "sources": [
                    {
                        "id": "coreweave",
                        "tier": "safe",
                        "enabled": True,
                        "endpoint": "https://api.coreweave.com/pricing",
                    }
                ],
            }
        )
    )
    cfg = load_sources(rogue)
    with pytest.raises(ComplianceError, match="denylist"):
        assert_host_allowed(cfg["sources"][0]["endpoint"])


@pytest.mark.parametrize("tier", ["medium", "avoid", "", None])
def test_non_safe_tier_refuses_to_run(tier):
    with pytest.raises(ComplianceError, match="tier"):
        assert_tier_safe("some_source", tier)


def test_safe_tier_passes():
    assert assert_tier_safe("azure_retail", "safe") is None


def test_robots_404_fails_open():
    """RFC 9309: absent robots.txt means unrestricted crawling."""
    assert robots_allows(None, "https://prices.azure.com/api/retail/prices") is True


def test_robots_explicit_disallow_fails_closed():
    txt = "User-Agent: *\nDisallow: /\n"
    assert robots_allows(txt, "https://example.com/api/models") is False


def test_robots_allows_unrelated_disallow():
    """openrouter.ai disallows only /seo/ — our path must remain allowed."""
    txt = "User-Agent: *\nAllow: /\nDisallow: /seo/\n"
    assert robots_allows(txt, "https://openrouter.ai/api/v1/models") is True
    assert robots_allows(txt, "https://openrouter.ai/seo/page") is False


def test_shipped_config_is_all_safe_tier():
    cfg = load_sources(CONFIG)
    assert cfg["sources"], "no sources configured"
    for s in cfg["sources"]:
        assert s["tier"] == "safe", f"{s['id']} is not safe tier"


def test_shipped_config_endpoints_not_denylisted():
    cfg = load_sources(CONFIG)
    for s in cfg["sources"]:
        if "endpoint" in s:
            assert_host_allowed(s["endpoint"])
```

- [ ] **Step 6: Run the tests to verify they fail**

Run: `uv run pytest tests/test_compliance.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cpt.compliance'`

- [ ] **Step 7: Write `src/cpt/compliance.py`**

```python
"""Compliance guardrails, enforced in code.

See COMPLIANCE.md at the repo root for the full rules. This module is
deliberately network-free so every guardrail is testable in isolation.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import yaml

USER_AGENT = (
    "compute-price-tracker/0.1 "
    "(+https://github.com/mrmonroe90/compute-price-tracker; mrmonroe12@gmail.com)"
)

# COMPLIANCE.md rule 8. Hard-coded, never read from config, so a careless
# config commit cannot add these.
DENYLIST = frozenset({"coreweave.com", "runpod.io"})

SAFE_TIER = "safe"


class ComplianceError(RuntimeError):
    """Raised when a hard compliance rule would be violated."""


def _host(url: str) -> str:
    return (urlparse(url).hostname or "").lower()


def assert_host_allowed(url: str) -> None:
    """Raise if the URL's host is on the permanent denylist (rule 8)."""
    host = _host(url)
    for blocked in DENYLIST:
        if host == blocked or host.endswith("." + blocked):
            raise ComplianceError(
                f"host {host!r} is on the permanent denylist (COMPLIANCE.md rule 8); "
                f"this source prohibits automated access in its ToS"
            )


def assert_tier_safe(source_id: str, tier: str | None) -> None:
    """Raise unless the source is explicitly marked safe tier (rule 7)."""
    if tier != SAFE_TIER:
        raise ComplianceError(
            f"source {source_id!r} has tier {tier!r}; only {SAFE_TIER!r} sources may "
            f"run (COMPLIANCE.md rule 7). Adding a medium source requires a "
            f"deliberate flag and a note in docs/compliance-log.md"
        )


def robots_allows(
    robots_txt: str | None, url: str, user_agent: str = USER_AGENT
) -> bool:
    """Decide whether robots.txt permits fetching `url`.

    Per RFC 9309 an absent robots.txt means unrestricted crawling, so a
    missing file (HTTP 404) fails OPEN. Both prices.azure.com and the AWS
    pricing host return 404; failing closed would block every source.
    """
    if robots_txt is None:
        return True
    parser = RobotFileParser()
    parser.parse(robots_txt.splitlines())
    return parser.can_fetch(user_agent, url)


def load_sources(path: Path) -> dict:
    """Load sources.yaml. Does not validate — callers must gate each source."""
    with open(path) as fh:
        return yaml.safe_load(fh)
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `uv run pytest tests/test_compliance.py -v`
Expected: PASS — 13 tests

- [ ] **Step 9: Commit**

```bash
git add pyproject.toml uv.lock config/ src/cpt/ tests/test_compliance.py
git commit -m "feat: compliance guardrails with denylist and tier enforcement

Denylist is hard-coded rather than config-driven so a careless commit
cannot add a prohibited source. robots.txt fails open on 404 per RFC
9309, which every source we have relies on."
```

---

## Task 2: BaseFetcher

**Files:**
- Create: `src/cpt/fetchers/__init__.py`, `src/cpt/fetchers/base.py`
- Test: `tests/test_base_fetcher.py`

**Interfaces:**
- Consumes: `cpt.compliance.{USER_AGENT, assert_host_allowed, assert_tier_safe, robots_allows, ComplianceError}`
- Produces: `RequestCapExceeded(RuntimeError)`, `BaseFetcher(source_id: str, config: dict, client: httpx.Client | None = None, sleep=time.sleep)` with `.get(url: str, params: dict | None = None) -> httpx.Response` and `.request_count: int`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_base_fetcher.py`:

```python
import httpx
import pytest

from cpt.compliance import ComplianceError, USER_AGENT
from cpt.fetchers.base import BaseFetcher, RequestCapExceeded

CONFIG = {
    "id": "test_source",
    "tier": "safe",
    "rate_limit_per_sec": 1000.0,   # effectively no delay in tests
    "daily_request_cap": 3,
}


def make_fetcher(handler, config=None, sleeps=None):
    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    cfg = dict(config or CONFIG)
    return BaseFetcher(
        source_id=cfg["id"],
        config=cfg,
        client=client,
        sleep=(sleeps.append if sleeps is not None else (lambda _s: None)),
    )


def test_refuses_non_safe_tier():
    with pytest.raises(ComplianceError, match="tier"):
        make_fetcher(lambda r: httpx.Response(200), config={**CONFIG, "tier": "medium"})


def test_sets_user_agent_on_every_request():
    seen = []

    def handler(request):
        seen.append(request.headers.get("user-agent"))
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, json={"ok": True})

    f = make_fetcher(handler)
    f.get("https://example.com/api")
    assert seen and all(ua == USER_AGENT for ua in seen)


def test_blocks_denylisted_host_before_any_request():
    def handler(request):
        raise AssertionError("must not issue a request to a denylisted host")

    f = make_fetcher(handler)
    with pytest.raises(ComplianceError, match="denylist"):
        f.get("https://api.coreweave.com/pricing")


def test_robots_404_allows_fetch():
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, json={"ok": True})

    f = make_fetcher(handler)
    assert f.get("https://example.com/api").json() == {"ok": True}


def test_robots_disallow_blocks_fetch():
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-Agent: *\nDisallow: /\n")
        raise AssertionError("must not fetch a disallowed path")

    f = make_fetcher(handler)
    with pytest.raises(ComplianceError, match="robots"):
        f.get("https://example.com/api")


def test_robots_fetched_once_per_host_per_run():
    calls = {"robots": 0}

    def handler(request):
        if request.url.path == "/robots.txt":
            calls["robots"] += 1
            return httpx.Response(404)
        return httpx.Response(200, json={})

    f = make_fetcher(handler)
    f.get("https://example.com/a")
    f.get("https://example.com/b")
    assert calls["robots"] == 1


def test_daily_request_cap_enforced():
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, json={})

    f = make_fetcher(handler)
    for _ in range(3):
        f.get("https://example.com/api")
    with pytest.raises(RequestCapExceeded):
        f.get("https://example.com/api")


def test_retries_on_429_then_succeeds():
    state = {"n": 0}

    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        state["n"] += 1
        if state["n"] < 3:
            return httpx.Response(429)
        return httpx.Response(200, json={"ok": True})

    sleeps = []
    f = make_fetcher(handler, sleeps=sleeps)
    assert f.get("https://example.com/api").json() == {"ok": True}
    assert state["n"] == 3
    assert sleeps == [1.0, 2.0], "backoff must be exponential"


def test_gives_up_after_max_attempts():
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(503)

    f = make_fetcher(handler, sleeps=[])
    with pytest.raises(httpx.HTTPStatusError):
        f.get("https://example.com/api")


def test_rate_limit_sleeps_between_requests():
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, json={})

    sleeps = []
    f = make_fetcher(handler, config={**CONFIG, "rate_limit_per_sec": 1.0}, sleeps=sleeps)
    f.get("https://example.com/a")
    f.get("https://example.com/b")
    assert any(s > 0 for s in sleeps), "must throttle to the configured rate"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_base_fetcher.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cpt.fetchers'`

- [ ] **Step 3: Write `src/cpt/fetchers/base.py`**

```python
"""HTTP mechanics for all fetchers.

Every guardrail in COMPLIANCE.md that concerns outbound requests is
enforced here or in cpt.compliance, which this module calls before each
request. No fetcher may issue an HTTP request by any other route.
"""

from __future__ import annotations

import time
from urllib.parse import urlparse

import httpx

from cpt.compliance import (
    USER_AGENT,
    ComplianceError,
    assert_host_allowed,
    assert_tier_safe,
    robots_allows,
)

MAX_ATTEMPTS = 5
RETRY_STATUS = {429, 500, 502, 503, 504}


class RequestCapExceeded(RuntimeError):
    """Raised when a source exceeds its configured daily request cap."""


class BaseFetcher:
    def __init__(self, source_id, config, client=None, sleep=time.sleep):
        assert_tier_safe(source_id, config.get("tier"))
        self.source_id = source_id
        self.config = config
        self.rate_limit = float(config.get("rate_limit_per_sec", 1.0))
        self.cap = int(config.get("daily_request_cap", 100))
        self.request_count = 0
        self._client = client or httpx.Client(timeout=60.0, follow_redirects=True)
        self._sleep = sleep
        self._robots: dict[str, str | None] = {}
        self._last_request_at: dict[str, float] = {}

    # --- compliance gate -------------------------------------------------

    def _robots_for(self, url: str) -> str | None:
        host = urlparse(url).netloc
        if host not in self._robots:
            robots_url = f"{urlparse(url).scheme}://{host}/robots.txt"
            try:
                resp = self._client.get(
                    robots_url, headers={"User-Agent": USER_AGENT}
                )
                # 404 means no restrictions (RFC 9309) -> fail open
                self._robots[host] = resp.text if resp.status_code == 200 else None
            except httpx.HTTPError:
                self._robots[host] = None
        return self._robots[host]

    def _gate(self, url: str) -> None:
        assert_host_allowed(url)
        if not robots_allows(self._robots_for(url), url):
            raise ComplianceError(f"robots.txt disallows {url!r} for our user agent")
        if self.request_count >= self.cap:
            raise RequestCapExceeded(
                f"source {self.source_id!r} hit its daily cap of {self.cap} requests"
            )

    def _throttle(self, url: str) -> None:
        host = urlparse(url).netloc
        min_interval = 1.0 / self.rate_limit if self.rate_limit > 0 else 0.0
        last = self._last_request_at.get(host)
        now = time.monotonic()
        if last is not None:
            wait = min_interval - (now - last)
            if wait > 0:
                self._sleep(wait)
        self._last_request_at[host] = time.monotonic()

    # --- public API ------------------------------------------------------

    def get(self, url: str, params: dict | None = None) -> httpx.Response:
        self._gate(url)
        self._throttle(url)

        last_response = None
        for attempt in range(MAX_ATTEMPTS):
            self.request_count += 1
            resp = self._client.get(
                url, params=params, headers={"User-Agent": USER_AGENT}
            )
            if resp.status_code not in RETRY_STATUS:
                resp.raise_for_status()
                return resp
            last_response = resp
            if attempt < MAX_ATTEMPTS - 1:
                self._sleep(2.0**attempt)

        last_response.raise_for_status()
        raise AssertionError("unreachable")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_base_fetcher.py -v`
Expected: PASS — 10 tests

- [ ] **Step 5: Commit**

```bash
git add src/cpt/fetchers/ tests/test_base_fetcher.py
git commit -m "feat: BaseFetcher with rate limiting, backoff, and request caps

Every outbound request passes the compliance gate first: denylist check,
robots decision cached per host per run, then the daily cap."
```

---

## Task 3: Immutable archive writer

**Files:**
- Create: `src/cpt/store/__init__.py`, `src/cpt/store/archive.py`
- Test: `tests/test_archive.py`

**Interfaces:**
- Consumes: nothing
- Produces: `ArchiveRef(raw_hash: str, raw_ref: str)` (NamedTuple), `LocalArchive(root: Path)`, `GCSArchive(bucket: str)`, both with `.put(key: str, data: bytes) -> str` and `.exists(key: str) -> bool`; `archive_payload(backend, source_id: str, observed_date: date, payload: bytes) -> ArchiveRef`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_archive.py`:

```python
import gzip
import hashlib
import json
from datetime import date

from cpt.store.archive import ArchiveRef, LocalArchive, archive_payload

PAYLOAD = json.dumps({"Items": [{"a": 1}]}).encode()
DAY = date(2026, 7, 27)


def test_archive_returns_sha256_of_uncompressed_payload(tmp_path):
    ref = archive_payload(LocalArchive(tmp_path), "azure_retail", DAY, PAYLOAD)
    assert isinstance(ref, ArchiveRef)
    assert ref.raw_hash == hashlib.sha256(PAYLOAD).hexdigest()


def test_archive_writes_gzipped_payload_that_round_trips(tmp_path):
    ref = archive_payload(LocalArchive(tmp_path), "azure_retail", DAY, PAYLOAD)
    stored = (tmp_path / ref.raw_ref).read_bytes()
    assert gzip.decompress(stored) == PAYLOAD


def test_archive_key_is_date_partitioned(tmp_path):
    ref = archive_payload(LocalArchive(tmp_path), "azure_retail", DAY, PAYLOAD)
    assert ref.raw_ref == "raw/azure_retail/2026-07-27.json.gz"


def test_rerun_never_overwrites_the_earlier_payload(tmp_path):
    """COMPLIANCE.md rule: raw rows are never deleted or updated."""
    backend = LocalArchive(tmp_path)
    first = archive_payload(backend, "azure_retail", DAY, PAYLOAD)
    second = archive_payload(backend, "azure_retail", DAY, b'{"Items": []}')

    assert first.raw_ref != second.raw_ref
    assert second.raw_ref == "raw/azure_retail/2026-07-27.1.json.gz"
    assert gzip.decompress((tmp_path / first.raw_ref).read_bytes()) == PAYLOAD


def test_third_rerun_increments_again(tmp_path):
    backend = LocalArchive(tmp_path)
    for _ in range(3):
        archive_payload(backend, "azure_retail", DAY, PAYLOAD)
    assert (tmp_path / "raw/azure_retail/2026-07-27.json.gz").exists()
    assert (tmp_path / "raw/azure_retail/2026-07-27.1.json.gz").exists()
    assert (tmp_path / "raw/azure_retail/2026-07-27.2.json.gz").exists()


def test_compression_is_worthwhile(tmp_path):
    big = json.dumps({"Items": [{"sku": f"s{i}", "price": 1.0} for i in range(5000)]}).encode()
    ref = archive_payload(LocalArchive(tmp_path), "gcp_catalog", DAY, big)
    assert (tmp_path / ref.raw_ref).stat().st_size < len(big) / 5
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_archive.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cpt.store'`

- [ ] **Step 3: Write `src/cpt/store/archive.py`**

```python
"""Immutable raw payload archive.

Raw payloads are the project's insurance policy: a parser bug found weeks
later is repairable only if the original response still exists. Nothing
here ever overwrites an existing object.
"""

from __future__ import annotations

import gzip
import hashlib
from datetime import date
from pathlib import Path
from typing import NamedTuple, Protocol


class ArchiveRef(NamedTuple):
    raw_hash: str
    raw_ref: str


class ArchiveBackend(Protocol):
    def exists(self, key: str) -> bool: ...
    def put(self, key: str, data: bytes) -> str: ...


class LocalArchive:
    """Filesystem backend. Used by tests and for local development."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def exists(self, key: str) -> bool:
        return (self.root / key).exists()

    def put(self, key: str, data: bytes) -> str:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return key


class GCSArchive:
    """Google Cloud Storage backend. Write-only in steady state."""

    def __init__(self, bucket: str, client=None):
        from google.cloud import storage

        self._client = client or storage.Client()
        self._bucket = self._client.bucket(bucket)

    def exists(self, key: str) -> bool:
        return self._bucket.blob(key).exists()

    def put(self, key: str, data: bytes) -> str:
        blob = self._bucket.blob(key)
        # if_generation_match=0 makes this fail if the object already exists,
        # enforcing immutability at the storage layer rather than by convention
        blob.upload_from_string(
            data, content_type="application/gzip", if_generation_match=0
        )
        return key


def archive_payload(
    backend: ArchiveBackend, source_id: str, observed_date: date, payload: bytes
) -> ArchiveRef:
    """Gzip and store `payload`, never overwriting an existing object.

    Returns the sha256 of the *uncompressed* payload and the storage key.
    """
    raw_hash = hashlib.sha256(payload).hexdigest()
    stem = f"raw/{source_id}/{observed_date.isoformat()}"

    key = f"{stem}.json.gz"
    n = 0
    while backend.exists(key):
        n += 1
        key = f"{stem}.{n}.json.gz"

    backend.put(key, gzip.compress(payload))
    return ArchiveRef(raw_hash=raw_hash, raw_ref=key)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_archive.py -v`
Expected: PASS — 6 tests

- [ ] **Step 5: Commit**

```bash
git add src/cpt/store/ tests/test_archive.py
git commit -m "feat: immutable gzipped raw archive with local and GCS backends

A re-run writes {date}.{n}.json.gz rather than overwriting. GCS uses
if_generation_match=0 so immutability is enforced by the storage layer,
not by convention."
```

---

## Task 4: Observations and collection log writers

**Files:**
- Create: `src/cpt/store/observations.py`, `src/cpt/store/collection_log.py`
- Test: `tests/test_observations.py`

**Interfaces:**
- Consumes: nothing
- Produces: `PRICE_TYPES: frozenset[str]`, `INDEX_ELIGIBLE: frozenset[str]`, `OBSERVATION_SCHEMA: pa.Schema`, `Observation` (TypedDict), `write_observations(root: Path, source_id: str, observed_date: date, rows: list[dict]) -> Path`, `read_observations(path: Path) -> list[dict]`, `dedupe(rows: list[dict]) -> list[dict]`, `append_run(root: Path, source_id: str, observed_date: date, status: str, row_count: int, error: str | None = None) -> None`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_observations.py`:

```python
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from cpt.store.collection_log import append_run, read_log
from cpt.store.observations import (
    INDEX_ELIGIBLE,
    PRICE_TYPES,
    dedupe,
    read_observations,
    write_observations,
)

DAY = date(2026, 7, 27)
NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)


def row(**over):
    base = dict(
        source_id="azure_retail",
        observed_at_utc=DAY,
        fetched_at_utc=NOW,
        provider="azure",
        sku="Standard_ND96isr_H100_v5",
        region="eastus",
        az=None,
        price_type="ondemand",
        price=Decimal("98.32"),
        currency="USD",
        unit="1 Hour",
        reservation_term=None,
        meter_id="03f48dbf",
        meter_name="ND96isrH100v5",
        sku_name="ND96isrH100v5",
        product_name="Virtual Machines NDsr H100 v5 Series",
        source_url="https://prices.azure.com/api/retail/prices",
        raw_hash="abc123",
        raw_ref="raw/azure_retail/2026-07-27.json.gz",
    )
    base.update(over)
    return base


def test_price_type_enum_is_exactly_five_values():
    assert PRICE_TYPES == {
        "ondemand", "spot", "lowpriority", "devtest", "reservation"
    }


def test_only_ondemand_and_spot_are_index_eligible():
    assert INDEX_ELIGIBLE == {"ondemand", "spot"}


def test_write_then_read_round_trips_price_as_decimal(tmp_path):
    write_observations(tmp_path, "azure_retail", DAY, [row()])
    out = read_observations(tmp_path / "observations/azure_retail/2026-07-27.parquet")
    assert len(out) == 1
    assert out[0]["price"] == Decimal("98.32")
    assert isinstance(out[0]["price"], Decimal), "price must never become a float"


def test_rejects_unknown_price_type(tmp_path):
    with pytest.raises(ValueError, match="price_type"):
        write_observations(tmp_path, "azure_retail", DAY, [row(price_type="onDemand")])


def test_rejects_row_missing_provenance(tmp_path):
    """COMPLIANCE.md rule 10: if we can't say where a number came from, we
    don't publish it."""
    for field in ("source_url", "raw_hash", "fetched_at_utc"):
        with pytest.raises(ValueError, match=field):
            write_observations(tmp_path, "azure_retail", DAY, [row(**{field: None})])


def test_dedupe_collapses_identical_idempotency_keys():
    rows = [row(price=Decimal("98.32")), row(price=Decimal("98.32"))]
    assert len(dedupe(rows)) == 1


def test_dedupe_keeps_rows_differing_in_key():
    rows = [row(price_type="ondemand"), row(price_type="spot")]
    assert len(dedupe(rows)) == 2


def test_rerun_overwrites_derived_parquet_without_duplicating(tmp_path):
    """Observations are a derived projection: a re-run rewrites the day."""
    write_observations(tmp_path, "azure_retail", DAY, [row()])
    write_observations(tmp_path, "azure_retail", DAY, [row()])
    out = read_observations(tmp_path / "observations/azure_retail/2026-07-27.parquet")
    assert len(out) == 1


def test_collection_log_records_success_and_failure(tmp_path):
    append_run(tmp_path, "azure_retail", DAY, "ok", 128)
    append_run(tmp_path, "openrouter", DAY, "failed", 0, error="timeout")
    log = read_log(tmp_path)
    assert len(log) == 2
    failed = [r for r in log if r["status"] == "failed"][0]
    assert failed["source_id"] == "openrouter"
    assert failed["error"] == "timeout"
    assert failed["row_count"] == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_observations.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cpt.store.observations'`

- [ ] **Step 3: Write `src/cpt/store/observations.py`**

```python
"""Parsed price observations, stored as one Parquet file per source per day.

Unlike the raw archive, this layer is a derived projection: it can always
be rebuilt from archived payloads, so a re-run rewrites the day's file
rather than appending.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PRICE_TYPES = frozenset(
    {"ondemand", "spot", "lowpriority", "devtest", "reservation"}
)
INDEX_ELIGIBLE = frozenset({"ondemand", "spot"})

REQUIRED_PROVENANCE = ("source_url", "raw_hash", "fetched_at_utc")

IDEMPOTENCY_KEY = (
    "source_id", "sku", "region", "az", "price_type", "observed_at_utc"
)

# Decimal128(38, 12) holds per-token prices (1e-8) and reservation totals
# (1e6) without loss. Never use a float type here.
OBSERVATION_SCHEMA = pa.schema(
    [
        ("source_id", pa.string()),
        ("observed_at_utc", pa.date32()),
        ("fetched_at_utc", pa.timestamp("us", tz="UTC")),
        ("provider", pa.string()),
        ("sku", pa.string()),
        ("region", pa.string()),
        ("az", pa.string()),
        ("price_type", pa.string()),
        ("price", pa.decimal128(38, 12)),
        ("currency", pa.string()),
        ("unit", pa.string()),
        ("reservation_term", pa.string()),
        ("meter_id", pa.string()),
        ("meter_name", pa.string()),
        ("sku_name", pa.string()),
        ("product_name", pa.string()),
        ("source_url", pa.string()),
        ("raw_hash", pa.string()),
        ("raw_ref", pa.string()),
    ]
)


def _validate(rows: list[dict]) -> None:
    for i, r in enumerate(rows):
        pt = r.get("price_type")
        if pt not in PRICE_TYPES:
            raise ValueError(
                f"row {i}: price_type {pt!r} is not one of {sorted(PRICE_TYPES)}"
            )
        for field in REQUIRED_PROVENANCE:
            if r.get(field) in (None, ""):
                raise ValueError(f"row {i}: missing provenance field {field!r}")
        if not isinstance(r.get("price"), Decimal):
            raise ValueError(f"row {i}: price must be Decimal, got {type(r.get('price'))}")


def dedupe(rows: list[dict]) -> list[dict]:
    """Collapse rows sharing an idempotency key, keeping the last."""
    seen: dict[tuple, dict] = {}
    for r in rows:
        seen[tuple(r.get(k) for k in IDEMPOTENCY_KEY)] = r
    return list(seen.values())


def write_observations(
    root: Path, source_id: str, observed_date: date, rows: list[dict]
) -> Path:
    _validate(rows)
    rows = dedupe(rows)

    path = Path(root) / "observations" / source_id / f"{observed_date.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pylist(rows, schema=OBSERVATION_SCHEMA)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)   # atomic
    return path


def read_observations(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()
```

- [ ] **Step 4: Write `src/cpt/store/collection_log.py`**

```python
"""Per-run collection log.

This is the uptime record published on /about-data. A silent failure that
loses a day of unrecoverable pricing is the worst outcome in the project,
so every run writes a row whether it succeeded or not.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

LOG_SCHEMA = pa.schema(
    [
        ("source_id", pa.string()),
        ("observed_at_utc", pa.date32()),
        ("run_at_utc", pa.timestamp("us", tz="UTC")),
        ("status", pa.string()),
        ("row_count", pa.int64()),
        ("error", pa.string()),
    ]
)

STATUSES = frozenset({"ok", "failed", "skipped"})


def _path(root: Path) -> Path:
    return Path(root) / "collection_log.parquet"


def read_log(root: Path) -> list[dict]:
    path = _path(root)
    if not path.exists():
        return []
    return pq.read_table(path).to_pylist()


def append_run(
    root: Path,
    source_id: str,
    observed_date: date,
    status: str,
    row_count: int,
    error: str | None = None,
) -> None:
    if status not in STATUSES:
        raise ValueError(f"status {status!r} not in {sorted(STATUSES)}")

    rows = read_log(root)
    rows.append(
        {
            "source_id": source_id,
            "observed_at_utc": observed_date,
            "run_at_utc": datetime.now(timezone.utc),
            "status": status,
            "row_count": int(row_count),
            "error": error,
        }
    )
    path = _path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=LOG_SCHEMA), tmp, compression="zstd")
    tmp.replace(path)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_observations.py -v`
Expected: PASS — 9 tests

- [ ] **Step 6: Commit**

```bash
git add src/cpt/store/ tests/test_observations.py
git commit -m "feat: Parquet observation writer and collection log

Prices are decimal128(38,12), never floats. Rows missing provenance are
rejected at write time rather than published without a source."
```

---

## Task 5: Azure Retail fetcher

**Files:**
- Create: `src/cpt/fetchers/azure_retail.py`
- Test: `tests/test_azure_parse.py`
- Fixture (already committed): `tests/fixtures/azure_retail_eastus_nd_2026-07-27.json`

**Interfaces:**
- Consumes: `BaseFetcher`, `cpt.store.observations.{PRICE_TYPES, INDEX_ELIGIBLE}`
- Produces: `classify(item: dict) -> str`, `to_hourly(row: dict) -> Decimal` (raises `ValueError` for non-hourly types), `parse(payload: dict, fetched_at: datetime, observed_date: date, source_url: str, raw_hash: str, raw_ref: str) -> list[dict]`, `AzureRetailFetcher(config, client=None)` with `.fetch(region: str) -> tuple[bytes, dict]`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_azure_parse.py`:

```python
import json
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from cpt.fetchers.azure_retail import classify, parse, to_hourly
from cpt.store.observations import INDEX_ELIGIBLE

FIXTURE = Path(__file__).parent / "fixtures" / "azure_retail_eastus_nd_2026-07-27.json"
DAY = date(2026, 7, 27)
NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def rows():
    payload = json.loads(FIXTURE.read_text())
    return parse(
        payload,
        fetched_at=NOW,
        observed_date=DAY,
        source_url="https://prices.azure.com/api/retail/prices",
        raw_hash="deadbeef",
        raw_ref="raw/azure_retail/2026-07-27.json.gz",
    )


def test_fixture_classification_counts(rows):
    """Hand-verified against the live 2026-07-27 eastus ND response."""
    assert Counter(r["price_type"] for r in rows) == {
        "ondemand": 2,
        "spot": 2,
        "lowpriority": 2,
        "devtest": 2,
        "reservation": 2,
    }


def test_reservation_rows_are_never_hourly(rows):
    """The $551,221 trap: Azure reports 1-year term totals under
    unitOfMeasure '1 Hour'. Publishing that as an hourly rate would show an
    H100 at half a million dollars an hour."""
    res = [r for r in rows if r["price_type"] == "reservation"]
    assert res, "fixture must contain reservation rows"
    for r in res:
        assert r["unit"] == "1 Hour", "Azure really does mislabel these"
        assert r["reservation_term"] in {"1 Year", "3 Years"}
        with pytest.raises(ValueError, match="reservation"):
            to_hourly(r)


def test_reservation_prices_are_captured_verbatim(rows):
    prices = {r["price"] for r in rows if r["price_type"] == "reservation"}
    assert Decimal("551221.0") in prices


def test_devtest_never_classified_as_ondemand():
    assert classify({"type": "DevTestConsumption", "meterName": "ND96isrH100v5"}) == "devtest"
    assert classify(
        {"type": "DevTestConsumption", "meterName": "ND40rs v2 Spot"}
    ) == "devtest"


def test_low_priority_never_classified_as_spot():
    assert classify(
        {"type": "Consumption", "meterName": "ND96asr_A100_v4 Low Priority"}
    ) == "lowpriority"


def test_spot_classified_from_meter_name():
    assert classify({"type": "Consumption", "meterName": "ND40rs v2 Spot"}) == "spot"


def test_plain_consumption_is_ondemand():
    assert classify({"type": "Consumption", "meterName": "ND96isrH100v5"}) == "ondemand"


def test_unknown_type_raises():
    with pytest.raises(ValueError, match="unknown"):
        classify({"type": "SomethingNew", "meterName": "x"})


def test_missing_reservation_term_does_not_fail_consumption_rows(rows):
    """reservationTerm is absent on every Consumption row; a required-field
    schema would reject all of them."""
    consumption = [r for r in rows if r["price_type"] != "reservation"]
    assert consumption
    assert all(r["reservation_term"] is None for r in consumption)


def test_index_eligible_excludes_devtest_lowpriority_reservation(rows):
    eligible = [r for r in rows if r["price_type"] in INDEX_ELIGIBLE]
    assert len(eligible) == 4
    assert {r["price_type"] for r in eligible} == {"ondemand", "spot"}


def test_prices_are_decimal_not_float(rows):
    assert all(isinstance(r["price"], Decimal) for r in rows)
    ondemand = [r for r in rows if r["price_type"] == "ondemand"]
    assert Decimal("98.32") in {r["price"] for r in ondemand}


def test_provenance_present_on_every_row(rows):
    for r in rows:
        assert r["raw_hash"] == "deadbeef"
        assert r["raw_ref"] == "raw/azure_retail/2026-07-27.json.gz"
        assert r["source_url"].startswith("https://prices.azure.com")
        assert r["fetched_at_utc"] == NOW
        assert r["provider"] == "azure"


def test_to_hourly_returns_price_for_hourly_types():
    row = {"price_type": "ondemand", "unit": "1 Hour", "price": Decimal("98.32")}
    assert to_hourly(row) == Decimal("98.32")


def test_to_hourly_rejects_non_hourly_unit():
    row = {"price_type": "ondemand", "unit": "1 Month", "price": Decimal("1")}
    with pytest.raises(ValueError, match="unit"):
        to_hourly(row)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_azure_parse.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cpt.fetchers.azure_retail'`

- [ ] **Step 3: Write `src/cpt/fetchers/azure_retail.py`**

```python
"""Azure Retail Prices API.

Verified live 2026-07-27. Six deltas from the original build spec are
encoded here; see docs/source-verification.md. The one that matters most:
Reservation rows report term totals under unitOfMeasure "1 Hour", so
price type must be decided on `type`, never on the unit field.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

from cpt.fetchers.base import BaseFetcher

API_VERSION = "2023-01-01-preview"

# The response field is `type`, not `priceType`. `priceType` is a valid
# OData filter name but appears nowhere in the payload.
_TYPE_FIELD = "type"


def classify(item: dict) -> str:
    """Map an Azure meter to our explicit price-type enum.

    Spot and Low Priority both ride inside `type == "Consumption"` and are
    distinguishable only by meterName.
    """
    t = item.get(_TYPE_FIELD)
    meter = (item.get("meterName") or "").lower()

    if t == "Reservation":
        return "reservation"
    if t == "DevTestConsumption":
        return "devtest"
    if t == "Consumption":
        if "spot" in meter:
            return "spot"
        if "low priority" in meter:
            return "lowpriority"
        return "ondemand"
    raise ValueError(f"unknown Azure price type {t!r} on meter {item.get('meterName')!r}")


def to_hourly(row: dict) -> Decimal:
    """Return the hourly rate, refusing types where that is meaningless.

    Reservation rows carry a whole-term total mislabelled as "1 Hour";
    dividing or publishing that as an hourly figure is the single easiest
    way to put a catastrophically wrong number on a chart.
    """
    if row["price_type"] == "reservation":
        raise ValueError(
            "reservation rows carry a term total, not an hourly rate; "
            "Azure mislabels the unit as '1 Hour'"
        )
    if row.get("unit") != "1 Hour":
        raise ValueError(f"unit {row.get('unit')!r} is not hourly")
    return row["price"]


def parse(
    payload: dict,
    fetched_at: datetime,
    observed_date: date,
    source_url: str,
    raw_hash: str,
    raw_ref: str,
) -> list[dict]:
    rows = []
    for item in payload.get("Items", []):
        rows.append(
            {
                "source_id": "azure_retail",
                "observed_at_utc": observed_date,
                "fetched_at_utc": fetched_at,
                "provider": "azure",
                "sku": item.get("armSkuName"),
                "region": item.get("armRegionName"),
                "az": None,
                "price_type": classify(item),
                "price": Decimal(str(item["retailPrice"])),
                "currency": item.get("currencyCode", "USD"),
                "unit": item.get("unitOfMeasure"),
                # present only on Reservation rows
                "reservation_term": item.get("reservationTerm"),
                "meter_id": item.get("meterId"),
                "meter_name": item.get("meterName"),
                "sku_name": item.get("skuName"),
                "product_name": item.get("productName"),
                "source_url": source_url,
                "raw_hash": raw_hash,
                "raw_ref": raw_ref,
            }
        )
    return rows


class AzureRetailFetcher(BaseFetcher):
    def __init__(self, config, client=None):
        super().__init__("azure_retail", config, client=client)
        self.endpoint = config["endpoint"]
        self.api_version = config.get("api_version", API_VERSION)

    def fetch(self, region: str) -> tuple[bytes, dict]:
        """Fetch all GPU VM meters for a region, following NextPageLink.

        Returns the raw payload bytes (for archiving) and the merged dict.
        """
        odata = (
            f"serviceName eq 'Virtual Machines' and armRegionName eq '{region}' "
            f"and (contains(armSkuName, 'ND') or contains(armSkuName, 'NC'))"
        )
        url = self.endpoint
        params = {"api-version": self.api_version, "$filter": odata}
        items: list[dict] = []

        while url:
            resp = self.get(url, params=params)
            body = resp.json()
            items.extend(body.get("Items", []))
            url = body.get("NextPageLink")
            params = None   # NextPageLink already carries the query string

        merged = {"Items": items, "Count": len(items)}
        return json.dumps(merged).encode(), merged
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_azure_parse.py -v`
Expected: PASS — 14 tests

- [ ] **Step 5: Commit**

```bash
git add src/cpt/fetchers/azure_retail.py tests/test_azure_parse.py
git commit -m "feat: Azure Retail fetcher with explicit four-way classification

Reservation rows report 1-year term totals under unitOfMeasure '1 Hour',
so to_hourly() refuses them structurally rather than trusting the unit.
DevTest and Low Priority are captured but never conflated with on-demand
or spot."
```

---

## Task 6: OpenRouter fetcher

**Files:**
- Create: `src/cpt/fetchers/openrouter.py`
- Test: `tests/test_openrouter_parse.py`
- Fixture (already committed): `tests/fixtures/openrouter_models_2026-07-27.json`

**Interfaces:**
- Consumes: `BaseFetcher`
- Produces: `parse(payload: dict, fetched_at: datetime, observed_date: date, source_url: str, raw_hash: str, raw_ref: str) -> list[dict]`, `OpenRouterFetcher(config, client=None)` with `.fetch() -> tuple[bytes, dict]`

Token prices are stored one row per `(model_id, price_key)`. The `sku` column
holds the model id and `meter_name` holds the price key, so these rows share
the observation schema with VM prices. `price_type` is `ondemand` for all
OpenRouter rows — the input/output distinction lives in `meter_name`, and is
never blended at query time.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_openrouter_parse.py`:

```python
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from cpt.fetchers.openrouter import parse

FIXTURE = Path(__file__).parent / "fixtures" / "openrouter_models_2026-07-27.json"
DAY = date(2026, 7, 27)
NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def rows():
    payload = json.loads(FIXTURE.read_text())
    return parse(
        payload,
        fetched_at=NOW,
        observed_date=DAY,
        source_url="https://openrouter.ai/api/v1/models",
        raw_hash="cafe1234",
        raw_ref="raw/openrouter/2026-07-27.json.gz",
    )


def test_emits_one_row_per_scalar_pricing_key(rows):
    """4 models in the fixture: 5 + 2 + 6 + 6 scalar keys = 19 rows.
    The list-valued 'overrides' key is skipped, not coerced."""
    assert len(rows) == 19


def test_list_valued_pricing_keys_are_skipped_not_crashed(rows):
    """openai/gpt-5.6-luna-pro carries a list-valued 'overrides' key.
    Decimal() over every pricing value would raise."""
    assert not any(r["meter_name"] == "overrides" for r in rows)
    luna = [r for r in rows if r["sku"] == "openai/gpt-5.6-luna-pro"]
    assert len(luna) == 5


def test_prices_are_decimal_never_float(rows):
    assert all(isinstance(r["price"], Decimal) for r in rows)


def test_known_price_parsed_exactly(rows):
    """Per-token USD, not per-million. 0.00001/token = $10/M tokens."""
    match = [
        r for r in rows
        if r["sku"] == "anthropic/claude-opus-5-fast" and r["meter_name"] == "prompt"
    ]
    assert len(match) == 1
    assert match[0]["price"] == Decimal("0.00001")


def test_input_output_cached_ratio_holds(rows):
    """Build spec 6.1 claims a 1:5:0.1 input/output/cached ratio. Confirmed."""
    by_key = {
        r["meter_name"]: r["price"]
        for r in rows
        if r["sku"] == "anthropic/claude-opus-5-fast"
    }
    assert by_key["completion"] == by_key["prompt"] * 5
    assert by_key["input_cache_read"] == by_key["prompt"] / 10


def test_free_models_captured_but_flagged(rows):
    free = [r for r in rows if r["sku"] == "inclusionai/ling-3.0-flash:free"]
    assert len(free) == 2
    assert all(r["price"] == Decimal("0") for r in free)


def test_unit_is_per_token(rows):
    assert all(r["unit"] == "1 Token" for r in rows)


def test_all_rows_are_ondemand_price_type(rows):
    """Input vs output is a meter distinction, not a price-type one."""
    assert {r["price_type"] for r in rows} == {"ondemand"}


def test_provenance_present_on_every_row(rows):
    for r in rows:
        assert r["provider"] == "openrouter"
        assert r["raw_hash"] == "cafe1234"
        assert r["source_url"] == "https://openrouter.ai/api/v1/models"
        assert r["region"] is None


def test_model_id_lands_in_sku_column(rows):
    assert "anthropic/claude-opus-5" in {r["sku"] for r in rows}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_openrouter_parse.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cpt.fetchers.openrouter'`

- [ ] **Step 3: Write `src/cpt/fetchers/openrouter.py`**

```python
"""OpenRouter models API — the Layer 4 observed token price.

Verified live 2026-07-27: 341 models, 13 distinct pricing keys across the
corpus, and pricing values that are strings *or lists*. The build spec
said "returned as strings"; parsing every value as Decimal raises.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from cpt.fetchers.base import BaseFetcher

# Prices are USD per token, not per million tokens.
UNIT = "1 Token"


def parse(
    payload: dict,
    fetched_at: datetime,
    observed_date: date,
    source_url: str,
    raw_hash: str,
    raw_ref: str,
) -> list[dict]:
    """Emit one row per (model, scalar pricing key).

    The pricing dict is heterogeneous per model, so it is stored as
    key/value rows rather than fixed columns. Non-scalar values are
    skipped rather than coerced.
    """
    rows = []
    for model in payload.get("data", []):
        model_id = model.get("id")
        for key, value in (model.get("pricing") or {}).items():
            if isinstance(value, (list, dict)):
                continue   # e.g. 'overrides'
            try:
                price = Decimal(str(value))
            except (InvalidOperation, TypeError):
                continue
            rows.append(
                {
                    "source_id": "openrouter",
                    "observed_at_utc": observed_date,
                    "fetched_at_utc": fetched_at,
                    "provider": "openrouter",
                    "sku": model_id,
                    "region": None,
                    "az": None,
                    "price_type": "ondemand",
                    "price": price,
                    "currency": "USD",
                    "unit": UNIT,
                    "reservation_term": None,
                    "meter_id": None,
                    "meter_name": key,
                    "sku_name": model.get("canonical_slug"),
                    "product_name": model.get("name"),
                    "source_url": source_url,
                    "raw_hash": raw_hash,
                    "raw_ref": raw_ref,
                }
            )
    return rows


class OpenRouterFetcher(BaseFetcher):
    def __init__(self, config, client=None):
        super().__init__("openrouter", config, client=client)
        self.endpoint = config["endpoint"]

    def fetch(self) -> tuple[bytes, dict]:
        resp = self.get(self.endpoint)
        body = resp.json()
        if not body.get("data"):
            raise RuntimeError("OpenRouter returned no models")
        return json.dumps(body).encode(), body
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_openrouter_parse.py -v`
Expected: PASS — 10 tests

- [ ] **Step 5: Commit**

```bash
git add src/cpt/fetchers/openrouter.py tests/test_openrouter_parse.py
git commit -m "feat: OpenRouter fetcher with heterogeneous pricing keys

Pricing values are strings or lists depending on the model, so list
values are skipped rather than coerced. One row per (model, price key)
keeps input, output, and cached prices as separate series."
```

---

## Task 7: GCP catalog fetcher (archive only)

**Files:**
- Create: `src/cpt/fetchers/gcp_catalog.py`
- Test: `tests/test_gcp_capture.py`

**Interfaces:**
- Consumes: `BaseFetcher`
- Produces: `EmptyCatalogError(RuntimeError)`, `GCPCatalogFetcher(config, api_key: str, client=None)` with `.fetch() -> tuple[bytes, dict]`

This fetcher archives only — no parsing this cycle. The three-shape
normalization problem (attached-GPU SKUs, family-specific CPU/RAM SKUs,
`{units, nanos}` price encoding) is Week 2 work, and the immutable archive
lets that parser reconstruct history back to today.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_gcp_capture.py`:

```python
import json

import httpx
import pytest

from cpt.fetchers.gcp_catalog import EmptyCatalogError, GCPCatalogFetcher

CONFIG = {
    "id": "gcp_catalog",
    "tier": "safe",
    "endpoint": "https://cloudbilling.googleapis.com/v1/services/6F81-5844-456A/skus",
    "rate_limit_per_sec": 1000.0,
    "daily_request_cap": 50,
    "archive_only": True,
}


def make(handler):
    return GCPCatalogFetcher(
        CONFIG, api_key="test-key", client=httpx.Client(transport=httpx.MockTransport(handler))
    )


def sku(n):
    return {
        "skuId": f"SKU-{n}",
        "description": f"Nvidia H100 GPU running in Iowa {n}",
        "category": {"resourceFamily": "Compute", "resourceGroup": "GPU", "usageType": "OnDemand"},
        "pricingInfo": [
            {"pricingExpression": {"usageUnit": "h", "tieredRates": [
                {"unitPrice": {"currencyCode": "USD", "units": "0", "nanos": 77800000}}
            ]}}
        ],
    }


def test_empty_sku_list_raises_loudly():
    """A wrong service ID returns HTTP 200 with an empty list, not an error.
    Left unchecked the collector runs green while archiving nothing."""
    def handler(request):
        if request.url.path.endswith("/robots.txt"):
            return httpx.Response(404)
        return httpx.Response(200, json={"skus": []})

    with pytest.raises(EmptyCatalogError, match="empty"):
        make(handler).fetch()


def test_follows_page_tokens_and_merges():
    pages = [
        {"skus": [sku(1), sku(2)], "nextPageToken": "t1"},
        {"skus": [sku(3)]},
    ]
    state = {"i": 0}

    def handler(request):
        if request.url.path.endswith("/robots.txt"):
            return httpx.Response(404)
        body = pages[state["i"]]
        state["i"] += 1
        return httpx.Response(200, json=body)

    raw, merged = make(handler).fetch()
    assert len(merged["skus"]) == 3
    assert json.loads(raw)["skus"][0]["skuId"] == "SKU-1"


def test_api_key_never_appears_in_archived_payload():
    """The key is a secret; the archive is committed context."""
    def handler(request):
        if request.url.path.endswith("/robots.txt"):
            return httpx.Response(404)
        return httpx.Response(200, json={"skus": [sku(1)]})

    raw, _ = make(handler).fetch()
    assert b"test-key" not in raw


def test_api_key_is_sent_as_query_param():
    seen = {}

    def handler(request):
        if request.url.path.endswith("/robots.txt"):
            return httpx.Response(404)
        seen["key"] = request.url.params.get("key")
        return httpx.Response(200, json={"skus": [sku(1)]})

    make(handler).fetch()
    assert seen["key"] == "test-key"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_gcp_capture.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cpt.fetchers.gcp_catalog'`

- [ ] **Step 3: Write `src/cpt/fetchers/gcp_catalog.py`**

```python
"""GCP Cloud Billing Catalog — raw capture only this cycle.

Verified live 2026-07-27: 31,561 Compute Engine SKUs across 7 pages,
27.2 MB raw / 1.5 MB gzipped per day. There is no unauthenticated path;
an API key is mandatory.

Parsing is deliberately deferred. Prices are encoded as {units, nanos}
(a naive float parse silently yields zero), usageUnit is not always
hourly, and A3/A4 machine prices are split across GPU, family-specific
CPU, and family-specific RAM SKUs. That work is Week 2; the immutable
archive means deferring it costs no history.
"""

from __future__ import annotations

import json

from cpt.fetchers.base import BaseFetcher

PAGE_SIZE = 5000


class EmptyCatalogError(RuntimeError):
    """Raised when the catalog returns no SKUs.

    A wrong service ID fails by returning HTTP 200 with an empty list
    rather than an error, so this must be checked explicitly.
    """


class GCPCatalogFetcher(BaseFetcher):
    def __init__(self, config, api_key: str, client=None):
        super().__init__("gcp_catalog", config, client=client)
        self.endpoint = config["endpoint"]
        self._api_key = api_key

    def fetch(self) -> tuple[bytes, dict]:
        skus: list[dict] = []
        token = None

        while True:
            params = {"key": self._api_key, "pageSize": PAGE_SIZE}
            if token:
                params["pageToken"] = token
            body = self.get(self.endpoint, params=params).json()
            skus.extend(body.get("skus", []))
            token = body.get("nextPageToken")
            if not token:
                break

        if not skus:
            raise EmptyCatalogError(
                f"catalog at {self.endpoint!r} returned an empty SKU list; "
                f"the service ID is probably wrong (this fails with HTTP 200, "
                f"not an error status)"
            )

        merged = {"skus": skus}
        return json.dumps(merged).encode(), merged
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_gcp_capture.py -v`
Expected: PASS — 4 tests

- [ ] **Step 5: Commit**

```bash
git add src/cpt/fetchers/gcp_catalog.py tests/test_gcp_capture.py
git commit -m "feat: GCP catalog fetcher, archive only

Fails loudly on an empty SKU list because a wrong service ID returns
HTTP 200 rather than an error. Parsing is deferred to Week 2 at no cost
to history, since the archive is immutable."
```

---

## Task 8: AWS spot fetcher (dark)

**Files:**
- Create: `src/cpt/fetchers/aws_spot.py`, `scripts/backfill_aws_spot.py`
- Test: `tests/test_aws_spot.py`

**Interfaces:**
- Consumes: `cpt.compliance.assert_tier_safe`
- Produces: `parse_spot(entries: list[dict], observed_date, source_url, raw_hash, raw_ref) -> list[dict]`, `AWSSpotFetcher(config, ec2_client=None)` with `.fetch(instance_types: list[str], start, end) -> tuple[bytes, dict]`

Ships with `enabled: false`. It runs the moment read-only IAM credentials
exist, with no code change. Required permissions: `ec2:DescribeSpotPriceHistory`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_aws_spot.py`:

```python
from datetime import date, datetime, timezone
from decimal import Decimal

from cpt.fetchers.aws_spot import AWSSpotFetcher, parse_spot

DAY = date(2026, 7, 27)
ENTRIES = [
    {
        "AvailabilityZone": "us-east-1a",
        "InstanceType": "p5.48xlarge",
        "ProductDescription": "Linux/UNIX",
        "SpotPrice": "12.2934",
        "Timestamp": datetime(2026, 7, 27, 6, 0, tzinfo=timezone.utc),
    },
    {
        "AvailabilityZone": "us-east-1b",
        "InstanceType": "p5.48xlarge",
        "ProductDescription": "Linux/UNIX",
        "SpotPrice": "11.8010",
        "Timestamp": datetime(2026, 7, 27, 6, 0, tzinfo=timezone.utc),
    },
]


def rows():
    return parse_spot(
        ENTRIES,
        observed_date=DAY,
        source_url="ec2:DescribeSpotPriceHistory",
        raw_hash="f00d",
        raw_ref="raw/aws_spot/2026-07-27.json.gz",
    )


def test_price_type_is_always_spot():
    assert {r["price_type"] for r in rows()} == {"spot"}


def test_az_granularity_is_preserved():
    """Roll up to region median at query time, not ingest time."""
    assert {r["az"] for r in rows()} == {"us-east-1a", "us-east-1b"}


def test_region_derived_from_az():
    assert {r["region"] for r in rows()} == {"us-east-1"}


def test_prices_are_decimal():
    r = rows()[0]
    assert isinstance(r["price"], Decimal)
    assert r["price"] == Decimal("12.2934")


def test_fetcher_is_disabled_in_shipped_config():
    """Guards against enabling AWS before credentials exist."""
    import yaml
    from pathlib import Path

    cfg = yaml.safe_load((Path(__file__).parent.parent / "config" / "sources.yaml").read_text())
    aws = [s for s in cfg["sources"] if s["id"] == "aws_spot"][0]
    assert aws["enabled"] is False


def test_fetch_paginates_and_returns_raw():
    class FakePaginator:
        def paginate(self, **kwargs):
            yield {"SpotPriceHistory": ENTRIES[:1]}
            yield {"SpotPriceHistory": ENTRIES[1:]}

    class FakeEC2:
        def get_paginator(self, name):
            assert name == "describe_spot_price_history"
            return FakePaginator()

    f = AWSSpotFetcher({"id": "aws_spot", "tier": "safe"}, ec2_client=FakeEC2())
    raw, merged = f.fetch(
        ["p5.48xlarge"],
        start=datetime(2026, 7, 26, tzinfo=timezone.utc),
        end=datetime(2026, 7, 27, tzinfo=timezone.utc),
    )
    assert len(merged["SpotPriceHistory"]) == 2
    assert b"p5.48xlarge" in raw
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_aws_spot.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cpt.fetchers.aws_spot'`

- [ ] **Step 3: Write `src/cpt/fetchers/aws_spot.py`**

```python
"""AWS EC2 spot price history.

Built dark: ships with enabled=false until read-only IAM credentials
exist, then runs with no code change.

DescribeSpotPriceHistory returns a trailing 90-day window, which is a
grace period rather than an emergency — starting within ~90 days loses
nothing. Per-AZ granularity is preserved at ingest; roll up to a region
median at query time.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

from cpt.compliance import assert_tier_safe

PRODUCT_DESCRIPTION = "Linux/UNIX"


def _region_of(az: str) -> str:
    return az[:-1] if az and az[-1].isalpha() else az


def parse_spot(
    entries: list[dict],
    observed_date: date,
    source_url: str,
    raw_hash: str,
    raw_ref: str,
) -> list[dict]:
    rows = []
    for e in entries:
        az = e["AvailabilityZone"]
        rows.append(
            {
                "source_id": "aws_spot",
                "observed_at_utc": observed_date,
                "fetched_at_utc": e["Timestamp"],
                "provider": "aws",
                "sku": e["InstanceType"],
                "region": _region_of(az),
                "az": az,
                "price_type": "spot",
                "price": Decimal(str(e["SpotPrice"])),
                "currency": "USD",
                "unit": "1 Hour",
                "reservation_term": None,
                "meter_id": None,
                "meter_name": e.get("ProductDescription"),
                "sku_name": e["InstanceType"],
                "product_name": None,
                "source_url": source_url,
                "raw_hash": raw_hash,
                "raw_ref": raw_ref,
            }
        )
    return rows


class AWSSpotFetcher:
    """Uses boto3 directly rather than BaseFetcher — the AWS SDK owns its
    own retry, throttling, and signing, and there is no URL to gate."""

    def __init__(self, config, ec2_client=None):
        assert_tier_safe("aws_spot", config.get("tier"))
        self.config = config
        self._ec2 = ec2_client

    def _client(self):
        if self._ec2 is None:
            import boto3

            self._ec2 = boto3.client("ec2")
        return self._ec2

    def fetch(self, instance_types, start: datetime, end: datetime):
        paginator = self._client().get_paginator("describe_spot_price_history")
        entries: list[dict] = []
        for page in paginator.paginate(
            InstanceTypes=list(instance_types),
            ProductDescriptions=[PRODUCT_DESCRIPTION],
            StartTime=start,
            EndTime=end,
        ):
            entries.extend(page.get("SpotPriceHistory", []))

        merged = {"SpotPriceHistory": entries}
        return json.dumps(merged, default=str).encode(), merged
```

- [ ] **Step 4: Write `scripts/backfill_aws_spot.py`**

```python
"""One-time 90-day AWS spot backfill, run on first activation.

Usage: uv run python scripts/backfill_aws_spot.py --days 90
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from cpt.fetchers.aws_spot import AWSSpotFetcher, parse_spot
from cpt.store.archive import LocalArchive, archive_payload
from cpt.store.observations import write_observations

INSTANCE_TYPES = [
    "p5.48xlarge", "p5e.48xlarge", "p5en.48xlarge", "p6-b200.48xlarge",
    "p4d.24xlarge", "p4de.24xlarge", "g6e.48xlarge", "g5.48xlarge",
    "g6.48xlarge",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    args = ap.parse_args()

    cfg = yaml.safe_load(Path("config/sources.yaml").read_text())
    source = [s for s in cfg["sources"] if s["id"] == "aws_spot"][0]

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)

    fetcher = AWSSpotFetcher(source)
    raw, merged = fetcher.fetch(INSTANCE_TYPES, start=start, end=end)

    ref = archive_payload(
        LocalArchive(args.data_root), "aws_spot", end.date(), raw
    )
    rows = parse_spot(
        merged["SpotPriceHistory"],
        observed_date=end.date(),
        source_url="ec2:DescribeSpotPriceHistory",
        raw_hash=ref.raw_hash,
        raw_ref=ref.raw_ref,
    )

    by_day: dict = {}
    for r in rows:
        by_day.setdefault(r["fetched_at_utc"].date(), []).append(r)
    for day, day_rows in by_day.items():
        for r in day_rows:
            r["observed_at_utc"] = day
        write_observations(args.data_root, "aws_spot", day, day_rows)

    print(f"backfilled {len(rows)} rows across {len(by_day)} days")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_aws_spot.py -v`
Expected: PASS — 6 tests

- [ ] **Step 6: Commit**

```bash
git add src/cpt/fetchers/aws_spot.py scripts/backfill_aws_spot.py tests/test_aws_spot.py
git commit -m "feat: AWS spot fetcher, shipped dark pending IAM credentials

Per-AZ granularity preserved at ingest; region rollup happens at query
time. A test asserts the source stays disabled in the shipped config."
```

---

## Task 9: Orchestrator CLI

**Files:**
- Create: `src/cpt/cli.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: every fetcher, `archive_payload`, `write_observations`, `append_run`
- Produces: `run_source(source: dict, ctx: RunContext) -> int` (returns row count), `main(argv=None) -> int` (process exit code)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_cli.py`:

```python
from datetime import date
from pathlib import Path

import pytest

from cpt.cli import RunContext, run_all
from cpt.store.collection_log import read_log

DAY = date(2026, 7, 27)


class OkSource:
    id = "ok_source"

    def collect(self, ctx):
        return 5


class FailingSource:
    id = "bad_source"

    def collect(self, ctx):
        raise RuntimeError("upstream exploded")


def test_one_source_failing_does_not_block_others(tmp_path):
    ctx = RunContext(data_root=tmp_path, observed_date=DAY)
    exit_code = run_all([FailingSource(), OkSource()], ctx)

    log = read_log(tmp_path)
    statuses = {r["source_id"]: r["status"] for r in log}
    assert statuses == {"bad_source": "failed", "ok_source": "ok"}
    assert exit_code != 0, "a failed source must fail the job so CI alerts"


def test_all_success_exits_zero(tmp_path):
    ctx = RunContext(data_root=tmp_path, observed_date=DAY)
    assert run_all([OkSource()], ctx) == 0


def test_failure_records_error_text(tmp_path):
    ctx = RunContext(data_root=tmp_path, observed_date=DAY)
    run_all([FailingSource()], ctx)
    log = read_log(tmp_path)
    assert "upstream exploded" in log[0]["error"]


def test_row_counts_logged(tmp_path):
    ctx = RunContext(data_root=tmp_path, observed_date=DAY)
    run_all([OkSource()], ctx)
    assert read_log(tmp_path)[0]["row_count"] == 5
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cpt.cli'`

- [ ] **Step 3: Write `src/cpt/cli.py`**

```python
"""Daily ingestion orchestrator.

One source failing must never block the others — a missed day of a
zero-grace-period source is unrecoverable. Every source writes a
collection_log row regardless of outcome, and any failure exits nonzero
so GitHub Actions raises its built-in failure alert.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

from cpt.store.archive import GCSArchive, LocalArchive, archive_payload
from cpt.store.collection_log import append_run
from cpt.store.observations import write_observations


@dataclass
class RunContext:
    data_root: Path
    observed_date: date
    archive_backend: object | None = None
    fetched_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


def run_all(sources, ctx: RunContext) -> int:
    """Run every source, isolating failures. Returns a process exit code."""
    failures = 0
    for source in sources:
        try:
            count = source.collect(ctx)
            append_run(ctx.data_root, source.id, ctx.observed_date, "ok", count)
        except Exception as exc:   # noqa: BLE001 - deliberate isolation boundary
            failures += 1
            append_run(
                ctx.data_root,
                source.id,
                ctx.observed_date,
                "failed",
                0,
                error=f"{type(exc).__name__}: {exc}",
            )
            print(f"[{source.id}] FAILED: {exc}", file=sys.stderr)
    return 1 if failures else 0


# --- source adapters -----------------------------------------------------


class AzureSource:
    id = "azure_retail"

    def __init__(self, config, regions):
        self.config = config
        self.regions = regions

    def collect(self, ctx: RunContext) -> int:
        from cpt.fetchers.azure_retail import AzureRetailFetcher, parse

        fetcher = AzureRetailFetcher(self.config)
        total = 0
        for region in self.regions:
            raw, payload = fetcher.fetch(region)
            ref = archive_payload(
                ctx.archive_backend, f"azure_retail/{region}", ctx.observed_date, raw
            )
            rows = parse(
                payload,
                fetched_at=ctx.fetched_at,
                observed_date=ctx.observed_date,
                source_url=self.config["endpoint"],
                raw_hash=ref.raw_hash,
                raw_ref=ref.raw_ref,
            )
            write_observations(ctx.data_root, "azure_retail", ctx.observed_date, rows)
            total += len(rows)
        return total


class OpenRouterSource:
    id = "openrouter"

    def __init__(self, config):
        self.config = config

    def collect(self, ctx: RunContext) -> int:
        from cpt.fetchers.openrouter import OpenRouterFetcher, parse

        raw, payload = OpenRouterFetcher(self.config).fetch()
        ref = archive_payload(ctx.archive_backend, "openrouter", ctx.observed_date, raw)
        rows = parse(
            payload,
            fetched_at=ctx.fetched_at,
            observed_date=ctx.observed_date,
            source_url=self.config["endpoint"],
            raw_hash=ref.raw_hash,
            raw_ref=ref.raw_ref,
        )
        write_observations(ctx.data_root, "openrouter", ctx.observed_date, rows)
        return len(rows)


class GCPSource:
    """Archive only — no observations written this cycle."""

    id = "gcp_catalog"

    def __init__(self, config, api_key):
        self.config = config
        self.api_key = api_key

    def collect(self, ctx: RunContext) -> int:
        from cpt.fetchers.gcp_catalog import GCPCatalogFetcher

        raw, payload = GCPCatalogFetcher(self.config, self.api_key).fetch()
        archive_payload(ctx.archive_backend, "gcp_catalog", ctx.observed_date, raw)
        return len(payload["skus"])


# --- entry point ---------------------------------------------------------


def build_sources(config: dict, regions: dict) -> list:
    enabled = {s["id"]: s for s in config["sources"] if s.get("enabled")}
    built = []
    if "azure_retail" in enabled:
        built.append(AzureSource(enabled["azure_retail"], regions.get("azure", [])))
    if "openrouter" in enabled:
        built.append(OpenRouterSource(enabled["openrouter"]))
    if "gcp_catalog" in enabled:
        key = os.environ.get("GCP_BILLING_API_KEY")
        if not key:
            raise RuntimeError("GCP_BILLING_API_KEY is not set")
        built.append(GCPSource(enabled["gcp_catalog"], key))
    return built


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--bucket", default=os.environ.get("GCS_BUCKET"))
    args = ap.parse_args(argv)

    config = yaml.safe_load(Path("config/sources.yaml").read_text())
    regions = yaml.safe_load(Path("config/regions.yaml").read_text())

    backend = GCSArchive(args.bucket) if args.bucket else LocalArchive(args.data_root)
    ctx = RunContext(
        data_root=args.data_root,
        observed_date=datetime.now(timezone.utc).date(),
        archive_backend=backend,
    )
    return run_all(build_sources(config, regions), ctx)


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_cli.py -v`
Expected: PASS — 4 tests

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest -v`
Expected: PASS — 53 tests

- [ ] **Step 6: Commit**

```bash
git add src/cpt/cli.py tests/test_cli.py
git commit -m "feat: daily orchestrator with per-source failure isolation

One source failing never blocks the others, but any failure exits
nonzero so GitHub Actions raises its failure alert."
```

---

## Task 10: GitHub Actions workflow, GCS bucket, and docs

**Files:**
- Create: `.github/workflows/ingest-daily.yml`
- Create: `docs/source-verification.md`, `docs/compliance-log.md`, `docs/backlog-sources.md`

**Interfaces:**
- Consumes: `cpt.cli.main`
- Produces: a green daily cron

- [ ] **Step 1: Create the GCS bucket and service account**

```bash
gcloud storage buckets create gs://compute-price-tracker-raw \
  --location=us-central1 --uniform-bucket-level-access
gcloud iam service-accounts create cpt-ingest \
  --display-name="compute-price-tracker ingest"
gcloud storage buckets add-iam-policy-binding gs://compute-price-tracker-raw \
  --member="serviceAccount:cpt-ingest@$(gcloud config get-value project).iam.gserviceaccount.com" \
  --role=roles/storage.objectCreator
gcloud iam service-accounts keys create /tmp/cpt-sa.json \
  --iam-account=cpt-ingest@$(gcloud config get-value project).iam.gserviceaccount.com
gh secret set GCP_SA_KEY --repo mrmonroe90/compute-price-tracker < /tmp/cpt-sa.json
rm /tmp/cpt-sa.json
```

Note `roles/storage.objectCreator` — not `objectAdmin`. The ingest account
can create objects but cannot overwrite or delete them, so the archive's
immutability is enforced by IAM rather than by our own code alone.

- [ ] **Step 2: Write `.github/workflows/ingest-daily.yml`**

```yaml
name: ingest-daily

on:
  schedule:
    - cron: "17 5 * * *"   # 05:17 UTC daily
  workflow_dispatch:

permissions:
  contents: write

concurrency:
  group: ingest-daily
  cancel-in-progress: false

jobs:
  ingest:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4

      - uses: astral-sh/setup-uv@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: uv sync --frozen

      - name: Run tests
        run: uv run pytest -q

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}

      - name: Run ingestion
        env:
          GCP_BILLING_API_KEY: ${{ secrets.GCP_BILLING_API_KEY }}
          GCS_BUCKET: compute-price-tracker-raw
        run: uv run python -m cpt.cli --data-root data

      - name: Commit observations
        run: |
          git config user.name "cpt-ingest[bot]"
          git config user.email "mrmonroe12@gmail.com"
          git add data/
          if git diff --staged --quiet; then
            echo "no new observations"
          else
            git commit -m "data: ingest $(date -u +%Y-%m-%d)"
            git push
          fi
```

- [ ] **Step 3: Write `docs/source-verification.md`**

```markdown
# Source verification log

Per BUILD-SPEC §15, every source is verified against live documentation
and live responses before implementation. Deltas from the spec are
recorded here and the spec is corrected, not forced.

## 2026-07-27 — Azure Retail Prices API

Endpoint: `https://prices.azure.com/api/retail/prices?api-version=2023-01-01-preview`
Auth: none. Probe: `eastus`, ND family, 128 records, no `NextPageLink`.

| # | Spec said | Reality |
|---|---|---|
| 1 | capture `priceType` | response field is `type`; `priceType` is filter-only |
| 2 | spot vs consumption | four `type` values; Spot/Low-Priority ride inside `Consumption` |
| 3 | — | `Reservation` rows report term totals under `unitOfMeasure: "1 Hour"` ($551,221 observed) |
| 4 | — | `DevTestConsumption` is a discounted rate ($5.439 vs $12.645) |
| 5 | — | "Low Priority" is a third pricing mode (24 meters) |
| 6 | capture `reservationTerm` | present only on `Reservation` rows |

Also: `armSkuName` is inconsistent (`Standard_ND96asr_v4` vs `NDasrA100v4_Type1`);
`Standard_ND128isr_NDR_GB200_v6` already listed.

## 2026-07-27 — OpenRouter Models API

Endpoint: `https://openrouter.ai/api/v1/models`. Auth: none. 341 models.

| # | Spec said | Reality |
|---|---|---|
| 7 | pricing returned as strings | values are strings **or lists**; 13 distinct keys |
| 8 | ~400 models | 341 |
| 9 | — | 18 models priced at zero |

Confirms the 1:5:0.1 input/output/cached ratio.

## 2026-07-27 — GCP Cloud Billing Catalog

Service ID `6F81-5844-456A` **verified** — returns Compute Engine SKUs.
31,561 SKUs across 7 pages; 27.2 MB raw, 1.5 MB gzipped per day.

No unauthenticated path exists: `/v1/skus` and `/v1/services` both 403,
`/v1beta/skus` 404 (not GA), legacy `pricelist.json` 404.

| # | Finding |
|---|---|
| 10 | prices are `{units, nanos}`; `float(units)` silently yields 0 |
| 11 | `usageType == "Spot"` does not exist; spot rides under `Preemptible` |
| 12 | `usageUnit` is not always hourly (`GiBy.mo` appears) |
| 13 | `tieredRates` length 1 for all 2,080 GPU SKUs |

Three pricing shapes, not two: A3/A3Ultra carry family-specific CPU and
RAM SKUs alongside GPU SKUs. `A4X` (GB200 NVL72) has **zero** SKUs — not
yet priced. No SKU field encodes topology.

## 2026-07-27 — robots.txt

| Host | Result |
|---|---|
| `openrouter.ai` | 200, `Disallow: /seo/` only — our path allowed |
| `prices.azure.com` | 404 — no restrictions |
| `pricing.us-east-1.amazonaws.com` | 404 — no restrictions |
| `cloudbilling.googleapis.com` | 404 — no restrictions |

## Pending

- **Vast.ai** — must verify whether the offers endpoint is reachable
  without an account. If it requires signup, drop it per rule 2 and log
  the decision here.
- **AWS Price List / spot** — pending AWS account creation.
```

- [ ] **Step 4: Write `docs/compliance-log.md` and `docs/backlog-sources.md`**

```markdown
# Compliance log

Decisions affecting COMPLIANCE.md rules, dated. Append only.

## 2026-07-27 — Initial sources

Azure Retail, OpenRouter, GCP Billing Catalog, AWS spot: all `tier: safe`,
all official documented APIs, none requiring a third-party account.

GCP requires an API key. This is our own GCP account used as documented,
permitted by rule 2, which distinguishes our own cloud relationships from
third-party services.

## 2026-07-27 — API key exposure and rotation

A GCP Billing API key was pasted in cleartext during setup and was
therefore exposed. It was revoked and replaced the same day. Scope was
limited to the Cloud Billing API (read-only public catalog data). No
project resources were reachable with it.

## 2026-07-27 — Denylist

`coreweave.com` and `runpod.io` hard-coded in `src/cpt/compliance.py`.
Both prohibit automated access in their AUP/ToS. Not config-driven, so a
careless commit cannot add them. Covered by `tests/test_compliance.py`.
```

```markdown
# Backlog sources

Sources considered but not in the MVP, with the reason.

| Source | Status | Reason |
|---|---|---|
| CoreWeave | **Permanently denylisted** | ToS prohibits automated access |
| RunPod | **Permanently denylisted** | AUP prohibits automated access |
| Vast.ai | Pending verification | Only marketplace-grade supply/demand signal available. Include only if the offers endpoint is reachable without an account (rule 2). |
| Vantage (instances.vantage.sh) | Week 2 | Cross-check only, never a published source. Derives from the same provider APIs we hit directly. |
| Lambda Labs, Together, Fireworks | Not assessed | Check for a documented public API before considering |
```

- [ ] **Step 5: Verify the workflow runs**

```bash
git add .github/ docs/
git commit -m "ci: daily ingestion workflow and source verification log"
git push
gh workflow run ingest-daily --repo mrmonroe90/compute-price-tracker
sleep 90
gh run list --workflow=ingest-daily --repo mrmonroe90/compute-price-tracker --limit 1
```

Expected: run completes successfully; `data/observations/` gains Parquet
files; `gs://compute-price-tracker-raw/raw/` gains gzipped payloads.

- [ ] **Step 6: Confirm the acceptance criterion**

The Week 1 milestone is met when the daily job has run **unattended for 48
hours** with Azure Retail and OpenRouter prices parsed and committed with
full provenance, GCP payloads archived raw, and a `collection_log` entry
per source per run.

Verify after two scheduled runs:

```bash
uv run python -c "
from cpt.store.collection_log import read_log
from pathlib import Path
log = read_log(Path('data'))
for r in log:
    print(r['observed_at_utc'], r['source_id'], r['status'], r['row_count'])
"
```

Expected: two consecutive days, every source `ok`.

---

## Self-Review

**Spec coverage:**

| Design section | Task |
|---|---|
| §2.2 Azure six deltas | Task 5 |
| §2.3 OpenRouter three deltas | Task 6 |
| §2.4 GCP deltas 10–13, empty-list hazard | Task 7 |
| §2.5 robots fails open on 404 | Tasks 1, 2 |
| §3 D2 AWS dark | Task 8 |
| §3 D3a GCP archive-only | Task 7 |
| §3 D4a three-tier storage | Tasks 3, 4, 10 |
| §3 D5 raw archive + parsed | Tasks 3, 9 |
| §4 module boundaries | Tasks 1–4 |
| §5 schema, price_type enum, idempotency | Task 4 |
| §5a storage economics | Task 10 (bucket, objectCreator role) |
| §6 compliance enforcement | Tasks 1, 2 |
| §7 testing | every task |
| §8 error handling, collection_log | Tasks 4, 9 |
| §9 AWS dark build + backfill script | Task 8 |
| §11 open items 4a, 4b | Task 10 |

**Gap found and closed:** the design's §11 item 4b (GCS bucket + service
account) had no task; it is now Task 10 Step 1, with `objectCreator`
rather than `objectAdmin` so IAM enforces archive immutability.

**Placeholder scan:** none. Every step carries runnable code or an exact command.

**Type consistency:** `archive_payload(backend, source_id, observed_date, payload)`
returns `ArchiveRef(raw_hash, raw_ref)`, consumed identically in Tasks 5, 6,
7, 9. `parse(...)` has the same signature in `azure_retail` and `openrouter`.
`write_observations(root, source_id, observed_date, rows)` and
`append_run(root, source_id, observed_date, status, row_count, error)` are
used consistently in Tasks 4 and 9. `price_type` values match `PRICE_TYPES`
everywhere.
